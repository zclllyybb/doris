// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.qe.runtime;

import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.RuntimeFilter;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRuntimeFilterParams;
import org.apache.doris.thrift.TRuntimeFilterTargetParamsV2;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/** RuntimeFiltersThriftBuilder */
public class RuntimeFiltersThriftBuilder {
    public final TNetworkAddress mergeAddress;

    private final List<RuntimeFilter> runtimeFilters;
    private final Set<Integer> broadcastRuntimeFilterIds;
    private final Map<RuntimeFilterId, List<RuntimeFilterTarget>> ridToTargets;
    private final Map<RuntimeFilterId, Integer> ridToBuilderNum;
    private final boolean limitBroadcastRuntimeFilterProducers;
    private final Map<Long, List<Integer>> workerIdToBroadcastRuntimeFilterIds;

    private RuntimeFiltersThriftBuilder(
            TNetworkAddress mergeAddress, List<RuntimeFilter> runtimeFilters,
            Set<Integer> broadcastRuntimeFilterIds,
            Map<RuntimeFilterId, List<RuntimeFilterTarget>> ridToTargets,
            Map<RuntimeFilterId, Integer> ridToBuilderNum,
            boolean limitBroadcastRuntimeFilterProducers,
            Map<Long, List<Integer>> workerIdToBroadcastRuntimeFilterIds) {
        this.mergeAddress = mergeAddress;
        this.runtimeFilters = runtimeFilters;
        this.broadcastRuntimeFilterIds = broadcastRuntimeFilterIds;
        this.ridToTargets = ridToTargets;
        this.ridToBuilderNum = ridToBuilderNum;
        this.limitBroadcastRuntimeFilterProducers = limitBroadcastRuntimeFilterProducers;
        this.workerIdToBroadcastRuntimeFilterIds = workerIdToBroadcastRuntimeFilterIds;
    }

    public void populateBroadcastRuntimeFilterProducerParams(
            TRuntimeFilterParams runtimeFilterParams, DistributedPlanWorker worker) {
        if (!limitBroadcastRuntimeFilterProducers) {
            return;
        }
        runtimeFilterParams.setBroadcastRuntimeFilterProducerFilterIds(
                workerIdToBroadcastRuntimeFilterIds.getOrDefault(worker.id(), Collections.emptyList()));
    }

    public void populateRuntimeFilterParams(TRuntimeFilterParams runtimeFilterParams) {
        for (RuntimeFilter rf : runtimeFilters) {
            List<RuntimeFilterTarget> targets = ridToTargets.get(rf.getFilterId());
            if (targets == null) {
                continue;
            }

            if (rf.hasRemoteTargets()) {
                Map<TNetworkAddress, TRuntimeFilterTargetParamsV2> targetToParams = new LinkedHashMap<>();
                for (RuntimeFilterTarget target : targets) {
                    TRuntimeFilterTargetParamsV2 targetParams = targetToParams.computeIfAbsent(
                            target.address, address -> {
                                TRuntimeFilterTargetParamsV2 params = new TRuntimeFilterTargetParamsV2();
                                params.target_fragment_instance_addr = address;
                                params.target_fragment_ids = new ArrayList<>();
                                // required field
                                params.target_fragment_instance_ids = new ArrayList<>();
                                return params;
                            });

                    targetParams.target_fragment_ids.add(target.fragmentId);
                }

                runtimeFilterParams.putToRidToTargetParamv2(
                        rf.getFilterId().asInt(), new ArrayList<>(targetToParams.values()));
            }
        }
        for (Map.Entry<RuntimeFilterId, Integer> entry : ridToBuilderNum.entrySet()) {
            boolean isBroadcastRuntimeFilter = broadcastRuntimeFilterIds.contains(entry.getKey().asInt());
            int builderNum = isBroadcastRuntimeFilter ? 1 : entry.getValue();
            runtimeFilterParams.putToRuntimeFilterBuilderNum(entry.getKey().asInt(), builderNum);
        }
        for (RuntimeFilter rf : runtimeFilters) {
            runtimeFilterParams.putToRidToRuntimeFilter(rf.getFilterId().asInt(), rf.toThrift());
        }
    }

    public static RuntimeFiltersThriftBuilder compute(
            List<RuntimeFilter> runtimeFilters, List<PipelineDistributedPlan> distributedPlans) {
        return compute(runtimeFilters, distributedPlans, 0);
    }

    public static RuntimeFiltersThriftBuilder compute(
            List<RuntimeFilter> runtimeFilters, List<PipelineDistributedPlan> distributedPlans,
            int broadcastRuntimeFilterProducerNum) {
        BackendWorker worker = selectMergeWorker(distributedPlans);
        TNetworkAddress mergeAddress = new TNetworkAddress(worker.host(), worker.brpcPort());

        Map<Integer, RuntimeFilter> idToRuntimeFilter = runtimeFilters
                .stream()
                .collect(Collectors.toMap(r -> r.getFilterId().asInt(), r -> r, (left, right) -> left,
                        LinkedHashMap::new));

        Set<Integer> broadcastRuntimeFilterIds = runtimeFilters
                .stream()
                .filter(RuntimeFilter::isBroadcast)
                .map(r -> r.getFilterId().asInt())
                .collect(Collectors.toSet());

        Map<RuntimeFilterId, List<RuntimeFilterTarget>> ridToTargetParam = Maps.newLinkedHashMap();
        Map<RuntimeFilterId, Integer> ridToBuilderNum = Maps.newLinkedHashMap();
        Map<Integer, List<BackendWorker>> builderNodeToProducerWorkers = Maps.newLinkedHashMap();
        Map<Long, List<Integer>> workerIdToBroadcastRuntimeFilterIds = Maps.newLinkedHashMap();
        boolean limitBroadcastRuntimeFilterProducers = broadcastRuntimeFilterProducerNum > 0;
        for (PipelineDistributedPlan plan : distributedPlans) {
            PlanFragment fragment = plan.getFragmentJob().getFragment();
            // Transform <fragment, runtimeFilterId> to <runtimeFilterId, fragment>
            for (RuntimeFilterId rid : fragment.getTargetRuntimeFilterIds()) {
                List<RuntimeFilterTarget> targetFragments = ridToTargetParam.computeIfAbsent(rid,
                        k -> new ArrayList<>());
                for (AssignedJob instanceJob : plan.getInstanceJobs()) {
                    BackendWorker backendWorker = (BackendWorker) instanceJob.getAssignedWorker();
                    Backend backend = backendWorker.getBackend();
                    targetFragments.add(new RuntimeFilterTarget(
                            fragment.getFragmentId().asInt(),
                            new TNetworkAddress(backend.getHost(), backend.getBrpcPort())));
                }
            }

            List<BackendWorker> builderWorkers = collectDistinctBackendWorkers(plan.getInstanceJobs());
            int distinctWorkerNum = builderWorkers.size();
            for (RuntimeFilterId rid : fragment.getBuilderRuntimeFilterIds()) {
                ridToBuilderNum.merge(rid, distinctWorkerNum, Integer::sum);
                RuntimeFilter rf = idToRuntimeFilter.get(rid.asInt());
                if (limitBroadcastRuntimeFilterProducers
                        && rf != null && rf.isBroadcast() && rf.hasRemoteTargets()) {
                    int builderNodeId = rf.getBuilderNode().getId().asInt();
                    List<BackendWorker> producerWorkers = builderNodeToProducerWorkers.computeIfAbsent(
                            builderNodeId,
                            id -> selectBroadcastRuntimeFilterProducerWorkers(
                                    builderWorkers, broadcastRuntimeFilterProducerNum));
                    for (BackendWorker producerWorker : producerWorkers) {
                        workerIdToBroadcastRuntimeFilterIds.computeIfAbsent(
                                producerWorker.id(), id -> new ArrayList<>()).add(rid.asInt());
                    }
                }
            }
        }
        return new RuntimeFiltersThriftBuilder(
                mergeAddress, runtimeFilters, broadcastRuntimeFilterIds, ridToTargetParam, ridToBuilderNum,
                limitBroadcastRuntimeFilterProducers, workerIdToBroadcastRuntimeFilterIds);
    }

    static List<BackendWorker> collectDistinctBackendWorkers(List<AssignedJob> instanceJobs) {
        Map<Long, BackendWorker> workerMap = Maps.newLinkedHashMap();
        for (AssignedJob instanceJob : instanceJobs) {
            BackendWorker worker = (BackendWorker) instanceJob.getAssignedWorker();
            workerMap.putIfAbsent(worker.id(), worker);
        }
        return new ArrayList<>(workerMap.values());
    }

    static List<BackendWorker> selectBroadcastRuntimeFilterProducerWorkers(
            List<BackendWorker> workers, int producerNum) {
        Preconditions.checkArgument(producerNum > 0,
                "broadcast runtime filter producer num must be positive");
        if (workers.size() <= producerNum) {
            return workers;
        }
        List<BackendWorker> selectedWorkers = new ArrayList<>(workers);
        Collections.shuffle(selectedWorkers, ThreadLocalRandom.current());
        return new ArrayList<>(selectedWorkers.subList(0, producerNum));
    }

    static BackendWorker selectMergeWorker(List<PipelineDistributedPlan> distributedPlans) {
        List<BackendWorker> workers = collectMergeWorkerCandidates(distributedPlans);
        return workers.get(ThreadLocalRandom.current().nextInt(workers.size()));
    }

    static List<BackendWorker> collectMergeWorkerCandidates(List<PipelineDistributedPlan> distributedPlans) {
        PipelineDistributedPlan topMostPlan = distributedPlans.get(distributedPlans.size() - 1);
        Map<Long, BackendWorker> candidateWorkers = Maps.newLinkedHashMap();
        for (AssignedJob instanceJob : topMostPlan.getInstanceJobs()) {
            BackendWorker worker = (BackendWorker) instanceJob.getAssignedWorker();
            candidateWorkers.putIfAbsent(worker.id(), worker);
        }
        Preconditions.checkState(!candidateWorkers.isEmpty(), "runtime filter merge worker is empty");
        return new ArrayList<>(candidateWorkers.values());
    }

    public static class RuntimeFilterTarget {
        public final int fragmentId;
        public final TNetworkAddress address;

        public RuntimeFilterTarget(int fragmentId, TNetworkAddress address) {
            this.fragmentId = fragmentId;
            this.address = address;
        }
    }
}

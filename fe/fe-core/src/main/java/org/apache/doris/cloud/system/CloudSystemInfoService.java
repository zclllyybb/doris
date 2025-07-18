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

package org.apache.doris.cloud.system;

import org.apache.doris.analysis.ModifyBackendClause;
import org.apache.doris.analysis.ModifyBackendHostNameClause;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.proto.Cloud.ClusterPB;
import org.apache.doris.cloud.proto.Cloud.InstanceInfoPB;
import org.apache.doris.cloud.qe.ComputeGroupException;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.RandomIdentifierGenerator;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.trees.plans.commands.info.ModifyBackendOp;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.Tag;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class CloudSystemInfoService extends SystemInfoService {
    private static final Logger LOG = LogManager.getLogger(CloudSystemInfoService.class);

    // use rw lock to make sure only one thread can change clusterIdToBackend and clusterNameToId
    private static final ReadWriteLock rwlock = new ReentrantReadWriteLock();

    private static final Lock rlock = rwlock.readLock();

    private static final Lock wlock = rwlock.writeLock();

    // for show cluster and cache user owned cluster
    // clusterId -> List<Backend>
    // pls make sure each cluster's backend list is sorted by backendId
    protected Map<String, List<Backend>> clusterIdToBackend = new ConcurrentHashMap<>();
    // clusterName -> clusterId
    protected Map<String, String> clusterNameToId = new ConcurrentHashMap<>();

    private InstanceInfoPB.Status instanceStatus;

    @Override
    public Pair<Map<Tag, List<Long>>, TStorageMedium> selectBackendIdsForReplicaCreation(
            ReplicaAllocation replicaAlloc, Map<Tag, Integer> nextIndexs,
            TStorageMedium storageMedium, boolean isStorageMediumSpecified,
            boolean isOnlyForCheck)
            throws DdlException {
        return Pair.of(Maps.newHashMap(), storageMedium);
    }

    /**
     * Gets cloud cluster from remote with either clusterId or clusterName
     *
     * @param clusterName cluster name
     * @param clusterId   cluster id
     * @return
     */
    public Cloud.GetClusterResponse getCloudCluster(String clusterName, String clusterId, String userName) {
        Cloud.GetClusterRequest.Builder builder = Cloud.GetClusterRequest.newBuilder();
        builder.setCloudUniqueId(Config.cloud_unique_id)
            .setClusterName(clusterName).setClusterId(clusterId).setMysqlUserName(userName);
        final Cloud.GetClusterRequest pRequest = builder.build();
        Cloud.GetClusterResponse response;
        try {
            response = MetaServiceProxy.getInstance().getCluster(pRequest);
            return response;
        } catch (RpcException e) {
            LOG.warn("rpcToMetaGetClusterInfo exception: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void updateCloudBackends(List<Backend> toAdd, List<Backend> toDel) {
        wlock.lock();
        try {
            updateCloudBackendsUnLock(toAdd, toDel);
        } finally {
            wlock.unlock();
        }
    }

    public void updateCloudBackendsUnLock(List<Backend> toAdd, List<Backend> toDel) {
        // Deduplicate and validate
        if (toAdd.isEmpty() && toDel.isEmpty()) {
            LOG.info("nothing to do");
            return;
        }
        Set<String> existedBes = idToBackendRef.values().stream()
                .map(i -> i.getHost() + ":" + i.getHeartbeatPort())
                .collect(Collectors.toSet());
        if (LOG.isDebugEnabled()) {
            LOG.debug("deduplication existedBes={}, before deduplication toAdd={} toDel={}", existedBes, toAdd, toDel);
        }
        toAdd = toAdd.stream().filter(i -> !existedBes.contains(i.getHost() + ":" + i.getHeartbeatPort()))
            .collect(Collectors.toList());
        toDel = toDel.stream().filter(i -> existedBes.contains(i.getHost() + ":" + i.getHeartbeatPort()))
            .collect(Collectors.toList());
        if (LOG.isDebugEnabled()) {
            LOG.debug("after deduplication toAdd={} toDel={}", toAdd, toDel);
        }

        Map<String, List<Backend>> existedHostToBeList = idToBackendRef.values().stream().collect(Collectors.groupingBy(
                Backend::getHost));
        for (Backend be : toAdd) {
            Env.getCurrentEnv().getEditLog().logAddBackend(be);
            LOG.info("added cloud backend={} ", be);
            // backends is changed, regenerated tablet number metrics
            MetricRepo.generateBackendsTabletMetrics();

            String host = be.getHost();
            if (existedHostToBeList.keySet().contains(host)) {
                if (be.isSmoothUpgradeDst()) {
                    LOG.info("a new BE process will start on the existed node for smooth upgrading");
                    int beNum = existedHostToBeList.get(host).size();
                    Backend colocatedBe = existedHostToBeList.get(host).get(0);
                    if (beNum != 1) {
                        LOG.warn("find multiple co-located BEs, num: {}, select the 1st {} as migration src", beNum,
                                colocatedBe.getId());
                    }
                    colocatedBe.setSmoothUpgradeSrc(true);
                    handleNewBeOnSameNode(colocatedBe, be);
                } else {
                    LOG.warn("a new BE process will start on the existed node, it should not happend unless testing");
                }
            }
        }
        for (Backend be : toDel) {
            // drop be, set it not alive
            be.setAlive(false);
            be.setLastMissingHeartbeatTime(System.currentTimeMillis());
            Env.getCurrentEnv().getEditLog().logDropBackend(be);
            LOG.info("dropped cloud backend={}, and lastMissingHeartbeatTime={}", be, be.getLastMissingHeartbeatTime());
            // backends is changed, regenerated tablet number metrics
            MetricRepo.generateBackendsTabletMetrics();
        }

        // Update idToBackendRef
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        toAdd.forEach(i -> copiedBackends.put(i.getId(), i));
        toDel.forEach(i -> copiedBackends.remove(i.getId()));
        ImmutableMap<Long, Backend> newIdToBackend = ImmutableMap.copyOf(copiedBackends);
        idToBackendRef = newIdToBackend;

        // Update idToReportVersionRef
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        toAdd.forEach(i -> copiedReportVersions.put(i.getId(), new AtomicLong(0L)));
        toDel.forEach(i -> copiedReportVersions.remove(i.getId()));
        ImmutableMap<Long, AtomicLong> newIdToReportVersion = ImmutableMap.copyOf(copiedReportVersions);
        idToReportVersionRef = newIdToReportVersion;

        updateCloudClusterMapNoLock(toAdd, toDel);
    }

    private void handleNewBeOnSameNode(Backend oldBe, Backend newBe) {
        LOG.info("new BE {} starts on the same node as existing BE {}", newBe.getId(), oldBe.getId());
        ((CloudEnv) Env.getCurrentEnv()).getCloudTabletRebalancer()
                .addTabletMigrationTask(oldBe.getId(), newBe.getId());
    }

    public void updateCloudClusterMap(List<Backend> toAdd, List<Backend> toDel) {
        wlock.lock();
        try {
            updateCloudClusterMapNoLock(toAdd, toDel);
        } finally {
            wlock.unlock();
        }
    }

    public void updateCloudClusterMapNoLock(List<Backend> toAdd, List<Backend> toDel) {
        Set<String> clusterNameSet = new HashSet<>();
        for (Backend b : toAdd) {
            String clusterName = b.getCloudClusterName();
            String clusterId = b.getCloudClusterId();
            if (clusterName.isEmpty() || clusterId.isEmpty()) {
                LOG.warn("cloud cluster name or id empty: id={}, name={}", clusterId, clusterName);
                continue;
            }
            clusterNameSet.add(clusterName);
            if (clusterNameSet.size() != 1) {
                LOG.warn("toAdd be list have multi clusterName, please check, Set: {}", clusterNameSet);
            }

            clusterNameToId.put(clusterName, clusterId);
            List<Backend> be = clusterIdToBackend.get(clusterId);
            if (be == null) {
                be = new ArrayList<>();
                clusterIdToBackend.put(clusterId, be);
            }
            Set<String> existed = be.stream().map(i -> i.getHost() + ":" + i.getHeartbeatPort())
                    .collect(Collectors.toSet());
            // Deduplicate
            boolean alreadyExisted = existed.contains(b.getHost() + ":" + b.getHeartbeatPort());
            if (alreadyExisted) {
                LOG.info("BE already existed, clusterName={} clusterId={} backendNum={} backend={}",
                        clusterName, clusterId, be.size(), b);
                continue;
            }
            List<Backend> sortBackends = Lists.newArrayList(be);
            sortBackends.add(b);
            Collections.sort(sortBackends, Comparator.comparing(Backend::getId));
            clusterIdToBackend.put(clusterId, sortBackends);
            MetricRepo.registerCloudMetrics(clusterId, clusterName);
            LOG.info("update (add) cloud cluster map, clusterName={} clusterId={} backendNum={} current backend={}",
                    clusterName, clusterId, sortBackends.size(), sortBackends);
        }

        for (Backend b : toDel) {
            String clusterName = b.getCloudClusterName();
            String clusterId = b.getCloudClusterId();
            // We actually don't care about cluster name here
            if (clusterName.isEmpty() || clusterId.isEmpty()) {
                LOG.warn("cloud cluster name or id empty: id={}, name={}", clusterId, clusterName);
                continue;
            }
            List<Backend> be = clusterIdToBackend.get(clusterId);
            if (be == null) {
                LOG.warn("try to remove a non-existing cluster, clusterId={} clusterName={}",
                        clusterId, clusterName);
                continue;
            }
            Set<Long> d = toDel.stream().map(Backend::getId).collect(Collectors.toSet());
            be = be.stream().filter(i -> !d.contains(i.getId())).collect(Collectors.toList());
            // ATTN: clusterId may have zero nodes
            clusterIdToBackend.replace(clusterId, be);
            // such as dropCluster, but no lock
            // ATTN: Empty clusters are treated as dropped clusters.
            if (be.isEmpty()) {
                LOG.info("del clusterId {} and clusterName {} due to be nodes eq 0", clusterId, clusterName);
                boolean succ = clusterNameToId.remove(clusterName, clusterId);
                if (!succ) {
                    LOG.warn("impossible, somewhere err, clusterNameToId {}, "
                            + "want remove cluster name {}, cluster id {}",
                            clusterNameToId, clusterName, clusterId);
                }
                clusterIdToBackend.remove(clusterId);
            }
            LOG.info("update (del) cloud cluster map, clusterName={} clusterId={} backendNum={} current backend={}",
                    clusterName, clusterId, be.size(), b);
        }
    }

    public synchronized void updateFrontends(List<Frontend> toAdd, List<Frontend> toDel)
            throws DdlException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("updateCloudFrontends toAdd={} toDel={}", toAdd, toDel);
        }
        String masterIp = Env.getCurrentEnv().getMasterHost();
        for (Frontend fe : toDel) {
            if (masterIp.equals(fe.getHost())) {
                continue;
            }
            try {
                Env.getCurrentEnv().dropFrontendFromBDBJE(fe.getRole(), fe.getHost(), fe.getEditLogPort());
                LOG.info("dropped cloud frontend={} ", fe);
            } catch (DdlException e) {
                LOG.warn("failed to drop cloud frontend={} ", fe);
            }
        }

        for (Frontend fe : toAdd) {
            if (masterIp.equals(fe.getHost())) {
                continue;
            }
            try {
                Env.getCurrentEnv().addFrontend(fe.getRole(),
                        fe.getHost(), fe.getEditLogPort(), fe.getNodeName(), fe.getCloudUniqueId());
                LOG.info("added cloud frontend={} ", fe);
            } catch (DdlException e) {
                LOG.warn("failed to add cloud frontend={} ", fe);
            }
        }
    }

    private void alterBackendCluster(List<HostInfo> hostInfos, String computeGroupId, String cloudUniqueId,
                                     Cloud.AlterClusterRequest.Operation operation) throws DdlException {
        if (Strings.isNullOrEmpty(((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId())) {
            throw new DdlException("unable to alter backends due to empty cloud_instance_id");
        }
        // Issue rpc to meta to alter node, then fe master would add this node to its frontends
        ClusterPB clusterPB = ClusterPB.newBuilder()
                .setClusterId(computeGroupId)
                .setType(ClusterPB.Type.COMPUTE)
                .build();

        for (HostInfo hostInfo : hostInfos) {
            Cloud.NodeInfoPB nodeInfoPB = Cloud.NodeInfoPB.newBuilder()
                    .setCloudUniqueId(cloudUniqueId)
                    .setIp(hostInfo.getHost())
                    .setHost(hostInfo.getHost())
                    .setHeartbeatPort(hostInfo.getPort())
                    .setCtime(System.currentTimeMillis() / 1000)
                    .build();
            clusterPB = clusterPB.toBuilder().addNodes(nodeInfoPB).build();
        }

        Cloud.AlterClusterRequest request = Cloud.AlterClusterRequest.newBuilder()
                .setInstanceId(((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId())
                .setOp(operation)
                .setCluster(clusterPB)
                .build();

        Cloud.AlterClusterResponse response;
        try {
            response = MetaServiceProxy.getInstance().alterCluster(request);
            LOG.info("alter cluster, request: {}, response: {}", request, response);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("alter backends not ok, response: {}", response);
                throw new DdlException("failed to alter backends errorCode: " + response.getStatus().getCode()
                        + " msg: " + response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException("failed to alter backends", e);
        }
    }

    /**
     * @param hostInfos : backend's ip, hostName and port
     * @throws DdlException
     */
    @Override
    public void addBackends(List<HostInfo> hostInfos, Map<String, String> tagMap) throws UserException {
        // issue rpc to meta to add this node, then fe master would add this node to its backends
        if (Strings.isNullOrEmpty(((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId())) {
            throw new DdlException("unable to add backends due to empty cloud_instance_id");
        }
        if (hostInfos.isEmpty()) {
            return;
        }
        String clusterName = tagMap.getOrDefault(Tag.COMPUTE_GROUP_NAME, Tag.VALUE_DEFAULT_COMPUTE_GROUP_NAME);
        if (clusterName.isEmpty()) {
            throw new UserException("ComputeGroup'name can not be empty");
        }

        String computeGroupId = tryCreateComputeGroup(clusterName,
                RandomIdentifierGenerator.generateRandomIdentifier(8));
        String instanceId = Config.cluster_id == -1 ? ((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId()
                : String.valueOf(Config.cluster_id);

        String cloudUniqueId = "1:" + instanceId + ":" + RandomIdentifierGenerator.generateRandomIdentifier(8);

        String publicEndpoint = tagMap.getOrDefault(Tag.PUBLIC_ENDPOINT, "");
        String privateEndpoint = tagMap.getOrDefault(Tag.PRIVATE_ENDPOINT, "");

        Cloud.ClusterPB clusterPB = Cloud.ClusterPB.newBuilder()
                .setClusterId(computeGroupId)
                .setType(Cloud.ClusterPB.Type.COMPUTE)
                .build();

        for (HostInfo hostInfo : hostInfos) {
            Cloud.NodeInfoPB nodeInfoPB = Cloud.NodeInfoPB.newBuilder()
                    .setCloudUniqueId(cloudUniqueId)
                    .setIp(hostInfo.getHost())
                    .setHost(hostInfo.getHost())
                    .setHeartbeatPort(hostInfo.getPort())
                    .setCtime(System.currentTimeMillis() / 1000)
                    .setPublicEndpoint(publicEndpoint)
                    .setPrivateEndpoint(privateEndpoint)
                    .build();
            clusterPB = clusterPB.toBuilder().addNodes(nodeInfoPB).build();
            LOG.info("adding backend node: host={}, port={}, publicEndpoint={}, privateEndpoint={}",
                    hostInfo.getHost(), hostInfo.getPort(), publicEndpoint, privateEndpoint);
        }

        Cloud.AlterClusterRequest request = Cloud.AlterClusterRequest.newBuilder()
                .setInstanceId(((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId())
                .setOp(Cloud.AlterClusterRequest.Operation.ADD_NODE)
                .setCluster(clusterPB)
                .build();

        Cloud.AlterClusterResponse response;
        try {
            response = MetaServiceProxy.getInstance().alterCluster(request);
            LOG.info("add backends, request: {}, response: {}", request, response);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("add backends not ok, response: {}", response);
                throw new DdlException("failed to add backends errorCode: " + response.getStatus().getCode()
                        + " msg: " + response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException("failed to add backends", e);
        }
    }

    // final entry of dropping backend
    // We can not do it in batch, because be may belong to different clusters.
    // Maybe we can opt it in the future.
    @Override
    public void dropBackend(String host, int heartbeatPort) throws DdlException {
        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);
        if (droppedBackend == null) {
            throw new DdlException("backend does not exists[" + NetUtils
                    .getHostPortInAccessibleFormat(host, heartbeatPort) + "]");
        }
        String computeGroupId = droppedBackend.getTagMap().get(Tag.CLOUD_CLUSTER_ID);
        if (computeGroupId == null || computeGroupId.isEmpty()) {
            throw new DdlException("Failed to get cluster ID for backend: " + droppedBackend.getId());
        }

        List<HostInfo> hostInfos = new ArrayList<>();
        hostInfos.add(new HostInfo(host, heartbeatPort));

        String cloudUniqueId = droppedBackend.getCloudUniqueId();
        alterBackendCluster(hostInfos, computeGroupId, cloudUniqueId,
                Cloud.AlterClusterRequest.Operation.DROP_NODE);
    }

    @Override
    public void decommissionBackend(Backend backend) throws UserException {
        String computeGroupId = backend.getTagMap().get(Tag.CLOUD_CLUSTER_ID);
        if (computeGroupId == null || computeGroupId.isEmpty()) {
            throw new UserException("Failed to get cluster ID for backend: " + backend.getId());
        }

        List<HostInfo> hostInfos = new ArrayList<>();
        hostInfos.add(new HostInfo(backend.getHost(), backend.getHeartbeatPort()));
        try {
            String cloudUniqueId = backend.getCloudUniqueId();
            alterBackendCluster(hostInfos, computeGroupId, cloudUniqueId,
                                Cloud.AlterClusterRequest.Operation.DECOMMISSION_NODE);
        } catch (DdlException e) {
            String errorMessage = e.getMessage();
            LOG.warn("Failed to decommission backend: {}", errorMessage);
            throw new UserException(errorMessage);
        }
    }

    @Override
    public void modifyBackends(ModifyBackendClause alterClause) throws UserException {
        throw new UserException("Modifying backends is not supported in cloud mode");
    }

    @Override
    public void modifyBackends(ModifyBackendOp op) throws UserException {
        throw new UserException("Modifying backends is not supported in cloud mode");
    }

    @Override
    public void modifyBackendHost(ModifyBackendHostNameClause clause) throws UserException {
        throw new UserException("Modifying backend hostname is not supported in cloud mode");
    }

    @Override
    public void modifyBackendHostName(String srcHost, int srcPort, String destHost) throws UserException {
        throw new UserException("Modifying backend hostname is not supported in cloud mode");
    }

    @Override
    public void replayAddBackend(Backend newBackend) {
        super.replayAddBackend(newBackend);
        List<Backend> toAdd = new ArrayList<>();
        toAdd.add(newBackend);
        updateCloudClusterMap(toAdd, new ArrayList<>());
    }

    @Override
    public void replayDropBackend(Backend backend) {
        super.replayDropBackend(backend);
        List<Backend> toDel = new ArrayList<>();
        toDel.add(backend);
        updateCloudClusterMap(new ArrayList<>(), toDel);
    }

    @Override
    public void replayModifyBackend(Backend backend) {
        Backend memBe = getBackend(backend.getId());
        if (memBe == null) {
            return;
        }
        // for rename cluster
        String originalClusterName = memBe.getCloudClusterName();
        String originalClusterId = memBe.getCloudClusterId();
        String newClusterName = backend.getTagMap().getOrDefault(Tag.CLOUD_CLUSTER_NAME, "");
        if (!originalClusterName.equals(newClusterName)) {
            // rename
            updateClusterNameToId(newClusterName, originalClusterName, originalClusterId);
            LOG.info("cloud mode replay rename cluster, "
                    + "originalClusterName: {}, originalClusterId: {}, newClusterName: {}",
                    originalClusterName, originalClusterId, newClusterName);
        }
        super.replayModifyBackend(backend);
    }

    public boolean availableBackendsExists() {
        if (FeConstants.runningUnitTest) {
            return true;
        }
        rlock.lock();
        try {
            if (null == clusterNameToId || clusterNameToId.isEmpty()) {
                return false;
            }

            return clusterIdToBackend != null && !clusterIdToBackend.isEmpty()
                && clusterIdToBackend.values().stream().anyMatch(list -> list != null && !list.isEmpty());
        } finally {
            rlock.unlock();
        }
    }

    public boolean containClusterName(String clusterName) {
        return clusterNameToId.containsKey(clusterName);
    }

    @Override
    public int getMinPipelineExecutorSize() {
        String clusterName = "";
        try {
            clusterName = ConnectContext.get().getCloudCluster(false);
        } catch (ComputeGroupException e) {
            LOG.warn("failed to get cluster name", e);
            return 1;
        }
        if (ConnectContext.get() != null
                && Strings.isNullOrEmpty(clusterName)) {
            return 1;
        }

        return super.getMinPipelineExecutorSize();
    }

    @Override
    public int getTabletNumByBackendId(long beId) {
        return ((CloudEnv) Env.getCurrentEnv())
                .getCloudTabletRebalancer()
                .getTabletNumByBackendId(beId);
    }

    @Override
    public ImmutableMap<Long, Backend> getBackendsByCurrentCluster() throws AnalysisException {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            throw new AnalysisException("connect context is null");
        }

        Map<Long, Backend> idToBackend = Maps.newHashMap();
        try {
            String cluster = ctx.getCloudCluster();
            if (Strings.isNullOrEmpty(cluster)) {
                throw new AnalysisException("cluster name is empty");
            }

            List<Backend> backends =  getBackendsByClusterName(cluster);
            for (Backend be : backends) {
                idToBackend.put(be.getId(), be);
            }
        } catch (ComputeGroupException e) {
            throw new AnalysisException(e.getMessage());
        }

        return ImmutableMap.copyOf(idToBackend);
    }

    public List<Backend> getBackendsByClusterName(final String clusterName) {
        String clusterId = clusterNameToId.getOrDefault(clusterName, "");
        if (clusterId.isEmpty()) {
            return new ArrayList<>();
        }
        // copy a new List
        return new ArrayList<>(clusterIdToBackend.getOrDefault(clusterId, new ArrayList<>()));
    }

    public List<Backend> getBackendsByClusterId(final String clusterId) {
        // copy a new List
        return new ArrayList<>(clusterIdToBackend.getOrDefault(clusterId, new ArrayList<>()));
    }

    public String getClusterNameByBeAddr(String beEndpoint) {
        rlock.lock();
        try {
            for (Map.Entry<String, List<Backend>> idBe : clusterIdToBackend.entrySet()) {
                if (idBe.getValue().stream().anyMatch(be -> be.getAddress().equals(beEndpoint))) {
                    return getClusterNameByClusterIdNoLock(idBe.getKey());
                }
            }
            return null;
        } finally {
            rlock.unlock();
        }
    }

    public List<String> getCloudClusterIds() {
        return new ArrayList<>(clusterIdToBackend.keySet());
    }

    public String getCloudStatusByName(final String clusterName) {
        String clusterId = clusterNameToId.getOrDefault(clusterName, "");
        if (Strings.isNullOrEmpty(clusterId)) {
            // for rename cluster or dropped cluster
            LOG.warn("cant find clusterId by clusteName {}", clusterName);
            return "";
        }
        // It is safe to return a null/empty status string, the caller handles it properly
        return getCloudStatusByIdNoLock(clusterId);
    }

    public String getCloudStatusById(final String clusterId) {
        rlock.lock();
        try {
            return getCloudStatusByIdNoLock(clusterId);
        } finally {
            rlock.unlock();
        }
    }

    public Set<String> getClusterStatus(List<Backend> backends) {
        // ATTN: found bug, In the same cluster, the cluster status in the tags of BE nodes is inconsistent.
        // Using a set to collect the cluster statuses from the BE nodes.
        return backends.stream().map(Backend::getCloudClusterStatus).collect(Collectors.toSet());
    }

    public String getCloudStatusByIdNoLock(final String clusterId) {
        List<Backend> bes = clusterIdToBackend.getOrDefault(clusterId, new ArrayList<>());
        Optional<String> hasNormal = bes.stream().map(Backend::getCloudClusterStatus)
                .filter(status -> status.equals(String.valueOf(Cloud.ClusterStatus.NORMAL))).findAny();
        return hasNormal.orElseGet(() -> bes.stream().map(Backend::getCloudClusterStatus).findFirst()
            .orElse(String.valueOf(Cloud.ClusterStatus.NORMAL)));
    }

    public void updateClusterNameToId(final String newName,
                                      final String originalName, final String clusterId) {
        wlock.lock();
        try {
            clusterNameToId.remove(originalName);
            clusterNameToId.put(newName, clusterId);
        } finally {
            wlock.unlock();
        }
    }

    public String getCloudClusterIdByName(String clusterName) {
        return clusterNameToId.get(clusterName);
    }

    public String getClusterNameByClusterId(final String clusterId) {
        rlock.lock();
        try {
            return getClusterNameByClusterIdNoLock(clusterId);
        } finally {
            rlock.unlock();
        }
    }

    public String getClusterNameByClusterIdNoLock(final String clusterId) {
        String clusterName = "";
        for (Map.Entry<String, String> entry : clusterNameToId.entrySet()) {
            if (entry.getValue().equals(clusterId)) {
                clusterName = entry.getKey();
                break;
            }
        }
        return clusterName;
    }

    public void dropCluster(final String clusterId, final String clusterName) {
        wlock.lock();
        try {
            clusterNameToId.remove(clusterName, clusterId);
            clusterIdToBackend.remove(clusterId);
        } finally {
            wlock.unlock();
        }
    }

    public List<String> getCloudClusterNames() {
        rlock.lock();
        try {
            return new ArrayList<>(clusterNameToId.keySet()).stream().filter(c -> !Strings.isNullOrEmpty(c))
                .sorted(Comparator.naturalOrder()).collect(Collectors.toList());
        } finally {
            rlock.unlock();
        }
    }

    // use cluster $clusterName
    // return clusterName for userName
    public String addCloudCluster(final String clusterName, final String userName) throws UserException {
        if ((Strings.isNullOrEmpty(clusterName) && Strings.isNullOrEmpty(userName))
                || (!Strings.isNullOrEmpty(clusterName) && !Strings.isNullOrEmpty(userName))) {
            // clusterName or userName just only need one.
            LOG.warn("addCloudCluster args err clusterName {}, userName {}", clusterName, userName);
            return "";
        }

        String clusterId;
        wlock.lock();
        try {
            // First time this method is called, build cloud cluster map
            if (clusterNameToId.isEmpty() || clusterIdToBackend.isEmpty()) {
                List<Backend> toAdd = Maps.newHashMap(idToBackendRef)
                        .values().stream()
                        .filter(i -> i.getTagMap().containsKey(Tag.CLOUD_CLUSTER_ID))
                        .filter(i -> i.getTagMap().containsKey(Tag.CLOUD_CLUSTER_NAME))
                        .collect(Collectors.toList());
                // The larger bakendId the later it was added, the order matters
                toAdd.sort((x, y) -> (int) (x.getId() - y.getId()));
                updateCloudClusterMapNoLock(toAdd, new ArrayList<>());
            }

            if (Strings.isNullOrEmpty(userName)) {
                // use clusterName
                LOG.info("try to add a cloud cluster, clusterName={}", clusterName);
                clusterId = clusterNameToId.get(clusterName);
                clusterId = clusterId == null ? "" : clusterId;
                if (clusterIdToBackend.containsKey(clusterId)) { // Cluster already added
                    LOG.info("cloud cluster already added, clusterName={}, clusterId={}", clusterName, clusterId);
                    return "";
                }
            }
        } finally {
            wlock.unlock();
        }

        LOG.info("begin to get cloud cluster from remote, clusterName={}, userName={}", clusterName, userName);

        // Get cloud cluster info from resource manager
        Cloud.GetClusterResponse response = getCloudCluster(clusterName, "", userName);
        if (!response.hasStatus() || !response.getStatus().hasCode()
                || response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            LOG.warn("get cluster info from meta failed, clusterName={}, incomplete response: {}",
                    clusterName, response);
            throw new UserException("no cluster clusterName: " + clusterName + " or userName: " + userName + " found");
        }

        // Note: get_cluster interface cluster(option -> repeated), so it has at least one cluster.
        if (response.getClusterCount() == 0) {
            LOG.warn("meta service error , return cluster zero, plz check it, "
                    + "cloud_unique_id={}, clusterId={}, response={}",
                    Config.cloud_unique_id, Config.cloud_sql_server_cluster_id, response);
            throw new UserException("get cluster return zero cluster info");
        }

        ClusterPB cpb = response.getCluster(0);
        clusterId = cpb.getClusterId();
        String clusterNameMeta = cpb.getClusterName();
        Cloud.ClusterStatus clusterStatus = cpb.hasClusterStatus()
                ? cpb.getClusterStatus() : Cloud.ClusterStatus.NORMAL;
        String clusterPublicEndpoint = cpb.getPublicEndpoint();
        String clusterPrivateEndpoint = cpb.getPrivateEndpoint();
        // Prepare backends
        List<Backend> backends = new ArrayList<>();
        for (Cloud.NodeInfoPB node : cpb.getNodesList()) {
            Map<String, String> newTagMap = Tag.DEFAULT_BACKEND_TAG.toMap();

            newTagMap.put(Tag.CLOUD_CLUSTER_NAME, clusterNameMeta);
            newTagMap.put(Tag.CLOUD_CLUSTER_ID, clusterId);
            newTagMap.put(Tag.CLOUD_CLUSTER_STATUS, String.valueOf(clusterStatus));
            // Prioritize node-level endpoint configuration, use cluster-level endpoint if
            // node endpoint is empty
            String nodePublicEndpoint = node.hasPublicEndpoint() && !node.getPublicEndpoint().isEmpty()
                    ? node.getPublicEndpoint()
                    : clusterPublicEndpoint;
            String nodePrivateEndpoint = node.hasPrivateEndpoint() && !node.getPrivateEndpoint().isEmpty()
                    ? node.getPrivateEndpoint()
                    : clusterPrivateEndpoint;

            newTagMap.put(Tag.PUBLIC_ENDPOINT, nodePublicEndpoint);
            newTagMap.put(Tag.PRIVATE_ENDPOINT, nodePrivateEndpoint);
            newTagMap.put(Tag.CLOUD_UNIQUE_ID, node.getCloudUniqueId());
            Backend b = new Backend(Env.getCurrentEnv().getNextId(), node.getIp(), node.getHeartbeatPort());
            b.setTagMap(newTagMap);
            backends.add(b);
            LOG.info(
                    "new backend to add, clusterName={} clusterId={} backend={}, publicEndpoint={}, privateEndpoint={}",
                    clusterNameMeta, clusterId, b.toString(), nodePublicEndpoint, nodePrivateEndpoint);
        }

        updateCloudBackends(backends, new ArrayList<>());
        return clusterNameMeta;
    }

    // Return the ref of concurrentMap clusterIdToBackend
    public Map<String, List<Backend>> getCloudClusterIdToBackend() {
        rlock.lock();
        try {
            return new ConcurrentHashMap<>(clusterIdToBackend);
        } finally {
            rlock.unlock();
        }
    }

    public ImmutableMap<Long, Backend> getCloudIdToBackend(String clusterName) {
        rlock.lock();
        try {
            return getCloudIdToBackendNoLock(clusterName);
        } finally {
            rlock.unlock();
        }
    }

    public ImmutableMap<Long, Backend> getCloudIdToBackendNoLock(String clusterName) {
        String clusterId = clusterNameToId.get(clusterName);
        if (Strings.isNullOrEmpty(clusterId)) {
            LOG.warn("cant find clusterId, this cluster may be has been dropped, clusterName={}", clusterName);
            return ImmutableMap.of();
        }
        List<Backend> backends = clusterIdToBackend.get(clusterId);
        Map<Long, Backend> idToBackend = Maps.newHashMap();
        for (Backend be : backends) {
            idToBackend.put(be.getId(), be);
        }
        return ImmutableMap.copyOf(idToBackend);
    }

    // Return the ref of concurrentMap clusterNameToId
    public Map<String, String> getCloudClusterNameToId() {
        rlock.lock();
        try {
            return new ConcurrentHashMap<>(clusterNameToId);
        } finally {
            rlock.unlock();
        }
    }

    // FrontendCluster = SqlServerCluster
    private void alterFrontendCluster(FrontendNodeType role, String host, int editLogPort,
            String cloudUnqiueID, Cloud.AlterClusterRequest.Operation op) throws DdlException {
        if (Strings.isNullOrEmpty(((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId())) {
            throw new DdlException("unable to alter frontend due to empty cloud_instance_id");
        }

        Cloud.NodeInfoPB.NodeType nodeType;
        if (role == FrontendNodeType.MASTER) {
            nodeType = Cloud.NodeInfoPB.NodeType.FE_MASTER;
        } else if (role == FrontendNodeType.FOLLOWER) {
            nodeType = Cloud.NodeInfoPB.NodeType.FE_FOLLOWER;
        } else if (role == FrontendNodeType.OBSERVER) {
            nodeType = Cloud.NodeInfoPB.NodeType.FE_OBSERVER;
        } else {
            throw new DdlException("unable to alter frontend due to invalid role");
        }

        // Issue rpc to meta to add this node, then fe master would add this node to its frontends
        Cloud.NodeInfoPB nodeInfoPB = Cloud.NodeInfoPB.newBuilder()
                .setCloudUniqueId(cloudUnqiueID)
                .setIp(host)
                .setHost(host)
                .setEditLogPort(editLogPort)
                .setNodeType(nodeType)
                .setCtime(System.currentTimeMillis() / 1000)
                .build();

        ClusterPB clusterPB = ClusterPB.newBuilder()
                .setClusterId(Config.cloud_sql_server_cluster_id)
                .setClusterName(Config.cloud_sql_server_cluster_name)
                .setType(ClusterPB.Type.SQL)
                .addNodes(nodeInfoPB)
                .build();

        Cloud.AlterClusterRequest request = Cloud.AlterClusterRequest.newBuilder()
                .setInstanceId(((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId())
                .setOp(op)
                .setCluster(clusterPB)
                .build();

        Cloud.AlterClusterResponse response;
        try {
            response = MetaServiceProxy.getInstance().alterCluster(request);
            LOG.info("alter cluster, request: {}, response: {}", request, response);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("alter frontend not ok, response: {}", response);
                throw new DdlException("failed to alter frontend errorCode: " + response.getStatus().getCode()
                        + " msg: " + response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new DdlException("failed to alter frontend", e);
        }
    }

    public void addFrontend(FrontendNodeType role, String host, int editLogPort) throws DdlException {
        Cloud.AlterClusterRequest.Operation op;
        op = role == FrontendNodeType.MASTER ? Cloud.AlterClusterRequest.Operation.ADD_CLUSTER
                            : Cloud.AlterClusterRequest.Operation.ADD_NODE;
        alterFrontendCluster(role, host, editLogPort, Config.cloud_unique_id, op);
    }

    public void dropFrontend(Frontend frontend) throws DdlException {
        alterFrontendCluster(frontend.getRole(), frontend.getHost(), frontend.getEditLogPort(),
                frontend.getCloudUniqueId(), Cloud.AlterClusterRequest.Operation.DROP_NODE);
    }

    private String tryCreateComputeGroup(String clusterName, String computeGroupId) throws UserException {
        if (Strings.isNullOrEmpty(((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId())) {
            throw new DdlException("unable to create compute group due to empty cloud_instance_id");
        }

        ClusterPB clusterPB = ClusterPB.newBuilder()
                .setClusterId(computeGroupId)
                .setClusterName(clusterName)
                .setType(ClusterPB.Type.COMPUTE)
                .build();

        Cloud.AlterClusterRequest request = Cloud.AlterClusterRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setOp(Cloud.AlterClusterRequest.Operation.ADD_CLUSTER)
                .setCluster(clusterPB)
                .build();

        Cloud.AlterClusterResponse response;
        try {
            response = MetaServiceProxy.getInstance().alterCluster(request);
            LOG.info("alter cluster, request: {}, response: {}", request, response);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK
                    && response.getStatus().getCode() != Cloud.MetaServiceCode.ALREADY_EXISTED) {
                LOG.warn("create cluster not ok, response: {}", response);
                throw new UserException("failed to create cluster errorCode: " + response.getStatus().getCode()
                        + " msg: " + response.getStatus().getMsg());
            }

            if (response.getStatus().getCode() == Cloud.MetaServiceCode.OK) {
                return computeGroupId;
            } else if (response.getStatus().getCode() == Cloud.MetaServiceCode.ALREADY_EXISTED) {
                Cloud.GetClusterResponse clusterResponse = getCloudCluster(clusterName, "", "");
                if (clusterResponse.getStatus().getCode() == Cloud.MetaServiceCode.OK) {
                    if (clusterResponse.getClusterCount() > 0) {
                        ClusterPB cluster = clusterResponse.getCluster(0);
                        return cluster.getClusterId();
                    } else {
                        throw new UserException("Cluster information not found in the response");
                    }
                } else {
                    throw new UserException("Failed to get cluster. Error code: "
                            + clusterResponse.getStatus().getCode() + ", message: "
                            + clusterResponse.getStatus().getMsg());
                }
            } else {
                throw new UserException("Failed to create or get cluster. Error code: "
                        + response.getStatus().getCode() + ", message: " + response.getStatus().getMsg());
            }
        } catch (RpcException e) {
            throw new UserException("failed to create cluster", e);
        }

    }

    public List<Pair<String, Integer>> getCurrentObFrontends() {
        List<Frontend> frontends = Env.getCurrentEnv().getFrontends(FrontendNodeType.OBSERVER);
        List<Pair<String, Integer>> frontendsPair = new ArrayList<>();
        for (Frontend frontend : frontends) {
            frontendsPair.add(Pair.of(frontend.getHost(), frontend.getEditLogPort()));
        }
        return frontendsPair;
    }

    public Cloud.GetInstanceResponse getCloudInstance() {
        Cloud.GetInstanceRequest.Builder builder = Cloud.GetInstanceRequest.newBuilder();
        builder.setCloudUniqueId(Config.cloud_unique_id);
        final Cloud.GetInstanceRequest pRequest = builder.build();
        Cloud.GetInstanceResponse response;
        try {
            response = MetaServiceProxy.getInstance().getInstance(pRequest);
            return response;
        } catch (RpcException e) {
            LOG.warn("rpcToGetInstance exception: {}", e.getMessage());
        }
        return null;
    }

    public InstanceInfoPB.Status getInstanceStatus() {
        return this.instanceStatus;
    }

    public void setInstanceStatus(InstanceInfoPB.Status instanceStatus) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("fe set instance status {}", instanceStatus);
        }
        if (this.instanceStatus != instanceStatus) {
            LOG.info("fe change instance status from {} to {}", this.instanceStatus, instanceStatus);
        }
        this.instanceStatus = instanceStatus;
    }

    public void waitForAutoStartCurrentCluster() throws DdlException {
        ConnectContext context = ConnectContext.get();
        if (context != null) {
            String cloudCluster = "";
            try {
                cloudCluster = context.getCloudCluster();
            } catch (ComputeGroupException e) {
                LOG.warn("failed to get cloud cluster", e);
                return;
            }
            if (!Strings.isNullOrEmpty(cloudCluster)) {
                waitForAutoStart(cloudCluster);
            }
        }
    }

    public String getClusterNameAutoStart(final String clusterName) {
        if (!Strings.isNullOrEmpty(clusterName)) {
            return clusterName;
        }

        ConnectContext context = ConnectContext.get();
        if (context == null) {
            LOG.warn("auto start cant get context so new it");
            context = new ConnectContext();
        }
        ConnectContext.CloudClusterResult cloudClusterTypeAndName = context.getCloudClusterByPolicy();
        if (cloudClusterTypeAndName == null) {
            LOG.warn("get cluster from ctx err");
            return null;
        }
        if (cloudClusterTypeAndName.comment
                == ConnectContext.CloudClusterResult.Comment.DEFAULT_CLUSTER_SET_BUT_NOT_EXIST) {
            LOG.warn("get default cluster from ctx err");
            return null;
        }

        Preconditions.checkState(!Strings.isNullOrEmpty(cloudClusterTypeAndName.clusterName),
                "get cluster name empty");
        LOG.info("get cluster to resume {}", cloudClusterTypeAndName);
        return cloudClusterTypeAndName.clusterName;
    }

    public String waitForAutoStart(String clusterName) throws DdlException {
        if (Config.isNotCloudMode()) {
            return null;
        }
        if (!Config.enable_auto_start_for_cloud_cluster) {
            return null;
        }
        clusterName = getClusterNameAutoStart(clusterName);
        if (Strings.isNullOrEmpty(clusterName)) {
            LOG.warn("auto start in cloud mode, but clusterName empty {}", clusterName);
            return null;
        }
        String clusterStatus = getCloudStatusByName(clusterName);
        if (Strings.isNullOrEmpty(clusterStatus)) {
            // for cluster rename or cluster dropped
            LOG.warn("cant find clusterStatus in fe, clusterName {}", clusterName);
            return null;
        }

        if (Cloud.ClusterStatus.valueOf(clusterStatus) == Cloud.ClusterStatus.MANUAL_SHUTDOWN) {
            LOG.warn("auto start cluster {} in manual shutdown status", clusterName);
            throw new DdlException("cluster " + clusterName + " is in manual shutdown");
        }

        // nofity ms -> wait for clusterStatus to normal
        LOG.debug("auto start wait cluster {} status {}", clusterName, clusterStatus);
        if (Cloud.ClusterStatus.valueOf(clusterStatus) != Cloud.ClusterStatus.NORMAL) {
            // ATTN: prevent `Automatic Analyzer` daemon threads from pulling up clusters
            // FeConstants.INTERNAL_DB_NAME ? see StatisticsUtil.buildConnectContext
            List<String> ignoreDbNameList = Arrays.asList(Config.auto_start_ignore_resume_db_names);
            if (ConnectContext.get() != null && ignoreDbNameList.contains(ConnectContext.get().getDatabase())) {
                LOG.warn("auto start daemon thread db {}, not resume cluster {}-{}",
                        ConnectContext.get().getDatabase(), clusterName, clusterStatus);
                return null;
            }
            Cloud.AlterClusterRequest.Builder builder = Cloud.AlterClusterRequest.newBuilder();
            builder.setCloudUniqueId(Config.cloud_unique_id);
            builder.setOp(Cloud.AlterClusterRequest.Operation.SET_CLUSTER_STATUS);

            ClusterPB.Builder clusterBuilder = ClusterPB.newBuilder();
            clusterBuilder.setClusterId(getCloudClusterIdByName(clusterName));
            clusterBuilder.setClusterStatus(Cloud.ClusterStatus.TO_RESUME);
            builder.setCluster(clusterBuilder);

            Cloud.AlterClusterResponse response;
            try {
                Cloud.AlterClusterRequest request = builder.build();
                response = MetaServiceProxy.getInstance().alterCluster(request);
                LOG.info("alter cluster, request: {}, response: {}", request, response);
                if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                    LOG.warn("notify to resume cluster not ok, cluster {}, response: {}", clusterName, response);
                }
                LOG.info("notify to resume cluster {}, response: {} ", clusterName, response);
            } catch (RpcException e) {
                LOG.warn("failed to notify to resume cluster {}", clusterName, e);
                throw new DdlException("notify to resume cluster not ok");
            }
        }
        // wait 5 mins
        int retryTimes = Config.auto_start_wait_to_resume_times < 0 ? 300 : Config.auto_start_wait_to_resume_times;
        int retryTime = 0;
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        boolean hasAutoStart = false;
        boolean existAliveBe = true;
        while ((!String.valueOf(Cloud.ClusterStatus.NORMAL).equals(clusterStatus) || !existAliveBe)
            && retryTime < retryTimes) {
            hasAutoStart = true;
            ++retryTime;
            // sleep random millis [0.5, 1] s
            int randomSeconds =  500 + (int) (Math.random() * (1000 - 500));
            LOG.info("resume cluster {} retry times {}, wait randomMillis: {}, current status: {}",
                    clusterName, retryTime, randomSeconds, clusterStatus);
            try {
                if (retryTime > retryTimes / 2) {
                    // sleep random millis [1, 1.5] s
                    randomSeconds =  1000 + (int) (Math.random() * (1000 - 500));
                }
                Thread.sleep(randomSeconds);
            } catch (InterruptedException e) {
                LOG.info("change cluster sleep wait InterruptedException: ", e);
            }
            clusterStatus = getCloudStatusByName(clusterName);
            // Check that the bes node in the cluster have at least one alive
            existAliveBe = getBackendsByClusterName(clusterName).stream().anyMatch(Backend::isAlive);
        }
        if (retryTime >= retryTimes) {
            // auto start timeout
            stopWatch.stop();
            LOG.warn("auto start cluster {} timeout, wait {} ms", clusterName, stopWatch.getTime());
            throw new DdlException("auto start cluster timeout");
        }

        stopWatch.stop();
        if (hasAutoStart) {
            LOG.info("auto start cluster {}, start cost {} ms", clusterName, stopWatch.getTime());
        }
        return clusterName;
    }

    public void tryCreateInstance(String instanceId, String name, boolean sseEnabled) throws DdlException {
        Cloud.CreateInstanceRequest.Builder builder = Cloud.CreateInstanceRequest.newBuilder();
        builder.setInstanceId(instanceId);
        builder.setName(name);
        builder.setSseEnabled(sseEnabled);

        Cloud.CreateInstanceResponse response;
        try {

            Cloud.CreateInstanceRequest request = builder.build();
            response = MetaServiceProxy.getInstance().createInstance(request);
            LOG.info("create instance, request: {}, response: {}", request, response);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK
                    && response.getStatus().getCode() != Cloud.MetaServiceCode.ALREADY_EXISTED) {
                LOG.warn("Failed to create instance {}, response: {}", instanceId, response);
                throw new DdlException("Failed to create instance");
            }
            LOG.info("Successfully created instance {}, response: {}", instanceId, response);
        } catch (RpcException e) {
            LOG.warn("Failed to create instance {}", instanceId, e);
            throw new DdlException("Failed to create instance");
        }
    }

    public String getInstanceId(String cloudUniqueId) throws IOException {
        Cloud.GetInstanceRequest.Builder builder = Cloud.GetInstanceRequest.newBuilder();
        builder.setCloudUniqueId(cloudUniqueId);

        Cloud.GetInstanceResponse response;
        try {
            Cloud.GetInstanceRequest request = builder.build();
            response = MetaServiceProxy.getInstance().getInstance(request);
            LOG.info("get instance info, request: {}, response: {}", request, response);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("Failed to get instance info, response: {}", response);
                throw new IOException("Failed to get instance info");
            }
            return response.getInstance().getInstanceId();
        } catch (RpcException e) {
            LOG.warn("Failed to get instance info {}", cloudUniqueId, e);
            throw new IOException("Failed to get instance info");
        }
    }

    public void renameComputeGroup(String originalName, String newGroupName) throws UserException {
        String cloudInstanceId = ((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId();
        if (Strings.isNullOrEmpty(cloudInstanceId)) {
            throw new DdlException("unable to rename compute group due to empty cloud_instance_id");
        }
        String originalComputeGroupId = ((CloudSystemInfoService) Env.getCurrentSystemInfo())
                .getCloudClusterIdByName(originalName);
        if (Strings.isNullOrEmpty(originalComputeGroupId)) {
            LOG.info("rename original compute group {} not found, unable to rename", originalName);
            throw new DdlException("compute group '" + originalName + "' not found, unable to rename");
        }
        // check newGroupName has existed
        if (((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames().contains(newGroupName)) {
            LOG.info("rename new compute group {} has existed in instance, unable to rename", newGroupName);
            throw new DdlException("compute group '" + newGroupName + "' has existed in warehouse, unable to rename");
        }

        ClusterPB clusterPB = ClusterPB.newBuilder()
                .setClusterId(originalComputeGroupId)
                .setClusterName(newGroupName)
                .setType(ClusterPB.Type.COMPUTE)
                .build();

        Cloud.AlterClusterRequest request = Cloud.AlterClusterRequest.newBuilder()
                .setInstanceId(((CloudEnv) Env.getCurrentEnv()).getCloudInstanceId())
                .setOp(Cloud.AlterClusterRequest.Operation.RENAME_CLUSTER)
                .setReplaceIfExistingEmptyTargetCluster(true)
                .setCluster(clusterPB)
                .build();


        Cloud.AlterClusterResponse response = null;
        try {
            response = MetaServiceProxy.getInstance().alterCluster(request);
            if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                LOG.warn("alter rename compute group not ok, response: {}", response);
                throw new UserException("failed to rename compute group errorCode: " + response.getStatus().getCode()
                    + " msg: " + response.getStatus().getMsg() + " may be you can try later");
            }
        } catch (RpcException e) {
            LOG.warn("alter rename compute group rpc exception");
            throw new UserException("failed to alter rename compute group", e);
        } finally {
            LOG.info("alter rename compute group, request: {}, response: {}", request, response);
        }
    }
}

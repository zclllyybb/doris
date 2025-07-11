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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.Profile;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.FailMsg;
import org.apache.doris.nereids.load.NereidsBrokerFileGroup;
import org.apache.doris.nereids.load.NereidsLoadingTaskPlanner;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.ErrorTabletInfo;
import org.apache.doris.transaction.TabletCommitInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class LoadLoadingTask extends LoadTask {
    private static final Logger LOG = LogManager.getLogger(LoadLoadingTask.class);

    /*
     * load id is used for plan.
     * It should be changed each time we retry this load plan
     */
    private TUniqueId loadId;
    private final Database db;
    private final OlapTable table;
    private final BrokerDesc brokerDesc;
    private final List<BrokerFileGroup> fileGroups;
    private final long jobDeadlineMs;
    private final long execMemLimit;
    private final boolean strictMode;
    private final boolean isPartialUpdate;
    private final TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy;
    private final long txnId;
    private final String timezone;
    // timeout of load job, in seconds
    private final long timeoutS;
    private final int loadParallelism;
    private final int sendBatchParallelism;
    private final boolean loadZeroTolerance;
    private final boolean singleTabletLoadPerSink;

    private final boolean enableMemTableOnSinkNode;
    private final int batchSize;

    private NereidsLoadingTaskPlanner planner;

    private Profile jobProfile;
    private long beginTime;

    private List<TPipelineWorkloadGroup> tWorkloadGroups = null;

    protected UserIdentity userInfo;

    public LoadLoadingTask(UserIdentity userInfo, Database db, OlapTable table,
            BrokerDesc brokerDesc, List<BrokerFileGroup> fileGroups,
            long jobDeadlineMs, long execMemLimit, boolean strictMode, boolean isPartialUpdate,
            TPartialUpdateNewRowPolicy partialUpdateNewKeyPolicy,
            long txnId, LoadTaskCallback callback, String timezone,
            long timeoutS, int loadParallelism, int sendBatchParallelism,
            boolean loadZeroTolerance, Profile jobProfile, boolean singleTabletLoadPerSink,
            Priority priority, boolean enableMemTableOnSinkNode, int batchSize) {
        super(callback, TaskType.LOADING, priority);
        this.userInfo = userInfo;
        this.db = db;
        this.table = table;
        this.brokerDesc = brokerDesc;
        this.fileGroups = fileGroups;
        this.jobDeadlineMs = jobDeadlineMs;
        this.execMemLimit = execMemLimit;
        this.strictMode = strictMode;
        this.isPartialUpdate = isPartialUpdate;
        this.partialUpdateNewKeyPolicy = partialUpdateNewKeyPolicy;
        this.txnId = txnId;
        this.failMsg = new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL);
        this.retryTime = 2; // 2 times is enough
        this.timezone = timezone;
        this.timeoutS = timeoutS;
        this.loadParallelism = loadParallelism;
        this.sendBatchParallelism = sendBatchParallelism;
        this.loadZeroTolerance = loadZeroTolerance;
        this.jobProfile = jobProfile;
        this.singleTabletLoadPerSink = singleTabletLoadPerSink;
        this.enableMemTableOnSinkNode = enableMemTableOnSinkNode;
        this.batchSize = batchSize;
    }

    public void init(TUniqueId loadId, List<List<TBrokerFileStatus>> fileStatusList,
                     int fileNum, UserIdentity userInfo) throws UserException {
        this.loadId = loadId;
        List<NereidsBrokerFileGroup> brokerFileGroups = new ArrayList<>(fileGroups.size());
        for (BrokerFileGroup fileGroup : fileGroups) {
            brokerFileGroups.add(fileGroup.toNereidsBrokerFileGroup());
        }
        planner = new NereidsLoadingTaskPlanner(callback.getCallbackId(), txnId, db.getId(), table, brokerDesc,
                brokerFileGroups, strictMode, isPartialUpdate, partialUpdateNewKeyPolicy, timezone, timeoutS,
                loadParallelism, sendBatchParallelism, userInfo, singleTabletLoadPerSink, enableMemTableOnSinkNode);
        planner.plan(loadId, fileStatusList, fileNum);
    }

    public TUniqueId getLoadId() {
        return loadId;
    }

    @Override
    protected void executeTask() throws Exception {
        LOG.info("begin to execute loading task. load id: {} job id: {}. db: {}, tbl: {}. left retry: {}",
                DebugUtil.printId(loadId), callback.getCallbackId(), db.getFullName(), table.getName(), retryTime);

        retryTime--;
        beginTime = System.currentTimeMillis();
        if (!((BrokerLoadJob) callback).updateState(JobState.LOADING)) {
            // job may already be cancelled
            return;
        }
        executeOnce();
    }

    protected void executeOnce() throws Exception {
        final boolean enableProfile = this.jobProfile != null;
        // New one query id,
        Coordinator curCoordinator =  EnvFactory.getInstance().createCoordinator(callback.getCallbackId(),
                loadId, planner.getDescTable(),
                planner.getFragments(), planner.getScanNodes(), planner.getTimezone(), loadZeroTolerance,
                enableProfile);
        if (enableProfile) {
            this.jobProfile.addExecutionProfile(curCoordinator.getExecutionProfile());
        }
        curCoordinator.setQueryType(TQueryType.LOAD);
        curCoordinator.setExecMemoryLimit(execMemLimit);

        curCoordinator.setMemTableOnSinkNode(enableMemTableOnSinkNode);
        curCoordinator.setBatchSize(batchSize);

        long leftTimeMs = getLeftTimeMs();
        if (leftTimeMs <= 0) {
            throw new LoadException("failed to execute loading task when timeout");
        }
        // 1 second is the minimum granularity of actual execution
        int timeoutS = Math.max((int) (leftTimeMs / 1000), 1);
        curCoordinator.setTimeout(timeoutS);

        // NOTE: currently broker load not enter workload group's queue
        // so we set tWorkloadGroup here directly.
        if (tWorkloadGroups != null) {
            curCoordinator.setTWorkloadGroups(tWorkloadGroups);
        }

        try {
            QeProcessorImpl.INSTANCE.registerQuery(loadId, new QeProcessorImpl.QueryInfo(curCoordinator));
            actualExecute(curCoordinator, timeoutS);
        } finally {
            QeProcessorImpl.INSTANCE.unregisterQuery(loadId);
        }
    }

    private void actualExecute(Coordinator curCoordinator, int waitSecond) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug(new LogBuilder(LogKey.LOAD_JOB, callback.getCallbackId())
                    .add("task_id", signature)
                    .add("query_id", DebugUtil.printId(curCoordinator.getQueryId()))
                    .add("msg", "begin to execute plan")
                    .build());
        }
        curCoordinator.exec();
        if (curCoordinator.join(waitSecond)) {
            Status status = curCoordinator.getExecStatus();
            if (status.ok() || status.getErrorCode() == TStatusCode.DATA_QUALITY_ERROR) {
                attachment = new BrokerLoadingTaskAttachment(signature,
                        curCoordinator.getLoadCounters(),
                        curCoordinator.getTrackingUrl(),
                        TabletCommitInfo.fromThrift(curCoordinator.getCommitInfos()),
                        ErrorTabletInfo.fromThrift(curCoordinator.getErrorTabletInfos()
                                .stream().limit(Config.max_error_tablet_of_broker_load).collect(Collectors.toList())),
                        status);
                curCoordinator.getErrorTabletInfos().clear();
            } else {
                throw new LoadException(status.getErrorMsg());
            }
        } else {
            throw new LoadException("coordinator could not finished before job timeout");
        }
    }

    public long getLeftTimeMs() {
        return jobDeadlineMs - System.currentTimeMillis();
    }

    @Override
    public void updateRetryInfo() {
        super.updateRetryInfo();
        UUID uuid = UUID.randomUUID();
        this.loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        planner.updateLoadId(this.loadId);
        // reset progress on each retry, otherwise the finished/total num will be incorrect
        Env.getCurrentProgressManager().registerProgressSimple(String.valueOf(callback.getCallbackId()));
    }

    public void settWorkloadGroups(List<TPipelineWorkloadGroup> tWorkloadGroups) {
        this.tWorkloadGroups = tWorkloadGroups;
    }

}

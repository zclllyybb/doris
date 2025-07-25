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

package org.apache.doris.alter;

import org.apache.doris.alter.AlterJobV2.JobState;
import org.apache.doris.analysis.AddRollupClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.CatalogTestUtil;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FakeEditLog;
import org.apache.doris.catalog.FakeEnv;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.FakeTransactionIDGenerator;
import org.apache.doris.transaction.GlobalTransactionMgrIface;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RollupJobV2Test {
    private static String fileName = "./RollupJobV2Test";

    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgrIface masterTransMgr;
    private static GlobalTransactionMgrIface slaveTransMgr;
    private static Env masterEnv;
    private static Env slaveEnv;

    private static String transactionSource = "localfe";
    private static AddRollupClause clause;
    private static AddRollupClause clause2;

    private FakeEnv fakeEnv;
    private FakeEditLog fakeEditLog;

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, AnalysisException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterEnv = CatalogTestUtil.createTestCatalog();
        slaveEnv = CatalogTestUtil.createTestCatalog();
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();
        masterTransMgr = masterEnv.getGlobalTransactionMgr();
        masterTransMgr.setEditLog(masterEnv.getEditLog());

        slaveTransMgr = slaveEnv.getGlobalTransactionMgr();
        slaveTransMgr.setEditLog(slaveEnv.getEditLog());
        clause = new AddRollupClause(CatalogTestUtil.testRollupIndex2, Lists.newArrayList("k1", "v"), null,
                CatalogTestUtil.testIndex1, null);
        clause.analyze();

        clause2 = new AddRollupClause(CatalogTestUtil.testRollupIndex3, Lists.newArrayList("k1", "v"), null,
                CatalogTestUtil.testIndex1, null);
        clause2.analyze();

        FeConstants.runningUnitTest = true;
        AgentTaskQueue.clearAllTasks();

        new MockUp<Env>() {
            @Mock
            public Env getCurrentEnv() {
                return masterEnv;
            }
        };
    }

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testRunRollupJobConcurrentLimit() throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        MaterializedViewHandler materializedViewHandler = Env.getCurrentEnv().getMaterializedViewHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        alterClauses.add(clause2);
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();

        materializedViewHandler.runAfterCatalogReady();

        Assert.assertEquals(Config.max_running_rollup_job_num_per_table, materializedViewHandler.getTableRunningJobMap().get(CatalogTestUtil.testTableId1).size());
        Assert.assertEquals(2, alterJobsV2.size());
        Assert.assertEquals(OlapTableState.ROLLUP, olapTable.getState());
    }

    @Test
    public void testAddSchemaChange() throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        MaterializedViewHandler materializedViewHandler = Env.getCurrentEnv().getMaterializedViewHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        Assert.assertEquals(OlapTableState.ROLLUP, olapTable.getState());
    }

    // start a schema change, then finished
    @Test
    public void testSchemaChange1() throws Exception {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        MaterializedViewHandler materializedViewHandler = Env.getCurrentEnv().getMaterializedViewHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        // runPendingJob
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.WAITING_TXN, rollupJob.getJobState());
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // runWaitingTxnJob, task not finished
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // finish all tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }
        MaterializedIndex shadowIndex = testPartition.getMaterializedIndices(IndexExtState.SHADOW).get(0);
        for (Tablet shadowTablet : shadowIndex.getTablets()) {
            for (Replica shadowReplica : shadowTablet.getReplicas()) {
                shadowReplica.updateVersion(testPartition.getVisibleVersion());
            }
        }

        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.FINISHED, rollupJob.getJobState());
    }

    @Test
    public void testSchemaChangeWhileTabletNotStable() throws Exception {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        MaterializedViewHandler materializedViewHandler = Env.getCurrentEnv().getMaterializedViewHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(CatalogTestUtil.testPartitionId1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        Assert.assertEquals(MaterializedIndex.IndexState.NORMAL, baseIndex.getState());
        Assert.assertEquals(Partition.PartitionState.NORMAL, testPartition.getState());
        Assert.assertEquals(OlapTableState.ROLLUP, olapTable.getState());

        Tablet baseTablet = baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getReplicas();
        Replica replica1 = replicas.get(0);
        Replica replica2 = replicas.get(1);
        Replica replica3 = replicas.get(2);

        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica1.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica2.getVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica3.getVersion());
        Assert.assertEquals(-1, replica1.getLastFailedVersion());
        Assert.assertEquals(-1, replica2.getLastFailedVersion());
        Assert.assertEquals(-1, replica3.getLastFailedVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica1.getLastSuccessVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica2.getLastSuccessVersion());
        Assert.assertEquals(CatalogTestUtil.testStartVersion, replica3.getLastSuccessVersion());

        // runPendingJob
        replica1.setState(Replica.ReplicaState.DECOMMISSION);
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.PENDING, rollupJob.getJobState());

        // table is stable, runPendingJob again
        replica1.setState(Replica.ReplicaState.NORMAL);
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.WAITING_TXN, rollupJob.getJobState());
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // runWaitingTxnJob, task not finished
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // finish all tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }
        MaterializedIndex shadowIndex = testPartition.getMaterializedIndices(IndexExtState.SHADOW).get(0);
        for (Tablet shadowTablet : shadowIndex.getTablets()) {
            for (Replica shadowReplica : shadowTablet.getReplicas()) {
                shadowReplica.updateVersion(testPartition.getVisibleVersion());
            }
        }

        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.FINISHED, rollupJob.getJobState());
    }


    @Test
    public void testSerializeOfRollupJob()
            throws IOException, AnalysisException {
        // prepare file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        short keysCount = 1;
        List<Column> columns = Lists.newArrayList();
        String mvColumnName = CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "to_bitmap_" + "c1";
        Column column = new Column(mvColumnName, Type.BITMAP, false, AggregateType.BITMAP_UNION, false, "1", "");
        columns.add(column);

        RollupJobV2 rollupJobV2 = AlterJobV2Factory.createRollupJobV2(
                "", 1, 1, 1, "test", 1, 1, 1, "test", "rollup", columns, null, 1, 1,
                KeysType.AGG_KEYS, keysCount,
                new OriginStatement("create materialized view rollup as select bitmap_union(to_bitmap(c1)) from test",
                        0));
        rollupJobV2.setStorageFormat(TStorageFormat.V2);

        // write rollup job
        rollupJobV2.write(out);
        out.flush();
        out.close();

        // read objects from file
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        RollupJobV2 result = (RollupJobV2) AlterJobV2.read(in);
        Assert.assertEquals(TStorageFormat.V2, Deencapsulation.getField(result, "storageFormat"));
        List<Column> resultColumns = Deencapsulation.getField(result, "rollupSchema");
        Assert.assertEquals(1, resultColumns.size());
        Column resultColumn1 = resultColumns.get(0);
        Assert.assertEquals(mvColumnName,
                resultColumn1.getName());
    }

    @Test
    public void testAddRollupForDupTable() throws UserException {
        fakeEnv = new FakeEnv();
        fakeEditLog = new FakeEditLog();
        FakeEnv.setEnv(masterEnv);
        MaterializedViewHandler materializedViewHandler = Env.getCurrentEnv().getMaterializedViewHandler();
        Database db = masterEnv.getInternalCatalog().getDbOrDdlException(CatalogTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTableOrDdlException(CatalogTestUtil.testTableId2);

        AddRollupClause addRollupClause = new AddRollupClause("r1", Lists.newArrayList("k1", "v1", "v2"), null, CatalogTestUtil.testIndex2, null);

        List<Column> columns = materializedViewHandler.checkAndPrepareMaterializedView(addRollupClause, olapTable, CatalogTestUtil.testIndexId2, false);
        for (Column column : columns) {
            if (column.nameEquals("v1", true)) {
                Assert.assertNull(column.getAggregationType());
                break;
            }
        }
    }
}

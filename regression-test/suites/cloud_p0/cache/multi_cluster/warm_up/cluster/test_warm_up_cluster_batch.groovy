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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_warm_up_cluster_batch") {
    def getJobState = { jobId ->
         def jobStateResult = sql """  SHOW WARM UP JOB WHERE ID = ${jobId} """
         return jobStateResult[0]
    }
    def table = "customer"

    List<String> ipList = new ArrayList<>();
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> brpcPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    int num = 0
    for(String values : bes) {
        if (num++ == 2) break;
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
        brpcPortList.add(beInfo[4]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);
    println("the brpc port is " + brpcPortList);

    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    sql new File("""${context.file.parent}/../ddl/supplier_delete.sql""").text
    // create table if not exists
    sql new File("""${context.file.parent}/../ddl/${table}.sql""").text
    sql new File("""${context.file.parent}/../ddl/supplier.sql""").text

    sql """ TRUNCATE TABLE __internal_schema.cloud_cache_hotspot; """
    sleep(30000)

    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}",
        |"provider" = "${getS3Provider()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()
    
    

    def clearFileCache = { ip, port ->
        httpTest {
            endpoint ""
            uri ip + ":" + port + """/api/file_cache?op=clear&sync=true"""
            op "get"
            body ""
        }
    }

    def getMetricsMethod = { ip, port, check_func ->
        httpTest {
            endpoint ip + ":" + port
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }

    clearFileCache.call(ipList[0], httpPortList[0]);
    clearFileCache.call(ipList[1], httpPortList[1]);

    def load_customer_once =  { 
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = table + "_" + uniqueID
        // load data from cos
        def loadSql = new File("""${context.file.parent}/../ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
        sql loadSql

        // check load state
        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                break
            }
            sleep(5000)
        }
    }
    
    sql "use @regression_cluster_name0"
    def waitJobDone = { jobId ->
        int retryTime = 120
        int i = 0
        for (; i < retryTime; i++) {
            sleep(1000)
            def statuses = getJobState(jobId[0][0])
            if (statuses.any { it != null && it.equals("CANCELLED") }) {
                assertTrue(false);
            }
            if (statuses.any { it != null && it.equals("FINISHED") }) {
                break;
            }
        }
        if (i == retryTime) {
            sql "cancel warm up job where id = ${jobId[0][0]}"
            assertTrue(false);
        }
    }
    for (int k = 0; k < 10; k++) {
        load_customer_once()
        for (int i = 0; i < 10; i++) {
            sql "select count(*) from customer"
        }
        if (k % 2 == 1) {
            sleep(40000)
            def jobId_ = sql "WARM UP CLUSTER regression_cluster_name1 WITH CLUSTER regression_cluster_name0"
            waitJobDone(jobId_)
            sleep(30000)
            long ttl_cache_size = 0
            getMetricsMethod.call(ipList[0], brpcPortList[0]) {
                respCode, body ->
                    assertEquals("${respCode}".toString(), "200")
                    String out = "${body}".toString()
                    def strs = out.split('\n')
                    Boolean flag = false;
                    for (String line in strs) {
                        if (line.contains("ttl_cache_size")) {
                            if (line.startsWith("#")) {
                                continue
                            }
                            def i = line.indexOf(' ')
                            ttl_cache_size = line.substring(i).toLong()
                            flag = true
                            break
                        }
                    }
                    assertTrue(flag)
            }

            getMetricsMethod.call(ipList[1], brpcPortList[1]) {
                respCode, body ->
                    assertEquals("${respCode}".toString(), "200")
                    String out = "${body}".toString()
                    def strs = out.split('\n')
                    Boolean flag = false;
                    for (String line in strs) {
                        if (line.contains("ttl_cache_size")) {
                            if (line.startsWith("#")) {
                                continue
                            }
                            def i = line.indexOf(' ')
                            assertEquals(ttl_cache_size, line.substring(i).toLong())
                            flag = true
                            break
                        }
                    }
                    assertTrue(flag)
            }
            sql "use @regression_cluster_name0"
        }
    }
    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    sql new File("""${context.file.parent}/../ddl/supplier_delete.sql""").text
}   

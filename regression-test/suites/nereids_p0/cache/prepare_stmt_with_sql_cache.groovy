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

import org.apache.doris.regression.util.JdbcUtils

import java.sql.PreparedStatement
import java.sql.ResultSet

suite("prepare_stmt_with_sql_cache") {

    multi_sql """
        drop table if exists test_prepare_stmt_with_sql_cache;
        create table test_prepare_stmt_with_sql_cache(id int)
        distributed by hash(id)
        properties('replication_num'='1');
        
        insert into test_prepare_stmt_with_sql_cache select * from numbers('number'='100');
        """

    sql "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '10')"

    def db = (sql "select database()")[0][0].toString()

    def serverPrepareUrl = getServerPrepareJdbcUrl(context.config.jdbcUrl, db)

    connect(context.config.jdbcUser, context.config.jdbcPassword, serverPrepareUrl) {
        sql "set enable_sql_cache=true"
        for (def i in 0..<10) {
            try (PreparedStatement pstmt = prepareStatement("select * from test_prepare_stmt_with_sql_cache where id=?")) {
                pstmt.setInt(1, i)
                try (ResultSet rs = pstmt.executeQuery()) {
                    def result = JdbcUtils.toList(rs).v1
                    logger.info("result: {}", result)
                }
            }
        }
    }

    sleep(10 * 1000)

    connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
        sql "use ${db}"
        sql "set enable_sql_cache=true"
        test {
            sql "select * from test_prepare_stmt_with_sql_cache where id=10"
            result([[10]])
        }
    }

    connect(context.config.jdbcUser, context.config.jdbcPassword, serverPrepareUrl) {
        sql "use ${db}"
        sql "set enable_sql_cache=true"
        test {
            sql "select * from test_prepare_stmt_with_sql_cache where id=10"
            result(([[10]]))
        }
    }
}

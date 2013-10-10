/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.wasp.jdbc;

import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.client.FClient;
import com.alibaba.wasp.jdbcx.JdbcConnectionPool;
import com.alibaba.wasp.jdbcx.JdbcDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestJdbcConnectionPool extends TestJdbcBase {

  final Log LOG = LogFactory.getLog(getClass());

  private final static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  private static FClient client;
  public static final String TABLE_NAME = "test";
  public static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);

  private static JdbcConnectionPool pool;

  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("wasp.client.retries.number", 3);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.createTable(TABLE);
    TEST_UTIL.getWaspAdmin().disableTable(TABLE);
    client = new FClient(TEST_UTIL.getConfiguration());
    client.execute("create index test_index on " + TABLE_NAME + "(column3);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    TEST_UTIL.getWaspAdmin().enableTable(TABLE);

    Class.forName("com.alibaba.wasp.jdbc.Driver");

    conf = TEST_UTIL.getConfiguration();
    conf.setInt(FConstants.JDBC_POOL_MAX_CONNECTIONS, 3);

    pool = JdbcConnectionPool.create(new JdbcDataSource(conf), conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetConnection() throws SQLException {
    Connection conn = pool.getConnection();
    ResultSet rs;
    Statement stat = conn.createStatement();
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES (1, 79999, 'testGetConnection')");

    rs = stat
        .executeQuery("SELECT column1,column2 FROM test where column3='testGetConnection'");

    assertTrue(rs.next());
    assertTrue(rs.getLong("column2") == 79999);
    conn.close();
  }

  @Test
  public void testConnectionSize() throws SQLException {

    try {
      for (int i = 0; i < 5; i++) {
        Connection conn = pool.getConnection();
      }
    } catch (SQLException e) {
      assertTrue(e.getMessage().indexOf("get connection timeout") != -1);
    }

    pool = JdbcConnectionPool.create(new JdbcDataSource(conf), conf);
    for (int i = 0; i < 5; i++) {
      Connection conn = pool.getConnection();
      conn.close();
    }

  }

}

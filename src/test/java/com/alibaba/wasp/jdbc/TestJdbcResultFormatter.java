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
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.client.FClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestJdbcResultFormatter extends TestJdbcBase {

  final Log LOG = LogFactory.getLog(getClass());

  private static Connection conn;
  private static Statement stat;

  private final static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  private static FClient client;
  public static final String TABLE_NAME = "test";
  public static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("wasp.client.retries.number", 3);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.createTable(TABLE);
    TEST_UTIL.getWaspAdmin().disableTable(TABLE);
    client = new FClient(TEST_UTIL.getConfiguration());
    client.execute("create index test_index on " + TABLE_NAME + "(column3);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    client.execute("create index test_index_column2 on " + TABLE_NAME
        + "(column2);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    TEST_UTIL.getWaspAdmin().enableTable(TABLE);

    Class.forName("com.alibaba.wasp.jdbc.Driver");
    conn = getConnection("test", TEST_UTIL.getConfiguration());
    conn.setClientInfo(FConstants.READ_MODEL, ReadModel.CURRENT.name());
    stat = conn.createStatement();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    stat.close();
    conn.close();
  }

  @Test
  public void testQuery() throws IOException, SQLException {
    String INSERT = "Insert into " + TABLE_NAME
        + "(column1,column2,column3) values (123,456,'binlijin');";
    // Pair<String, List<ExecuteResult>> insertRet = client.execute(INSERT);
    stat.execute(INSERT);
    // assertTrue(insertRet.getSecond().size() == 1);

    assertTrue(stat.getUpdateCount() == 1);

    JdbcResultFormatter format = new JdbcResultFormatter(conn);
    String sql = "select column1,column2,column3 from " + TABLE_NAME
        + " where column1=123 and column2=456";
    format.format(stat, sql);
    // String s = format.format(stat, sql);
    // LOG.info("=================\n" + s + "\n=================");
    stat.close();
  }

}
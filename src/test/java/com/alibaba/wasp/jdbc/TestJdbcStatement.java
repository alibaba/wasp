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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.SQLErrorCode;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.client.WaspAdmin;
import com.alibaba.wasp.util.ResultInHBasePrinter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJdbcStatement extends TestJdbcBase {

  private static Connection conn;
  private static Configuration conf;
  private final static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  public static final String TABLE = "TEST";
  final Log LOG = LogFactory.getLog(getClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    WaspTestingUtility.adjustLogLevel();
    TEST_UTIL.getConfiguration().setBoolean("wasp.online.schema.update.enable",
        true);
    TEST_UTIL.getConfiguration().setInt("wasp.fserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("wasp.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("wasp.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean(
        "wasp.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(3);
    conf = TEST_UTIL.getConfiguration();
    conn = getConnection("statement", TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    conn.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testStatement() throws SQLException, IOException,
      InterruptedException {
    Statement stat = conn.createStatement();

    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());
    conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
    // ignored
    stat.setCursorName("x");
    // fixed return value
    assertEquals(stat.getFetchDirection(), ResultSet.FETCH_FORWARD);
    // ignored
    stat.setFetchDirection(ResultSet.FETCH_REVERSE);
    // ignored
    stat.setMaxFieldSize(100);

    assertEquals(conf.getInt(FConstants.WASP_JDBC_FETCHSIZE,
        FConstants.DEFAULT_WASP_JDBC_FETCHSIZE), stat.getFetchSize());
    stat.setFetchSize(10);
    assertEquals(10, stat.getFetchSize());
    stat.setFetchSize(0);
    assertEquals(conf.getInt(FConstants.WASP_JDBC_FETCHSIZE,
        FConstants.DEFAULT_WASP_JDBC_FETCHSIZE), stat.getFetchSize());
    assertEquals(ResultSet.TYPE_FORWARD_ONLY, stat.getResultSetType());
    Statement stat2 = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
        ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, stat2.getResultSetType());
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT,
        stat2.getResultSetHoldability());
    assertEquals(ResultSet.CONCUR_READ_ONLY, stat2.getResultSetConcurrency());
    assertEquals(0, stat.getMaxFieldSize());
    assertTrue(!((JdbcStatement) stat2).isClosed());
    stat2.close();
    assertTrue(((JdbcStatement) stat2).isClosed());

    ResultSet rs;
    int count;
    boolean result;

    stat.execute("CREATE TABLE TEST {REQUIRED INT64 ID;"
        + "REQUIRED STRING VALUE; }PRIMARY KEY(ID), "
        + "ENTITY GROUP ROOT,ENTITY GROUP KEY(ID);");

    TEST_UTIL.waitTableEnabled(Bytes.toBytes("TEST"), 5000);

    ResultInHBasePrinter.printMETA(conf, LOG);
    ResultInHBasePrinter.printFMETA(conf, LOG);
    ResultInHBasePrinter.printTable("test", "WASP_ENTITY_TEST", conf, LOG);

    conn.getTypeMap();

    // this method should not throw an exception - if not supported, this
    // calls are ignored

    assertEquals(ResultSet.CONCUR_READ_ONLY, stat.getResultSetConcurrency());

    // stat.cancel();
    stat.setQueryTimeout(10);
    assertTrue(stat.getQueryTimeout() == 10);
    stat.setQueryTimeout(0);
    assertTrue(stat.getQueryTimeout() == 0);
    // assertThrows(SQLErrorCode.INVALID_VALUE_2, stat).setQueryTimeout(-1);
    assertTrue(stat.getQueryTimeout() == 0);
    trace("executeUpdate");
    count = stat
        .executeUpdate("INSERT INTO TEST (ID,VALUE) VALUES (1,'Hello')");
    assertEquals(1, count);
    count = stat.executeUpdate("INSERT INTO TEST (VALUE,ID) VALUES ('JDBC',2)");
    assertEquals(1, count);
    count = stat.executeUpdate("UPDATE TEST SET VALUE='LDBC' WHERE ID=1");
    assertEquals(1, count);

    count = stat.executeUpdate("DELETE FROM TEST WHERE ID=-1");
    assertEquals(0, count);
    count = stat.executeUpdate("DELETE FROM TEST WHERE ID=1");
    assertEquals(1, count);
    count = stat.executeUpdate("DELETE FROM TEST WHERE ID=2");
    assertEquals(1, count);

    result = stat.execute("INSERT INTO TEST(ID,VALUE) VALUES(1,'Hello')");
    assertTrue(!result);
    result = stat.execute("INSERT INTO TEST(VALUE,ID) VALUES('JDBC',2)");
    assertTrue(!result);
    result = stat.execute("UPDATE TEST SET VALUE='LDBC' WHERE ID=2");
    assertTrue(!result);
    result = stat.execute("DELETE FROM TEST WHERE ID=1");
    assertTrue(!result);
    result = stat.execute("DELETE FROM TEST WHERE ID=2");
    assertTrue(!result);
    result = stat.execute("DELETE FROM TEST WHERE ID=3");
    assertTrue(!result);

    // getMoreResults
    rs = stat.executeQuery("SELECT ID,VALUE FROM TEST WHERE ID=1");
    assertFalse(stat.getMoreResults());
    assertThrows(SQLErrorCode.OBJECT_CLOSED, rs).next();
    assertTrue(stat.getUpdateCount() == -1);
    count = stat.executeUpdate("DELETE FROM TEST WHERE ID=1");
    assertFalse(stat.getMoreResults());
    assertTrue(stat.getUpdateCount() == -1);

    WaspAdmin admin = new WaspAdmin(TEST_UTIL.getConfiguration());
    admin.disableTable("TEST");
    stat.execute("DROP TABLE TEST");
    admin.waitTableNotLocked("TEST".getBytes());
    stat.executeUpdate("DROP TABLE IF EXISTS TEST");

    assertTrue(stat.getWarnings() == null);
    stat.clearWarnings();
    assertTrue(stat.getWarnings() == null);
    assertTrue(conn == stat.getConnection());

    admin.close();
    stat.close();
  }
}
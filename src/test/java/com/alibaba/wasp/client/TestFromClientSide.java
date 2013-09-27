/**
 *
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
package com.alibaba.wasp.client;

import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.fserver.OperationStatus;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.parser.druid.DruidParserTestUtil;
import com.alibaba.wasp.util.ResultInHBasePrinter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Run tests that use the Wasp clients; {@link FClient} and
 * {@link com.alibaba.wasp.jdbc.Driver}. Sets up the wasp mini cluster once at
 * start and runs through all client tests. Each creates a table named for the
 * method and does its stuff against that.
 */
public class TestFromClientSide {
  final Log LOG = LogFactory.getLog(getClass());

  private final static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  private static FClient client;

  public static final String TABLE_NAME = "TEST_TABLE";
  public static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);

  public static String sessionId;

  public static final String INSERT = "insert";

  public static final String SELECT_FOR_INSERT = "selectForInsert";

  public static final String UPDATE = "update";

  public static final String SELECT_FOR_UPDATE = "selectForUpdate";

  public static final String DELETE = "delete";

  public static final String SELECT_FOR_DELETE = "selectForDelete";

  public static final String INDEX_NAME = "test_index";

  public static final int fetchSize = 20;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    WaspTestingUtility.adjustLogLevel();
    TEST_UTIL.getConfiguration().setInt("wasp.client.retries.number", 3);
    TEST_UTIL.startMiniCluster(3);
    TableSchemaCacheReader.getInstance(TEST_UTIL.getConfiguration())
        .clearCache();
    TEST_UTIL.createTable(TABLE);
    TEST_UTIL.getWaspAdmin().disableTable(TABLE);
    client = new FClient(TEST_UTIL.getConfiguration());
    client.execute("create index " + INDEX_NAME + " on " + TABLE_NAME
        + "(column3);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    TEST_UTIL.getWaspAdmin().enableTable(TABLE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testCreateTable() throws IOException {
    String sql1 = "CREATE TABLE User2 {Required Int64 user_id; Required String name; } "
        + "primary key(user_id), entity group root, entity group key(user_id);";
    Pair<String, Pair<Boolean, List<ExecuteResult>>> pair = client.execute(
        sql1, ReadModel.SNAPSHOT, fetchSize);
    sessionId = pair.getFirst();
    assertTrue(pair.getSecond().getSecond().get(0).getStatus()
        .getOperationStatusCode() == OperationStatusCode.SUCCESS);
  }

  @Test
  public void testLoopCreateDropTable() throws IOException,
      InterruptedException {
    for (int i = 0; i < 3; i++) {
      String sql1 = "CREATE TABLE Test_Loop {Required Int64 user_id; Required String name; } "
          + "primary key(user_id), entity group root, entity group key(user_id);";
      Pair<String, Pair<Boolean, List<ExecuteResult>>> pair = client
          .execute(sql1);
      assertTrue(pair.getSecond().getSecond().get(0).getStatus()
          .getOperationStatusCode() == OperationStatusCode.SUCCESS);

      TEST_UTIL.waitTableEnabled(Bytes.toBytes("Test_Loop"), 50000);

      TEST_UTIL.getWaspAdmin().disableTable("Test_Loop");

      String sql2 = "DROP TABLE IF EXISTS Test_Loop";
      pair = client.execute(sql2);
      TEST_UTIL.getWaspAdmin().waitTableNotLocked("Test_Loop".getBytes());
      assertTrue(pair.getSecond().getSecond().get(0).getStatus()
          .getOperationStatusCode() == OperationStatusCode.SUCCESS);
      Thread.currentThread().sleep(5000);
    }
    String sql1 = "CREATE TABLE Test_Loop {Required Int64 user_id; Required String name; } "
        + "primary key(user_id), entity group root, entity group key(user_id);";
    Pair<String, Pair<Boolean, List<ExecuteResult>>> pair = client
        .execute(sql1);
    assertTrue(pair.getSecond().getSecond().get(0).getStatus()
        .getOperationStatusCode() == OperationStatusCode.SUCCESS);

    TEST_UTIL.waitTableEnabled(Bytes.toBytes("Test_Loop"), 50000);

    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT,
        "Insert into Test_Loop (user_id,name) values (1,'binlijin');");
    sqlGroup.put(SELECT_FOR_INSERT,
        "select user_id,name from Test_Loop where user_id=1;");
    testSqlGroupExecute(sqlGroup);
  }

  public static void waitTableEnabled(WaspAdmin admin, byte[] table,
      long timeoutMillis) throws InterruptedException, IOException {
    long startWait = System.currentTimeMillis();
    while (!admin.isTableEnabled(table)) {
      assertTrue("Timed out waiting for table " + Bytes.toStringBinary(table),
          System.currentTimeMillis() - startWait < timeoutMillis);
      Thread.sleep(200);
    }
  }

  @Test
  public void testCreateTableAndIndex() throws Exception {
    byte[][] table = { Bytes.toBytes("User"), Bytes.toBytes("Photo") };
    for (int i = 0; i < DruidParserTestUtil.SEED.length; i++) {
      String sql = DruidParserTestUtil.SEED[i];
      Pair<String, Pair<Boolean, List<ExecuteResult>>> pair = client.execute(
          sql, ReadModel.SNAPSHOT, fetchSize);
      sessionId = pair.getFirst();
      assertTrue(pair.getSecond().getSecond().get(0).getStatus()
          .getOperationStatusCode() == OperationStatusCode.SUCCESS);
      TEST_UTIL.waitTableAvailable(table[i]);
    }
    TEST_UTIL.getWaspAdmin().disableTable(table[1]);
    boolean isTableDisabled = TEST_UTIL.getWaspAdmin()
        .isTableDisabled(table[1]);
    LOG.info(" ---------------- Photo isTableDisabled=" + isTableDisabled);

    for (String sql : DruidParserTestUtil.INDEX_SEED) {
      Pair<String, Pair<Boolean, List<ExecuteResult>>> pair = client.execute(
          sql, ReadModel.SNAPSHOT, fetchSize);
      TEST_UTIL.getWaspAdmin().waitTableNotLocked("Photo".getBytes());
      sessionId = pair.getFirst();
      assertTrue(pair.getSecond().getSecond().get(0).getStatus()
          .getOperationStatusCode() == OperationStatusCode.SUCCESS);
    }
    TEST_UTIL.getWaspAdmin().enableTable(table[1]);
    boolean isTableEnabled = TEST_UTIL.getWaspAdmin().isTableEnabled(table[1]);
    LOG.info(" ---------------- Photo isTableEnabled=" + isTableEnabled);
  }

  @Test
  public void testExecute() throws IOException {
    Pair<String, Pair<Boolean, List<ExecuteResult>>> pair = client.execute(
        "select column1,column2,column3 from " + TABLE_NAME
            + " where column1=1 and column2=2", ReadModel.CURRENT, fetchSize);
    sessionId = pair.getFirst();
    assertTrue(pair.getSecond().getSecond().size() == 0);
  }

  /**
   * 
   */
  @Test
  public void testSqlExecute() throws IOException {
    String INSERT = "Insert into " + TABLE_NAME
        + "(column1,column2,column3) values (10,10,'binlijin');";
    Pair<String, Pair<Boolean, List<ExecuteResult>>> insertRet = client
        .execute(INSERT);
    assertTrue(insertRet.getSecond().getSecond().get(0).getStatus()
        .getOperationStatusCode() == OperationStatusCode.SUCCESS);
    printTablesRs("testSqlExecute");
  }

  @Test
  public void testGetSql() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT, "Insert into " + TABLE_NAME
        + "(column1,column2,column3) values (0,1,'binlijin');");
    sqlGroup.put(SELECT_FOR_INSERT, "select column1,column2,column3 from "
        + TABLE_NAME + " where column1=0 and column2=1;");
    testSqlGroupExecute(sqlGroup);
  }

  @Test
  public void testScanSql() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT, "Insert into " + TABLE_NAME
        + "(column1,column2,column3) values (1,0,'binlijin');");
    sqlGroup.put(SELECT_FOR_INSERT, "select column1,column2,column3 from "
        + TABLE_NAME + " where column1=1 and column3='binlijin';");
    testSqlGroupExecute(sqlGroup);
  }

  @Test
  public void testNormalSql() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT, "Insert into " + TABLE_NAME
        + "(column1,column2,column3) values (1,2,'binlijin');");

    sqlGroup.put(SELECT_FOR_INSERT, "select column1,column2,column3 from "
        + TABLE_NAME + " where column1=1 and column3='binlijin';");
    sqlGroup.put(UPDATE, "UPDATE " + TABLE_NAME
        + " SET column3 = 'daizhiyuan' WHERE column1 = 1 and column2=2;");
    sqlGroup.put(SELECT_FOR_UPDATE, "select column1,column2,column3 from "
        + TABLE_NAME + " where column1=1 and column3='daizhiyuan';");
    sqlGroup.put(DELETE, "delete from " + TABLE_NAME
        + " where column1=1 and column2=2;");
    sqlGroup.put(SELECT_FOR_DELETE, "select column1,column2,column3 from "
        + TABLE_NAME + " where column1=1 and column3='daizhiyuan';");
    testSqlGroupExecute(sqlGroup);
  }

  @Test
  public void testMutilColSql() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup
        .put(
            INSERT,
            "Insert into "
                + TABLE_NAME
                + " (column1,column2,column3,column4,column5) values (1,54321,'luoshi',98.2,9999.9999);");
    sqlGroup.put(SELECT_FOR_INSERT,
        "select column1,column2,column3,column4,column5 from " + TABLE_NAME
            + " where column1=1 and column3='luoshi';");
    sqlGroup.put(UPDATE, "UPDATE " + TABLE_NAME
        + " SET column3 = 'tianwu' WHERE column1=1 and column2=54321;");
    sqlGroup.put(SELECT_FOR_UPDATE,
        "select column1,column2,column3,column4,column5 from " + TABLE_NAME
            + " where column1=1 and column3='tianwu';");
    sqlGroup.put(DELETE, "delete from " + TABLE_NAME
        + " where column1=1 and column2=54321;");
    sqlGroup.put(SELECT_FOR_DELETE,
        "select column1,column2,column3,column4,column5 from " + TABLE_NAME
            + " where column1=1 and column3='tianwu';");
    testSqlGroupExecute(sqlGroup);
  }

  @Test
  public void testSpecharsValue() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT, "Insert into " + TABLE_NAME
        + " (column1,column2,column3) values (2,54321,'#luoshi');");
    sqlGroup.put(SELECT_FOR_INSERT, "select column1,column2,column3 from "
        + TABLE_NAME + " where column1=2 and column3='#luoshi';");
    testSqlGroupExecute(sqlGroup);
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testInvalidDatatype() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT, "Insert into " + TABLE_NAME
        + " (column1,column2,column3) values ('abc','def',123);");
    sqlGroup.put(SELECT_FOR_INSERT,
        "select column1,column2,column3,column4,column5 from " + TABLE_NAME
            + " where column1='abc' and column3=123;");
    testSqlGroupExecute(sqlGroup);
  }

  @Test(expected = IOException.class)
  public void testSelectNotUseIndex() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT, "Insert into " + TABLE_NAME
        + " (column1,column2,column3) values (3,234,'abc');");
    sqlGroup.put(SELECT_FOR_INSERT,
        "select column1,column2,column3,column4,column5 from " + TABLE_NAME
            + " where column2=234;");
    testSqlGroupExecute(sqlGroup);
  }

  @Test(expected = IOException.class)
  public void testUpdateNotOnlyPK() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT, "Insert into " + TABLE_NAME
        + " (column1,column2,column3,column4) values (4,234,'abc','bcd');");
    sqlGroup.put(SELECT_FOR_INSERT,
        "select column1,column2,column3,column4,column5 from " + TABLE_NAME
            + " where column1=4 and column2=234 and column3='abc';");
    sqlGroup
        .put(
            UPDATE,
            "UPDATE "
                + TABLE_NAME
                + " set column4='def' WHERE column1 = 4 and column2=234 and column4='bcd';");
    testSqlGroupExecute(sqlGroup);
  }

  @Test(expected = IOException.class)
  public void testUpdatePK() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT, "Insert into " + TABLE_NAME
        + " (column1,column2,column3) values (5,234,'abc');");
    sqlGroup.put(SELECT_FOR_INSERT,
        "select column1,column2,column3,column4,column5 from " + TABLE_NAME
            + " where column1=5 and column2=234 and column3='abc';");
    sqlGroup.put(UPDATE, "UPDATE " + TABLE_NAME
        + " set column1=6  WHERE column3='abc';");
    testSqlGroupExecute(sqlGroup);
  }

  @Test(expected = IOException.class)
  public void testDeleteNotOnlyPK() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT, "Insert into " + TABLE_NAME
        + " (column1,column2,column3) values (6,234,'abc');");
    sqlGroup.put(SELECT_FOR_INSERT,
        "select column1,column2,column3,column4,column5 from " + TABLE_NAME
            + " where column1=6 and column2=234");
    sqlGroup.put(DELETE, "DELETE FROM " + TABLE_NAME + " WHERE column3='abc';");
    sqlGroup.put(SELECT_FOR_DELETE,
        "select column1,column2,column3,column4,column5 from " + TABLE_NAME
            + " where column1=6 and column2=234;");
    testSqlGroupExecute(sqlGroup);
  }

  @Test
  // (expected = PrimaryKeyAlreadyExistsException.class)
  public void testInsertWithSomePK() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT, "Insert into " + TABLE_NAME
        + " (column1,column2,column3) values (7,234,'abc');");
    sqlGroup.put(SELECT_FOR_INSERT,
        "select column1,column2,column3,column4,column5 from " + TABLE_NAME
            + " where column1=7 and column3='abc';");
    testSqlGroupExecute(sqlGroup);

    sqlGroup.clear();
    String insert = "Insert into " + TABLE_NAME
        + " (column1,column2,column3) values (7,234,'def');";
    Pair<String, Pair<Boolean, List<ExecuteResult>>> insertRet = client
        .execute(insert);

    OperationStatus operationStatus = insertRet.getSecond().getSecond().get(0)
        .getStatus();
    assertTrue(operationStatus.getOperationStatusCode() == OperationStatusCode.FAILURE);
    assertTrue(operationStatus.getExceptionClassname().equals(
        "com.alibaba.wasp.PrimaryKeyAlreadyExistsException"));
  }

  private void testSqlGroupExecute(Map<String, String> sqlGroup)
      throws IOException {

    String insert = sqlGroup.get(INSERT);
    if (insert != null) {
      Pair<String, Pair<Boolean, List<ExecuteResult>>> insertRet = client
          .execute(insert);
      assertTrue(insertRet.getSecond().getSecond().get(0).getStatus()
          .getOperationStatusCode() == OperationStatusCode.SUCCESS);
      printTablesRs("after " + INSERT);
    }

    String selectForInsert = sqlGroup.get(SELECT_FOR_INSERT);
    // test select
    if (selectForInsert != null) {
      Pair<String, Pair<Boolean, List<ExecuteResult>>> r1 = client.execute(
          selectForInsert, ReadModel.CURRENT, fetchSize);
      assertTrue(r1.getSecond().getSecond().size() > 0);
    }

    // test update
    String update = sqlGroup.get(UPDATE);
    if (update != null) {
      Pair<String, Pair<Boolean, List<ExecuteResult>>> updateRet = client
          .execute(update);
      assertTrue(updateRet.getSecond().getSecond().get(0).getStatus()
          .getOperationStatusCode() == OperationStatusCode.SUCCESS);
      printTablesRs("after " + UPDATE);
    }

    // test select
    String selectForUpdate = sqlGroup.get(SELECT_FOR_UPDATE);
    if (selectForUpdate != null) {
      Pair<String, Pair<Boolean, List<ExecuteResult>>> r2 = client.execute(
          selectForUpdate, ReadModel.CURRENT, fetchSize);
      assertTrue(r2.getSecond().getSecond().size() > 0);
    }

    // test delete
    String delete = sqlGroup.get(DELETE);
    if (delete != null) {
      Pair<String, Pair<Boolean, List<ExecuteResult>>> deleteRet = client
          .execute(delete);
      assertTrue(deleteRet.getSecond().getSecond().get(0).getStatus()
          .getOperationStatusCode() == OperationStatusCode.SUCCESS);
      printTablesRs("after " + DELETE);
    }

    String selectForDelete = sqlGroup.get(SELECT_FOR_DELETE);
    if (selectForDelete != null) {
      Pair<String, Pair<Boolean, List<ExecuteResult>>> r3 = client.execute(
          selectForDelete, ReadModel.CURRENT, fetchSize);
      assertTrue(r3.getSecond().getSecond().size() == 0);
    }
  }

  private void printTablesRs(String type) throws IOException {
    ResultInHBasePrinter.printTablesRs(type, TEST_UTIL.getConfiguration(),
        TABLE_NAME, INDEX_NAME, LOG);
  }

  @Test
  public void testQueryNext() throws IOException {
    String INSERT = "Insert into " + TABLE_NAME
        + "(column1,column2,column3) values (21,1,'binlijin');";
    Pair<String, Pair<Boolean, List<ExecuteResult>>> insertRet = client
        .execute(INSERT);
    assertTrue(insertRet.getSecond().getSecond().get(0).getStatus()
        .getOperationStatusCode() == OperationStatusCode.SUCCESS);
    INSERT = "Insert into " + TABLE_NAME
        + "(column1,column2,column3) values (21,2,'binlijin');";
    insertRet = client.execute(INSERT);
    assertTrue(insertRet.getSecond().getSecond().get(0).getStatus()
        .getOperationStatusCode() == OperationStatusCode.SUCCESS);

    String selectForInsert = "select column1,column2,column3 from "
        + TABLE_NAME + " where column1=21 and column3='binlijin'";

    LOG.debug("************ first begin");
    Pair<String, Pair<Boolean, List<ExecuteResult>>> r1 = client.execute(
        selectForInsert, ReadModel.CURRENT, 1);
    assertTrue(r1.getSecond().getSecond().size() > 0);
    String sessionId = r1.getFirst();

    LOG.debug("************ second begin");
    Pair<String, Pair<Boolean, List<ExecuteResult>>> r2 = client
        .next(sessionId);
    assertTrue(r2.getSecond().getSecond().size() > 0);
  }

  @Test
  public void testUpdateNullValueColumn() throws IOException {
    Map<String, String> sqlGroup = new HashMap<String, String>();
    sqlGroup.put(INSERT, "Insert into " + TABLE_NAME
        + "(column1,column2,column3) values (1,2,'binlijin');");

    sqlGroup.put(SELECT_FOR_INSERT, "select column1,column2,column3 from " + TABLE_NAME
        + " where column1=1 and column3='binlijin';");
    sqlGroup.put(UPDATE, "UPDATE " + TABLE_NAME
        + " SET column8 = 'aaabbb' WHERE column1 = 1 and column2=2;");
    sqlGroup.put(SELECT_FOR_UPDATE, "select column1,column2,column3,column8 from " + TABLE_NAME
        + " where column1=1 and column2=2;");
    sqlGroup.put(DELETE, "delete from " + TABLE_NAME + " where column1=1 and column2=2;");
    sqlGroup.put(SELECT_FOR_DELETE, "select column1,column2,column3,column8 from " + TABLE_NAME
        + " where column1=1 and column2=2;");
    testSqlGroupExecute(sqlGroup);
  }

}

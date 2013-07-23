/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.wasp.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.DataType;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.FConstants;
import org.apache.wasp.FieldKeyWord;
import org.apache.wasp.NotServingEntityGroupException;
import org.apache.wasp.ServerName;
import org.apache.wasp.TableExistsException;
import org.apache.wasp.TableNotDisabledException;
import org.apache.wasp.TableNotEnabledException;
import org.apache.wasp.TableNotFoundException;
import org.apache.wasp.UnknownEntityGroupException;
import org.apache.wasp.WaspTestingUtility;
import org.apache.wasp.executor.EventHandler;
import org.apache.wasp.executor.EventHandler.EventType;
import org.apache.wasp.executor.ExecutorService;
import org.apache.wasp.fserver.EntityGroup;
import org.apache.wasp.fserver.FServer;
import org.apache.wasp.master.FMasterServices;
import org.apache.wasp.meta.FMetaTestUtil;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.meta.Field;
import org.apache.wasp.zookeeper.ZKTable;
import org.apache.wasp.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Class to test WaspAdmin. Spins up the minicluster once at test start and then
 * takes it down afterward. Add any testing of WaspAdmin functionality here.
 */
public class TestAdmin {
  final Log LOG = LogFactory.getLog(getClass());
  private final static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  private static WaspAdmin admin;
  public static final String TABLE_NAME = "TEST_TABLE";

  public static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("wasp.online.schema.update.enable",
        true);
    TEST_UTIL.getConfiguration().set(FConstants.REDO_IMPL,
        "org.apache.wasp.fserver.redo.MemRedoLog");
    TEST_UTIL.getConfiguration().setInt("wasp.fserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("wasp.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("wasp.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean(
        "wasp.master.enabletable.roundrobin", true);
    TEST_UTIL.getConfiguration().setBoolean("wasp.ipc.server.blacklist.enable",
        true);
    TEST_UTIL.startMiniCluster(3);
    admin = TEST_UTIL.getWaspAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSplitUnknownTable() throws InterruptedException {
    final String unknowntable = "fubar";
    Exception exception = null;
    try {
      admin.split(unknowntable, "1234");
    } catch (IOException e) {
      exception = e;
    }
    assertTrue(exception instanceof TableNotFoundException);
  }

  @Test
  public void testDisableAndEnableTable() throws IOException {
    final byte[] table = Bytes.toBytes("testDisableAndEnableTable");
    TEST_UTIL.createTable(table);
    admin.disableTable(table);
    assertTrue("Table must be disabled.",
        TEST_UTIL.getWaspCluster().getMaster().getAssignmentManager()
            .getZKTable().isDisabledTable(Bytes.toString(table)));

    admin.enableTable(table);
    assertTrue("Table must be enabled.",
        TEST_UTIL.getWaspCluster().getMaster().getAssignmentManager()
            .getZKTable().isEnabledTable(Bytes.toString(table)));
  }

  @Test
  public void testDisableAndEnableTables() throws IOException {
    final byte[] table1 = Bytes.toBytes("testDisableAndEnableTable1");
    final byte[] table2 = Bytes.toBytes("testDisableAndEnableTable2");
    TEST_UTIL.createTable(table1);
    TEST_UTIL.createTable(table2);
    admin.disableTables("testDisableAndEnableTable.*");

    assertTrue("Table must be disabled.",
        TEST_UTIL.getWaspCluster().getMaster().getAssignmentManager()
            .getZKTable().isDisabledTable(Bytes.toString(table1)));
    assertTrue("Table must be disabled.",
        TEST_UTIL.getWaspCluster().getMaster().getAssignmentManager()
            .getZKTable().isDisabledTable(Bytes.toString(table2)));

    admin.enableTables("testDisableAndEnableTable.*");

    assertTrue("Table must be enabled.",
        TEST_UTIL.getWaspCluster().getMaster().getAssignmentManager()
            .getZKTable().isEnabledTable(Bytes.toString(table1)));

    assertTrue("Table must be enabled.",
        TEST_UTIL.getWaspCluster().getMaster().getAssignmentManager()
            .getZKTable().isEnabledTable(Bytes.toString(table2)));
  }

  @Test
  public void testCreateTable() throws IOException {
    FTable[] tables = admin.listTables();
    int numTables = tables.length;
    TEST_UTIL.createTable(Bytes.toBytes("testCreateTable"));
    tables = admin.listTables();
    assertEquals(numTables + 1, tables.length);
    assertTrue("Table must be enabled.", TEST_UTIL.getWaspCluster().getMaster()
        .getAssignmentManager().getZKTable().isEnabledTable("testCreateTable"));
  }

  /**
   * Verify schema modification takes.
   * 
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  @Test
  public void testOnlineChangeTableSchema() throws IOException,
      InterruptedException {

  }

  // not pass
  @Test
  public void testShouldFailOnlineSchemaUpdateIfOnlineSchemaIsNotEnabled()
      throws Exception {
    final byte[] tableName = Bytes.toBytes("changeTableSchemaOnlineFailure");
    TEST_UTIL.getMiniWaspCluster().getMaster().getConfiguration()
        .setBoolean("wasp.online.schema.update.enable", false);
    FTable[] tables = admin.listTables();
    int numTables = tables.length;
    TEST_UTIL.createTable(tableName);
    tables = admin.listTables();
    assertEquals(numTables + 1, tables.length);

    // FIRST, do FTable changes.
    FTable fTable = admin.getTableDescriptor(tableName);
    // Make a copy and assert copy is good.
    FTable copy = FMetaTestUtil.makeTable(fTable.getTableName());
    assertTrue(fTable.equals(copy));

    boolean expectedException = false;
    try {
      modifyTable(tableName, copy);
    } catch (TableNotDisabledException re) {
      expectedException = true;
    }
    assertTrue("Online schema update should not happen.", expectedException);
  }

  /**
   * Modify table is async so wait on completion of the table operation in
   * master.
   * 
   * @param tableName
   * @param htd
   * @throws java.io.IOException
   */
  private void modifyTable(final byte[] tableName, final FTable htd)
      throws IOException {
    FMasterServices services = TEST_UTIL.getMiniWaspCluster().getMaster();
    ExecutorService executor = services.getExecutorService();
    AtomicBoolean done = new AtomicBoolean(false);
    executor.registerListener(EventType.C_M_MODIFY_TABLE,
        new DoneListener(done));
    admin.modifyTable(tableName, htd);
    while (!done.get()) {
      synchronized (done) {
        try {
          done.wait(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    executor.unregisterListener(EventType.C_M_MODIFY_TABLE);
  }

  /**
   * Listens for when an event is done in Master.
   */
  static class DoneListener implements EventHandler.EventHandlerListener {
    private final AtomicBoolean done;

    DoneListener(final AtomicBoolean done) {
      super();
      this.done = done;
    }

    @Override
    public void afterProcess(EventHandler event) {
      this.done.set(true);
      synchronized (this.done) {
        // Wake anyone waiting on this value to change.
        this.done.notifyAll();
      }
    }

    @Override
    public void beforeProcess(EventHandler event) {
      // continue
    }
  }

  @Test
  public void testCreateTableWithEntityGroups() throws IOException,
      InterruptedException {

    byte[] tableName = Bytes.toBytes("testCreateTableWithEntityGroups");

    byte[][] splitKeys = { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 },
        new byte[] { 3, 3, 3 }, new byte[] { 4, 4, 4 }, new byte[] { 5, 5, 5 },
        new byte[] { 6, 6, 6 }, new byte[] { 7, 7, 7 }, new byte[] { 8, 8, 8 },
        new byte[] { 9, 9, 9 }, };
    int expectedEntityGroups = splitKeys.length + 1;

    FTable desc = FMetaTestUtil.makeTable(Bytes.toString(tableName));
    admin.createTable(desc, splitKeys);

    Map<EntityGroupInfo, ServerName> entityGroups = admin
        .getEntityGroupLocations(tableName);
    assertEquals("Tried to create " + expectedEntityGroups + " entityGroups "
        + "but only found " + entityGroups.size(), expectedEntityGroups,
        entityGroups.size());
    System.err.println("Found " + entityGroups.size() + " entityGroups");

    Iterator<EntityGroupInfo> egis = entityGroups.keySet().iterator();
    EntityGroupInfo egi = egis.next();
    assertTrue(egi.getStartKey() == null || egi.getStartKey().length == 0);
    assertTrue(Bytes.equals(egi.getEndKey(), splitKeys[0]));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), splitKeys[0]));
    assertTrue(Bytes.equals(egi.getEndKey(), splitKeys[1]));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), splitKeys[1]));
    assertTrue(Bytes.equals(egi.getEndKey(), splitKeys[2]));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), splitKeys[2]));
    assertTrue(Bytes.equals(egi.getEndKey(), splitKeys[3]));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), splitKeys[3]));
    assertTrue(Bytes.equals(egi.getEndKey(), splitKeys[4]));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), splitKeys[4]));
    assertTrue(Bytes.equals(egi.getEndKey(), splitKeys[5]));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), splitKeys[5]));
    assertTrue(Bytes.equals(egi.getEndKey(), splitKeys[6]));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), splitKeys[6]));
    assertTrue(Bytes.equals(egi.getEndKey(), splitKeys[7]));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), splitKeys[7]));
    assertTrue(Bytes.equals(egi.getEndKey(), splitKeys[8]));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), splitKeys[8]));
    assertTrue(egi.getEndKey() == null || egi.getEndKey().length == 0);

    // Now test using start/end with a number of entityGroups

    // Use 80 bit numbers to make sure we aren't limited
    byte[] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    byte[] endKey = { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };

    // Splitting into 10 entityGroups, we expect (null,1) ... (9, null)
    // with (1,2) (2,3) (3,4) (4,5) (5,6) (6,7) (7,8) (8,9) in the middle

    expectedEntityGroups = 10;

    byte[] TABLE_2 = Bytes.add(tableName, Bytes.toBytes("_2"));

    desc = FMetaTestUtil.makeTable(Bytes.toString(TABLE_2));
    admin = new WaspAdmin(TEST_UTIL.getConfiguration());
    admin.createTable(desc, startKey, endKey, expectedEntityGroups);

    entityGroups = admin.getEntityGroupLocations(TABLE_2);
    assertEquals("Tried to create " + expectedEntityGroups + " entityGroups "
        + "but only found " + entityGroups.size(), expectedEntityGroups,
        entityGroups.size());
    System.err.println("Found " + entityGroups.size() + " entityGroups");

    egis = entityGroups.keySet().iterator();
    egi = egis.next();
    assertTrue(egi.getStartKey() == null || egi.getStartKey().length == 0);
    assertTrue(Bytes.equals(egi.getEndKey(), new byte[] { 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1 }));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), new byte[] { 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1 }));
    assertTrue(Bytes.equals(egi.getEndKey(), new byte[] { 2, 2, 2, 2, 2, 2, 2,
        2, 2, 2 }));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), new byte[] { 2, 2, 2, 2, 2, 2,
        2, 2, 2, 2 }));
    assertTrue(Bytes.equals(egi.getEndKey(), new byte[] { 3, 3, 3, 3, 3, 3, 3,
        3, 3, 3 }));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), new byte[] { 3, 3, 3, 3, 3, 3,
        3, 3, 3, 3 }));
    assertTrue(Bytes.equals(egi.getEndKey(), new byte[] { 4, 4, 4, 4, 4, 4, 4,
        4, 4, 4 }));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), new byte[] { 4, 4, 4, 4, 4, 4,
        4, 4, 4, 4 }));
    assertTrue(Bytes.equals(egi.getEndKey(), new byte[] { 5, 5, 5, 5, 5, 5, 5,
        5, 5, 5 }));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), new byte[] { 5, 5, 5, 5, 5, 5,
        5, 5, 5, 5 }));
    assertTrue(Bytes.equals(egi.getEndKey(), new byte[] { 6, 6, 6, 6, 6, 6, 6,
        6, 6, 6 }));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), new byte[] { 6, 6, 6, 6, 6, 6,
        6, 6, 6, 6 }));
    assertTrue(Bytes.equals(egi.getEndKey(), new byte[] { 7, 7, 7, 7, 7, 7, 7,
        7, 7, 7 }));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), new byte[] { 7, 7, 7, 7, 7, 7,
        7, 7, 7, 7 }));
    assertTrue(Bytes.equals(egi.getEndKey(), new byte[] { 8, 8, 8, 8, 8, 8, 8,
        8, 8, 8 }));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), new byte[] { 8, 8, 8, 8, 8, 8,
        8, 8, 8, 8 }));
    assertTrue(Bytes.equals(egi.getEndKey(), new byte[] { 9, 9, 9, 9, 9, 9, 9,
        9, 9, 9 }));
    egi = egis.next();
    assertTrue(Bytes.equals(egi.getStartKey(), new byte[] { 9, 9, 9, 9, 9, 9,
        9, 9, 9, 9 }));
    assertTrue(egi.getEndKey() == null || egi.getEndKey().length == 0);

    // Try once more with something that divides into something infinite

    startKey = new byte[] { 0, 0, 0, 0, 0, 0 };
    endKey = new byte[] { 1, 0, 0, 0, 0, 0 };

    expectedEntityGroups = 5;

    byte[] TABLE_3 = Bytes.add(tableName, Bytes.toBytes("_3"));

    desc = FMetaTestUtil.makeTable(Bytes.toString(TABLE_3));
    admin = new WaspAdmin(TEST_UTIL.getConfiguration());
    admin.createTable(desc, startKey, endKey, expectedEntityGroups);

    entityGroups = admin.getEntityGroupLocations(TABLE_3);
    assertEquals("Tried to create " + expectedEntityGroups + " entityGroups "
        + "but only found " + entityGroups.size(), expectedEntityGroups,
        entityGroups.size());
    System.err.println("Found " + entityGroups.size() + " entityGroups");

    // Try an invalid case where there are duplicate split keys
    splitKeys = new byte[][] { new byte[] { 1, 1, 1 }, new byte[] { 2, 2, 2 },
        new byte[] { 3, 3, 3 }, new byte[] { 2, 2, 2 } };

    byte[] TABLE_4 = Bytes.add(tableName, Bytes.toBytes("_4"));
    desc = FMetaTestUtil.makeTable(Bytes.toString(TABLE_4));
    WaspAdmin ladmin = new WaspAdmin(TEST_UTIL.getConfiguration());
    try {
      ladmin.createTable(desc, splitKeys);
      assertTrue("Should not be able to create this table because of "
          + "duplicate split keys", false);
    } catch (IllegalArgumentException iae) {
      // Expected
    }
    ladmin.close();
  }

  @Test
  public void testCreateTableWithOnlyEmptyStartRow() throws IOException {
    byte[][] splitKeys = new byte[1][];
    splitKeys[0] = FConstants.EMPTY_BYTE_ARRAY;
    FTable desc = FMetaTestUtil
        .makeTable("testCreateTableWithOnlyEmptyStartRow");
    try {
      admin.createTable(desc, splitKeys);
      fail("Test case should fail as empty split key is passed.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testCreateTableWithEmptyRowInTheSplitKeys() throws IOException {
    byte[][] splitKeys = new byte[3][];
    splitKeys[0] = "entityGroup1".getBytes();
    splitKeys[1] = FConstants.EMPTY_BYTE_ARRAY;
    splitKeys[2] = "entityGroup2".getBytes();
    FTable desc = FMetaTestUtil
        .makeTable("testCreateTableWithEmptyRowInTheSplitKeys");
    try {
      admin.createTable(desc, splitKeys);
      fail("Test case should fail as empty split key is passed.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testTableExist() throws IOException {
    final byte[] table = Bytes.toBytes("testTableExist");
    boolean exist = false;
    exist = admin.tableExists(table);
    assertEquals(false, exist);
    TEST_UTIL.createTable(table);
    exist = admin.tableExists(table);
    assertEquals(true, exist);
  }

  /**
   * 
   * @throws java.io.IOException
   */
  @Test(expected = IllegalArgumentException.class)
  public void testEmptyFTable() throws IOException {
    admin.createTable(new FTable());
  }

  /**
   * 
   * @throws Exception
   */
  @Test
  public void testTableNameClash() throws Exception {
    String name = "testTableNameClash";
    admin.createTable(FMetaTestUtil.makeTable(name + "SOMEUPPERCASE"));
    admin.createTable(FMetaTestUtil.makeTable(name));
    // Before fix, below would fail throwing a NoServerForEntityGroupException.
  }

  /**
   * 
   * @throws java.io.IOException
   */
  @Test(expected = TableExistsException.class)
  public void testTableExistsExceptionWithATable() throws IOException {
    final byte[] name = Bytes.toBytes("testTableExistsExceptionWithATable");
    TEST_UTIL.createTable(name);
    TEST_UTIL.createTable(name);
  }

  /**
   * Can't disable a table if the table isn't in enabled state
   * 
   * @throws java.io.IOException
   */
  @Test(expected = TableNotEnabledException.class)
  public void testTableNotEnabledExceptionWithATable() throws IOException {
    final byte[] name = Bytes.toBytes("testTableNotEnabledExceptionWithATable");
    TEST_UTIL.createTable(name);
    admin.disableTable(name);
    admin.disableTable(name);
  }

  /**
   * Can't enable a table if the table isn't in disabled state
   * 
   * @throws java.io.IOException
   */
  // @Test(expected = TableNotDisabledException.class)
  @Test
  public void testTableNotDisabledExceptionWithATable() throws IOException {
    try {
      final byte[] name = Bytes
          .toBytes("testTableNotDisabledExceptionWithATable");
      TEST_UTIL.createTable(name);
      admin.enableTable(name);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // not pass
  @Test
  public void testShouldCloseTheEntityGroupBasedOnTheEncodedEntityGroupName()
      throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestWASPCloseEntityGroup");
    createTableWithDefaultConf(TABLENAME);
    EntityGroup eg = null;
    FServer fs = TEST_UTIL.getFSForFirstEntityGroupInTable(TABLENAME);
    for (EntityGroup entityGroup : fs.getOnlineEntityGroups()) {
      eg = entityGroup;
      admin.closeEntityGroupWithEncodedEntityGroupName(entityGroup
          .getEntityGroupInfo().getEncodedName(), fs.getServerName()
          .getServerName());
    }
    Thread.sleep(1000);
    assertFalse(
        "The entityGroup should not be present in online entityGroups list.",
        fs.getOnlineEntityGroups().contains(eg));
  }

  //
  @Test
  public void testCloseEntityGroupIfInvalidEntityGroupNameIsPassed()
      throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestWASPCloseEntityGroup1");
    createTableWithDefaultConf(TABLENAME);
    EntityGroup eg = null;
    FServer fs = TEST_UTIL.getFSForFirstEntityGroupInTable(TABLENAME);
    for (EntityGroup entityGroup : fs.getOnlineEntityGroups()) {
      if (entityGroup.getEntityGroupNameAsString().contains(
          "TestWASPCloseEntityGroup1")) {
        eg = entityGroup;
        try {
          admin.closeEntityGroup("sample", fs.getServerName().getServerName());
        } catch (UnknownEntityGroupException nsege) {
          // expected, ignore it
        }
      }
    }
    assertTrue(
        "The entityGroup should be present in online entityGroups list.", fs
            .getOnlineEntityGroups().contains(eg));
  }

  @Test(timeout = 36000)
  public void testEnableDisableAddColumnDeleteColumn() throws Exception {
    ZooKeeperWatcher zkw = WaspTestingUtility.getZooKeeperWatcher(TEST_UTIL);
    byte[] tableName = Bytes.toBytes("testMasterAdmin");
    TEST_UTIL.createTable(tableName);
    ZKTable zkTable = new ZKTable(zkw);
    while (!zkTable.isEnabledTable("testMasterAdmin")) {
      Thread.sleep(10);
    }
    admin.disableTable(tableName);
    admin.addColumn(tableName, new Field(FieldKeyWord.REQUIRED, "cf", "col2",
        DataType.STRING, null));
    admin.enableTable(tableName);
    try {
      admin.deleteColumn(tableName, Bytes.toBytes("col2"));
    } catch (TableNotDisabledException e) {
      LOG.info(e);
    }
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Test
  public void testCloseEntityGroupThatFetchesTheEGIFromMeta() throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestWASPCloseEntityGroup2");
    createTableWithDefaultConf(TABLENAME);
    EntityGroup eg = null;
    FServer fs = TEST_UTIL.getFSForFirstEntityGroupInTable(TABLENAME);
    for (EntityGroup entityGroup : fs.getOnlineEntityGroups()) {
      if (entityGroup.getEntityGroupNameAsString().contains(
          "TestWASPCloseEntityGroup2")) {
        eg = entityGroup;
        admin.closeEntityGroup(entityGroup.getEntityGroupNameAsString(), fs
            .getServerName().getServerName());
      }
    }
    boolean isInList = fs.getOnlineEntityGroups().contains(eg);
    long timeout = System.currentTimeMillis() + 2000;
    while ((System.currentTimeMillis() < timeout) && (isInList)) {
      Thread.sleep(100);
      isInList = fs.getOnlineEntityGroups().contains(eg);
    }
    assertFalse(
        "The EntityGroup should not be present in online EntityGroups list.",
        isInList);
  }

  @Test
  public void testCloseEntityGroupWhenServerNameIsEmpty() throws Exception {
    byte[] TABLENAME = Bytes
        .toBytes("TestWASPCloseEntityGroupWhenServerNameIsEmpty");
    createTableWithDefaultConf(TABLENAME);
    FServer fs = TEST_UTIL.getFSForFirstEntityGroupInTable(TABLENAME);
    try {
      for (EntityGroup entityGroup : fs.getOnlineEntityGroups()) {
        if (entityGroup.getEntityGroupNameAsString().contains(
            "TestWASPCloseEntityGroupWhenServerNameIsEmpty")) {
          admin.closeEntityGroup(entityGroup.getEntityGroupInfo()
              .getEncodedName(), " ");
        }
      }
      fail("The test should throw exception if the servername passed is empty.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testCloseEntityGroupWhenEncodedEntityGroupNameIsNotGiven()
      throws Exception {
    byte[] TABLENAME = Bytes.toBytes("TestWASPCloseEntityGroup4");
    createTableWithDefaultConf(TABLENAME);
    EntityGroup eg = null;
    FServer fs = TEST_UTIL.getFSForFirstEntityGroupInTable(TABLENAME);

    for (EntityGroup entityGroup : fs.getOnlineEntityGroups()) {
      if (entityGroup.getEntityGroupNameAsString().contains(
          "TestWASPCloseEntityGroup4")) {
        eg = entityGroup;
        try{
          admin.closeEntityGroupWithEncodedEntityGroupName(entityGroup.getEntityGroupNameAsString(), fs
              .getServerName().getServerName());
        } catch (NotServingEntityGroupException nseg) {
          // expected, ignore it.
        }
      }
    }
    assertTrue(
        "The entityGroup should be present in online entityGroups list.", fs
            .getOnlineEntityGroups().contains(eg));
  }

  private void createTableWithDefaultConf(byte[] TABLENAME) throws IOException {
    TEST_UTIL.createTable(TABLENAME);
  }

  /**
   * 
   * @throws java.io.IOException
   */
  @Test
  public void testGetTableEntityGroups() throws IOException {
    byte[] tableName = Bytes.toBytes("testGetTableEntityGroups");
    int expectedEntityGroups = 10;
    // Use 80 bit numbers to make sure we aren't limited
    byte[] startKey = { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
    byte[] endKey = { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 };
    FTable desc = FMetaTestUtil.makeTable("testGetTableEntityGroups");
    admin.createTable(desc, startKey, endKey, expectedEntityGroups);
    List<EntityGroupInfo> egis = admin.getTableEntityGroups(tableName);
    assertEquals("Tried to create " + expectedEntityGroups + " EntityGroups "
        + "but only found " + egis.size(), expectedEntityGroups, egis.size());
  }

  /**
   * checkWaspAvailable() doesn't close zk connections
   */
  @Test
  public void testCheckWaspAvailableClosesConnection() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    int initialCount = TestFConnectionUtility.getConnectionCount();
    WaspAdmin.checkWaspAvailable(conf);
    int finalCount = TestFConnectionUtility.getConnectionCount();
    Assert.assertEquals(initialCount, finalCount);
  }

  /**
   * Check if table locked
   * 
   * @throws java.io.IOException
   */
  @Test
  public void testTableLocked() throws IOException {
    try {
      final byte[] name = Bytes.toBytes("testTableLocked");
      TEST_UTIL.createTable(name);
      admin.isTableLocked(name);
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }
}
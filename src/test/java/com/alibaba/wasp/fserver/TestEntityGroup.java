/**
 * Copyright 2010 The Apache Software Foundation
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
package com.alibaba.wasp.fserver;

import com.alibaba.wasp.DataType;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FieldKeyWord;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.meta.FMetaEditor;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Field;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

import static com.alibaba.wasp.FConstants.UTF8_ENCODING;

/**
 * Basic stand-alone testing of EntityGroup.
 * 
 * A lot of the meta information for an EntityGroup now lives inside other
 * EntityGroups or in the FMaster, so only basic testing is possible.
 */
public class TestEntityGroup {
  // Do not spin up clusters in here. If you need to spin up a cluster, do it
  // over in TestEntityGroupOnCluster.
  static final Log LOG = LogFactory.getLog(TestEntityGroup.class);

  EntityGroup entityGroup = null;
  private static final WaspTestingUtility TEST_UTIL = new WaspTestingUtility();

  // FMetaTestUtil
  public static String CF = "default";
  private Field field1 = new Field(FieldKeyWord.REQUIRED, CF, "user_id",
      DataType.INT64, "none");
  private Field field2 = new Field(FieldKeyWord.REQUIRED, CF, "name",
      DataType.STRING, "none");
  private Field field3 = new Field(FieldKeyWord.REQUIRED, CF, "time",
      DataType.INT64, "none");

  private static final char FIRST_CHAR = 'a';
  private static final char LAST_CHAR = 'z';
  private static final byte[] START_KEY_BYTES = { FIRST_CHAR, FIRST_CHAR,
      FIRST_CHAR };

  private static String START_KEY;

  // Test names
  protected final byte[] tableName = Bytes.toBytes("testtable");;
  protected final byte[] qual1 = Bytes.toBytes("qual1");
  protected final byte[] qual2 = Bytes.toBytes("qual2");
  protected final byte[] qual3 = Bytes.toBytes("qual3");
  protected final byte[] value1 = Bytes.toBytes("value1");
  protected final byte[] value2 = Bytes.toBytes("value2");
  protected final byte[] row = Bytes.toBytes("rowA");
  protected final byte[] row2 = Bytes.toBytes("rowB");


  /**
   * @see org.apache.hadoop.hbase.HBaseTestCase#setUp()
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    START_KEY = new String(START_KEY_BYTES, UTF8_ENCODING);
    WaspTestingUtility.adjustLogLevel();
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    EnvironmentEdgeManagerTestHelper.reset();
  }

  void print(String message) {
    System.out.println("\n\n\n" + message + "\n\n\n");
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Merge test
  // ////////////////////////////////////////////////////////////////////////////
  @Test
  public void testMerge() throws IOException {
    byte[] tableName = Bytes.toBytes("testtable");
    List<Field> fileds = Arrays.asList(field1, field2, field3);
    Configuration hc = TEST_UTIL.getConfiguration(); // initSplit();
    // Setting up entityGroup
    String method = "testMerge";
    this.entityGroup = initEntityGroup(tableName, method, hc, fileds);
    try {
      entityGroup.internalCommitTransaction();

      byte[] sp = Bytes.toBytes(99999999);
      entityGroup.forceSplit(sp);
      byte[] splitRow = entityGroup.checkSplit();
      Assert.assertNotNull(splitRow);
      LOG.info("SplitRow: " + Bytes.toString(splitRow));
      EntityGroup[] subentityGroups = splitEntityGroup(entityGroup, splitRow);
      try {
        // Need to open the entityGroups.
        for (int i = 0; i < subentityGroups.length; i++) {
          openClosedEntityGroup(subentityGroups[i]);
        }
        long startTime = System.currentTimeMillis();
        entityGroup = EntityGroup.mergeAdjacent(subentityGroups[0],
            subentityGroups[1]);
        LOG.info("Merge entityGroups elapsed time: "
            + ((System.currentTimeMillis() - startTime) / 1000.0));
        FMetaEditor.deleteEntityGroup(TEST_UTIL.getConfiguration(),
            subentityGroups[0].getEntityGroupInfo());
        FMetaEditor.deleteEntityGroup(TEST_UTIL.getConfiguration(),
            subentityGroups[1].getEntityGroupInfo());
        LOG.info("merge completed.");
      } finally {
        for (int i = 0; i < subentityGroups.length; i++) {
          try {
            subentityGroups[i].close();
            subentityGroups[i].getLog().close();
          } catch (IOException e) {
            // Ignore.
          }
        }
      }
    } finally {
      EntityGroup.closeEntityGroup(this.entityGroup);
      this.entityGroup = null;
    }
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Split test
  // ////////////////////////////////////////////////////////////////////////////
  /**
   * Splits twice and verifies getting from each of the split entityGroups.
   * 
   * @throws Exception
   */
  @Test
  public void testBasicSplit() throws Exception {
    byte[] tableName = Bytes.toBytes("testtable");
    List<Field> fileds = Arrays.asList(field1, field2, field3);

    Configuration hc = TEST_UTIL.getConfiguration(); // initSplit();
    // Setting up entityGroup
    String method = "testBasicSplit";
    this.entityGroup = initEntityGroup(tableName, method, hc, fileds);

    try {
      entityGroup.internalCommitTransaction();

      byte[] sp = Bytes.toBytes("hijklmn");
      entityGroup.forceSplit(sp);
      byte[] splitRow = entityGroup.checkSplit();
      Assert.assertNotNull(splitRow);
      LOG.info("SplitRow: " + Bytes.toString(splitRow));
      EntityGroup[] entityGroups = splitEntityGroup(entityGroup, splitRow);
      try {
        // Need to open the entityGroups.
        // TODO: Add an 'open' to EntityGroup... don't do open by constructing
        // instance.
        for (int i = 0; i < entityGroups.length; i++) {
          openClosedEntityGroup(entityGroups[i]);
          // entityGroups[i] = openClosedEntityGroup(entityGroups[i]);
        }
        // Assert can get rows out of new entityGroups. Should be able to get
        // first
        // row from first entityGroup and the midkey from second entityGroup.
        // assertGet(entityGroups[0], field3, Bytes.toBytes(START_KEY));
        // assertGet(entityGroups[1], field3, splitRow);
        // Test I can get scanner and that it starts at right place.
        // assertScan(entityGroups[0], field3, Bytes.toBytes(START_KEY));
        // assertScan(entityGroups[1], field3, splitRow);
        // Now prove can't split entityGroups that have references.
        for (int i = 0; i < entityGroups.length; i++) {
          // Add so much data to this entityGroup, we create a store file that
          // is >
          // than one of our unsplitable references. it will.
          for (int j = 0; j < 2; j++) {
            // addContent(entityGroups[i], field3);
          }
          // addContent(entityGroups[i], field2);
          // addContent(entityGroups[i], field1);
          entityGroups[i].internalCommitTransaction();
        }

        byte[][] midkeys = new byte[entityGroups.length][];

        for (int i = 0; i < entityGroups.length; i++) {
          midkeys[i] = entityGroups[i].checkSplit();
          System.out.println(" tianzhaotianzhao 3 " + entityGroups[i]);
        }

        TreeMap<String, EntityGroup> sortedMap = new TreeMap<String, EntityGroup>();
        // Split these two daughter entityGroups so then I'll have 4
        // entityGroups. Will
        // split because added data above.
        for (int i = 0; i < entityGroups.length; i++) {
          EntityGroup[] rs = null;
          if (midkeys[i] != null) {
            rs = splitEntityGroup(entityGroups[i], midkeys[i]);
            for (int j = 0; j < rs.length; j++) {
              sortedMap.put(Bytes.toString(rs[j].getEntityGroupName()),
                  openClosedEntityGroup(rs[j]));
            }
          }
        }
        LOG.info("Made 4 entityGroups");
        // The splits should have been even. Test I can get some arbitrary row
        // out of each.
        int interval = (LAST_CHAR - FIRST_CHAR) / 3;
        byte[] b = Bytes.toBytes(START_KEY);
        for (EntityGroup r : sortedMap.values()) {
          b[0] += interval;
        }
      } finally {
        for (int i = 0; i < entityGroups.length; i++) {
          try {
            entityGroups[i].close();
          } catch (IOException e) {
            // Ignore.
          }
        }
      }
    } finally {
      EntityGroup.closeEntityGroup(this.entityGroup);
      this.entityGroup = null;
    }
  }

  /**
   * @param parent
   *          Region to split.
   * @param midkey
   *          Key to split around.
   * @return The Regions we created.
   * @throws java.io.IOException
   */
  EntityGroup[] splitEntityGroup(final EntityGroup parent, final byte[] midkey)
      throws IOException {
    PairOfSameType<EntityGroup> result = null;
    SplitTransaction st = new SplitTransaction(parent, midkey,
        TEST_UTIL.getConfiguration());
    // If prepare does not return true, for some reason -- logged inside in
    // the prepare call -- we are not ready to split just now. Just return.
    if (!st.prepare())
      return null;
    LOG.info("FirstDaughter:  " + st.getFirstDaughter().toString());
    LOG.info("SecondDaughter: " + st.getSecondDaughter().toString());
    try {
      result = st.execute(null, null);
    } catch (IOException ioe) {
      try {
        LOG.info("Running rollback of failed split of "
            + parent.getEntityGroupNameAsString() + "; " + ioe.getMessage());
        st.rollback(null, null);
        LOG.info("Successful rollback of failed split of "
            + parent.getEntityGroupNameAsString());
        return null;
      } catch (RuntimeException e) {
        // If failed rollback, kill this server to avoid having a hole in table.
        LOG.info(
            "Failed rollback of failed split of "
                + parent.getEntityGroupNameAsString() + " -- aborting server",
            e);
      }
    }
    return new EntityGroup[] { result.getFirst(), result.getSecond() };
  }

  @Test
  public void testSplitEntityGroup() throws IOException {
    byte[] tableName = Bytes.toBytes("testtable");
    // byte[] qualifier = Bytes.toBytes("qualifier");
    Configuration hc = TEST_UTIL.getConfiguration(); // initSplit();
    int numRows = 10;
    List<Field> fileds = Arrays.asList(field1, field3);

    // Setting up entityGroup
    String method = "testSplitEntityGroup";
    this.entityGroup = initEntityGroup(tableName, method, hc, fileds);

    // Put data in entityGroup
    int startRow = 100;
    int splitRow = startRow + numRows;
    entityGroup.internalCommitTransaction();

    EntityGroup[] entityGroups = null;
    try {
      entityGroups = splitEntityGroup(entityGroup, Bytes.toBytes("" + splitRow));
      // Opening the entityGroups returned.
      for (int i = 0; i < entityGroups.length; i++) {
        openClosedEntityGroup(entityGroups[i]);
        // entityGroups[i] = openClosedEntityGroup(entityGroups[i]);
      }
      // Verifying that the entityGroup has been split
      Assert.assertEquals(2, entityGroups.length);

    } finally {
      EntityGroup.closeEntityGroup(this.entityGroup);
      this.entityGroup = null;
    }
  }

  /**
   * Testcase to check state of entityGroup initialization task set to ABORTED
   * or not if any exceptions during initialization
   * 
   * @throws Exception
   */
  // @Test
  // public void
  // testStatusSettingToAbortIfAnyExceptionDuringEntityGroupInitilization()
  // throws Exception {
  // EntityGroupInfo info = null;
  // try {
  // FTable table = new FTable();
  // table.setTableName(Bytes.toString(tableName));
  //
  // info = new EntityGroupInfo(tableName, HConstants.EMPTY_BYTE_ARRAY,
  // HConstants.EMPTY_BYTE_ARRAY, false);
  //
  // RedoLog redoLog = new RedoLog(info, TEST_UTIL.getConfiguration());
  //
  // SchemaMetrics.setUseTableNameInTest(false);
  //
  // entityGroup = EntityGroup.newEntityGroup(TEST_UTIL.getConfiguration(),
  // info, table, null);
  // Mockito.when(
  // EntityGroupSplitPolicy.create(entityGroup,
  // TEST_UTIL.getConfiguration())).thenThrow(new IOException());
  // // entityGroup initialization throws IOException and set task state to
  // // ABORTED.
  // entityGroup.initialize();
  // Assert.fail("EntityGroup initialization should fail due to IOException");
  // } catch (IOException io) {
  // List<MonitoredTask> tasks = TaskMonitor.get().getTasks();
  // for (MonitoredTask monitoredTask : tasks) {
  // if (!(monitoredTask instanceof MonitoredRPCHandler)
  // && monitoredTask.getDescription().contains(entityGroup.toString())) {
  // Assert.assertTrue("EntityGroup state should be ABORTED.",
  // monitoredTask.getState().equals(MonitoredTask.State.ABORTED));
  // break;
  // }
  // }
  // } finally {
  // EntityGroup.closeEntityGroup(entityGroup);
  // }
  // }

  private EntityGroup openClosedEntityGroup(EntityGroup closedEntityGroup) {
    return null;
  }

  /**
   * @param tableName
   * @param callingMethod
   * @param conf
   * @param fields
   * @throws java.io.IOException
   * @return A entityGroup on which you must call
   *         {@link EntityGroup#closeEntityGroup(EntityGroup)} when done.
   */
  private static EntityGroup initEntityGroup(byte[] tableName,
      String callingMethod, Configuration conf, List<Field> fields)
      throws IOException {
    return initEntityGroup(tableName, null, null, callingMethod, conf, fields);
  }

  /**
   * @param tableName
   * @param startKey
   * @param stopKey
   * @param callingMethod
   * @param conf
   * @param fields
   * @throws java.io.IOException
   * @return A entityGroup on which you must call
   *         {@link EntityGroup#closeEntityGroup(EntityGroup)} when done.
   */
  private static EntityGroup initEntityGroup(byte[] tableName, byte[] startKey,
      byte[] stopKey, String callingMethod, Configuration conf,
      List<Field> fields) throws IOException {
    FTable table = new FTable();
    table.setTableName(Bytes.toString(tableName));
    LinkedHashMap<String, Field> finalFields = new LinkedHashMap<String, Field>();
    for (Field field : fields) {
      finalFields.put(field.getName(), field);
    }
    table.setColumns(finalFields);
    EntityGroupInfo info = new EntityGroupInfo(Bytes.toBytes(table
        .getTableName()), startKey, stopKey, false);

    if (FMetaReader.exists(TEST_UTIL.getConfiguration(), info)) {
      throw new IOException("All ready has a entityGroupInfo "
          + info.getEntityGroupNameAsString());
    }
    return EntityGroup.openEntityGroup(info, table, conf, TEST_UTIL
        .getWaspCluster().getFServer(0), null);
  }
}
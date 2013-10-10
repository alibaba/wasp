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
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Field;
import com.alibaba.wasp.plan.action.Action;
import com.alibaba.wasp.plan.action.InsertAction;
import com.alibaba.wasp.storage.StorageActionManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestTwoPhaseCommit {

  private List<EntityGroup> entityGroups = new ArrayList<EntityGroup>();

  private static final WaspTestingUtility TEST_UTIL = new WaspTestingUtility();

  // FMetaTestUtil
  public static String CF = "default";
  private Field field1 = new Field(FieldKeyWord.REQUIRED, CF, "user_id",
      DataType.INT64, "none");
  private Field field2 = new Field(FieldKeyWord.REQUIRED, CF, "name",
      DataType.STRING, "none");
  private Field field3 = new Field(FieldKeyWord.REQUIRED, CF, "time",
      DataType.INT64, "none");

  /**
   * @see org.apache.hadoop.hbase.HBaseTestCase#setUp()
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    WaspTestingUtility.adjustLogLevel();
    TEST_UTIL.startMiniCluster(3);
//    TEST_UTIL.getHBaseTestingUtility().createTable(Bytes.toBytes(FConstants.MESSAGEQUEUE_TABLENAME),
//        Bytes.toBytes(FConstants.MESSAGEQUEUE_FAMILIY));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    EnvironmentEdgeManagerTestHelper.reset();
  }

  private Map<EntityGroupInfo, List<Action>> getTranscation() throws IOException {

    byte[] tableName1 = Bytes.toBytes("testtable1");
    byte[] tableName2 = Bytes.toBytes("testtable2");
    byte[] tableName3 = Bytes.toBytes("testtable3");
    List<Field> fileds = Arrays.asList(field1, field2, field3);

    Configuration conf = TEST_UTIL.getConfiguration(); // initSplit();
    // Setting up entityGroup
    String method1 = "getTranscation1";
    String method2 = "getTranscation2";
    String method3 = "getTranscation3";
    EntityGroup entityGroup1 = initEntityGroup(tableName1, method1, conf, fileds);
    EntityGroup entityGroup2 = initEntityGroup(tableName2, method2, conf, fileds);
    EntityGroup entityGroup3 = initEntityGroup(tableName3, method3, conf, fileds);
    entityGroups.add(entityGroup1);
    entityGroups.add(entityGroup2);
    entityGroups.add(entityGroup3);

    List<Action> actions1 = new ArrayList<Action>();
    List<Action> actions2 = new ArrayList<Action>();
    List<Action> actions3 = new ArrayList<Action>();
    Action action1 = new InsertAction("testtable1", Bytes.toBytes("PK1"));
    Action action2 = new InsertAction("testtable2", Bytes.toBytes("PK2"));
    Action action3 = new InsertAction("testtable3", Bytes.toBytes("PK3"));
    actions1.add(action1);
    actions2.add(action2);
    actions3.add(action3);

    Map<EntityGroupInfo, List<Action>> transcation = new HashMap<EntityGroupInfo, List<Action>>();
    transcation.put(entityGroup1.getEntityGroupInfo(), actions1);
    transcation.put(entityGroup2.getEntityGroupInfo(), actions2);
    transcation.put(entityGroup3.getEntityGroupInfo(), actions3);
    return transcation;
  }

  @Test
  public void testTwoPhaseCommit() throws IOException {
    StorageActionManager actionManager = new StorageActionManager(TEST_UTIL.getConfiguration());
    TwoPhaseCommitProtocol twoPhaseCommit =  TwoPhaseCommit.getInstance(actionManager);
    boolean ret = twoPhaseCommit.submit(getTranscation());
    assertTrue(ret);
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

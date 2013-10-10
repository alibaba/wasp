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
package com.alibaba.wasp.messagequeue;

import com.alibaba.wasp.DataType;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FieldKeyWord;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.fserver.EntityGroup;
import com.alibaba.wasp.fserver.OperationStatus;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Field;
import com.alibaba.wasp.plan.action.InsertAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestSubscriber {

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
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    EnvironmentEdgeManagerTestHelper.reset();
  }

  private void publisher() throws IOException {

    byte[] tableName1 = Bytes.toBytes("testtable1");
    byte[] tableName2 = Bytes.toBytes("testtable2");
    byte[] tableName3 = Bytes.toBytes("testtable3");
    List<Field> fileds = Arrays.asList(field1, field2, field3);

    Configuration conf = TEST_UTIL.getConfiguration(); // initSplit();
    // Setting up entityGroup
    String method1 = "TestSubscriber1";
    String method2 = "TestSubscriber2";
    String method3 = "TestSubscriber3";
    EntityGroup entityGroup1 = initEntityGroup(tableName1, method1, conf, fileds);
    EntityGroup entityGroup2 = initEntityGroup(tableName2, method2, conf, fileds);
    EntityGroup entityGroup3 = initEntityGroup(tableName3, method3, conf, fileds);
    entityGroups.add(entityGroup1);
    entityGroups.add(entityGroup2);
    entityGroups.add(entityGroup3);

    PublisherBuilder builder = new PublisherBuilder();
    Publisher publisher1 = builder.build(conf, entityGroup1.getEntityGroupInfo());
    Publisher publisher2 = builder.build(conf, entityGroup2.getEntityGroupInfo());
    Publisher publisher3 = builder.build(conf, entityGroup3.getEntityGroupInfo());

    Message message1 = new InsertAction("testtable1", Bytes.toBytes("PK1"));
    Message message2 = new InsertAction("testtable2", Bytes.toBytes("PK2"));
    Message message3 = new InsertAction("testtable3", Bytes.toBytes("PK3"));

    publisher1.doAsynchronous(message1);
    publisher2.doAsynchronous(message2);
    publisher3.doAsynchronous(message3);
  }

  @Test
  public void testReceive() throws IOException {
    publisher();
    assertTrue(entityGroups.size() > 0);
    SubscriberBuilder builder = new SubscriberBuilder();
    for (EntityGroup entityGroup : entityGroups) {
      Subscriber subscriber = builder.build(entityGroup);
      List<Message> messages = subscriber.receive();
      assertTrue(messages != null && messages.size() == 1);
      for (Message message : messages) {
        OperationStatus status = subscriber.doAsynchronous(message);
        assertTrue(status == OperationStatus.SUCCESS);
      }
    }
  }



   /**
   * @param tableName
   * @param callingMethod
   * @param conf
   * @param fields
   * @throws java.io.IOException
   * @return A entityGroup on which you must call
   *         {@link com.alibaba.wasp.fserver.EntityGroup#closeEntityGroup(com.alibaba.wasp.fserver.EntityGroup)} when done.
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
   *         {@link com.alibaba.wasp.fserver.EntityGroup#closeEntityGroup(com.alibaba.wasp.fserver.EntityGroup)} when done.
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

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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestPublisher {

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

  @Test
  public void testPublisher() throws IOException {

    byte[] tableName = Bytes.toBytes("testtable");
    List<Field> fileds = Arrays.asList(field1, field2, field3);

    Configuration conf = TEST_UTIL.getConfiguration(); // initSplit();
    // Setting up entityGroup
    String method = "testDoAsynchronous";
    this.entityGroup = initEntityGroup(tableName, method, conf, fileds);

    PublisherBuilder builder = new PublisherBuilder();
    Publisher publisher = builder.build(conf, entityGroup.getEntityGroupInfo());

    Message message = new InsertAction("testtable", Bytes.toBytes("PK"));

    MessageID mId = publisher.doAsynchronous(message);

    assertTrue(mId != null);

    assertTrue(publisher.commit(mId));

    assertTrue(publisher.delete(mId));

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

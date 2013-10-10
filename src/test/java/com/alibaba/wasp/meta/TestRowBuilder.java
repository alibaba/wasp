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
package com.alibaba.wasp.meta;

import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.plan.parser.QueryInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.NavigableMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRowBuilder {

  private Index index;

  private QueryInfo queryInfo;

  private RowBuilder builder;

  private String indexName = "TEST_INDEX";

  private byte[] PK = Bytes.toBytes("PK");

  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> values;

  private List<String> indexs;

  @Before
  public void setUp() throws Exception {
    builder = RowBuilder.build();
    Pair<Index, List<IndexField>> pair = RowBuilderTestUtil.buildTestIndex();
    index = pair.getFirst();
    values = RowBuilderTestUtil.buildValues();
    indexs = RowBuilderTestUtil.buildIndexNames(indexName);
    queryInfo = RowBuilderTestUtil.buildQueryInfo();
  }

  @Test
  public void testBuildStartKeyAndEndKey() throws Exception {
    Pair<byte[], byte[]> pair = builder
        .buildStartkeyAndEndkey(index, queryInfo);
    byte[] first = pair.getFirst();
    byte[] second = pair.getSecond();

    // test0110
    byte[] firstRet = Bytes.add(Bytes.toBytes("test"),
        FConstants.DATA_ROW_SEP_STORE);
    firstRet = Bytes.add(firstRet, Bytes.toBytes(11),
        FConstants.DATA_ROW_SEP_STORE);

    // test0110199
    byte[] secondRet = Bytes.add(Bytes.toBytes("test"),
        FConstants.DATA_ROW_SEP_STORE);
    secondRet = Bytes.add(secondRet, Bytes.toBytes(11),
        FConstants.DATA_ROW_SEP_STORE);
    secondRet = Bytes.add(secondRet, Bytes.toBytes(199L));

    assertTrue(Bytes.equals(first, firstRet));
    assertTrue(Bytes.equals(second, secondRet));
  }

  @Test
  public void testGetIndexTableName() {
    String indexTableName = StorageTableNameBuilder.buildIndexTableName(index);
    assertTrue(indexs.contains(indexTableName));
  }

  @Test
  public void testGetIndexFields() {
    List<IndexField> indexFieldsList = RowBuilder.getIndexFields(index, values);

    assertTrue(indexFieldsList.size() == (queryInfo.getEqConditions().size() + queryInfo
        .getRangeConditions().size()));

    for (IndexField field : indexFieldsList) {
      assertEquals("cf", field.getFamily());
      if (field.getName().equals("index1")) {
        assertEquals("test", Bytes.toString(field.getValue()));
      } else if (field.getName().equals("index2")) {
        assertEquals(11, Bytes.toInt(field.getValue()));
      } else if (field.getName().equals("index3")) {
        assertEquals(199, Bytes.toInt(field.getValue()));
      }
    }
  }

  @Test
  public void testBuildIndexKeys() {
    Pair<byte[], String> indexKeys = builder.buildIndexKey(index, values, PK);

    // test01101990PK
    byte[] first = Bytes.add(Bytes.toBytes("test"),
        FConstants.DATA_ROW_SEP_STORE);
    first = Bytes.add(first, Bytes.toBytes(199), FConstants.DATA_ROW_SEP_STORE);
    first = Bytes.add(first, Bytes.toBytes(11), FConstants.DATA_ROW_SEP_STORE);
    first = Bytes.add(first, PK);

    // test01990110PK
    byte[] second = Bytes.add(Bytes.toBytes("test"),
        FConstants.DATA_ROW_SEP_STORE);
    second = Bytes
        .add(second, Bytes.toBytes(11), FConstants.DATA_ROW_SEP_STORE);
    second = Bytes.add(second, Bytes.toBytes(199),
        FConstants.DATA_ROW_SEP_STORE);
    second = Bytes.add(second, PK);

    assertTrue(Bytes.equals(indexKeys.getFirst(), first)
        || Bytes.equals(indexKeys.getFirst(), second));
    assertTrue(indexs.contains(indexKeys.getSecond()));
  }
}
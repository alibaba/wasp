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

package org.apache.wasp.plan.action;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.DataType;

import java.util.ArrayList;
import java.util.List;

public class ActionTestUtil {

  public static String[] TestTableName = { "Test", "Test", "Test", "Test",
      "Test" };
  public static String[] TestFamilyName = { "default", "default", "default",
      "default", "default" };
  public static String[] TestColumnName = { "testColumn1", "testColumn2",
      "testColumn3", "testColumn4", "testColumn5" };
  public static byte[][] TestValue = { Bytes.toBytes(1), Bytes.toBytes(12),
      Bytes.toBytes(123), Bytes.toBytes(1234), Bytes.toBytes(12345) };
  public static DataType[] TestType = { DataType.INT32, DataType.INT32,
      DataType.INT32, DataType.INT32, DataType.INT32 };

  public static ColumnStruct makeTestColumnAction() {
    return new ColumnStruct(TestTableName[0], TestFamilyName[0],
        TestColumnName[0], TestType[0], TestValue[0]);
  }

  public static List<ColumnStruct> makeTestColumnActions() {
    List<ColumnStruct> columnActions = new ArrayList<ColumnStruct>();
    for (int i = 0; i < TestTableName.length; i++) {
      ColumnStruct columnAction = new ColumnStruct(TestTableName[i],
          TestFamilyName[i], TestColumnName[i], TestType[i], TestValue[i]);
      columnActions.add(columnAction);
    }
    return columnActions;
  }

  public static ColumnStruct makeTestColumnAction(String tableName,
      String familyName, String columnName, DataType dataType, byte[] value) {
    return new ColumnStruct(tableName, familyName, columnName, dataType, value);
  }

  public static byte[] TestPrimayKey = Bytes.toBytes(12345);
  public static byte[] TestValueOfEntityGroupKey = Bytes.toBytes(12345);

  public static InsertAction makeTestInsertAction() {
    return new InsertAction(TestTableName[0], TestValueOfEntityGroupKey,
        TestPrimayKey, makeTestColumnActions());
  }

}

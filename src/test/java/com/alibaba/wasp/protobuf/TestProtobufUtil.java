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
package com.alibaba.wasp.protobuf;

import com.alibaba.wasp.DataType;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.plan.action.DeleteAction;
import com.alibaba.wasp.plan.action.GetAction;
import com.alibaba.wasp.plan.action.InsertAction;
import com.alibaba.wasp.plan.action.ScanAction;
import com.alibaba.wasp.plan.action.UpdateAction;
import com.alibaba.wasp.protobuf.generated.MetaProtos.GetActionProto;
import com.alibaba.wasp.protobuf.generated.MetaProtos.MessageProto;
import com.alibaba.wasp.protobuf.generated.MetaProtos.ScanActionProto;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * Class to test ProtobufUtil.
 */

public class TestProtobufUtil {

  @Test
  public void testDelete() throws InvalidProtocolBufferException {
    String tableName = "test";
    String primary = "1234";
    DeleteAction action = new DeleteAction(tableName, Bytes.toBytes(primary));
    MessageProto message = ProtobufUtil.convertDeleteAction(action);
    action = (DeleteAction) ProtobufUtil.convertWriteAction(message);
    Assert.assertTrue(tableName.equals(action.getFTableName()));
    Assert.assertTrue(primary.equals(Bytes.toString(action.getCombinedPrimaryKey())));
  }

  @Test
  public void testInsert() throws InvalidProtocolBufferException {
    String tableName = "test";
    String primary = "1234";
    String familyName = "info";
    String columnName = "col";
    String value = "8899";
    InsertAction action = new InsertAction(tableName, Bytes.toBytes(primary));
    action.addEntityColumn(tableName, familyName, columnName, DataType.STRING,
        Bytes.toBytes(value));
    MessageProto message = ProtobufUtil.convertInsertAction(action);
    action = (InsertAction) ProtobufUtil.convertWriteAction(message);
    Assert.assertTrue(tableName.equals(action.getFTableName()));
    Assert.assertTrue(primary.equals(Bytes.toString(action.getCombinedPrimaryKey())));
    Assert.assertTrue(action.getColumns().get(0).getFamilyName()
        .equals(familyName)
        && action.getColumns().get(0).getColumnName().equals(columnName)
        && Bytes.toString(action.getColumns().get(0).getValue()).equals(value));
  }

  @Test
  public void testUpdate() throws InvalidProtocolBufferException {
    String tableName = "test";
    String primary = "1234";
    String familyName = "info";
    String columnName = "col";
    String value = "8899";
    UpdateAction action = new UpdateAction(tableName, Bytes.toBytes(primary));
    action.addEntityColumn(tableName, familyName, columnName, DataType.STRING,
        Bytes.toBytes(value));
    MessageProto message = ProtobufUtil.convertUpdateAction(action);
    action = (UpdateAction) ProtobufUtil.convertWriteAction(message);
    Assert.assertTrue(tableName.equals(action.getFTableName()));
    Assert.assertTrue(primary.equals(Bytes.toString(action.getCombinedPrimaryKey())));
    Assert.assertTrue(action.getColumns().get(0).getFamilyName()
        .equals(familyName)
        && action.getColumns().get(0).getColumnName().equals(columnName)
        && Bytes.toString(action.getColumns().get(0).getValue()).equals(value));
  }

  @Test
  public void testGet() {
    String tableName = "test";
    String primary = "1234";
    String familyName = "info";
    String columnName = "col";
    ReadModel model = ReadModel.SNAPSHOT;
    GetAction action = new GetAction(model, tableName, Bytes.toBytes(primary));
    action.addEntityColumn(tableName, familyName, columnName, DataType.STRING, null);
    action.addEntityColumn(tableName, familyName, columnName, DataType.STRING, null);
    GetActionProto proto = ProtobufUtil.toGetActionProto(action);
    action = (GetAction) ProtobufUtil.toGetAction(proto);
    Assert.assertTrue(model == action.getReaderMode());
    Assert.assertTrue(tableName.equals(action.getFTableName()));
    Assert.assertTrue(primary.equals(Bytes.toString(action.getCombinedPrimaryKey())));
    Assert.assertTrue(action.getColumns().get(0).getFamilyName()
        .equals(familyName)
        && action.getColumns().get(0).getColumnName().equals(columnName));
    Assert.assertTrue(action.getColumns().get(1).getFamilyName()
        .equals(familyName)
        && action.getColumns().get(1).getColumnName().equals(columnName));
  }

  @Test
  public void testScan() {
    ReadModel readerMode = ReadModel.INCONSISTENT;
    String indexTableName = "test_index";
    String entityTableName = "test";
    byte[] startKey = "000".getBytes();
    byte[] endKey = "111".getBytes();
    int limit = 100;
    int batch = 20;
    ScanAction action = new ScanAction(readerMode, indexTableName, entityTableName,
        startKey, endKey);
    action.addEntityColumn("test", "f", "c", DataType.STRING, Bytes.toBytes(""));
    action.addEntityColumn("test", "f", "c", DataType.STRING, Bytes.toBytes(""));
    action.setBatch(batch);
    action.setLimit(limit);
    ScanActionProto proto = ProtobufUtil.convertScanAction(action);
    action = (ScanAction) ProtobufUtil.convertScanAction(proto);
    Assert.assertTrue(readerMode == action.getReaderMode());
    Assert.assertTrue(indexTableName.equals(action.getIndexTableName()));
    Assert.assertTrue(entityTableName.equals(action.getEntityTableName()));
    Assert.assertTrue(Bytes.toString(action.getStartKey()).equals("000"));
    Assert.assertTrue(Bytes.toString(action.getEndKey()).equals("111"));
    Assert.assertTrue(action.getBatch() == batch);
    Assert.assertTrue(action.getLimit() == limit);
    Assert.assertTrue(action.getColumns().get(0).getFamilyName().equals("f")
        && action.getColumns().get(0).getColumnName().equals("c")
        && Bytes.toString(action.getColumns().get(0).getValue()).equals(""));
    Assert.assertTrue(action.getColumns().get(1).getFamilyName().equals("f")
        && action.getColumns().get(1).getColumnName().equals("c")
        && Bytes.toString(action.getColumns().get(1).getValue()).equals(""));
  }
}
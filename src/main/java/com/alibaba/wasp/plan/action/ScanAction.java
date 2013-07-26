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
package com.alibaba.wasp.plan.action;

import com.google.protobuf.ByteString;
import com.alibaba.wasp.DataType;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.generated.MetaProtos.ColumnStructProto;
import com.alibaba.wasp.protobuf.generated.MetaProtos.ReadModelProto;
import com.alibaba.wasp.protobuf.generated.MetaProtos.ScanActionProto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Action of query used to record some property.
 * 
 */
public class ScanAction extends NoPrimaryReadAction {

  /** null or string **/
  private final String indexTableName;

  /** start key of scan **/
  private final byte[] startKey;

  /** end key of scan **/
  private final byte[] endKey;

  private int limit = -1;

  private int batch = -1;

  private List<ColumnStruct> storingColumns = new ArrayList<ColumnStruct>();

  public ScanAction(ReadModel readerMode, String indexTableName,
      String entityTableName, byte[] startKey, byte[] endKey) {
    this(readerMode, indexTableName, entityTableName, startKey, endKey,
        new ArrayList<ColumnStruct>());
  }

  public ScanAction(ReadModel readerMode, String indexTableName,
      String fTableName, byte[] startKey, byte[] endKey,
      List<ColumnStruct> columns) {
    this.readerMode = readerMode;
    this.indexTableName = indexTableName;
    this.fTableName = fTableName;
    this.startKey = startKey;
    this.endKey = endKey;
    this.setColumns(columns);
  }

  /**
   * @return the indexTableName
   */
  public String getIndexTableName() {
    return indexTableName;
  }

  /**
   * @return the startKey
   */
  public byte[] getStartKey() {
    return startKey;
  }

  /**
   * @return the endKey
   */
  public byte[] getEndKey() {
    return endKey;
  }

  /**
   * @return the limit
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @param limit
   *          the limit to set
   */
  public void setLimit(int limit) {
    this.limit = limit;
  }

  /**
   * @return the batch
   */
  public int getBatch() {
    return batch;
  }

  public String getEntityTableName() {
    return this.getFTableName();
  }

  /**
   * @param batch
   *          the batch to set
   */
  public void setBatch(int batch) {
    this.batch = batch;
  }

  /**
   * Generate ColumnAction instance and then add it to list.
   * 
   * @param tableName
   * @param familyName
   * @param columnName
   * @param value
   * @param columns
   */
  public void addStoringColumn(String tableName, String familyName,
      String columnName, DataType dataType, byte[] value) {
    ColumnStruct col = new ColumnStruct(tableName, familyName, columnName,
       dataType, value);
    storingColumns.add(col);
  }

  /**
   * Generate ColumnAction instance and then add it to list.
   * 
   * @param fTableName
   * @param familyName
   * @param columnName
   * @param value
   * @param columns
   */
  public void addStoringColumn(ColumnStruct col) {
    storingColumns.add(col);
  }

  /**
   * @return the storingColumns
   */
  public List<ColumnStruct> getStoringColumns() {
    return storingColumns;
  }

  /**
   * @param storingColumns
   *          the storingColumns to set
   */
  public void setStoringColumns(List<ColumnStruct> storingColumns) {
    this.storingColumns = storingColumns;
  }

  /**
   * 
   * 
   * @param scanActionProto
   * @return
   */
  public static ScanAction convert(ScanActionProto scanActionProto) {
    ScanAction action = new ScanAction(ReadModel.valueOf(scanActionProto
        .getReadMode().name()), scanActionProto.getIndexTableName(),
        scanActionProto.getEntityTableName(), scanActionProto.getStartKey()
            .toByteArray(), scanActionProto.getEndKey().toByteArray());
    for (ColumnStructProto col : scanActionProto.getEntityColsList()) {
      action.addEntityColumn(ProtobufUtil.toColumnAction(col));
    }
    for (ColumnStructProto col : scanActionProto.getIndexColsList()) {
      action.addStoringColumn(ProtobufUtil.toColumnAction(col));
    }
    action.setBatch(scanActionProto.getBatch());
    action.setLimit(scanActionProto.getLimit());
    return action;
  }

  /**
   * 
   * 
   * @param scanAction
   * @return
   */
  public static ScanActionProto convert(ScanAction scanAction) {
    ScanActionProto.Builder builder = ScanActionProto.newBuilder();
    builder.setReadMode(ReadModelProto.valueOf(scanAction.getReaderMode()
        .name()));
    builder.setLimit(scanAction.getLimit());
    builder.setBatch(scanAction.getBatch());
    builder.setEntityTableName(scanAction.getFTableName());
    builder.setIndexTableName(scanAction.getIndexTableName());
    builder.setStartKey(ByteString.copyFrom(scanAction.getStartKey()));
    builder.setEndKey(ByteString.copyFrom(scanAction.getEndKey()));
    for (ColumnStruct col : scanAction.getColumns()) {
      builder.addEntityCols(ProtobufUtil.toColumnStructProto(col));
    }
    for (ColumnStruct col : scanAction.getStoringColumns()) {
      builder.addEntityCols(ProtobufUtil.toColumnStructProto(col));
    }
    return builder.build();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "ScanAction [readerMode=" + readerMode + ", indexTableName="
        + indexTableName + ", entityTableName=" + fTableName + ", startKey="
        + Arrays.toString(startKey) + ", endKey=" + Arrays.toString(endKey)
        + ", limit=" + limit + ", batch=" + batch + ", storingColumns="
        + storingColumns + ", columns=" + this.getColumns() + "]";
  }
}
/**
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

import com.alibaba.wasp.DataType;
import com.alibaba.wasp.protobuf.generated.MetaProtos.ColumnStructProto;
import com.alibaba.wasp.util.ParserUtils;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/** Properties of Column in action. **/
public class ColumnStruct {
  private final String tableName;

  private final String familyName;

  private final String columnName;

  private byte[] value;

  private boolean isIndex = false;

  private DataType dataType;

  private final int compareOp;

  public ColumnStruct(String tableName, String familyName, String columnName,
      DataType datatype) {
    this(tableName, familyName, columnName, datatype, null);
  }

  public ColumnStruct(String tableName, String familyName, String columnName,
      DataType datatype, byte[] value) {
    this(tableName, familyName, columnName, datatype, value, ParserUtils.OperatorOpValue.NO_OP);
  }

  public ColumnStruct(String tableName, String familyName, String columnName,
      DataType datatype, byte[] value, int compareOp) {
    this.tableName = tableName;
    this.familyName = familyName;
    this.columnName = columnName;
    this.dataType = datatype;
    this.value = value;
    this.compareOp = compareOp;
  }

  /**
   * @return the tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @return the columnName
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * @return the value
   */
  public byte[] getValue() {
    return value;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  /**
   * @return the familyName
   */
  public String getFamilyName() {
    return familyName;
  }

  /**
   * @return the isIndex
   */
  public boolean isIndex() {
    return isIndex;
  }

  public int getCompareOp() {
    return compareOp;
  }

  /**
   * @param isIndex
   *          the isIndex to set
   */
  public void setIndex(boolean isIndex) {
    this.isIndex = isIndex;
  }

  /**
   * Convert ColumnActionProto to ColumnAction.
   * 
   * @param col
   * @return
   */
  public static ColumnStruct convert(ColumnStructProto col) {
    if (col.hasValue()) {
      return new ColumnStruct(col.getTableName(), col.getFamilyName(),
          col.getColumnName(),
          col.getDataType() != null ? DataType
              .convertDataTypeProtosToDataType(col.getDataType()) : null, col
              .getValue().toByteArray(), col.getCompareOp());
    } else {
      return new ColumnStruct(col.getTableName(), col.getFamilyName(),
          col.getColumnName(),
          col.getDataType() != null ? DataType
              .convertDataTypeProtosToDataType(col.getDataType()) : null, null, col.getCompareOp());
    }
  }

  /**
   * Convert ColumnActionProto to ColumnAction.
   * 
   * @param col
   * @return
   */
  public static ColumnStructProto convert(ColumnStruct col) {
    ColumnStructProto.Builder colProto = ColumnStructProto.newBuilder();
    colProto.setColumnName(col.getColumnName());
    colProto.setFamilyName(col.getFamilyName());
    colProto.setTableName(col.getTableName());
    if (col.getDataType() != null) {
      colProto.setDataType(DataType.convertDataTypeToDataTypeProtos(col
          .getDataType()));
    }
    if (col.getValue() != null) {
      colProto.setValue(ByteString.copyFrom(col.getValue()));
    }
    colProto.setCompareOp(col.getCompareOp());
    return colProto.build();
  }

  /**
   * 
   * @see Object#equals(Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ColumnStruct)) {
      return false;
    }
    ColumnStruct instance = (ColumnStruct) obj;
    return this.tableName.equals(instance.getTableName())
        && this.columnName.equals(instance.getColumnName())
        && Bytes.BYTES_COMPARATOR.compare(this.value == null ? new byte[0]
            : this.value,
            instance.getValue() == null ? new byte[0] : instance.getValue()) == 0
        && this.familyName.equals(instance.getFamilyName())
        && this.isIndex == instance.isIndex()
        && this.compareOp == instance.getCompareOp();
  }

  /**
   * @see Object#toString()
   */
  @Override
  public String toString() {
    return "ColumnAction [tableName=" + tableName + ", familyName="
        + familyName + ", columnName=" + columnName + ", value="
        + Arrays.toString(value) + ", isIndex=" + isIndex + "]";
  }
}

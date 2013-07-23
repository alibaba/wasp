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
package org.apache.wasp.plan.action;

import com.google.protobuf.ByteString;
import org.apache.wasp.DataType;
import org.apache.wasp.ReadModel;
import org.apache.wasp.protobuf.ProtobufUtil;
import org.apache.wasp.protobuf.generated.MetaProtos.ColumnStructProto;
import org.apache.wasp.protobuf.generated.MetaProtos.GetActionProto;
import org.apache.wasp.protobuf.generated.MetaProtos.ReadModelProto;

import java.util.Arrays;
import java.util.List;

/**
 * Get operator's basic args.
 * 
 */
public class GetAction extends ReadAction {

  /**
   * 
   * 
   * @param readerMode
   * @param tableName
   * @param primayKey
   * @param columns
   */
  public GetAction(ReadModel readerMode, String tableName, byte[] primayKey,
      List<ColumnStruct> columns) {
    this.readerMode = readerMode;
    this.fTableName = tableName;
    this.combinedPrimaryKey = primayKey;
    this.setColumns(columns);
  }

  /**
   * 
   * 
   * @param readerMode
   * @param tableName
   * @param primayKey
   */
  public GetAction(ReadModel readerMode, String tableName, byte[] primayKey) {
    this.readerMode = readerMode;
    this.fTableName = tableName;
    this.combinedPrimaryKey = primayKey;
  }

  /**
   * 
   * 
   * @param getAction
   * @return
   */
  public static GetAction convert(GetActionProto getAction) {
    GetAction action = new GetAction(ReadModel.valueOf(getAction.getReadMode()
        .name()), getAction.getEntityTableName(), getAction.getRow()
        .toByteArray());
    for (ColumnStructProto col : getAction.getColsList()) {
      action.addEntityColumn(col.getTableName(), col.getFamilyName(),
          col.getColumnName(), DataType.convertDataTypeProtosToDataType(col
          .getDataType()), col.getValue().toByteArray());
    }
    return action;
  }

  /**
   * 
   * 
   * @param getAction
   * @return
   */
  public static GetActionProto convert(GetAction getAction) {
    GetActionProto.Builder builder = GetActionProto.newBuilder();
    builder.setEntityTableName(getAction.getFTableName());
    builder.setReadMode(ReadModelProto
        .valueOf(getAction.getReaderMode().name()));
    builder.setRow(ByteString.copyFrom(getAction.getCombinedPrimaryKey()));
    for (ColumnStruct col : getAction.getColumns()) {
      builder.addCols(ProtobufUtil.toColumnStructProto(col));
    }
    return builder.build();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "GetAction [readerMode=" + readerMode + ", tableName=" + fTableName
        + ", primayKey=" + Arrays.toString(combinedPrimaryKey) + ", columns="
        + this.getColumns() + "]";
  }
}
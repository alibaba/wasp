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
import org.apache.wasp.messagequeue.Message;
import org.apache.wasp.messagequeue.MessageID;
import org.apache.wasp.protobuf.ProtobufUtil;
import org.apache.wasp.protobuf.generated.MetaProtos.ColumnStructProto;
import org.apache.wasp.protobuf.generated.MetaProtos.InsertActionProto;

import java.util.Arrays;
import java.util.List;

/**
 * Insert execute action.
 * 
 */
public class InsertAction extends PrimaryAction implements Message {

  // ///////////////// used for message //////////////
  private boolean isCommited = false;
  private MessageID messageId;

  // ///////////////// used for message //////////////

  /** No need to serialize **/
  private byte[] valueOfEntityGroupKey;

  public InsertAction() {
  }

  /**
   * @param tableName
   * @param primayKey
   * @param cols
   */
  public InsertAction(String tableName, byte[] valueOfEntityGroupKey,
      byte[] primayKey, List<ColumnStruct> cols) {
    this.valueOfEntityGroupKey = valueOfEntityGroupKey;
    this.fTableName = tableName;
    this.combinedPrimaryKey = primayKey;
    this.setColumns(cols);
  }

  /**
   * @param tableName
   * @param primayKey
   */
  public InsertAction(String tableName, byte[] primayKey) {
    this.fTableName = tableName;
    this.combinedPrimaryKey = primayKey;
  }

  /**
   * @return the valueOfEntityGroupKey
   */
  public byte[] getValueOfEntityGroupKey() {
    return valueOfEntityGroupKey;
  }

  /**
   * @param valueOfEntityGroupKey
   *          the valueOfEntityGroupKey to set
   */
  public void setValueOfEntityGroupKey(byte[] valueOfEntityGroupKey) {
    this.valueOfEntityGroupKey = valueOfEntityGroupKey;
  }

  /**
   * 
   * @see org.apache.wasp.messagequeue.Message#getMessageID()
   */
  @Override
  public MessageID getMessageID() {
    return messageId;
  }

  /**
   * 
   * @param messageId
   */
  public void setMessageId(MessageID messageId) {
    this.messageId = messageId;
  }

  /**
   * @return the isCommited
   */
  public boolean isCommited() {
    return isCommited;
  }

  /**
   * @param isCommited
   *          the isCommited to set
   */
  public void setCommited(boolean isCommited) {
    this.isCommited = isCommited;
  }

  /**
   * Convert InsertAction to InsertActionProto.
   * 
   * @param action
   * @return
   */
  public static InsertActionProto convert(InsertAction action) {
    InsertActionProto.Builder builder = InsertActionProto.newBuilder();
    builder.setTableName(action.getFTableName());
    builder.setPrimayKey(ByteString.copyFrom(action.getCombinedPrimaryKey()));
    for (ColumnStruct col : action.getColumns()) {
      builder.addCols(ProtobufUtil.toColumnStructProto(col));
    }
    return builder.build();
  }

  /**
   * Convert InsertActionProto to InsertAction.
   * 
   * @param insertActionProto
   * @return
   */
  public static InsertAction convert(InsertActionProto insertActionProto) {
    InsertAction action = new InsertAction(insertActionProto.getTableName(),
        insertActionProto.getPrimayKey().toByteArray());
    for (ColumnStructProto col : insertActionProto.getColsList()) {
      action.addEntityColumn(col.getTableName(), col.getFamilyName(),
          col.getColumnName(), DataType.convertDataTypeProtosToDataType(col
          .getDataType()), col.getValue().toByteArray());
    }
    return action;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "InsertAction [tableName=" + fTableName + ", primayKey="
        + Arrays.toString(combinedPrimaryKey) + ", columns="
        + this.getColumns() + "]";
  }
}
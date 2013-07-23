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

import com.google.protobuf.ByteString;
import org.apache.wasp.DataType;
import org.apache.wasp.messagequeue.Message;
import org.apache.wasp.messagequeue.MessageID;
import org.apache.wasp.protobuf.ProtobufUtil;
import org.apache.wasp.protobuf.generated.MetaProtos.ColumnStructProto;
import org.apache.wasp.protobuf.generated.MetaProtos.UpdateActionProto;

import java.util.Arrays;

public class UpdateAction extends PrimaryAction implements Message {

  // ///////////////// used for message //////////////
  private boolean isCommited = false;
  private MessageID messageId;

  // ///////////////// used for message //////////////

  public UpdateAction(String tableName, byte[] primayKey) {
    this.combinedPrimaryKey = primayKey;
    this.fTableName = tableName;
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
   * Convert UpdateActionProto to UpdateAction.
   * 
   * @param proto
   * @return
   */
  public static UpdateAction convert(UpdateActionProto updateActionProto) {
    UpdateAction action = new UpdateAction(updateActionProto.getTableName(),
        updateActionProto.getPrimayKey().toByteArray());
    for (ColumnStructProto col : updateActionProto.getColsList()) {
      action.addEntityColumn(col.getTableName(), col.getFamilyName(),
          col.getColumnName(), DataType.convertDataTypeProtosToDataType(col
          .getDataType()), col.getValue().toByteArray());
    }
    return action;
  }

  /**
   * Convert UpdateAction to UpdateActionProto.
   * 
   * @param action
   * @return
   */
  public static UpdateActionProto convert(UpdateAction action) {
    UpdateActionProto.Builder builder = UpdateActionProto.newBuilder();
    builder.setTableName(action.getFTableName());
    builder.setPrimayKey(ByteString.copyFrom(action.getCombinedPrimaryKey()));
    for (ColumnStruct col : action.getColumns()) {
      builder.addCols(ProtobufUtil.toColumnStructProto(col));
    }
    return builder.build();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "UpdateAction [primayKey=" + Arrays.toString(combinedPrimaryKey)
        + ", tableName=" + fTableName + ", columns=" + this.getColumns() + "]";
  }
}
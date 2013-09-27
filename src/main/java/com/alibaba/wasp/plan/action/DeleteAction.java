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

import com.alibaba.wasp.messagequeue.Message;
import com.alibaba.wasp.messagequeue.MessageID;
import com.alibaba.wasp.protobuf.generated.MetaProtos.DeleteActionProto;
import com.google.protobuf.ByteString;

import java.util.Arrays;

public class DeleteAction extends NoColumnPrimaryAction implements Message, DMLAction {

  // ///////////////// used for message //////////////
  private boolean isCommited = false;
  private MessageID messageId;
  // ///////////////// used for message //////////////

  /**
   * @param tableName
   * @param primayKey
   */
  public DeleteAction(String tableName, byte[] primayKey) {
    super();
    this.fTableName = tableName;
    this.combinedPrimaryKey = primayKey;
  }

  /**
   * convert DeleteActionProto to DeleteAction.
   * 
   * @param proto
   * @return
   */
  public static DeleteAction convert(DeleteActionProto deleteActionProto) {
    return new DeleteAction(deleteActionProto.getTableName(), deleteActionProto
        .getPrimayKey().toByteArray());
  }

  /**
   * Convert DeleteAction to DeleteActionProto.
   * 
   * @param action
   * @return
   */
  public static DeleteActionProto convert(DeleteAction action) {
    DeleteActionProto.Builder builder = DeleteActionProto.newBuilder();
    builder.setTableName(action.getFTableName());
    builder.setPrimayKey(ByteString.copyFrom(action.getCombinedPrimaryKey()));
    return builder.build();
  }

  /**
   * 
   * @see com.alibaba.wasp.messagequeue.Message#getMessageID()
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
   * @see Object#toString()
   */
  @Override
  public String toString() {
    return "DeleteAction [tableName=" + fTableName + ", primayKey="
        + Arrays.toString(combinedPrimaryKey) + "]";
  }

  @Override
  public String getTableName() {
    return fTableName;
  }
}
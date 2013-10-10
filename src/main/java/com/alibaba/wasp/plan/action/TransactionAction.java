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
import com.alibaba.wasp.protobuf.generated.MetaProtos;
import com.alibaba.wasp.protobuf.generated.MetaProtos.TransactionActionProto;

import java.util.ArrayList;
import java.util.List;

public class TransactionAction extends Action implements Message, DMLAction {

  // ///////////////// used for message //////////////
  private boolean isCommited = false;
  private MessageID messageId;
  // ///////////////// used for message //////////////

  private List<DMLAction> dmlActions;

  /**
   * @param parentTableName
   * @param dmlActions
   */
  public TransactionAction(String parentTableName, List<DMLAction> dmlActions) {
    super();
    this.fTableName = parentTableName;
    this.dmlActions = dmlActions;
  }

  /**
   * convert TransactionActionProto to TransactionAction.
   *
   * @param proto
   * @return
   */
  public static TransactionAction convert(TransactionActionProto proto) {
    List<DMLAction> dmlActions = new ArrayList<DMLAction>();
    for (MetaProtos.InsertActionProto insertActionProto : proto.getInsertsList()) {
      dmlActions.add(InsertAction.convert(insertActionProto));
    }
    for (MetaProtos.UpdateActionProto updateActionProto : proto.getUpdatesList()) {
      dmlActions.add(UpdateAction.convert(updateActionProto));
    }
    for (MetaProtos.DeleteActionProto deleteActionProto : proto.getDeletesList()) {
      dmlActions.add(DeleteAction.convert(deleteActionProto));
    }
    return new TransactionAction(proto.getTableName(), dmlActions);
  }

  /**
   * Convert TransactionAction to TransactionActionProto.
   *
   * @param action
   * @return
   */
  public static TransactionActionProto convert(TransactionAction action) {
    TransactionActionProto.Builder builder = TransactionActionProto.newBuilder();
    builder.setTableName(action.getFTableName());
    for (DMLAction dmlAction : action.getDmlActions()) {
      if(dmlAction instanceof InsertAction) {
        builder.addInserts(InsertAction.convert((InsertAction) dmlAction));
      }
      if(dmlAction instanceof UpdateAction) {
        builder.addUpdates(UpdateAction.convert((UpdateAction) dmlAction));
      }
      if(dmlAction instanceof DeleteAction) {
        builder.addDeletes(DeleteAction.convert((DeleteAction) dmlAction));
      }
    }
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

  public List<DMLAction> getDmlActions() {
    return dmlActions;
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

  @Override
  public String getTableName() {
    return fTableName;
  }

  /**
   * @see Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("TransactionAction [parentTableName=" + fTableName).append("\n");
    for (DMLAction dmlAction : dmlActions) {
      builder.append(dmlAction).append("\n");
    }
    builder.append("]");
    return builder.toString();
  }
}
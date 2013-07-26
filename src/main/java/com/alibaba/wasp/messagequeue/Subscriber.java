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
package com.alibaba.wasp.messagequeue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.Delete;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.fserver.EntityGroup;
import com.alibaba.wasp.fserver.OperationStatus;
import com.alibaba.wasp.storage.StorageActionManager;
import com.alibaba.wasp.storage.StorageTableNotFoundException;

/**
 * Subscriber of message queue.
 */
public class Subscriber implements MessageQueue<OperationStatus> {

  private final EntityGroup entityGroup;

  private byte[] currentMessageRow;

  private final MessageQueueClient client;

  private StorageActionManager action;

  /**
   * @param entityGroup
   */
  public Subscriber(EntityGroup entityGroup, StorageActionManager action) {
    super();
    this.entityGroup = entityGroup;
    this.client = new MessageQueueClient(this.entityGroup.getEntityGroupInfo(),
        action);
    this.action = action;
  }

  /**
   * @return the entityGroup
   */
  public EntityGroup getEntityGroup() {
    return entityGroup;
  }

  /**
   * @return the currentMessageRow
   */
  public byte[] getCurrentMessageRow() {
    return currentMessageRow;
  }

  /**
   * @param currentMessageRow
   *          the currentMessageRow to set
   */
  public void setCurrentMessageRow(byte[] currentMessageRow) {
    this.currentMessageRow = currentMessageRow;
  }

  /**
   * return recently messages.
   * 
   * @return
   * @throws IOException
   */
  public List<Message> receive() throws IOException {
    try {
      return client.receive();
    } catch (StorageTableNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * @see com.alibaba.wasp.messagequeue.MessageQueue#doAsynchronous()
   */
  @Override
  public OperationStatus doAsynchronous(Message message) {
    if (!message.isCommited()) {
      return OperationStatus.SUCCESS;
    }
    OperationStatus status = entityGroup.doAsynchronous(message);
    if (status.getOperationStatusCode() != OperationStatusCode.SUCCESS) {
      return status;
    } else {
      Delete delete = new Delete(currentMessageRow);
      try {
        this.action.delete(FConstants.MESSAGEQUEUE_TABLENAME, delete);
      } catch (Exception e) {
        return OperationStatus.FAILURE;
      }
      return OperationStatus.SUCCESS;
    }
  }
}

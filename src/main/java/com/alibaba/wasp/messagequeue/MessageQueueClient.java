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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.plan.action.Action;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.generated.MetaProtos.MessageProto;
import com.alibaba.wasp.storage.StorageActionManager;
import com.alibaba.wasp.storage.StorageTableNotFoundException;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Client for message queue.
 * 
 */
public class MessageQueueClient {

  private StorageActionManager action;

  private EntityGroupInfo entityGroupInfo;

  private AtomicLong uniqueID = new AtomicLong(System.currentTimeMillis());

  public MessageQueueClient(EntityGroupInfo entityGroupInfo,
      StorageActionManager action) {
    this.action = action;
    this.entityGroupInfo = entityGroupInfo;
  }

  /**
   * Send message to message queue.
   * 
   * @param message
   * @return
   * @throws IOException
   */
  public MessageID send(Message message) throws IOException {
    MessageID messageID = generateMessageID();
    Put put = new Put(messageID.getMessageId());
    put.add(FConstants.BYTE_MESSAGEQUEUE_FAMILIY,
        FConstants.BYTE_MESSAGEQUEUE_QUALIFIER, toMessage(message));
    put.add(FConstants.BYTE_MESSAGEQUEUE_FAMILIY,
        FConstants.BYTE_COMMIT_QUALIFIER, Bytes.toBytes(0));
    try {
      action.put(FConstants.MESSAGEQUEUE_TABLENAME, put);
    } catch (StorageTableNotFoundException e) {
      throw new IOException(e);
    }
    return messageID;
  }

  /**
   * Receive messages.
   * 
   * @return
   * @throws IOException
   * @throws HBaseTableNotFoundException
   */
  public List<Message> receive() throws IOException,
      StorageTableNotFoundException {
    Scan scan = new Scan();
    byte[] startKey = entityGroupInfo.getEntityGroupName();
    scan.setStartRow(startKey);
    byte[] stopRow = Bytes.add(entityGroupInfo.getEntityGroupName(),
        Bytes.toBytes('a'));
    scan.setStopRow(stopRow);
    ResultScanner scanner = action
        .scan(FConstants.MESSAGEQUEUE_TABLENAME, scan);

    List<Message> messages = new ArrayList<Message>();

    while (true) {
      Result next = scanner.next();
      if (next != null) {
        messages.add(toMessage(next));
      } else {
        break;
      }
    }
    return messages;
  }

  /**
   * unique.
   * 
   * @return
   */
  public MessageID generateMessageID() {
    try {
      return new MessageID(Bytes.add(entityGroupInfo.getEntityGroupName(),
          InetAddress.getLocalHost().getAddress(),
          Bytes.toBytes(uniqueID.incrementAndGet())));
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Let the message to take effect.
   * 
   * @param messageId
   * @throws IOException
   */
  public void commitMessage(MessageID messageId) throws IOException {
    Put put = new Put(messageId.getMessageId());
    put.add(FConstants.BYTE_MESSAGEQUEUE_FAMILIY,
        FConstants.BYTE_COMMIT_QUALIFIER, Bytes.toBytes(1));
    try {
      action.put(FConstants.MESSAGEQUEUE_TABLENAME, put);
    } catch (StorageTableNotFoundException e) {
      throw new IOException(e);
    }
  }

  public void deleteMessage(MessageID messageId) throws IOException {
    Delete delete = new Delete(messageId.getMessageId());
    try {
      action.delete(FConstants.MESSAGEQUEUE_TABLENAME, delete);
    } catch (StorageTableNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Convert value to action.
   * 
   * @param value
   * @return
   * @throws InvalidProtocolBufferException
   */
  public static Message toMessage(org.apache.hadoop.hbase.client.Result result)
      throws InvalidProtocolBufferException {
    byte[] value = result.getValue(FConstants.BYTE_MESSAGEQUEUE_FAMILIY,
        FConstants.BYTE_MESSAGEQUEUE_QUALIFIER);
    byte[] commit = result.getValue(FConstants.BYTE_MESSAGEQUEUE_FAMILIY,
        FConstants.BYTE_COMMIT_QUALIFIER);
    Message message = (Message) ProtobufUtil.convertWriteAction(value);
    message.setCommited(Bytes.toInt(commit) == 1 ? true : false);
    return (Message) message;
  }

  /**
   * Convert message to byte[].
   * 
   * @param message
   * @return
   * @throws InvalidProtocolBufferException
   */
  public static byte[] toMessage(Message message)
      throws InvalidProtocolBufferException {
    Action action = (Action) message;
    MessageProto proto = ProtobufUtil.convertWriteAction(action);
    return proto.toByteArray();
  }
}
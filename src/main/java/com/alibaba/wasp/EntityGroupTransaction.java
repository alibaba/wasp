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
package com.alibaba.wasp;

import com.alibaba.wasp.protobuf.generated.WaspProtos;import com.alibaba.wasp.protobuf.generated.ZooKeeperProtos;import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.executor.EventHandler.EventType;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.generated.WaspProtos;
import com.alibaba.wasp.protobuf.generated.ZooKeeperProtos.EntityGroupTransitionProtos;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Current state of a EntityGroup in transition. Holds state of a EntityGroup as
 * it moves through the steps that take it from offline to open, etc. Used by
 * fserver, fmaster, and zk packages. Encapsulates protobuf
 * serialization/deserialization so we don't leak generated pb outside this
 * class. Create an instance using
 * {@link #createEntityGroupTransition(EventType, byte[], ServerName)}.
 * <p>
 * Immutable
 */
public class EntityGroupTransaction {
  private final ZooKeeperProtos.EntityGroupTransitionProtos egt;

  private EntityGroupTransaction(final ZooKeeperProtos.EntityGroupTransitionProtos egt) {
    this.egt = egt;
  }

  public EventHandler.EventType getEventType() {
    return EventHandler.EventType.get(this.egt.getEventTypeCode());
  }

  public ServerName getServerName() {
    return ProtobufUtil.toServerName(this.egt.getOriginServerName());
  }

  public long getCreateTime() {
    return this.egt.getCreateTime();
  }

  /**
   * @return Full entityGroup name
   */
  public byte[] getEntityGroupName() {
    return this.egt.getEntityGroupName().toByteArray();
  }

  public byte[] getPayload() {
    return this.egt.getPayload().toByteArray();
  }

  @Override
  public String toString() {
    byte[] payload = getPayload();
    return "entityGroup=" + Bytes.toStringBinary(getEntityGroupName())
        + ", state=" + getEventType() + ", servername=" + getServerName()
        + ", createTime=" + this.getCreateTime() + ", payload.length="
        + (payload == null ? 0 : payload.length);
  }

  /**
   * @param type
   * @param entityGroupName
   * @param sn
   * @return a serialized pb {@link EntityGroupTransaction}
   * @see #parseEntityGroupTransition(byte[])
   */
  public static EntityGroupTransaction createEntityGroupTransition(
      final EventType type, final byte[] entityGroupName, final ServerName sn) {
    return createEntityGroupTransition(type, entityGroupName, sn, null);
  }

  /**
   * @param type
   * @param entityGroupName
   * @param sn
   * @param payload
   *          May be null
   * @return a serialized pb {@link com.alibaba.wasp.protobuf.generated.ZooKeeperProtos.EntityGroupTransitionProtos}
   * 
   */
  public static EntityGroupTransaction createEntityGroupTransition(
      final EventType type, final byte[] entityGroup, final ServerName sn,
      final byte[] payload) {
    WaspProtos.ServerName pbsn = WaspProtos.ServerName.newBuilder()
        .setHostName(sn.getHostname()).setPort(sn.getPort())
        .setStartCode(sn.getStartcode()).build();
    ZooKeeperProtos.EntityGroupTransitionProtos.Builder builder = ZooKeeperProtos.EntityGroupTransitionProtos
        .newBuilder().setEventTypeCode(type.getCode())
        .setEntityGroupName(ByteString.copyFrom(entityGroup))
        .setOriginServerName(pbsn);
    builder.setCreateTime(System.currentTimeMillis());
    if (payload != null)
      builder.setPayload(ByteString.copyFrom(payload));
    return new EntityGroupTransaction(builder.build());
  }

  /**
   * @param data
   *          Serialized date to parse.
   * @return A EntityGroupTransition instance made of the passed
   *         <code>data</code>
   * @throws DeserializationException
   * @see #toByteArray()
   */
  public static EntityGroupTransaction parseFrom(final byte[] data)
      throws DeserializationException {
    try {
      ZooKeeperProtos.EntityGroupTransitionProtos rt = ZooKeeperProtos.EntityGroupTransitionProtos.newBuilder()
          .mergeFrom(data, 0, data.length).build();
      return new EntityGroupTransaction(rt);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
  }

  /**
   * @return This instance serialized into a byte array
   * @see #parseFrom(byte[])
   */
  public byte[] toByteArray() {
    return this.egt.toByteArray();
  }
}
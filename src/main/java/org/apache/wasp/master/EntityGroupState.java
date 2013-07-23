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
package org.apache.wasp.master;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.ServerName;
import org.apache.wasp.protobuf.generated.ClusterStatusProtos;

/**
 * State of a entityGroup while undergoing transitions.
 * entityGroup state cannot be modified except the stamp field.
 * So it is almost immutable.
 */
@InterfaceAudience.Private
public class EntityGroupState {
  public enum State {
    OFFLINE,        // entityGroup is in an offline state
    PENDING_OPEN,   // sent rpc to server to open but has not begun
    OPENING,        // server has begun to open but not yet done
    OPEN,           // server opened entityGroup and updated meta
    PENDING_CLOSE,  // sent rpc to server to close but has not begun
    CLOSING,        // server has begun to close but not yet done
    CLOSED,         // server closed entityGroup and updated meta
    SPLITTING,      // server started split of a entityGroup
    SPLIT           // server completed split of a entityGroup
  }

  // Many threads can update the state at the stamp at the same time
  private final AtomicLong stamp;
  private EntityGroupInfo entityGroupInfo;

  private volatile ServerName serverName;
  private volatile State state;

  public EntityGroupState() {
    this.stamp = new AtomicLong(System.currentTimeMillis());
  }

  public EntityGroupState(EntityGroupInfo entityGroupInfo, State state) {
    this(entityGroupInfo, state, System.currentTimeMillis(), null);
  }

  public EntityGroupState(EntityGroupInfo entityGroupInfo,
      State state, long stamp, ServerName serverName) {
    this.entityGroupInfo = entityGroupInfo;
    this.state = state;
    this.stamp = new AtomicLong(stamp);
    this.serverName = serverName;
  }

  public void updateTimestampToNow() {
    this.stamp.set(System.currentTimeMillis());
  }

  public State getState() {
    return state;
  }

  public long getStamp() {
    return stamp.get();
  }

  public EntityGroupInfo getEntityGroup() {
    return entityGroupInfo;
  }

  public ServerName getServerName() {
    return serverName;
  }

  public boolean isClosing() {
    return state == State.CLOSING;
  }

  public boolean isClosed() {
    return state == State.CLOSED;
  }

  public boolean isPendingClose() {
    return state == State.PENDING_CLOSE;
  }

  public boolean isOpening() {
    return state == State.OPENING;
  }

  public boolean isOpened() {
    return state == State.OPEN;
  }

  public boolean isPendingOpen() {
    return state == State.PENDING_OPEN;
  }

  public boolean isOffline() {
    return state == State.OFFLINE;
  }

  public boolean isSplitting() {
    return state == State.SPLITTING;
  }

  public boolean isSplit() {
    return state == State.SPLIT;
  }

  public boolean isPendingOpenOrOpeningOnServer(final ServerName sn) {
    return isOnServer(sn) && (isPendingOpen() || isOpening());
  }

  public boolean isPendingCloseOrClosingOnServer(final ServerName sn) {
    return isOnServer(sn) && (isPendingClose() || isClosing());
  }

  @Override
  public String toString() {
    return "{" + entityGroupInfo.getEntityGroupNameAsString()
      + " state=" + state
      + ", ts=" + stamp
      + ", server=" + serverName + "}";
  }

  /**
   * A slower (but more easy-to-read) stringification 
   */
  public String toDescriptiveString() {
    long lstamp = stamp.get();
    long relTime = System.currentTimeMillis() - lstamp;
    
    return entityGroupInfo.getEntityGroupNameAsString()
      + " state=" + state
      + ", ts=" + new Date(lstamp) + " (" + (relTime/1000) + "s ago)"
      + ", server=" + serverName;
  }

  /**
   * Convert a EntityGroupState to an HBaseProtos.EntityGroupState
   *
   * @return the converted HBaseProtos.EntityGroupState
   */
  public ClusterStatusProtos.EntityGroupStateProtos convert() {
    ClusterStatusProtos.EntityGroupStateProtos.Builder entityGroupState = ClusterStatusProtos.EntityGroupStateProtos
        .newBuilder();
    ClusterStatusProtos.EntityGroupStateProtos.State rs;
    switch (entityGroupState.getState()) {
    case OFFLINE:
      rs = ClusterStatusProtos.EntityGroupStateProtos.State.OFFLINE;
      break;
    case PENDING_OPEN:
      rs = ClusterStatusProtos.EntityGroupStateProtos.State.PENDING_OPEN;
      break;
    case OPENING:
      rs = ClusterStatusProtos.EntityGroupStateProtos.State.OPENING;
      break;
    case OPEN:
      rs = ClusterStatusProtos.EntityGroupStateProtos.State.OPEN;
      break;
    case PENDING_CLOSE:
      rs = ClusterStatusProtos.EntityGroupStateProtos.State.PENDING_CLOSE;
      break;
    case CLOSING:
      rs = ClusterStatusProtos.EntityGroupStateProtos.State.CLOSING;
      break;
    case CLOSED:
      rs = ClusterStatusProtos.EntityGroupStateProtos.State.CLOSED;
      break;
    case SPLITTING:
      rs = ClusterStatusProtos.EntityGroupStateProtos.State.SPLITTING;
      break;
    case SPLIT:
      rs = ClusterStatusProtos.EntityGroupStateProtos.State.SPLIT;
      break;
    default:
      throw new IllegalStateException("");
    }
    entityGroupState.setEntityGroupInfo(EntityGroupInfo
        .convert(entityGroupInfo));
    entityGroupState.setState(rs);
    entityGroupState.setStamp(getStamp());
    return entityGroupState.build();
  }

  /**
   * Convert a protobuf HBaseProtos.EntityGroupState to a EntityGroupState
   *
   * @return the EntityGroupState
   */
  public static EntityGroupState convert(
      ClusterStatusProtos.EntityGroupStateProtos proto) {
    EntityGroupState.State state;
    switch (proto.getState()) {
    case OFFLINE:
      state = State.OFFLINE;
      break;
    case PENDING_OPEN:
      state = State.PENDING_OPEN;
      break;
    case OPENING:
      state = State.OPENING;
      break;
    case OPEN:
      state = State.OPEN;
      break;
    case PENDING_CLOSE:
      state = State.PENDING_CLOSE;
      break;
    case CLOSING:
      state = State.CLOSING;
      break;
    case CLOSED:
      state = State.CLOSED;
      break;
    case SPLITTING:
      state = State.SPLITTING;
      break;
    case SPLIT:
      state = State.SPLIT;
      break;
    default:
      throw new IllegalStateException("");
    }

    return new EntityGroupState(EntityGroupInfo.convert(proto
        .getEntityGroupInfo()), state, proto.getStamp(), null);
  }

  public boolean isOnServer(final ServerName sn) {
    return serverName != null && serverName.equals(sn);
  }

}

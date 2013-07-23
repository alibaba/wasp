/**
 * Copyright The Apache Software Foundation
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

package org.apache.wasp;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.wasp.protobuf.generated.WaspProtos.EntityGroupLoadProtos;
import org.apache.wasp.protobuf.generated.WaspProtos.ServerLoadProtos;

/**
 * This class is used for exporting current state of load on a FServer.
 */
public class ServerLoad {
  private int readRequestsCount = 0;
  private int writeRequestsCount = 0;

  public ServerLoad(ServerLoadProtos serverLoad) {
    this.serverLoad = serverLoad;
    for (EntityGroupLoadProtos egl : serverLoad.getEntityGroupsLoadsList()) {
      readRequestsCount += egl.getReadRequestsCount();
      writeRequestsCount += egl.getWriteRequestsCount();
    }
  }

  public ServerLoadProtos obtainServerLoadPB() {
    return serverLoad;
  }

  protected ServerLoadProtos serverLoad;

  /* @return number of requests since last report. */
  public int getNumberOfRequests() {
    return serverLoad.getNumberOfRequests();
  }

  public boolean hasNumberOfRequests() {
    return serverLoad.hasNumberOfRequests();
  }

  /* @return total Number of requests from the start of the fServer. */
  public int getTotalNumberOfRequests() {
    return serverLoad.getTotalNumberOfRequests();
  }

  public boolean hasTotalNumberOfRequests() {
    return serverLoad.hasTotalNumberOfRequests();
  }

  public int getReadRequestsCount() {
    return readRequestsCount;
  }

  public int getWriteRequestsCount() {
    return writeRequestsCount;
  }

  /**
   * @return the number of entityGroups
   */
  public int getNumberOfEntityGroups() {
    return serverLoad.getEntityGroupsLoadsCount();
  }

  /**
   * Originally, this method factored in the effect of requests going to the
   * server as well. However, this does not interact very well with the current
   * entityGroup rebalancing code, which only factors number of entityGroups.
   * For the interim, until we can figure out how to make rebalancing use all
   * the info available, we're just going to make load purely the number of
   * entityGroups.
   * 
   * @return load factor for this server
   */
  public int getLoad() {
    return getNumberOfEntityGroups();
  }

  /**
   * @return entityGroup load metrics
   */
  public Map<byte[], EntityGroupLoad> getEntityGroupLoad() {
    Map<byte[], EntityGroupLoad> entityGroupLoads = new TreeMap<byte[], EntityGroupLoad>(
        Bytes.BYTES_COMPARATOR);
    for (EntityGroupLoadProtos egl : serverLoad.getEntityGroupsLoadsList()) {
      EntityGroupLoad entityGroupLoad = new EntityGroupLoad(egl);
      entityGroupLoads.put(entityGroupLoad.getName(), entityGroupLoad);
    }
    return entityGroupLoads;
  }

  /**
   * @return number of requests per second received since the last report
   */
  public double getRequestsPerSecond() {
    return getNumberOfRequests();
  }

  /**
   * @return the number of clients that connect to this server.
   */
  public int getNumberOfConnections() {
    return serverLoad.getNumberOfConnections();
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder sb = Strings.appendKeyValue(new StringBuilder(),
        "requestsPerSecond", Double.valueOf(getRequestsPerSecond()));
    Strings.appendKeyValue(sb, "numberOfOnlineEntityGroups", Integer
        .valueOf(getNumberOfEntityGroups()));
    sb = Strings.appendKeyValue(sb, "readRequestsCount", Long
        .valueOf(this.readRequestsCount));
    sb = Strings.appendKeyValue(sb, "writeRequestsCount", Long
        .valueOf(this.writeRequestsCount));
    sb = Strings.appendKeyValue(sb, "NumberOfConnections", Long.valueOf(getNumberOfConnections()));

    return sb.toString();
  }

  public static final ServerLoad EMPTY_SERVERLOAD = new ServerLoad(
      ServerLoadProtos.newBuilder().build());

  public int getInfoServerPort() {
    return serverLoad.getInfoServerPort();
  }
}

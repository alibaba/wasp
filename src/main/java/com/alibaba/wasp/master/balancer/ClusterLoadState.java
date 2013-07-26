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
package com.alibaba.wasp.master.balancer;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.alibaba.wasp.ServerName;import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.ServerName;

/**
 * Class used to hold the current state of the cluster and how balanced it is.
 */
public class ClusterLoadState {
  private final Map<ServerName, List<EntityGroupInfo>> clusterState;
  private final NavigableMap<ServerAndLoad, List<EntityGroupInfo>> serversByLoad;
  private boolean emptyFServerPresent = false;
  private int numEntityGroups = 0;
  private int numServers = 0;

  public ClusterLoadState(Map<ServerName, List<EntityGroupInfo>> clusterState) {
    super();
    this.numEntityGroups = 0;
    this.numServers = clusterState.size();
    this.clusterState = clusterState;
    serversByLoad = new TreeMap<ServerAndLoad, List<EntityGroupInfo>>();
    // Iterate so we can count entityGroups as we build the map
    for (Map.Entry<ServerName, List<EntityGroupInfo>> server : clusterState
        .entrySet()) {
      List<EntityGroupInfo> entityGroups = server.getValue();
      int sz = entityGroups.size();
      if (sz == 0)
        emptyFServerPresent = true;
      numEntityGroups += sz;
      serversByLoad.put(new ServerAndLoad(server.getKey(), sz), entityGroups);
    }
  }

  Map<ServerName, List<EntityGroupInfo>> getClusterState() {
    return clusterState;
  }

  NavigableMap<ServerAndLoad, List<EntityGroupInfo>> getServersByLoad() {
    return serversByLoad;
  }

  boolean isEmptyFServerPresent() {
    return emptyFServerPresent;
  }

  int getNumEntityGroups() {
    return numEntityGroups;
  }

  int getNumServers() {
    return numServers;
  }

  float getLoadAverage() {
    return (float) numEntityGroups / numServers;
  }

  int getMinLoad() {
    return getServersByLoad().lastKey().getLoad();
  }

  int getMaxLoad() {
    return getServersByLoad().firstKey().getLoad();
  }

}

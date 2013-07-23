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

package org.apache.wasp.master.balancer;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.ServerName;
import org.apache.wasp.master.EntityGroupPlan;

public class TestBalancerBase {

  private static Random rand = new Random();
  static int entityGroupId = 0;

  /**
   * Invariant is that all servers have between floor(avg) and ceiling(avg)
   * number of entityGroups.
   */
  public void assertClusterAsBalanced(List<ServerAndLoad> servers) {
    int numServers = servers.size();
    int numEntityGroups = 0;
    int maxEntityGroups = 0;
    int minEntityGroups = Integer.MAX_VALUE;
    for (ServerAndLoad server : servers) {
      int nr = server.getLoad();
      if (nr > maxEntityGroups) {
        maxEntityGroups = nr;
      }
      if (nr < minEntityGroups) {
        minEntityGroups = nr;
      }
      numEntityGroups += nr;
    }
    if (maxEntityGroups - minEntityGroups < 2) {
      // less than 2 between max and min, can't balance
      return;
    }
    int min = numEntityGroups / numServers;
    int max = numEntityGroups % numServers == 0 ? min : min + 1;

    for (ServerAndLoad server : servers) {
      assertTrue(server.getLoad() >= 0);
      assertTrue(server.getLoad() <= max);
      assertTrue(server.getLoad() >= min);
    }
  }

  protected String printStats(List<ServerAndLoad> servers) {
    int numServers = servers.size();
    int totalEntityGroups = 0;
    for (ServerAndLoad server : servers) {
      totalEntityGroups += server.getLoad();
    }
    float average = (float) totalEntityGroups / numServers;
    int max = (int) Math.ceil(average);
    int min = (int) Math.floor(average);
    return "[srvr=" + numServers + " rgns=" + totalEntityGroups + " avg=" + average
        + " max=" + max + " min=" + min + "]";
  }

  protected List<ServerAndLoad> convertToList(
      final Map<ServerName, List<EntityGroupInfo>> servers) {
    List<ServerAndLoad> list = new ArrayList<ServerAndLoad>(servers.size());
    for (Map.Entry<ServerName, List<EntityGroupInfo>> e : servers.entrySet()) {
      list.add(new ServerAndLoad(e.getKey(), e.getValue().size()));
    }
    return list;
  }

  protected String printMock(List<ServerAndLoad> balancedCluster) {
    SortedSet<ServerAndLoad> sorted = new TreeSet<ServerAndLoad>(
        balancedCluster);
    ServerAndLoad[] arr = sorted.toArray(new ServerAndLoad[sorted.size()]);
    StringBuilder sb = new StringBuilder(sorted.size() * 4 + 4);
    sb.append("{ ");
    for (int i = 0; i < arr.length; i++) {
      if (i != 0) {
        sb.append(" , ");
      }
      sb.append(arr[i].getServerName().getHostname());
      sb.append(":");
      sb.append(arr[i].getLoad());
    }
    sb.append(" }");
    return sb.toString();
  }

  /**
   * This assumes the EntityGroupPlan HSI instances are the same ones in the map, so
   * actually no need to even pass in the map, but I think it's clearer.
   * 
   * @param list
   * @param plans
   * @return
   */
  protected List<ServerAndLoad> reconcile(List<ServerAndLoad> list,
      List<EntityGroupPlan> plans) {
    List<ServerAndLoad> result = new ArrayList<ServerAndLoad>(list.size());
    if (plans == null)
      return result;
    Map<ServerName, ServerAndLoad> map = new HashMap<ServerName, ServerAndLoad>(
        list.size());
    for (ServerAndLoad sl : list) {
      map.put(sl.getServerName(), sl);
    }
    for (EntityGroupPlan plan : plans) {
      ServerName source = plan.getSource();
      updateLoad(map, source, -1);
      ServerName destination = plan.getDestination();
      updateLoad(map, destination, +1);
    }
    result.clear();
    result.addAll(map.values());
    return result;
  }

  protected void updateLoad(final Map<ServerName, ServerAndLoad> map,
      final ServerName sn, final int diff) {
    ServerAndLoad sal = map.get(sn);
    if (sal == null)
      sal = new ServerAndLoad(sn, 0);
    sal = new ServerAndLoad(sn, sal.getLoad() + diff);
    map.put(sn, sal);
  }

  protected Map<ServerName, List<EntityGroupInfo>> mockClusterServers(
      int[] mockCluster) {
    int numServers = mockCluster.length;
    Map<ServerName, List<EntityGroupInfo>> servers = new TreeMap<ServerName, List<EntityGroupInfo>>();
    for (int i = 0; i < numServers; i++) {
      int numEntityGroups = mockCluster[i];
      ServerAndLoad sal = randomServer(0);
      List<EntityGroupInfo> entityGroups = randomEntityGroups(numEntityGroups);
      servers.put(sal.getServerName(), entityGroups);
    }
    return servers;
  }

  private Queue<EntityGroupInfo> entityGroupQueue = new LinkedList<EntityGroupInfo>();

  protected List<EntityGroupInfo> randomEntityGroups(int numEntityGroups) {
    List<EntityGroupInfo> entityGroups = new ArrayList<EntityGroupInfo>(numEntityGroups);
    byte[] start = new byte[16];
    byte[] end = new byte[16];
    rand.nextBytes(start);
    rand.nextBytes(end);
    for (int i = 0; i < numEntityGroups; i++) {
      if (!entityGroupQueue.isEmpty()) {
        entityGroups.add(entityGroupQueue.poll());
        continue;
      }
      Bytes.putInt(start, 0, numEntityGroups << 1);
      Bytes.putInt(end, 0, (numEntityGroups << 1) + 1);
      EntityGroupInfo egi = new EntityGroupInfo(Bytes.toBytes("table" + i),
          start, end, false, entityGroupId++);
      entityGroups.add(egi);
    }
    return entityGroups;
  }

  protected void returnEntityGroups(List<EntityGroupInfo> entityGroups) {
    entityGroupQueue.addAll(entityGroups);
  }

  private Queue<ServerName> serverQueue = new LinkedList<ServerName>();

  protected ServerAndLoad randomServer(final int numEntityGroupsPerServer) {
    if (!this.serverQueue.isEmpty()) {
      ServerName sn = this.serverQueue.poll();
      return new ServerAndLoad(sn, numEntityGroupsPerServer);
    }
    String host = "srv" + rand.nextInt(100000);
    int port = rand.nextInt(60000);
    long startCode = rand.nextLong();
    ServerName sn = new ServerName(host, port, startCode);
    return new ServerAndLoad(sn, numEntityGroupsPerServer);
  }

  protected List<ServerAndLoad> randomServers(int numServers,
      int numEntityGroupsPerServer) {
    List<ServerAndLoad> servers = new ArrayList<ServerAndLoad>(numServers);
    for (int i = 0; i < numServers; i++) {
      servers.add(randomServer(numEntityGroupsPerServer));
    }
    return servers;
  }

  protected void returnServer(ServerName server) {
    serverQueue.add(server);
  }

  protected void returnServers(List<ServerName> servers) {
    this.serverQueue.addAll(servers);
  }

}

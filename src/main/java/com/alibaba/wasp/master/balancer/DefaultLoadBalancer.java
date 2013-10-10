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

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.master.EntityGroupPlan;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

/**
 * Makes decisions about the placement and movement of EntityGroups across
 * FServers.
 * 
 * <p>
 * Cluster-wide load balancing will occur only when there are no entityGroups in
 * transition and according to a fixed period of a time using
 * {@link #balanceCluster(java.util.Map)}.
 * 
 * <p>
 * Inline entityGroup placement with {@link #immediateAssignment} can be used
 * when the Master needs to handle closed entityGroups that it currently does
 * not have a destination set for. This can happen during master failover.
 * 
 * <p>
 * On cluster startup, bulk assignment can be used to determine locations for
 * all EntityGroups in a cluster.
 * 
 * <p>
 * This classes produces plans for the {@link EntityGroupManager} to execute.
 */
public class DefaultLoadBalancer extends BaseLoadBalancer {
  private static final Log LOG = LogFactory.getLog(DefaultLoadBalancer.class);
  private static final Random RANDOM = new Random(System.currentTimeMillis());

  private EntityGroupInfoComparator riComparator = new EntityGroupInfoComparator();
  private EntityGroupPlan.EntityGroupPlanComparator rpComparator = new EntityGroupPlan.EntityGroupPlanComparator();

  /**
   * Stores additional per-server information about the entityGroups added/removed
   * during the run of the balancing algorithm.
   * 
   * For servers that shed entityGroups, we need to track which entityGroups we have
   * already shed. <b>nextEntityGroupForUnload</b> contains the index in the list of
   * entityGroups on the server that is the next to be shed.
   */
  static class BalanceInfo {

    private final int nextEntityGroupForUnload;
    private int numEntityGroupsAdded;

    public BalanceInfo(int nextEntityGroupForUnload, int numEntityGroupsAdded) {
      this.nextEntityGroupForUnload = nextEntityGroupForUnload;
      this.numEntityGroupsAdded = numEntityGroupsAdded;
    }

    int getNextEntityGroupForUnload() {
      return nextEntityGroupForUnload;
    }

    int getNumEntityGroupsAdded() {
      return numEntityGroupsAdded;
    }

    void setNumEntityGroupsAdded(int numAdded) {
      this.numEntityGroupsAdded = numAdded;
    }
  }

  /**
   * Generate a global load balancing plan according to the specified map of
   * server information to the most loaded entityGroups of each server.
   * 
   * The load balancing invariant is that all servers are within 1 entityGroup of the
   * average number of entityGroups per server. If the average is an integer number,
   * all servers will be balanced to the average. Otherwise, all servers will
   * have either floor(average) or ceiling(average) entityGroups.
   * 
   * HBASE-3609 Modeled entityGroupsToMove using Guava's MinMaxPriorityQueue so that
   * we can fetch from both ends of the queue. At the beginning, we check
   * whether there was empty entityGroup server just discovered by Master. If so, we
   * alternately choose new / old entityGroups from head / tail of entityGroupsToMove,
   * respectively. This alternation avoids clustering young entityGroups on the newly
   * discovered entityGroup server. Otherwise, we choose new entityGroups from head of
   * entityGroupsToMove.
   * 
   * Another improvement from HBASE-3609 is that we assign entityGroups from
   * entityGroupsToMove to underloaded servers in round-robin fashion. Previously one
   * underloaded server would be filled before we move onto the next underloaded
   * server, leading to clustering of young entityGroups.
   * 
   * Finally, we randomly shuffle underloaded servers so that they receive
   * offloaded entityGroups relatively evenly across calls to balanceCluster().
   * 
   * The algorithm is currently implemented as such:
   * 
   * <ol>
   * <li>Determine the two valid numbers of entityGroups each server should have,
   * <b>MIN</b>=floor(average) and <b>MAX</b>=ceiling(average).
   * 
   * <li>Iterate down the most loaded servers, shedding entityGroups from each so
   * each server hosts exactly <b>MAX</b> entityGroups. Stop once you reach a server
   * that already has &lt;= <b>MAX</b> entityGroups.
   * <p>
   * Order the entityGroups to move from most recent to least.
   * 
   * <li>Iterate down the least loaded servers, assigning entityGroups so each server
   * has exactly </b>MIN</b> entityGroups. Stop once you reach a server that already
   * has &gt;= <b>MIN</b> entityGroups.
   * 
   * EntityGroups being assigned to underloaded servers are those that were shed in
   * the previous step. It is possible that there were not enough entityGroups shed
   * to fill each underloaded server to <b>MIN</b>. If so we end up with a
   * number of entityGroups required to do so, <b>neededEntityGroups</b>.
   * 
   * It is also possible that we were able to fill each underloaded but ended up
   * with entityGroups that were unassigned from overloaded servers but that still do
   * not have assignment.
   * 
   * If neither of these conditions hold (no entityGroups needed to fill the
   * underloaded servers, no entityGroups leftover from overloaded servers), we are
   * done and return. Otherwise we handle these cases below.
   * 
   * <li>If <b>neededEntityGroups</b> is non-zero (still have underloaded servers),
   * we iterate the most loaded servers again, shedding a single server from
   * each (this brings them from having <b>MAX</b> entityGroups to having <b>MIN</b>
   * entityGroups).
   * 
   * <li>We now definitely have more entityGroups that need assignment, either from
   * the previous step or from the original shedding from overloaded servers.
   * Iterate the least loaded servers filling each to <b>MIN</b>.
   * 
   * <li>If we still have more entityGroups that need assignment, again iterate the
   * least loaded servers, this time giving each one (filling them to
   * </b>MAX</b>) until we run out.
   * 
   * <li>All servers will now either host <b>MIN</b> or <b>MAX</b> entityGroups.
   * 
   * In addition, any server hosting &gt;= <b>MAX</b> entityGroups is guaranteed to
   * end up with <b>MAX</b> entityGroups at the end of the balancing. This ensures
   * the minimal number of entityGroups possible are moved.
   * </ol>
   * 
   * TODO: We can at-most reassign the number of entityGroups away from a particular
   * server to be how many they report as most loaded. Should we just keep all
   * assignment in memory? Any objections? Does this mean we need HeapSize on
   * HMaster? Or just careful monitor? (current thinking is we will hold all
   * assignments in memory)
   * 
   * @param clusterState Map of entityGroupservers and their load/entityGroup information
   *          to a list of their most loaded entityGroups
   * @return a list of entityGroups to be moved, including source and destination, or
   *         null if cluster is already balanced
   */
  public List<EntityGroupPlan> balanceCluster(
      Map<ServerName, List<EntityGroupInfo>> clusterMap) {
    boolean emptyFServerPresent = false;
    long startTime = System.currentTimeMillis();

    ClusterLoadState cs = new ClusterLoadState(clusterMap);

    int numServers = cs.getNumServers();
    if (numServers == 0) {
      LOG.debug("numServers=0 so skipping load balancing");
      return null;
    }
    NavigableMap<ServerAndLoad, List<EntityGroupInfo>> serversByLoad = cs
        .getServersByLoad();

    int numEntityGroups = cs.getNumEntityGroups();

    if (!this.needsBalance(cs)) {
      // Skipped because no server outside (min,max) range
      float average = cs.getLoadAverage(); // for logging
      LOG.info("Skipping load balancing because balanced cluster; "
          + "servers=" + numServers + " " + "entityGroups=" + numEntityGroups
          + " average=" + average + " " + "mostloaded="
          + serversByLoad.lastKey().getLoad() + " leastloaded="
          + serversByLoad.firstKey().getLoad());
      return null;
    }

    int min = numEntityGroups / numServers;
    int max = numEntityGroups % numServers == 0 ? min : min + 1;

    // Using to check balance result.
    StringBuilder strBalanceParam = new StringBuilder();
    strBalanceParam.append("Balance parameter: numEntityGroups=").append(numEntityGroups)
        .append(", numServers=").append(numServers).append(", max=")
        .append(max).append(", min=").append(min);
    LOG.debug(strBalanceParam.toString());

    // Balance the cluster
    // TODO: Look at data block locality or a more complex load to do this
    MinMaxPriorityQueue<EntityGroupPlan> entityGroupsToMove = MinMaxPriorityQueue
        .orderedBy(rpComparator).create();
    List<EntityGroupPlan> entityGroupsToReturn = new ArrayList<EntityGroupPlan>();

    // Walk down most loaded, pruning each to the max
    int serversOverloaded = 0;
    // flag used to fetch entityGroups from head and tail of list, alternately
    boolean fetchFromTail = false;
    Map<ServerName, BalanceInfo> serverBalanceInfo = new TreeMap<ServerName, BalanceInfo>();
    for (Map.Entry<ServerAndLoad, List<EntityGroupInfo>> server : serversByLoad
        .descendingMap().entrySet()) {
      ServerAndLoad sal = server.getKey();
      int entityGroupCount = sal.getLoad();
      if (entityGroupCount <= max) {
        serverBalanceInfo.put(sal.getServerName(), new BalanceInfo(0, 0));
        break;
      }
      serversOverloaded++;
      List<EntityGroupInfo> entityGroups = server.getValue();
      int numToOffload = Math.min(entityGroupCount - max, entityGroups.size());
      // account for the out-of-band entityGroups which were assigned to this server
      // after some other entityGroup server crashed
      Collections.sort(entityGroups, riComparator);
      int numTaken = 0;
      for (int i = 0; i <= numToOffload;) {
        EntityGroupInfo egInfo = entityGroups.get(i); // fetch from head
        if (fetchFromTail) {
          egInfo = entityGroups.get(entityGroups.size() - 1 - i);
        }
        i++;
        entityGroupsToMove.add(new EntityGroupPlan(egInfo, sal.getServerName(),
            null));
        numTaken++;
        if (numTaken >= numToOffload)
          break;
        // fetch in alternate order if there is new entityGroup server
        if (emptyFServerPresent) {
          fetchFromTail = !fetchFromTail;
        }
      }
      serverBalanceInfo.put(sal.getServerName(), new BalanceInfo(numToOffload,
          (-1) * numTaken));
    }
    int totalNumMoved = entityGroupsToMove.size();

    // Walk down least loaded, filling each to the min
    int neededEntityGroups = 0; // number of entityGroups needed to bring all up to min
    fetchFromTail = false;

    Map<ServerName, Integer> underloadedServers = new HashMap<ServerName, Integer>();
    for (Map.Entry<ServerAndLoad, List<EntityGroupInfo>> server : serversByLoad
        .entrySet()) {
      int entityGroupCount = server.getKey().getLoad();
      if (entityGroupCount >= min) {
        break;
      }
      underloadedServers
          .put(server.getKey().getServerName(), min - entityGroupCount);
    }
    // number of servers that get new entityGroups
    int serversUnderloaded = underloadedServers.size();
    int incr = 1;
    List<ServerName> sns = Arrays.asList(underloadedServers.keySet().toArray(
        new ServerName[serversUnderloaded]));
    Collections.shuffle(sns, RANDOM);
    while (entityGroupsToMove.size() > 0) {
      int cnt = 0;
      int i = incr > 0 ? 0 : underloadedServers.size() - 1;
      for (; i >= 0 && i < underloadedServers.size(); i += incr) {
        if (entityGroupsToMove.isEmpty())
          break;
        ServerName si = sns.get(i);
        int numToTake = underloadedServers.get(si);
        if (numToTake == 0)
          continue;

        addEntityGroupPlan(entityGroupsToMove, fetchFromTail, si, entityGroupsToReturn);
        if (emptyFServerPresent) {
          fetchFromTail = !fetchFromTail;
        }

        underloadedServers.put(si, numToTake - 1);
        cnt++;
        BalanceInfo bi = serverBalanceInfo.get(si);
        if (bi == null) {
          bi = new BalanceInfo(0, 0);
          serverBalanceInfo.put(si, bi);
        }
        bi.setNumEntityGroupsAdded(bi.getNumEntityGroupsAdded() + 1);
      }
      if (cnt == 0)
        break;
      // iterates underloadedServers in the other direction
      incr = -incr;
    }
    for (Integer i : underloadedServers.values()) {
      // If we still want to take some, increment needed
      neededEntityGroups += i;
    }

    // If none needed to fill all to min and none left to drain all to max,
    // we are done
    if (neededEntityGroups == 0 && entityGroupsToMove.isEmpty()) {
      long endTime = System.currentTimeMillis();
      LOG.info("Calculated a load balance in " + (endTime - startTime) + "ms. "
          + "Moving " + totalNumMoved + " entityGroups off of " + serversOverloaded
          + " overloaded servers onto " + serversUnderloaded
          + " less loaded servers");
      return entityGroupsToReturn;
    }

    // Need to do a second pass.
    // Either more entityGroups to assign out or servers that are still underloaded

    // If we need more to fill min, grab one from each most loaded until enough
    if (neededEntityGroups != 0) {
      // Walk down most loaded, grabbing one from each until we get enough
      for (Map.Entry<ServerAndLoad, List<EntityGroupInfo>> server : serversByLoad
          .descendingMap().entrySet()) {
        BalanceInfo balanceInfo = serverBalanceInfo.get(server.getKey()
            .getServerName());
        int idx = balanceInfo == null ? 0 : balanceInfo
            .getNextEntityGroupForUnload();
        if (idx >= server.getValue().size())
          break;
        EntityGroupInfo entityGroup = server.getValue().get(idx);
        entityGroupsToMove.add(new EntityGroupPlan(entityGroup, server.getKey()
            .getServerName(), null));
        totalNumMoved++;
        if (--neededEntityGroups == 0) {
          // No more entityGroups needed, done shedding
          break;
        }
      }
    }

    // Now we have a set of entityGroups that must be all assigned out
    // Assign each underloaded up to the min, then if leftovers, assign to max

    // Walk down least loaded, assigning to each to fill up to min
    for (Map.Entry<ServerAndLoad, List<EntityGroupInfo>> server : serversByLoad
        .entrySet()) {
      int entityGroupCount = server.getKey().getLoad();
      if (entityGroupCount >= min)
        break;
      BalanceInfo balanceInfo = serverBalanceInfo.get(server.getKey()
          .getServerName());
      if (balanceInfo != null) {
        entityGroupCount += balanceInfo.getNumEntityGroupsAdded();
      }
      if (entityGroupCount >= min) {
        continue;
      }
      int numToTake = min - entityGroupCount;
      int numTaken = 0;
      while (numTaken < numToTake && 0 < entityGroupsToMove.size()) {
        addEntityGroupPlan(entityGroupsToMove, fetchFromTail, server.getKey()
            .getServerName(), entityGroupsToReturn);
        numTaken++;
        if (emptyFServerPresent) {
          fetchFromTail = !fetchFromTail;
        }
      }
    }

    // If we still have entityGroups to dish out, assign underloaded to max
    if (0 < entityGroupsToMove.size()) {
      for (Map.Entry<ServerAndLoad, List<EntityGroupInfo>> server : serversByLoad
          .entrySet()) {
        int entityGroupCount = server.getKey().getLoad();
        if (entityGroupCount >= max) {
          break;
        }
        addEntityGroupPlan(entityGroupsToMove, fetchFromTail, server.getKey()
            .getServerName(), entityGroupsToReturn);
        if (emptyFServerPresent) {
          fetchFromTail = !fetchFromTail;
        }
        if (entityGroupsToMove.isEmpty()) {
          break;
        }
      }
    }

    long endTime = System.currentTimeMillis();

    if (!entityGroupsToMove.isEmpty() || neededEntityGroups != 0) {
      // Emit data so can diagnose how balancer went astray.
      LOG.warn("entityGroupsToMove=" + totalNumMoved + ", numServers=" + numServers
          + ", serversOverloaded=" + serversOverloaded
          + ", serversUnderloaded=" + serversUnderloaded);
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<ServerName, List<EntityGroupInfo>> e : clusterMap.entrySet()) {
        if (sb.length() > 0)
          sb.append(", ");
        sb.append(e.getKey().toString());
        sb.append(" ");
        sb.append(e.getValue().size());
      }
      LOG.warn("Input " + sb.toString());
    }

    // All done!
    LOG.info("Done. Calculated a load balance in " + (endTime - startTime)
        + "ms. " + "Moving " + totalNumMoved + " entityGroups off of "
        + serversOverloaded + " overloaded servers onto " + serversUnderloaded
        + " less loaded servers");

    return entityGroupsToReturn;
  }

  /**
   * Add a entityGroup from the head or tail to the List of entityGroups to return.
   */
  private void addEntityGroupPlan(
      final MinMaxPriorityQueue<EntityGroupPlan> entityGroupsToMove,
      final boolean fetchFromTail, final ServerName sn,
      List<EntityGroupPlan> entityGroupsToReturn) {
    EntityGroupPlan rp = null;
    if (!fetchFromTail)
      rp = entityGroupsToMove.remove();
    else
      rp = entityGroupsToMove.removeLast();
    rp.setDestination(sn);
    entityGroupsToReturn.add(rp);
  }
}

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
package org.apache.wasp.master.balancer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.wasp.ClusterStatus;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.ServerName;
import org.apache.wasp.master.FMasterServices;
import org.apache.wasp.master.LoadBalancer;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;

/**
 * The base class for load balancers. It provides the the functions used to by
 * {@link EntityGroupManager} to assign entityGroups in the edge cases. It
 * doesn't provide an implementation of the actual balancing algorithm.
 * 
 */
public abstract class BaseLoadBalancer implements LoadBalancer {

  // slop for entityGroups
  private float slop;
  private Configuration config;
  private static final Random RANDOM = new Random(System.currentTimeMillis());
  private static final Log LOG = LogFactory.getLog(BaseLoadBalancer.class);

  protected FMasterServices services;

  @Override
  public void setConf(Configuration conf) {
    this.slop = conf.getFloat("hbase.entityGroups.slop", (float) 0.2);
    if (slop < 0)
      slop = 0;
    else if (slop > 1)
      slop = 1;
    this.config = conf;
  }

  @Override
  public Configuration getConf() {
    return this.config;
  }

  public void setClusterStatus(ClusterStatus st) {
    // Not used except for the StocasticBalancer
  }

  public void setMasterServices(FMasterServices masterServices) {
    this.services = masterServices;
  }

  protected boolean needsBalance(ClusterLoadState cs) {
    // Check if we even need to do any load balancing
    float average = cs.getLoadAverage(); // for logging
    // HBASE-3681 check sloppiness first
    int floor = (int) Math.floor(average * (1 - slop));
    int ceiling = (int) Math.ceil(average * (1 + slop));

    return cs.getMinLoad() > ceiling || cs.getMaxLoad() < floor;
  }

  /**
   * Generates a bulk assignment plan to be used on cluster startup using a
   * simple round-robin assignment.
   * <p>
   * Takes a list of all the entityGroups and all the servers in the cluster and
   * returns a map of each server to the entityGroups that it should be assigned.
   * <p>
   * Currently implemented as a round-robin assignment. Same invariant as load
   * balancing, all servers holding floor(avg) or ceiling(avg).
   * 
   * TODO: Use block locations from HDFS to place entityGroups with their blocks
   * 
   * @param entityGroups all entityGroups
   * @param servers all servers
   * @return map of server to the entityGroups it should take, or null if no
   *         assignment is possible (ie. no entityGroups or no servers)
   */
  public Map<ServerName, List<EntityGroupInfo>> roundRobinAssignment(
      List<EntityGroupInfo> entityGroups, List<ServerName> servers) {
    if (entityGroups.isEmpty() || servers.isEmpty()) {
      return null;
    }
    Map<ServerName, List<EntityGroupInfo>> assignments = new TreeMap<ServerName, List<EntityGroupInfo>>();
    int numEntityGroups = entityGroups.size();
    int numServers = servers.size();
    int max = (int) Math.ceil((float) numEntityGroups / numServers);
    int serverIdx = 0;
    if (numServers > 1) {
      serverIdx = RANDOM.nextInt(numServers);
    }
    int entityGroupIdx = 0;
    for (int j = 0; j < numServers; j++) {
      ServerName server = servers.get((j + serverIdx) % numServers);
      List<EntityGroupInfo> serverEntityGroups = new ArrayList<EntityGroupInfo>(max);
      for (int i = entityGroupIdx; i < numEntityGroups; i += numServers) {
        serverEntityGroups.add(entityGroups.get(i % numEntityGroups));
      }
      assignments.put(server, serverEntityGroups);
      entityGroupIdx++;
    }
    return assignments;
  }

  /**
   * Generates an immediate assignment plan to be used by a new master for
   * entityGroups in transition that do not have an already known destination.
   * 
   * Takes a list of entityGroups that need immediate assignment and a list of all
   * available servers. Returns a map of entityGroups to the server they should be
   * assigned to.
   * 
   * This method will return quickly and does not do any intelligent balancing.
   * The goal is to make a fast decision not the best decision possible.
   * 
   * Currently this is random.
   * 
   * @param entityGroups
   * @param servers
   * @return map of entityGroups to the server it should be assigned to
   */
  public Map<EntityGroupInfo, ServerName> immediateAssignment(
      List<EntityGroupInfo> entityGroups, List<ServerName> servers) {
    Map<EntityGroupInfo, ServerName> assignments = new TreeMap<EntityGroupInfo, ServerName>();
    for (EntityGroupInfo entityGroup : entityGroups) {
      assignments.put(entityGroup, randomAssignment(entityGroup, servers));
    }
    return assignments;
  }

  /**
   * Used to assign a single entityGroup to a random server.
   */
  public ServerName randomAssignment(EntityGroupInfo entityGroupInfo,
      List<ServerName> servers) {
    if (servers == null || servers.isEmpty()) {
      LOG.warn("Wanted to do random assignment but no servers to assign to");
      return null;
    }
    return servers.get(RANDOM.nextInt(servers.size()));
  }

  /**
   * Generates a bulk assignment startup plan, attempting to reuse the existing
   * assignment information from META, but adjusting for the specified list of
   * available/online servers available for assignment.
   * <p>
   * Takes a map of all entityGroups to their existing assignment from META. Also
   * takes a list of online servers for entityGroups to be assigned to. Attempts to
   * retain all assignment, so in some instances initial assignment will not be
   * completely balanced.
   * <p>
   * Any leftover entityGroups without an existing server to be assigned to will be
   * assigned randomly to available servers.
   * 
   * @param entityGroups entityGroups and existing assignment from meta
   * @param servers available servers
   * @return map of servers and entityGroups to be assigned to them
   */
  public Map<ServerName, List<EntityGroupInfo>> retainAssignment(
      Map<EntityGroupInfo, ServerName> entityGroups, List<ServerName> servers) {
    // Group all of the old assignments by their hostname.
    // We can't group directly by ServerName since the servers all have
    // new start-codes.

    // Group the servers by their hostname. It's possible we have multiple
    // servers on the same host on different ports.
    ArrayListMultimap<String, ServerName> serversByHostname = ArrayListMultimap
        .create();
    for (ServerName server : servers) {
      serversByHostname.put(server.getHostname(), server);
    }

    // Now come up with new assignments
    Map<ServerName, List<EntityGroupInfo>> assignments = new TreeMap<ServerName, List<EntityGroupInfo>>();

    for (ServerName server : servers) {
      assignments.put(server, new ArrayList<EntityGroupInfo>());
    }

    // Collection of the hostnames that used to have entityGroups
    // assigned, but for which we no longer have any RS running
    // after the cluster restart.
    Set<String> oldHostsNoLongerPresent = Sets.newTreeSet();

    int numRandomAssignments = 0;
    int numRetainedAssigments = 0;
    for (Map.Entry<EntityGroupInfo, ServerName> entry : entityGroups.entrySet()) {
      EntityGroupInfo entityGroup = entry.getKey();
      ServerName oldServerName = entry.getValue();
      List<ServerName> localServers = new ArrayList<ServerName>();
      if (oldServerName != null) {
        localServers = serversByHostname.get(oldServerName.getHostname());
      }
      if (localServers.isEmpty()) {
        // No servers on the new cluster match up with this hostname,
        // assign randomly.
        ServerName randomServer = servers.get(RANDOM.nextInt(servers.size()));
        assignments.get(randomServer).add(entityGroup);
        numRandomAssignments++;
        if (oldServerName != null)
          oldHostsNoLongerPresent.add(oldServerName.getHostname());
      } else if (localServers.size() == 1) {
        // the usual case - one new server on same host
        assignments.get(localServers.get(0)).add(entityGroup);
        numRetainedAssigments++;
      } else {
        // multiple new servers in the cluster on this same host
        int size = localServers.size();
        ServerName target = localServers.get(RANDOM.nextInt(size));
        assignments.get(target).add(entityGroup);
        numRetainedAssigments++;
      }
    }

    String randomAssignMsg = "";
    if (numRandomAssignments > 0) {
      randomAssignMsg = numRandomAssignments + " entityGroups were assigned "
          + "to random hosts, since the old hosts for these entityGroups are no "
          + "longer present in the cluster. These hosts were:\n  "
          + Joiner.on("\n  ").join(oldHostsNoLongerPresent);
    }

    LOG.info("Reassigned " + entityGroups.size() + " entityGroups. "
        + numRetainedAssigments + " retained the pre-restart assignment. "
        + randomAssignMsg);
    return assignments;
  }

}

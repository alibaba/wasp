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
package com.alibaba.wasp.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.alibaba.wasp.EntityGroupInfo;import com.alibaba.wasp.EntityGroupTransaction;import com.alibaba.wasp.Server;import com.alibaba.wasp.ServerName;import com.alibaba.wasp.meta.FMetaReader;import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupTransaction;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.ServerLoad;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.master.EntityGroupState.State;
import com.alibaba.wasp.meta.FMetaReader;

/**
 * EntityGroup state accountant. It holds the states of all entityGroups in the
 * memory. In normal scenario, it should match the meta table and the true
 * entityGroup states.
 * 
 * This map is used by AssignmentManager to track entityGroup states.
 */
@InterfaceAudience.Private
public class EntityGroupStates {
  private static final Log LOG = LogFactory.getLog(EntityGroupStates.class);

  /**
   * EntityGroups currently in transition.
   */
  final HashMap<String, EntityGroupState> entityGroupsInTransition;

  /**
   * EntityGroup encoded name to state map. All the EntityGroups should be in
   * this map.
   */
  private final Map<String, EntityGroupState> entityGroupStates;

  /**
   * Server to EntityGroups assignment map. Contains the set of EntityGroups
   * currently assigned to a given fserver.
   */
  private final Map<ServerName, Set<EntityGroupInfo>> serverHoldings;

  /**
   * EntityGroup to server assignment map. Contains the server a given
   * EntityGroup is currently assigned to.
   */
  private final TreeMap<EntityGroupInfo, ServerName> entityGroupAssignments;

  private final FServerManager fserverManager;
  private final Server server;

  EntityGroupStates(final Server master, final FServerManager fserverManager) {
    entityGroupStates = new HashMap<String, EntityGroupState>();
    entityGroupsInTransition = new HashMap<String, EntityGroupState>();
    serverHoldings = new HashMap<ServerName, Set<EntityGroupInfo>>();
    entityGroupAssignments = new TreeMap<EntityGroupInfo, ServerName>();
    this.fserverManager = fserverManager;
    this.server = master;
  }

  /**
   * @return an unmodifiable the entityGroup assignment map
   */
  @SuppressWarnings("unchecked")
  public synchronized Map<EntityGroupInfo, ServerName> getEntityGroupAssignments() {
    return (Map<EntityGroupInfo, ServerName>) entityGroupAssignments.clone();
  }

  public synchronized ServerName getFServerOfEntityGroup(EntityGroupInfo egi) {
    return entityGroupAssignments.get(egi);
  }

  /**
   * Get entityGroups in transition and their states
   */
  @SuppressWarnings("unchecked")
  public synchronized Map<String, EntityGroupState> getEntityGroupsInTransition() {
    return (Map<String, EntityGroupState>) entityGroupsInTransition.clone();
  }

  /**
   * @return True if specified entityGroup in transition.
   */
  public synchronized boolean isEntityGroupInTransition(
      final EntityGroupInfo egi) {
    return entityGroupsInTransition.containsKey(egi.getEncodedName());
  }

  /**
   * @return True if specified entityGroup in transition.
   */
  public synchronized boolean isEntityGroupInTransition(
      final String entityGroupName) {
    return entityGroupsInTransition.containsKey(entityGroupName);
  }

  /**
   * @return True if any entityGroup in transition.
   */
  public synchronized boolean isEntityGroupsInTransition() {
    return !entityGroupsInTransition.isEmpty();
  }

  /**
   * @return True if specified entityGroup assigned.
   */
  public synchronized boolean isEntityGroupAssigned(final EntityGroupInfo egi) {
    return entityGroupAssignments.containsKey(egi);
  }

  /**
   * @return the server the specified entityGroup assigned to; null if not
   *         assigned.
   */
  public synchronized ServerName getAssignedServer(final EntityGroupInfo egi) {
    return entityGroupAssignments.get(egi);
  }

  /**
   * Wait for the state map to be updated by assignment manager.
   */
  public synchronized void waitForUpdate(
      final long timeout) throws InterruptedException {
    this.wait(timeout);
  }

  /**
   * Get entityGroup transition state
   */
  public synchronized EntityGroupState getEntityGroupTransitionState(
      final EntityGroupInfo egi) {
    return entityGroupsInTransition.get(egi.getEncodedName());
  }

  /**
   * Get entityGroup transition state
   */
  public synchronized EntityGroupState getEntityGroupTransitionState(
      final String entityGroupName) {
    return entityGroupsInTransition.get(entityGroupName);
  }

  /**
   * Add a list of entityGroups to EntityGroupStates. The initial state is
   * OFFLINE. If any entityGroup is already in EntityGroupStates, that
   * entityGroup will be skipped.
   */
  public synchronized void createEntityGroupStates(
      final List<EntityGroupInfo> egis) {
    for (EntityGroupInfo egi : egis) {
      createEntityGroupState(egi);
    }
  }

  /**
   * Add a entityGroup to EntityGroupStates. The initial state is OFFLINE. If it
   * is already in EntityGroupStates, this call has no effect, and the original
   * state is returned.
   */
  public synchronized EntityGroupState createEntityGroupState(
      final EntityGroupInfo egi) {
    String entityGroupName = egi.getEncodedName();
    EntityGroupState entityGroupState = entityGroupStates.get(entityGroupName);
    if (entityGroupState != null) {
      LOG.warn("Tried to create a state of a entityGroup already in EntityGroupStates "
          + egi + ", used existing state: " + entityGroupState
        + ", ignored new state: state=OFFLINE, server=null");
    } else {
      entityGroupState = new EntityGroupState(egi, EntityGroupState.State.OFFLINE);
      entityGroupStates.put(entityGroupName, entityGroupState);
    }
    return entityGroupState;
  }

  /**
   * Update a entityGroup state. If it is not splitting, it will be put in
   * transition if not already there.
   */
  public synchronized EntityGroupState updateEntityGroupState(
      final EntityGroupInfo egi, final EntityGroupState.State state) {
    EntityGroupState entityGroupState = entityGroupStates.get(egi
        .getEncodedName());
    ServerName serverName = (entityGroupState == null || state == EntityGroupState.State.CLOSED || state == EntityGroupState.State.OFFLINE) ? null
        : entityGroupState.getServerName();
    return updateEntityGroupState(egi, state, serverName);
  }

  /**
   * Update a entityGroup state. If it is not splitting, it will be put in
   * transition if not already there.
   * 
   * If we can't find the entityGroup info based on the entityGroup name in the
   * transition, log a warning and return null.
   */
  public synchronized EntityGroupState updateEntityGroupState(
      final EntityGroupTransaction transition, final EntityGroupState.State state) {
    byte[] entityGroupName = transition.getEntityGroupName();
    EntityGroupInfo entityGroupInfo = getEntityGroupInfo(entityGroupName);
    if (entityGroupInfo == null) {
      String prettyEntityGroupName = EntityGroupInfo
          .encodeEntityGroupName(entityGroupName);
      LOG.warn("Failed to find entityGroup " + prettyEntityGroupName
        + " in updating its state to " + state
          + " based on entityGroup transition " + transition);
      return null;
    }
    return updateEntityGroupState(entityGroupInfo, state,
      transition.getServerName());
  }

  /**
   * Update a entityGroup state. If it is not splitting, it will be put in
   * transition if not already there.
   */
  public synchronized EntityGroupState updateEntityGroupState(
      final EntityGroupInfo egi, final EntityGroupState.State state, final ServerName serverName) {
    ServerName newServerName = serverName;
    if (serverName != null &&
        (state == EntityGroupState.State.CLOSED || state == EntityGroupState.State.OFFLINE)) {
      LOG.warn("Closed entityGroup " + egi + " still on "
        + serverName + "? Ignored, reset it to null");
      newServerName = null;
    }

    String entityGroupName = egi.getEncodedName();
    EntityGroupState entityGroupState = new EntityGroupState(
      egi, state, System.currentTimeMillis(), newServerName);
    EntityGroupState oldState = entityGroupStates.put(entityGroupName,
        entityGroupState);
    LOG.info("EntityGroup " + egi + " transitioned from " + oldState + " to "
        + entityGroupState);
    if (state != EntityGroupState.State.SPLITTING && (newServerName != null
        || (state != EntityGroupState.State.PENDING_CLOSE && state != EntityGroupState.State.CLOSING))) {
      entityGroupsInTransition.put(entityGroupName, entityGroupState);
    }

    // notify the change
    this.notifyAll();
    return entityGroupState;
  }

  /**
   * A entityGroup is online, won't be in transition any more. We can't confirm
   * it is really online on specified entityGroup server because it hasn't been
   * put in entityGroup server's online entityGroup list yet.
   */
  public synchronized void entityGroupOnline(final EntityGroupInfo egi,
      final ServerName serverName) {
    String entityGroupName = egi.getEncodedName();
    EntityGroupState oldState = entityGroupStates.get(entityGroupName);
    if (oldState == null) {
      LOG.warn("Online a entityGroup not in EntityGroupStates: " + egi);
    } else {
      EntityGroupState.State state = oldState.getState();
      ServerName sn = oldState.getServerName();
      if (state != EntityGroupState.State.OPEN || sn == null || !sn.equals(serverName)) {
        LOG.debug("Online a entityGroup with current state=" + state
            + ", expected state=OPEN"
          + ", assigned to server: " + sn + " expected " + serverName);
      }
    }
    updateEntityGroupState(egi, EntityGroupState.State.OPEN, serverName);
    entityGroupsInTransition.remove(entityGroupName);

    ServerName oldServerName = entityGroupAssignments.put(egi, serverName);
    if (!serverName.equals(oldServerName)) {
      LOG.info("Onlined entityGroup " + egi + " on " + serverName);
      Set<EntityGroupInfo> entityGroups = serverHoldings.get(serverName);
      if (entityGroups == null) {
        entityGroups = new HashSet<EntityGroupInfo>();
        serverHoldings.put(serverName, entityGroups);
      }
      entityGroups.add(egi);
      if (oldServerName != null) {
        LOG.info("Offlined entityGroup " + egi + " from " + oldServerName);
        serverHoldings.get(oldServerName).remove(egi);
      }
    }
  }

  /**
   * A entityGroup is offline, won't be in transition any more.
   */
  public synchronized void entityGroupOffline(final EntityGroupInfo egi) {
    String entityGroupName = egi.getEncodedName();
    EntityGroupState oldState = entityGroupStates.get(entityGroupName);
    if (oldState == null) {
      LOG.warn("Offline a entityGroup not in EntityGroupStates: " + egi);
    } else {
      EntityGroupState.State state = oldState.getState();
      ServerName sn = oldState.getServerName();
      if (state != EntityGroupState.State.OFFLINE || sn != null) {
        LOG.debug("Offline a entityGroup with current state=" + state
            + ", expected state=OFFLINE"
          + ", assigned to server: " + sn + ", expected null");
      }
    }
    updateEntityGroupState(egi, EntityGroupState.State.OFFLINE);
    entityGroupsInTransition.remove(entityGroupName);

    ServerName oldServerName = entityGroupAssignments.remove(egi);
    if (oldServerName != null) {
      LOG.info("Offlined entityGroup " + egi + " from " + oldServerName);
      serverHoldings.get(oldServerName).remove(egi);
    }
  }

  /**
   * A server is offline, all entityGroups on it are dead.
   */
  public synchronized List<EntityGroupState> serverOffline(final ServerName sn) {
    // Clean up this server from map of servers to entityGroups, and remove all
    // entityGroups
    // of this server from online map of entityGroups.
    List<EntityGroupState> rits = new ArrayList<EntityGroupState>();
    Set<EntityGroupInfo> assignedEntityGroups = serverHoldings.get(sn);
    if (assignedEntityGroups == null || assignedEntityGroups.isEmpty()) {
      // No entityGroups on this server, we are done, return empty list of RITs
      return rits;
    }

    for (EntityGroupInfo entityGroup : assignedEntityGroups) {
      entityGroupAssignments.remove(entityGroup);
    }

    // See if any of the entityGroups that were online on this server were in
    // RIT
    // If they are, normal timeouts will deal with them appropriately so
    // let's skip a manual re-assignment.
    for (EntityGroupState state : entityGroupsInTransition.values()) {
      if (assignedEntityGroups.contains(state.getEntityGroup())) {
        rits.add(state);
      }
    }
    assignedEntityGroups.clear();
    this.notifyAll();
    return rits;
  }

  /**
   * Gets the online entityGroups of the specified table. This method looks at
   * the in-memory state. It does not go to <code>.META.</code>. Only returns
   * <em>online</em> entityGroups. If a entityGroup on this table has been
   * closed during a disable, etc., it will be included in the returned list.
   * So, the returned list may not necessarily be ALL entityGroups in this
   * table, its all the ONLINE entityGroups in the table.
   * 
   * @param tableName
   * @return Online entityGroups from <code>tableName</code>
   */
  public synchronized List<EntityGroupInfo> getEntityGroupsOfTable(
      byte[] tableName) {
    List<EntityGroupInfo> tableEntityGroups = new ArrayList<EntityGroupInfo>();
    // boundary needs to have table's name but entityGroupID 0 so that it is
    // sorted
    // before all table's entityGroups.
    EntityGroupInfo boundary = new EntityGroupInfo(tableName,  null, null,
        false, 0L);
    for (EntityGroupInfo egi : entityGroupAssignments.tailMap(boundary)
        .keySet()) {
      if(Bytes.equals(egi.getTableName(), tableName)) {
        tableEntityGroups.add(egi);
      } else {
        break;
      }
    }
    return tableEntityGroups;
  }


  /**
   * Wait on entityGroup to clear entityGroups-in-transition.
   * <p>
   * If the entityGroup isn't in transition, returns immediately. Otherwise,
   * method blocks until the entityGroup is out of transition.
   */
  public synchronized void waitOnEntityGroupToClearEntityGroupsInTransition(
      final EntityGroupInfo egi) throws InterruptedException {
    if (!isEntityGroupInTransition(egi))
      return;

    while (!server.isStopped() && isEntityGroupInTransition(egi)) {
      EntityGroupState rs = getEntityGroupState(egi);
      LOG.info("Waiting on " + rs + " to clear entityGroups-in-transition");
      waitForUpdate(100);
    }

    if (server.isStopped()) {
      LOG.info("Giving up wait on entityGroup in "
          +
        "transition because stoppable.isStopped is set");
    }
  }

  /**
   * Waits until the specified entityGroup has completed assignment.
   * <p>
   * If the entityGroup is already assigned, returns immediately. Otherwise,
   * method blocks until the entityGroup is assigned.
   */
  public synchronized void waitForAssignment(final EntityGroupInfo egi)
      throws InterruptedException {
    if (!isEntityGroupAssigned(egi))
      return;

    while (!server.isStopped() && !isEntityGroupAssigned(egi)) {
      EntityGroupState rs = getEntityGroupState(egi);
      LOG.info("Waiting on " + rs + " to be assigned");
      waitForUpdate(100);
    }

    if (server.isStopped()) {
      LOG.info("Giving up wait on entityGroup "
          +
        "assignment because stoppable.isStopped is set");
    }
  }

  /**
   * Compute the average load across all entityGroup servers. Currently, this
   * uses a very naive computation - just uses the number of entityGroups being
   * served, ignoring stats about number of requests.
   * 
   * @return the average load
   */
  protected synchronized double getAverageLoad() {
    int numServers = 0, totalLoad = 0;
    for (Map.Entry<ServerName, Set<EntityGroupInfo>> e : serverHoldings
        .entrySet()) {
      Set<EntityGroupInfo> entityGroups = e.getValue();
      ServerName serverName = e.getKey();
      int entityGroupCount = entityGroups.size();
      if (entityGroupCount > 0 || fserverManager.isServerOnline(serverName)) {
        totalLoad += entityGroupCount;
        numServers++;
      }
    }
    return numServers == 0 ? 0.0 :
      (double)totalLoad / (double)numServers;
  }

  /**
   * This is an EXPENSIVE clone. Cloning though is the safest thing to do. Can't
   * let out original since it can change and at least the load balancer wants
   * to iterate this exported list. We need to synchronize on entityGroups since
   * all access to this.servers is under a lock on this.entityGroups.
   * 
   * @return A clone of current assignments by table.
   */
  protected Map<String, Map<ServerName, List<EntityGroupInfo>>> getAssignmentsByTable() {
    Map<String, Map<ServerName, List<EntityGroupInfo>>> result = new HashMap<String, Map<ServerName, List<EntityGroupInfo>>>();
    synchronized (this) {
      if (!server.getConfiguration().getBoolean("wasp.master.loadbalance.bytable", false)) {
        Map<ServerName, List<EntityGroupInfo>> svrToEntityGroups = new HashMap<ServerName, List<EntityGroupInfo>>(
            serverHoldings.size());
        for (Map.Entry<ServerName, Set<EntityGroupInfo>> e : serverHoldings
            .entrySet()) {
          svrToEntityGroups.put(e.getKey(),
              new ArrayList<EntityGroupInfo>(e.getValue()));
        }
        result.put("ensemble", svrToEntityGroups);
      } else {
        for (Map.Entry<ServerName, Set<EntityGroupInfo>> e : serverHoldings
            .entrySet()) {
          for (EntityGroupInfo egi : e.getValue()) {
            String tablename = egi.getTableNameAsString();
            Map<ServerName, List<EntityGroupInfo>> svrToEntityGroups = result
                .get(tablename);
            if (svrToEntityGroups == null) {
              svrToEntityGroups = new HashMap<ServerName, List<EntityGroupInfo>>(
                  serverHoldings.size());
              result.put(tablename, svrToEntityGroups);
            }
            List<EntityGroupInfo> entityGroups = svrToEntityGroups.get(e
                .getKey());
            if (entityGroups == null) {
              entityGroups = new ArrayList<EntityGroupInfo>();
              svrToEntityGroups.put(e.getKey(), entityGroups);
            }
            entityGroups.add(egi);
          }
        }
      }
    }

    Map<ServerName, ServerLoad> onlineSvrs = fserverManager.getOnlineServers();
    // Take care of servers w/o assignments.
    for (Map<ServerName, List<EntityGroupInfo>> map : result.values()) {
      for (ServerName svr: onlineSvrs.keySet()) {
        if (!map.containsKey(svr)) {
          map.put(svr, new ArrayList<EntityGroupInfo>());
        }
      }
    }
    return result;
  }

  protected synchronized EntityGroupState getEntityGroupState(
      final EntityGroupInfo egi) {
    return entityGroupStates.get(egi.getEncodedName());
  }

  protected synchronized EntityGroupState getEntityGroupState(
      final String entityGroupName) {
    return entityGroupStates.get(entityGroupName);
  }

  /**
   * Get the EntityGroupInfo from cache, if not there, from the META table
   * 
   * @param entityGroupName
   * @return EntityGroupInfo for the entityGroup
   */
  protected EntityGroupInfo getEntityGroupInfo(final byte[] entityGroupName) {
    String encodedName = EntityGroupInfo.encodeEntityGroupName(entityGroupName);
    EntityGroupState entityGroupState = entityGroupStates.get(encodedName);
    if (entityGroupState != null) {
      return entityGroupState.getEntityGroup();
    }

    try {
      Pair<EntityGroupInfo, ServerName> p = FMetaReader
          .getEntityGroupAndLocation(server.getConfiguration(), entityGroupName);
      EntityGroupInfo egi = p == null ? null : p.getFirst();
      if (egi != null) {
        createEntityGroupState(egi);
      }
      return egi;
    } catch (IOException e) {
      server.abort(
          "Aborting because error occoured while reading "
              + Bytes.toStringBinary(entityGroupName) + " from .FMETA.", e);
      return null;
    }
  }
}

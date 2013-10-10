/**
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
package com.alibaba.wasp.zookeeper;

import com.alibaba.wasp.DeserializationException;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupTransaction;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.executor.EventHandler.EventType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * Utility class for doing entityGroup assignment in ZooKeeper. This class
 * extends stuff done in {@link ZKUtil} to cover specific assignment operations.
 * <p>
 * Contains only static methods and constants.
 * <p>
 * Used by both the FMaster and FServer.
 * <p>
 * All valid transitions outlined below:
 * <p>
 * <b>FMASTER</b>
 * <ol>
 * <li>
 * FMaster creates an unassigned node as OFFLINE. - Cluster startup and table
 * enabling.</li>
 * <li>
 * FMaster forces an existing unassigned node to OFFLINE. - FServer failure. -
 * Allows transitions from all states to OFFLINE.</li>
 * <li>
 * FMaster deletes an unassigned node that was in a OPENED state. - Normal
 * entityGroup transitions. Besides cluster startup, no other deletions of
 * unassigned nodes is allowed.</li>
 * <li>
 * FMaster deletes all unassigned nodes regardless of state. - Cluster startup
 * before any assignment happens.</li>
 * </ol>
 * <p>
 * <b>FSERVER</b>
 * <ol>
 * <li>
 * FServer creates an unassigned node as CLOSING. - All entityGroups closes will
 * do this in response to a CLOSE RPC from FMaster. - A node can never be
 * transitioned to CLOSING, only created.</li>
 * <li>
 * FServer transitions an unassigned node from CLOSING to CLOSED. - Normal
 * entityGroup closes. CAS operation.</li>
 * <li>
 * FServer transitions an unassigned node from OFFLINE to OPENING. - All
 * entityGroup opens will do this in response to an OPEN RPC from the FMaster. -
 * Normal entityGroup opens. CAS operation.</li>
 * <li>
 * FServer transitions an unassigned node from OPENING to OPENED. - Normal
 * entityGroup opens. CAS operation.</li>
 * </ol>
 */
public class ZKAssign {

  private static final Log LOG = LogFactory.getLog(ZKAssign.class);

  /**
   * Gets the full path node name for the unassigned node for the specified
   * entityGroup.
   * @param zkw zk reference
   * @param entityGroupName entityGroup name
   * @return full path node name
   */
  public static String getNodeName(ZooKeeperWatcher zkw, String entityGroupName) {
    return ZKUtil.joinZNode(zkw.assignmentZNode, entityGroupName);
  }

  /**
   * Gets the entityGroup name from the full path node name of an unassigned
   * node.
   * @param path full zk path
   * @return entityGroup name
   */
  public static String getEntityGroupName(ZooKeeperWatcher zkw, String path) {
    return path.substring(zkw.assignmentZNode.length() + 1);
  }

  // Master methods

  /**
   * Creates a new unassigned node in the OFFLINE state for the specified
   * entityGroup.
   *
   * <p>
   * Does not transition nodes from other states. If a node already exists for
   * this entityGroup, a {@link org.apache.zookeeper.KeeperException.NodeExistsException} will be thrown.
   *
   * <p>
   * Sets a watcher on the unassigned entityGroup node if the method is
   * successful.
   *
   * <p>
   * This method should only be used during cluster startup and the enabling of
   * a table.
   *
   * @param zkw zk reference
   * @param entityGroup entityGroup to be created as offline
   * @param serverName server transition will happen on
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   * @throws org.apache.zookeeper.KeeperException.NodeExistsException if node already exists
   */
  public static void createNodeOffline(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup, ServerName serverName) throws KeeperException,
      KeeperException.NodeExistsException {
    createNodeOffline(zkw, entityGroup, serverName,
        EventType.M_ZK_ENTITYGROUP_OFFLINE);
  }

  public static void createNodeOffline(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup, ServerName serverName, final EventType event)
      throws KeeperException, KeeperException.NodeExistsException {
    LOG.debug(zkw.prefix("Creating unassigned node for "
        + entityGroup.getEncodedName() + " in OFFLINE state"));
    EntityGroupTransaction rt = EntityGroupTransaction.createEntityGroupTransition(event,
        entityGroup.getEntityGroupName(), serverName);
    String node = getNodeName(zkw, entityGroup.getEncodedName());
    ZKUtil.createAndWatch(zkw, node, rt.toByteArray());
  }

  /**
   * Creates an unassigned node in the OFFLINE state for the specified
   * entityGroup.
   * <p>
   * Runs asynchronously. Depends on no pre-existing znode.
   *
   * <p>
   * Sets a watcher on the unassigned entityGroup node.
   *
   * @param zkw zk reference
   * @param entityGroup entityGroup to be created as offline
   * @param serverName server transition will happen on
   * @param cb
   * @param ctx
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   * @throws org.apache.zookeeper.KeeperException.NodeExistsException if node already exists
   */
  public static void asyncCreateNodeOffline(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup, ServerName serverName,
      final AsyncCallback.StringCallback cb, final Object ctx)
      throws KeeperException {
    LOG.debug(zkw.prefix("Async create of unassigned node for "
        + entityGroup.getEncodedName() + " with OFFLINE state"));
    EntityGroupTransaction rt = EntityGroupTransaction.createEntityGroupTransition(
        EventType.M_ZK_ENTITYGROUP_OFFLINE, entityGroup.getEntityGroupName(), serverName);
    String node = getNodeName(zkw, entityGroup.getEncodedName());
    ZKUtil.asyncCreate(zkw, node, rt.toByteArray(), cb, ctx);
  }

  /**
   * Creates or force updates an unassigned node to the OFFLINE state for the
   * specified entityGroup.
   * <p>
   * Attempts to create the node but if it exists will force it to transition to
   * and OFFLINE state.
   *
   * <p>
   * Sets a watcher on the unassigned entityGroup node if the method is
   * successful.
   *
   * <p>
   * This method should be used when assigning a entityGroup.
   *
   * @param zkw zk reference
   * @param entityGroup entityGroup to be created as offline
   * @param serverName server transition will happen on
   * @return the version of the znode created in OFFLINE state, -1 if
   *         unsuccessful.
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   * @throws org.apache.zookeeper.KeeperException.NodeExistsException if node already exists
   */
  public static int createOrForceNodeOffline(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup, ServerName serverName) throws KeeperException {
    LOG.debug(zkw.prefix("Creating (or updating) unassigned node for "
        + entityGroup.getEncodedName() + " with OFFLINE state"));
    EntityGroupTransaction rt = EntityGroupTransaction.createEntityGroupTransition(
        EventType.M_ZK_ENTITYGROUP_OFFLINE, entityGroup.getEntityGroupName(), serverName,
        HConstants.EMPTY_BYTE_ARRAY);
    byte[] data = rt.toByteArray();
    String node = getNodeName(zkw, entityGroup.getEncodedName());
    zkw.sync(node);
    int version = ZKUtil.checkExists(zkw, node);
    if (version == -1) {
      return ZKUtil.createAndWatch(zkw, node, data);
    } else {
      boolean setData = false;
      try {
        setData = ZKUtil.setData(zkw, node, data, version);
        // Setdata throws KeeperException which aborts the Master. So we are
        // catching it here.
        // If just before setting the znode to OFFLINE if the RS has made any
        // change to the
        // znode state then we need to return -1.
      } catch (KeeperException kpe) {
        LOG.info("Version mismatch while setting the node to OFFLINE state.");
        return -1;
      }
      if (!setData) {
        return -1;
      } else {
        // We successfully forced to OFFLINE, reset watch and handle if
        // the state changed in between our set and the watch
        byte[] bytes = ZKAssign.getData(zkw, entityGroup.getEncodedName());
        rt = getEntityGroupTransition(bytes);
        if (rt.getEventType() != EventType.M_ZK_ENTITYGROUP_OFFLINE) {
          // state changed, need to process
          return -1;
        }
      }
    }
    return version + 1;
  }

  /**
   * Deletes an existing unassigned node that is in the OPENED state for the
   * specified entityGroup.
   *
   * <p>
   * If a node does not already exist for this entityGroup, a
   * {@link org.apache.zookeeper.KeeperException.NoNodeException} will be thrown.
   *
   * <p>
   * No watcher is set whether this succeeds or not.
   *
   * <p>
   * Returns false if the node was not in the proper state but did exist.
   *
   * <p>
   * This method is used during normal entityGroup transitions when a
   * entityGroup finishes successfully opening. This is the Master acknowledging
   * completion of the specified entityGroups transition.
   *
   * @param zkw zk reference
   * @param entityGroupName opened entityGroup to be deleted from zk
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   * @throws org.apache.zookeeper.KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteOpenedNode(ZooKeeperWatcher zkw,
      String entityGroupName)
      throws KeeperException, KeeperException.NoNodeException {
    return deleteNode(zkw, entityGroupName, EventType.FSERVER_ZK_ENTITYGROUP_OPENED);
  }

  /**
   * Deletes an existing unassigned node that is in the OFFLINE state for the
   * specified entityGroup.
   *
   * <p>
   * If a node does not already exist for this entityGroup, a
   * {@link org.apache.zookeeper.KeeperException.NoNodeException} will be thrown.
   *
   * <p>
   * No watcher is set whether this succeeds or not.
   *
   * <p>
   * Returns false if the node was not in the proper state but did exist.
   *
   * <p>
   * This method is used during master failover when the entityGroups on an RS
   * that has died are all set to OFFLINE before being processed.
   *
   * @param zkw zk reference
   * @param entityGroupName closed entityGroup to be deleted from zk
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   * @throws org.apache.zookeeper.KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteOfflineNode(ZooKeeperWatcher zkw,
      String entityGroupName) throws KeeperException,
      KeeperException.NoNodeException {
    return deleteNode(zkw, entityGroupName, EventType.M_ZK_ENTITYGROUP_OFFLINE);
  }

  /**
   * Deletes an existing unassigned node that is in the CLOSED state for the
   * specified entityGroup.
   *
   * <p>
   * If a node does not already exist for this entityGroup, a
   * {@link org.apache.zookeeper.KeeperException.NoNodeException} will be thrown.
   *
   * <p>
   * No watcher is set whether this succeeds or not.
   *
   * <p>
   * Returns false if the node was not in the proper state but did exist.
   *
   * <p>
   * This method is used during table disables when a entityGroup finishes
   * successfully closing. This is the Master acknowledging completion of the
   * specified entityGroups transition to being closed.
   *
   * @param zkw zk reference
   * @param entityGroupName closed entityGroup to be deleted from zk
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   * @throws org.apache.zookeeper.KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteClosedNode(ZooKeeperWatcher zkw,
      String entityGroupName)
      throws KeeperException, KeeperException.NoNodeException {
    return deleteNode(zkw, entityGroupName, EventType.FSERVER_ZK_ENTITYGROUP_CLOSED);
  }

  /**
   * Deletes an existing unassigned node that is in the CLOSING state for the
   * specified entityGroup.
   *
   * <p>
   * If a node does not already exist for this entityGroup, a
   * {@link org.apache.zookeeper.KeeperException.NoNodeException} will be thrown.
   *
   * <p>
   * No watcher is set whether this succeeds or not.
   *
   * <p>
   * Returns false if the node was not in the proper state but did exist.
   *
   * <p>
   * This method is used during table disables when a entityGroup finishes
   * successfully closing. This is the Master acknowledging completion of the
   * specified entityGroups transition to being closed.
   *
   * @param zkw zk reference
   * @param entityGroup closing entityGroup to be deleted from zk
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   * @throws org.apache.zookeeper.KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteClosingNode(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup) throws KeeperException,
      KeeperException.NoNodeException {
    String entityGroupName = entityGroup.getEncodedName();
    return deleteNode(zkw, entityGroupName, EventType.M_ZK_ENTITYGROUP_CLOSING);
  }

  /**
   * Deletes an existing unassigned node that is in the specified state for the
   * specified entityGroup.
   *
   * <p>
   * If a node does not already exist for this entityGroup, a
   * {@link org.apache.zookeeper.KeeperException.NoNodeException} will be thrown.
   *
   * <p>
   * No watcher is set whether this succeeds or not.
   *
   * <p>
   * Returns false if the node was not in the proper state but did exist.
   *
   * <p>
   * This method is used when a entityGroup finishes opening/closing. The Master
   * acknowledges completion of the specified entityGroups transition to being
   * closed/opened.
   *
   * @param zkw zk reference
   * @param entityGroupName entityGroup to be deleted from zk
   * @param expectedState state entityGroup must be in for delete to complete
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   * @throws org.apache.zookeeper.KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteNode(ZooKeeperWatcher zkw,
      String entityGroupName,
      EventType expectedState) throws KeeperException,
      KeeperException.NoNodeException {
    return deleteNode(zkw, entityGroupName, expectedState, -1);
  }

  /**
   * Deletes an existing unassigned node that is in the specified state for the
   * specified entityGroup.
   *
   * <p>
   * If a node does not already exist for this entityGroup, a
   * {@link org.apache.zookeeper.KeeperException.NoNodeException} will be thrown.
   *
   * <p>
   * No watcher is set whether this succeeds or not.
   *
   * <p>
   * Returns false if the node was not in the proper state but did exist.
   *
   * <p>
   * This method is used when a entityGroup finishes opening/closing. The Master
   * acknowledges completion of the specified entityGroups transition to being
   * closed/opened.
   *
   * @param zkw zk reference
   * @param entityGroupName entityGroup to be deleted from zk
   * @param expectedState state entityGroup must be in for delete to complete
   * @param expectedVersion of the znode that is to be deleted. If
   *          expectedVersion need not be compared while deleting the znode pass
   *          -1
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   * @throws org.apache.zookeeper.KeeperException.NoNodeException if node does not exist
   */
  public static boolean deleteNode(ZooKeeperWatcher zkw,
      String entityGroupName,
      EventType expectedState, int expectedVersion) throws KeeperException,
      KeeperException.NoNodeException {
    LOG.debug(zkw.prefix("Deleting existing unassigned " + "node for "
        + entityGroupName + " that is in expected state " + expectedState));
    String node = getNodeName(zkw, entityGroupName);
    zkw.sync(node);
    Stat stat = new Stat();
    byte[] bytes = ZKUtil.getDataNoWatch(zkw, node, stat);
    if (bytes == null) {
      // If it came back null, node does not exist.
      throw KeeperException.create(Code.NONODE);
    }
    EntityGroupTransaction rt = getEntityGroupTransition(bytes);
    EventType et = rt.getEventType();
    if (!et.equals(expectedState)) {
      LOG.warn(zkw.prefix("Attempting to delete unassigned node "
          + entityGroupName
          + " in " + expectedState + " state but node is in " + et + " state"));
      return false;
    }
    if (expectedVersion != -1 && stat.getVersion() != expectedVersion) {
      LOG.warn("The node " + entityGroupName
          + " we are trying to delete is not"
          + " the expected one. Got a version mismatch");
      return false;
    }
    if (!ZKUtil.deleteNode(zkw, node, stat.getVersion())) {
      LOG.warn(zkw.prefix("Attempting to delete " + "unassigned node "
          + entityGroupName + " in " + expectedState
          + " state but after verifying state, we got a version mismatch"));
      return false;
    }
    LOG.debug(zkw
        .prefix("Successfully deleted unassigned node for entityGroup "
            + entityGroupName + " in expected state " + expectedState));
    return true;
  }

  /**
   * Deletes all unassigned nodes regardless of their state.
   *
   * <p>
   * No watchers are set.
   *
   * <p>
   * This method is used by the Master during cluster startup to clear out any
   * existing state from other cluster runs.
   *
   * @param zkw zk reference
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   */
  public static void deleteAllNodes(ZooKeeperWatcher zkw)
      throws KeeperException {
    LOG.debug(zkw.prefix("Deleting any existing unassigned nodes"));
    ZKUtil.deleteChildrenRecursively(zkw, zkw.assignmentZNode);
  }

  // FServer methods

  /**
   * Creates a new unassigned node in the CLOSING state for the specified
   * entityGroup.
   *
   * <p>
   * Does not transition nodes from any states. If a node already exists for
   * this entityGroup, a {@link org.apache.zookeeper.KeeperException.NodeExistsException} will be thrown.
   *
   * <p>
   * If creation is successful, returns the version number of the CLOSING node
   * created.
   *
   * <p>
   * Does not set any watches.
   *
   * <p>
   * This method should only be used by a FServer when initiating a close
   * of a entityGroup after receiving a CLOSE RPC from the Master.
   *
   * @param zkw zk reference
   * @param entityGroup entityGroup to be created as closing
   * @param serverName server transition will happen on
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   * @throws org.apache.zookeeper.KeeperException.NodeExistsException if node already exists
   */
  public static int createNodeClosing(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup,
      ServerName serverName) throws KeeperException,
      KeeperException.NodeExistsException {
    LOG.debug(zkw.prefix("Creating unassigned node for "
        + entityGroup.getEncodedName() + " in a CLOSING state"));
    EntityGroupTransaction rt = EntityGroupTransaction.createEntityGroupTransition(
        EventType.M_ZK_ENTITYGROUP_CLOSING, entityGroup.getEntityGroupName(), serverName,
        HConstants.EMPTY_BYTE_ARRAY);
    String node = getNodeName(zkw, entityGroup.getEncodedName());
    return ZKUtil.createAndWatch(zkw, node, rt.toByteArray());
  }

  /**
   * Transitions an existing unassigned node for the specified entityGroup which
   * is currently in the CLOSING state to be in the CLOSED state.
   *
   * <p>
   * Does not transition nodes from other states. If for some reason the node
   * could not be transitioned, the method returns -1. If the transition is
   * successful, the version of the node after transition is returned.
   *
   * <p>
   * This method can fail and return false for three different reasons:
   * <ul>
   * <li>Unassigned node for this entityGroup does not exist</li>
   * <li>Unassigned node for this entityGroup is not in CLOSING state</li>
   * <li>After verifying CLOSING state, update fails because of wrong version
   * (someone else already transitioned the node)</li>
   * </ul>
   *
   * <p>
   * Does not set any watches.
   *
   * <p>
   * This method should only be used by a FServer when initiating a close
   * of a entityGroup after receiving a CLOSE RPC from the Master.
   *
   * @param zkw zk reference
   * @param entityGroup entityGroup to be transitioned to closed
   * @param serverName server transition happens on
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   */
  public static int transitionNodeClosed(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup, ServerName serverName, int expectedVersion)
      throws KeeperException {
    return transitionNode(zkw, entityGroup, serverName,
        EventType.M_ZK_ENTITYGROUP_CLOSING, EventType.FSERVER_ZK_ENTITYGROUP_CLOSED,
        expectedVersion);
  }

  /**
   * Transitions an existing unassigned node for the specified entityGroup which
   * is currently in the OFFLINE state to be in the OPENING state.
   *
   * <p>
   * Does not transition nodes from other states. If for some reason the node
   * could not be transitioned, the method returns -1. If the transition is
   * successful, the version of the node written as OPENING is returned.
   *
   * <p>
   * This method can fail and return -1 for three different reasons:
   * <ul>
   * <li>Unassigned node for this entityGroup does not exist</li>
   * <li>Unassigned node for this entityGroup is not in OFFLINE state</li>
   * <li>After verifying OFFLINE state, update fails because of wrong version
   * (someone else already transitioned the node)</li>
   * </ul>
   *
   * <p>
   * Does not set any watches.
   *
   * <p>
   * This method should only be used by a FServer when initiating an open
   * of a entityGroup after receiving an OPEN RPC from the Master.
   *
   * @param zkw zk reference
   * @param entityGroup entityGroup to be transitioned to opening
   * @param serverName server transition happens on
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   */
  public static int transitionNodeOpening(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup, ServerName serverName) throws KeeperException {
    return transitionNodeOpening(zkw, entityGroup, serverName,
        EventType.M_ZK_ENTITYGROUP_OFFLINE);
  }

  public static int transitionNodeOpening(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup, ServerName serverName, final EventType beginState)
      throws KeeperException {
    return transitionNode(zkw, entityGroup, serverName, beginState,
        EventType.FSERVER_ZK_ENTITYGROUP_OPENING, -1);
  }

  /**
   * Retransitions an existing unassigned node for the specified entityGroup
   * which is currently in the OPENING state to be in the OPENING state.
   *
   * <p>
   * Does not transition nodes from other states. If for some reason the node
   * could not be transitioned, the method returns -1. If the transition is
   * successful, the version of the node rewritten as OPENING is returned.
   *
   * <p>
   * This method can fail and return -1 for three different reasons:
   * <ul>
   * <li>Unassigned node for this entityGroup does not exist</li>
   * <li>Unassigned node for this entityGroup is not in OPENING state</li>
   * <li>After verifying OPENING state, update fails because of wrong version
   * (someone else already transitioned the node)</li>
   * </ul>
   *
   * <p>
   * Does not set any watches.
   *
   * <p>
   * This method should only be used by a FServer when initiating an open
   * of a entityGroup after receiving an OPEN RPC from the Master.
   *
   * @param zkw zk reference
   * @param entityGroup entityGroup to be transitioned to opening
   * @param serverName server transition happens on
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   */
  public static int retransitionNodeOpening(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup, ServerName serverName, int expectedVersion)
      throws KeeperException {
    return transitionNode(zkw, entityGroup, serverName,
        EventType.FSERVER_ZK_ENTITYGROUP_OPENING, EventType.FSERVER_ZK_ENTITYGROUP_OPENING,
        expectedVersion);
  }

  /**
   * Transitions an existing unassigned node for the specified entityGroup which
   * is currently in the OPENING state to be in the OPENED state.
   *
   * <p>
   * Does not transition nodes from other states. If for some reason the node
   * could not be transitioned, the method returns -1. If the transition is
   * successful, the version of the node after transition is returned.
   *
   * <p>
   * This method can fail and return false for three different reasons:
   * <ul>
   * <li>Unassigned node for this entityGroup does not exist</li>
   * <li>Unassigned node for this entityGroup is not in OPENING state</li>
   * <li>After verifying OPENING state, update fails because of wrong version
   * (this should never actually happen since an RS only does this transition
   * following a transition to OPENING. if two RS are conflicting, one would
   * fail the original transition to OPENING and not this transition)</li>
   * </ul>
   *
   * <p>
   * Does not set any watches.
   *
   * <p>
   * This method should only be used by a FServer when completing the open
   * of a entityGroup.
   *
   * @param zkw zk reference
   * @param entityGroup entityGroup to be transitioned to opened
   * @param serverName server transition happens on
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   */
  public static int transitionNodeOpened(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup, ServerName serverName, int expectedVersion)
      throws KeeperException {
    return transitionNode(zkw, entityGroup, serverName,
        EventType.FSERVER_ZK_ENTITYGROUP_OPENING, EventType.FSERVER_ZK_ENTITYGROUP_OPENED,
        expectedVersion);
  }

  /**
   * Method that actually performs unassigned node transitions.
   *
   * <p>
   * Attempts to transition the unassigned node for the specified entityGroup
   * from the expected state to the state in the specified transition data.
   *
   * <p>
   * Method first reads existing data and verifies it is in the expected state.
   * If the node does not exist or the node is not in the expected state, the
   * method returns -1. If the transition is successful, the version number of
   * the node following the transition is returned.
   *
   * <p>
   * If the read state is what is expected, it attempts to write the new state
   * and data into the node. When doing this, it includes the expected version
   * (determined when the existing state was verified) to ensure that only one
   * transition is successful. If there is a version mismatch, the method
   * returns -1.
   *
   * <p>
   * If the write is successful, no watch is set and the method returns true.
   *
   * @param zkw zk reference
   * @param entityGroup entityGroup to be transitioned to opened
   * @param serverName server transition happens on
   * @param endState state to transition node to if all checks pass
   * @param beginState state the node must currently be in to do transition
   * @param expectedVersion expected version of data before modification, or -1
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   */
  public static int transitionNode(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup,
      ServerName serverName, EventType beginState, EventType endState,
      int expectedVersion) throws KeeperException {
    return transitionNode(zkw, entityGroup, serverName, beginState, endState,
        expectedVersion, null);
  }

  public static int transitionNode(ZooKeeperWatcher zkw,
      EntityGroupInfo entityGroup,
      ServerName serverName, EventType beginState, EventType endState,
      int expectedVersion, final byte[] payload) throws KeeperException {
    String encoded = entityGroup.getEncodedName();
    if (LOG.isDebugEnabled()) {
      LOG.debug(zkw.prefix("Attempting to transition node "
          + encoded + " from " + beginState.toString()
          + " to " + endState.toString()));
    }

    String node = getNodeName(zkw, encoded);
    zkw.sync(node);

    // Read existing data of the node
    Stat stat = new Stat();
    byte[] existingBytes = ZKUtil.getDataNoWatch(zkw, node, stat);
    if (existingBytes == null) {
      // Node no longer exists. Return -1. It means unsuccessful transition.
      return -1;
    }
    EntityGroupTransaction rt = getEntityGroupTransition(existingBytes);

    // Verify it is the expected version
    if (expectedVersion != -1 && stat.getVersion() != expectedVersion) {
      LOG.warn(zkw.prefix("Attempt to transition the " + "unassigned node for "
          + encoded + " from " + beginState + " to " + endState + " failed, "
          + "the node existed but was version " + stat.getVersion()
          + " not the expected version " + expectedVersion));
      return -1;
    } else if (beginState.equals(EventType.M_ZK_ENTITYGROUP_OFFLINE)
        && endState.equals(EventType.FSERVER_ZK_ENTITYGROUP_OPENING)
        && expectedVersion == -1 && stat.getVersion() != 0) {
      // the below check ensures that double assignment doesnot happen.
      // When the node is created for the first time then the expected version
      // that is passed will be -1 and the version in znode will be 0.
      // In all other cases the version in znode will be > 0.
      LOG.warn(zkw.prefix("Attempt to transition the " + "unassigned node for "
          + encoded + " from " + beginState + " to " + endState + " failed, "
          + "the node existed but was version " + stat.getVersion()
          + " not the expected version " + expectedVersion));
      return -1;
    }

    // Verify it is in expected state
    EventType et = rt.getEventType();
    if (!et.equals(beginState)) {
      LOG.warn(zkw.prefix("Attempt to transition the " + "unassigned node for "
          + encoded + " from " + beginState + " to " + endState + " failed, "
          + "the node existed but was in the state " + et
          + " set by the server " + serverName));
      return -1;
    }

    // Write new data, ensuring data has not changed since we last read it
    try {
      rt = EntityGroupTransaction.createEntityGroupTransition(endState,
          entityGroup.getEntityGroupName(), serverName, payload);
      if (!ZKUtil.setData(zkw, node, rt.toByteArray(), stat.getVersion())) {
        LOG.warn(zkw.prefix("Attempt to transition the "
            + "unassigned node for " + encoded + " from " + beginState + " to "
            + endState + " failed, "
            + "the node existed and was in the expected state but then when "
            + "setting data we got a version mismatch"));
        return -1;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug(zkw.prefix("Successfully transitioned node " + encoded
            + " from " + beginState + " to " + endState));
      }
      return stat.getVersion() + 1;
    } catch (KeeperException.NoNodeException nne) {
      LOG.warn(zkw.prefix("Attempt to transition the " + "unassigned node for "
          + encoded + " from " + beginState + " to " + endState + " failed, "
          + "the node existed and was in the expected state but then when "
          + "setting data it no longer existed"));
      return -1;
    }
  }

  private static EntityGroupTransaction getEntityGroupTransition(final byte[] bytes)
      throws KeeperException {
    try {
      return EntityGroupTransaction.parseFrom(bytes);
    } catch (DeserializationException e) {
      // Convert to a zk exception for now. Otherwise have to change API
      throw ZKUtil.convert(e);
    }
  }

  /**
   * Gets the current data in the unassigned node for the specified entityGroup
   * name or fully-qualified path.
   *
   * <p>
   * Returns null if the entityGroup does not currently have a node.
   *
   * <p>
   * Sets a watch on the node if the node exists.
   *
   * @param zkw zk reference
   * @param pathOrEntityGroupName fully-specified path or entityGroup name
   * @return znode content
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   */
  public static byte[] getData(ZooKeeperWatcher zkw, String pathOrEntityGroupName)
      throws KeeperException {
    String node = getPath(zkw, pathOrEntityGroupName);
    return ZKUtil.getDataAndWatch(zkw, node);
  }

  /**
   * Gets the current data in the unassigned node for the specified entityGroup
   * name or fully-qualified path.
   *
   * <p>
   * Returns null if the entityGroup does not currently have a node.
   *
   * <p>
   * Sets a watch on the node if the node exists.
   *
   * @param zkw zk reference
   * @param pathOrEntityGroupName fully-specified path or entityGroup name
   * @param stat object to populate the version.
   * @return znode content
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   */
  public static byte[] getDataAndWatch(ZooKeeperWatcher zkw,
      String pathOrEntityGroupName, Stat stat) throws KeeperException {
    String node = getPath(zkw, pathOrEntityGroupName);
    return ZKUtil.getDataAndWatch(zkw, node, stat);
  }

  /**
   * Gets the current data in the unassigned node for the specified entityGroup
   * name or fully-qualified path.
   *
   * <p>
   * Returns null if the entityGroup does not currently have a node.
   *
   * <p>
   * Does not set a watch.
   *
   * @param zkw zk reference
   * @param pathOrEntityGroupName fully-specified path or entityGroup name
   * @param stat object to store node info into on getData call
   * @return znode content
   * @throws org.apache.zookeeper.KeeperException if unexpected zookeeper exception
   */
  public static byte[] getDataNoWatch(ZooKeeperWatcher zkw,
      String pathOrEntityGroupName, Stat stat) throws KeeperException {
    String node = getPath(zkw, pathOrEntityGroupName);
    return ZKUtil.getDataNoWatch(zkw, node, stat);
  }

  /**
   * @param zkw
   * @param pathOrEntityGroupName
   * @return Path to znode
   */
  public static String getPath(final ZooKeeperWatcher zkw,
      final String pathOrEntityGroupName) {
    return pathOrEntityGroupName.startsWith("/") ? pathOrEntityGroupName : getNodeName(
        zkw, pathOrEntityGroupName);
  }

  /**
   * Get the version of the specified znode
   * @param zkw zk reference
   * @param entityGroup entityGroup's info
   * @return the version of the znode, -1 if it doesn't exist
   * @throws org.apache.zookeeper.KeeperException
   */
  public static int getVersion(ZooKeeperWatcher zkw, EntityGroupInfo entityGroup)
      throws KeeperException {
    String znode = getNodeName(zkw, entityGroup.getEncodedName());
    return ZKUtil.checkExists(zkw, znode);
  }

  /**
   * Delete the assignment node regardless of its current state.
   * <p>
   * Fail silent even if the node does not exist at all.
   * @param watcher
   * @param entityGroupInfo
   * @throws org.apache.zookeeper.KeeperException
   */
  public static void deleteNodeFailSilent(ZooKeeperWatcher watcher,
      EntityGroupInfo entityGroupInfo) throws KeeperException {
    String node = getNodeName(watcher, entityGroupInfo.getEncodedName());
    ZKUtil.deleteNodeFailSilent(watcher, node);
  }

  /**
   * Blocks until there are no node in entityGroups in transition.
   * <p>
   * Used in testing only.
   * @param zkw zk reference
   * @throws org.apache.zookeeper.KeeperException
   * @throws InterruptedException
   */
  public static void blockUntilNoRIT(ZooKeeperWatcher zkw)
      throws KeeperException, InterruptedException {
    while (ZKUtil.nodeHasChildren(zkw, zkw.assignmentZNode)) {
      List<String> znodes = ZKUtil.listChildrenAndWatchForNewChildren(zkw,
          zkw.assignmentZNode);
      if (znodes != null && !znodes.isEmpty()) {
        for (String znode : znodes) {
          LOG.debug("ZK RIT -> " + znode);
        }
      }
      Thread.sleep(100);
    }
  }

  /**
   * Blocks until there is at least one node in entityGroups in transition.
   * <p>
   * Used in testing only.
   * @param zkw zk reference
   * @throws org.apache.zookeeper.KeeperException
   * @throws InterruptedException
   */
  public static void blockUntilRIT(ZooKeeperWatcher zkw)
      throws KeeperException, InterruptedException {
    while (!ZKUtil.nodeHasChildren(zkw, zkw.assignmentZNode)) {
      List<String> znodes = ZKUtil.listChildrenAndWatchForNewChildren(zkw,
          zkw.assignmentZNode);
      if (znodes == null || znodes.isEmpty()) {
        LOG.debug("No RIT in ZK");
      }
      Thread.sleep(100);
    }
  }

  /**
   * Presume bytes are serialized unassigned data structure
   * @param znodeBytes
   * @return String of the deserialized znode bytes.
   */
  static String toString(final byte[] znodeBytes) {
    // This method should not exist. Used by ZKUtil stringifying
    // EntityGroupTransition. Have the
    // method in here so EntityGroupTransition does not leak into ZKUtil.
    try {
      EntityGroupTransaction rt = EntityGroupTransaction.parseFrom(znodeBytes);
      return rt.toString();
    } catch (DeserializationException e) {
      return "";
    }
  }

}

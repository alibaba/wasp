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
package com.alibaba.wasp.master.handler;

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.master.AssignmentManager;
import com.alibaba.wasp.master.EntityGroupState;
import com.alibaba.wasp.zookeeper.ZKAssign;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

/**
 * Handles OPENED entityGroup event on FMaster.
 */
public class OpenedEntityGroupHandler extends EventHandler {
  private static final Log LOG = LogFactory
      .getLog(OpenedEntityGroupHandler.class);
  private final AssignmentManager assignmentManager;
  private final EntityGroupInfo entityGroupInfo;
  private final ServerName sn;
  private final int expectedVersion;

  public OpenedEntityGroupHandler(Server server,
      AssignmentManager assignmentManager, EntityGroupInfo entityGroupInfo,
      ServerName sn, int expectedVersion) {
    super(server, EventType.FSERVER_ZK_ENTITYGROUP_OPENED);
    this.assignmentManager = assignmentManager;
    this.entityGroupInfo = entityGroupInfo;
    this.sn = sn;
    this.expectedVersion = expectedVersion;
  }

  @Override
  public void process() {
    // Code to defend against case where we get SPLIT before EntityGroup open
    // processing completes; temporary till we make SPLITs go via zk -- 0.92.
    EntityGroupState entityGroupState = this.assignmentManager
        .getEntityGroupStates().getEntityGroupTransitionState(
            entityGroupInfo.getEncodedName());
    boolean openedNodeDeleted = false;
    if (entityGroupState != null && entityGroupState.isOpened()) {
      openedNodeDeleted = deleteOpenedNode(expectedVersion);
      if (!openedNodeDeleted) {
        LOG.error("The znode of entityGroup "
            + entityGroupInfo.getEntityGroupNameAsString()
            + " could not be deleted.");
      }
    } else {
      LOG.warn("Skipping the onlining of "
          + entityGroupInfo.getEntityGroupNameAsString()
          + " because entityGroups is NOT in EGIT -- presuming this is because it SPLIT");
    }
    if (!openedNodeDeleted) {
      if (this.assignmentManager.getZKTable().isDisablingOrDisabledTable(
          entityGroupInfo.getTableNameAsString())) {
        debugLog(entityGroupInfo,
            "Opened EntityGroup "
                + entityGroupInfo.getEntityGroupNameAsString()
                + " but this table is disabled, triggering close of EntityGroup");
        assignmentManager.unassign(entityGroupInfo);
      }
    }

  }

  private boolean deleteOpenedNode(int expectedVersion) {
    debugLog(entityGroupInfo, "Handling OPENED event for "
        + this.entityGroupInfo.getEntityGroupNameAsString()
            + " from " + this.sn.toString() + "; deleting unassigned node");
    try {
      // delete the opened znode only if the version matches.
      return ZKAssign.deleteNode(server.getZooKeeper(),
          entityGroupInfo.getEncodedName(),
          EventType.FSERVER_ZK_ENTITYGROUP_OPENED,
          expectedVersion);
    } catch (KeeperException.NoNodeException e) {
      // Getting no node exception here means that already the entityGroup has been
      // opened.
      LOG.warn("The znode of the entityGroup "
          + entityGroupInfo.getEntityGroupNameAsString()
          + " would have already been deleted");
      return false;
    } catch (KeeperException e) {
      server.abort(
          "Error deleting OPENED node in ZK ("
              + entityGroupInfo.getEntityGroupNameAsString() + ")", e);
    }
    return false;
  }

  private void debugLog(EntityGroupInfo entityGroupInfo, String string) {
    LOG.debug(string);
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if (server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid();
  }

}

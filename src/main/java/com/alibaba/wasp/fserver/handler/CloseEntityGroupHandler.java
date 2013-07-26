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
package com.alibaba.wasp.fserver.handler;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.fserver.EntityGroup;
import com.alibaba.wasp.fserver.FServerServices;
import com.alibaba.wasp.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * Handles closing of a entityGroup on a FServer.
 */
public class CloseEntityGroupHandler extends EventHandler {
  // NOTE on priorities shutting down. There are none for close. There are some
  // for open. I think that is right. On shutdown, we want the meta to close
  // before root and both to close after the user entityGroups have closed. What
  // about the case where master tells us to shutdown a catalog entityGroup and
  // we
  // have a running queue of user entityGroups to close?
  private static final Log LOG = LogFactory
      .getLog(CloseEntityGroupHandler.class);

  private final int FAILED = -1;
  int expectedVersion = FAILED;

  private final FServerServices fsServices;

  private final EntityGroupInfo entityGroupInfo;

  // If true, the hosting server is aborting. EntityGroup close process is
  // different
  // when we are aborting.
  private final boolean abort;

  // Update zk on closing transitions. Usually true. Its false if cluster
  // is going down. In this case, its the rs that initiates the entityGroup
  // close -- not the master process so state up in zk will unlikely be
  // CLOSING.
  private final boolean zk;

  public CloseEntityGroupHandler(final Server server,
      final FServerServices fsServices, EntityGroupInfo entityGroupInfo,
      boolean abort, final boolean zk, final int versionOfClosingNode,
      EventType eventType) {
    super(server, eventType);
    this.server = server;
    this.fsServices = fsServices;
    this.entityGroupInfo = entityGroupInfo;
    this.abort = abort;
    this.zk = zk;
    this.expectedVersion = versionOfClosingNode;
  }

  public EntityGroupInfo getEntityGroupInfo() {
    return entityGroupInfo;
  }

  @Override
  public void process() {
    // whether removed the entityGroup from rsServices's RIT
    boolean removedFromRIT = false;
    try {
      String name = entityGroupInfo.getEntityGroupNameAsString();
      LOG.debug("Processing close of " + name);
      String encodedEntityGroupName = entityGroupInfo.getEncodedName();
      // Check that this entityGroup is being served here
      EntityGroup entityGroup = this.fsServices
          .getFromOnlineEntityGroups(encodedEntityGroupName);
      if (entityGroup == null) {
        LOG.warn("Received CLOSE for entityGroup " + name
            + " but currently not serving");
        return;
      }

      // Close the entityGroup
      try {
        // If we need to keep updating CLOSING stamp to prevent against
        // a timeout if this is long-running, need to spin up a thread?
        if (!entityGroup.close(abort)) {
          // This entityGroup got closed. Most likely due to a split. So instead
          // of doing the setClosedState() below, let's just ignore cont
          // The split message will clean up the master state.
          LOG.warn("Can't close entityGroup: was already closed during close(): "
              + entityGroupInfo.getEntityGroupNameAsString());
          return;
        }
      } catch (Throwable t) {
        // A throwable here indicates that we couldn't successfully flush the
        // memstore before closing. So, we need to abort the server and allow
        // the master to split our logs in order to recover the data.
        server.abort("Unrecoverable exception while closing entityGroup "
            + entityGroupInfo.getEntityGroupNameAsString()
            + ", still finishing close", t);
        throw new RuntimeException(t);
      }

      this.fsServices.removeFromOnlineEntityGroups(entityGroupInfo
          .getEncodedName());

      // Remove it from RIT before set ZK data
      this.fsServices.getEntityGroupsInTransitionInFS().remove(
          this.entityGroupInfo.getEncodedNameAsBytes());
      removedFromRIT = true;

      if (this.zk) {
        if (setClosedState(this.expectedVersion, entityGroup)) {
          LOG.debug("set entityGroup closed state in zk successfully for entityGroup "
              + name + " sn name: " + this.server.getServerName());
        } else {
          LOG.debug("set entityGroup closed state in zk unsuccessfully for entityGroup "
              + name + " sn name: " + this.server.getServerName());
        }
      }

      // Done! EntityGroup is closed on this FS
      LOG.debug("Closed entityGroup "
          + entityGroup.getEntityGroupNameAsString());
    } finally {
      if (!removedFromRIT) {
        this.fsServices.getEntityGroupsInTransitionInFS().remove(
            this.entityGroupInfo.getEncodedNameAsBytes());
      }
    }
  }

  /**
   * Transition ZK node to CLOSED
   * 
   * @param expectedVersion
   * @return If the state is set successfully
   */
  private boolean setClosedState(final int expectedVersion,
      final EntityGroup entityGroup) {
    try {
      if (ZKAssign.transitionNodeClosed(server.getZooKeeper(), entityGroupInfo,
          server.getServerName(), expectedVersion) == FAILED) {
        LOG.warn("Completed the CLOSE of a entityGroup but when transitioning from "
            + " CLOSING to CLOSED got a version mismatch, someone else clashed "
            + "so now unassigning");
        entityGroup.close();
        return false;
      }
    } catch (NullPointerException e) {
      // I've seen NPE when table was deleted while close was running in unit
      // tests.
      LOG.warn("NPE during close -- catching and continuing...", e);
      return false;
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node from CLOSING to CLOSED", e);
      return false;
    } catch (IOException e) {
      LOG.error("Failed to close entityGroup after failing to transition", e);
      return false;
    }
    return true;
  }
}
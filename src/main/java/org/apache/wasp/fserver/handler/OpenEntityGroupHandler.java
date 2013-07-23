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
package org.apache.wasp.fserver.handler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.Server;
import org.apache.wasp.executor.EventHandler;
import org.apache.wasp.fserver.EntityGroup;
import org.apache.wasp.fserver.FServerServices;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * Handles opening of a entityGroup on a FServer.
 */
public class OpenEntityGroupHandler extends EventHandler {
  // NOTE on priorities shutting down. There are none for close. There are some
  // for open. I think that is right. On shutdown, we want the meta to close
  // before root and both to close after the user entityGroups have closed. What
  // about the case where master tells us to shutdown a catalog entityGroup and
  // we
  // have a running queue of user entityGroups to close?
  private static final Log LOG = LogFactory
      .getLog(OpenEntityGroupHandler.class);

  private final int FAILED = -1;
  int expectedVersion = FAILED;

  // We get version of our znode at start of open process and monitor it across
  // the total open. We'll fail the open if someone hijacks our znode; we can
  // tell this has happened if version is not as expected.
  private volatile int version = -1;

  private final FServerServices fsServices;

  private final EntityGroupInfo entityGroupInfo;

  private final FTable table;

  // version of the offline node that was set by the master
  private volatile int versionOfOfflineNode = -1;

  // If true, the hosting server is aborting. EntityGroup close process is
  // different
  // when we are aborting.

  // Update zk on closing transitions. Usually true. Its false if cluster
  // is going down. In this case, its the rs that initiates the entityGroup
  // close -- not the master process so state up in zk will unlikely be
  // CLOSING.

   /**
   * Default base class constructor.
   */
  public OpenEntityGroupHandler(final Server server,
      final FServerServices fsServices, EntityGroupInfo entityGroupInfo,
      FTable table) {
    this(server, fsServices, entityGroupInfo, table, EventType.M_FSERVER_OPEN_ENTITYGROUP, -1);
  }

  public OpenEntityGroupHandler(final Server server,
      final FServerServices fsServices, EntityGroupInfo entityGroupInfo,
      final FTable table, EventType eventType, int versionOfOfflineNode) {
    super(server, eventType);
    this.fsServices = fsServices;
    this.entityGroupInfo = entityGroupInfo;
    this.table = table;
    this.versionOfOfflineNode = versionOfOfflineNode;
  }
  
  public OpenEntityGroupHandler(final Server server,
      final FServerServices fsServices, EntityGroupInfo entityGroupInfo,
      FTable ftd, int versionOfOfflineNode) {
    this(server, fsServices, entityGroupInfo, ftd, EventType.M_FSERVER_OPEN_ENTITYGROUP,
        versionOfOfflineNode);
  }
  
  public EntityGroupInfo getEntityGroupInfo() {
    return entityGroupInfo;
  }

  @Override
  public void process() throws IOException {
    try {
      final String name = entityGroupInfo.getEntityGroupNameAsString();
      if (this.server.isStopped() || this.fsServices.isStopping()) {
        return;
      }
      final String encodedName = entityGroupInfo.getEncodedName();

      // Check that this entityGroup is not already online
      EntityGroup entityGroup = this.fsServices
          .getFromOnlineEntityGroups(encodedName);

      // If fails, just return. Someone stole the entityGroup from under us.
      // Calling transitionZookeeperOfflineToOpening initalizes this.version.
      if (!transitionZookeeperOfflineToOpening(encodedName,
          versionOfOfflineNode)) {
        LOG.warn("EntityGroup was hijacked? It no longer exists, encodedName="
            + encodedName);
        return;
      }

      // Open entityGroup. After a successful open, failures in subsequent
      // processing needs to do a close as part of cleanup.
      entityGroup = openEntityGroup();
      if (entityGroup == null) {
        tryTransitionToFailedOpen(entityGroupInfo);
        return;
      }
      boolean failed = true;
      if (tickleOpening("post_entitygroup_open")) {
        if (updateMeta(entityGroup)) {
          failed = false;
        }
      }
      if (failed || this.server.isStopped() || this.fsServices.isStopping()) {
        cleanupFailedOpen(entityGroup);
        tryTransitionToFailedOpen(entityGroupInfo);
        return;
      }

      if (!transitionToOpened(entityGroup)) {
        // If we fail to transition to opened, it's because of one of two cases:
        // (a) we lost our ZK lease
        // OR (b) someone else opened the entityGroup before us
        // In either case, we don't need to transition to FAILED_OPEN state.
        // In case (a), the Master will process us as a dead server. In case
        // (b) the entityGroup is already being handled elsewhere anyway.
        cleanupFailedOpen(entityGroup);
        return;
      }
      // Successful entityGroup open, and add it to OnlineEntityGroups
      this.fsServices.addToOnlineEntityGroups(entityGroup);

      // Done! Successful entityGroup open
      LOG.debug("Opened " + name + " on server:" + this.server.getServerName());
    } finally {
      this.fsServices.getEntityGroupsInTransitionInFS().remove(
          this.entityGroupInfo.getEncodedNameAsBytes());
    }
  }

  private void cleanupFailedOpen(EntityGroup entityGroup) throws IOException {
    if (entityGroup != null)
      entityGroup.close();

  }

  /**
   * Update ZK, ROOT or META. This can take a while if for example the .META. is
   * not available -- if server hosting .META. crashed and we are waiting on it
   * to come back -- so run in a thread and keep updating znode state meantime
   * so master doesn't timeout our entityGroup-in-transition. Caller must
   * cleanup entityGroup if this fails.
   */
  boolean updateMeta(final EntityGroup entityGroup) {
    if (this.server.isStopped() || this.fsServices.isStopping()) {
      return false;
    }
    // Object we do wait/notify on. Make it boolean. If set, we're done.
    // Else, wait.
    final AtomicBoolean signaller = new AtomicBoolean(false);
    PostOpenDeployTasksThread t = new PostOpenDeployTasksThread(entityGroup,
        this.server, this.fsServices, signaller);
    t.start();
    int assignmentTimeout = this.server.getConfiguration().getInt(
        "wasp.master.assignment.timeoutmonitor.period", 10000);
    // Total timeout for meta edit. If we fail adding the edit then close out
    // the entityGroup and let it be assigned elsewhere.
    long timeout = assignmentTimeout * 10;
    long now = System.currentTimeMillis();
    long endTime = now + timeout;
    // Let our period at which we update OPENING state to be be 1/3rd of the
    // entityGroups-in-transition timeout period.
    long period = Math.max(1, assignmentTimeout / 3);
    long lastUpdate = now;
    boolean tickleOpening = true;
    while (!signaller.get() && t.isAlive() && !this.server.isStopped()
        && !this.fsServices.isStopping() && (endTime > now)) {
      long elapsed = now - lastUpdate;
      if (elapsed > period) {
        // Only tickle OPENING if postOpenDeployTasks is taking some time.
        lastUpdate = now;
        tickleOpening = tickleOpening("post_open_deploy");
      }
      synchronized (signaller) {
        try {
          signaller.wait(period);
        } catch (InterruptedException e) {
          // Go to the loop check.
        }
      }
      now = System.currentTimeMillis();
    }
    // Is thread still alive? We may have left above loop because server is
    // stopping or we timed out the edit. Is so, interrupt it.
    if (t.isAlive()) {
      if (!signaller.get()) {
        // Thread still running; interrupt
        LOG.debug("Interrupting thread " + t);
        t.interrupt();
      }
      try {
        t.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted joining "
            + entityGroup.getEntityGroupInfo().getEntityGroupNameAsString(), ie);
        Thread.currentThread().interrupt();
      }
    }

    // Was there an exception opening the entityGroup? This should trigger on
    // InterruptedException too. If so, we failed. Even if tickle opening fails
    // then it is a failure.
    return ((!Thread.interrupted() && t.getException() == null) && tickleOpening);
  }

  /**
   * Thread to run entityGroup post open tasks. Call {@link #getException()}
   * after the thread finishes to check for exceptions running
   * {@link FServerServices#postOpenDeployTasks(EntityGroup, boolean)} .
   */
  static class PostOpenDeployTasksThread extends Thread {
    private Exception exception = null;
    private final Server server;
    private final FServerServices services;
    private final EntityGroup entityGroup;
    private final AtomicBoolean signaller;

    PostOpenDeployTasksThread(final EntityGroup entityGroup,
        final Server server, final FServerServices services,
        final AtomicBoolean signaller) {
      super("PostOpenDeployTasks:"
          + entityGroup.getEntityGroupInfo().getEncodedName());
      this.setDaemon(true);
      this.server = server;
      this.services = services;
      this.entityGroup = entityGroup;
      this.signaller = signaller;
    }

    public void run() {
      try {
        this.services.postOpenDeployTasks(this.entityGroup, false);
      } catch (Exception e) {
        LOG.warn("Exception running postOpenDeployTasks; entityGroup="
            + this.entityGroup.getEntityGroupInfo().getEncodedName(), e);
        this.exception = e;
      }
      // We're done. Set flag then wake up anyone waiting on thread to complete.
      this.signaller.set(true);
      synchronized (this.signaller) {
        this.signaller.notify();
      }
    }

    /**
     * @return Null or the run exception; call this method after thread is done.
     */
    Exception getException() {
      return this.exception;
    }
  }

  /**
   * Transition ZK node from OFFLINE to OPENING.
   * 
   * @param encodedName
   *          Name of the znode file (EntityGroup encodedName is the znode
   *          name).
   * @param versionOfOfflineNode
   *          - version Of OfflineNode that needs to be compared before changing
   *          the node's state from OFFLINE
   * @return True if successful transition.
   */
  boolean transitionZookeeperOfflineToOpening(final String encodedName,
      int versionOfOfflineNode) {
    try {
      // Initialize the znode version.
      this.version = ZKAssign.transitionNode(server.getZooKeeper(),
          entityGroupInfo, server.getServerName(),
          EventType.M_ZK_ENTITYGROUP_OFFLINE,
          EventType.FSERVER_ZK_ENTITYGROUP_OPENING, versionOfOfflineNode);
    } catch (KeeperException e) {
      LOG.error("Error transition from OFFLINE to OPENING for entityGroup="
          + encodedName, e);
    }
    boolean b = isGoodVersion();
    if (!b) {
      LOG.warn("Failed transition from OFFLINE to OPENING for entityGroup="
          + encodedName);
    }
    return b;
  }

  /**
   * @param entityGroup
   *          EntityGroup we're working on.
   * @return whether znode is successfully transitioned to OPENED state.
   * @throws java.io.IOException
   */
  private boolean transitionToOpened(final EntityGroup entityGroup)
      throws IOException {
    boolean result = false;
    EntityGroupInfo egi = entityGroup.getEntityGroupInfo();
    final String name = egi.getEntityGroupNameAsString();
    // Finally, Transition ZK node to OPENED
    try {
      if (ZKAssign.transitionNodeOpened(this.server.getZooKeeper(), egi,
          this.server.getServerName(), this.version) == -1) {
        LOG.warn("Completed the OPEN of entityGroup "
            + name
            + " but when transitioning from "
            + " OPENING to OPENED got a version mismatch, someone else clashed "
            + "so now unassigning -- closing entityGroup on server: "
            + this.server.getServerName());
      } else {
        LOG.debug("entityGroup transitioned to opened in zookeeper: "
            + entityGroup.getEntityGroupInfo() + ", server: "
            + this.server.getServerName());
        result = true;
      }
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node " + name
          + " from OPENING to OPENED -- closing entityGroup", e);
    }
    return result;
  }

  /**
   * @param egi
   *          we're working on. This is not guaranteed to succeed, we just do
   *          our best.
   * @return whether znode is successfully transitioned to FAILED_OPEN state.
   */
  private boolean tryTransitionToFailedOpen(final EntityGroupInfo egi) {
    boolean result = false;
    final String name = egi.getEntityGroupNameAsString();
    try {
      LOG.info("Opening of entityGroup " + egi
          + " failed, marking as FAILED_OPEN in ZK");
      if (ZKAssign.transitionNode(this.server.getZooKeeper(), egi,
          this.server.getServerName(),
          EventType.FSERVER_ZK_ENTITYGROUP_OPENING,
          EventType.FSERVER_ZK_ENTITYGROUP_FAILED_OPEN, this.version) == -1) {
        LOG.warn("Unable to mark entityGroup " + egi + " as FAILED_OPEN. "
            + "It's likely that the master already timed out this open "
            + "attempt, and thus another RS already has the entityGroup.");
      } else {
        result = true;
      }
    } catch (KeeperException e) {
      LOG.error("Failed transitioning node " + name
          + " from OPENING to FAILED_OPEN", e);
    }
    return result;
  }

  /**
   * @return Instance of EntityGroup if successful open else null.
   */
  EntityGroup openEntityGroup() {
    EntityGroup entityGroup = null;
    try {
      // Instantiate the entityGroup. This also periodically tickles our zk
      // OPENING
      // state so master doesn't timeout this entityGroup in transition.
      entityGroup = EntityGroup.openEntityGroup(this.entityGroupInfo,
          this.table, this.server.getConfiguration(), this.fsServices,
          new CancelableProgressable() {
            public boolean progress() {
              // We may lose the znode ownership during the open. Currently its
              // too hard interrupting ongoing entityGroup open. Just let it
              // complete
              // and check we still have the znode after entityGroup open.
              return tickleOpening("open_entitygroup_progress");
            }
          });
    } catch (Throwable t) {
      // We failed open. Our caller will see the 'null' return value
      // and transition the node back to FAILED_OPEN. If that fails,
      // we rely on the Timeout Monitor in the master to reassign.
      LOG.error(
          "Failed open of entityGroup="
              + this.entityGroupInfo.getEntityGroupNameAsString()
              + ", starting to roll back the global memstore size.", t);
    }
    return entityGroup;
  }

  /**
   * Transition ZK node to CLOSED
   * 
   * @param expectedVersion
   * @return If the state is set successfully
   */
  private boolean setClosedState(final int expectedVersion,
      final EntityGroupInfo entityGroupInfo) {
    return false;
  }

  /**
   * Update our OPENING state in zookeeper. Do this so master doesn't timeout
   * this entityGroup-in-transition.
   * 
   * @param context
   *          Some context to add to logs if failure
   * @return True if successful transition.
   */
  boolean tickleOpening(final String context) {
    // If previous checks failed... do not try again.
    if (!isGoodVersion())
      return false;
    String encodedName = this.entityGroupInfo.getEncodedName();
    try {
      this.version = ZKAssign.retransitionNodeOpening(server.getZooKeeper(),
          this.entityGroupInfo, this.server.getServerName(), this.version);
    } catch (KeeperException e) {
      LOG.warn("Exception refreshing OPENING; entityGroup=" + encodedName
          + ", context=" + context, e);
      this.version = -1;
    }
    boolean b = isGoodVersion();
    if (!b) {
      LOG.warn("Failed refreshing OPENING; entityGroup=" + encodedName
          + ", context=" + context);
    }
    return b;
  }

  private boolean isGoodVersion() {
    return this.version != -1;
  }

}

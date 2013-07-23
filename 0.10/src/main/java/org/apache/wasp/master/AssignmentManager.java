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
package org.apache.wasp.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.wasp.DeserializationException;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.EntityGroupTransaction;
import org.apache.wasp.FConstants;
import org.apache.wasp.NotServingEntityGroupException;
import org.apache.wasp.Server;
import org.apache.wasp.ServerName;
import org.apache.wasp.TableNotFoundException;
import org.apache.wasp.executor.EventHandler;
import org.apache.wasp.executor.EventHandler.EventType;
import org.apache.wasp.executor.ExecutorService;
import org.apache.wasp.fserver.EntityGroupAlreadyInTransitionException;
import org.apache.wasp.fserver.EntityGroupOpeningState;
import org.apache.wasp.fserver.FServerStoppedException;
import org.apache.wasp.ipc.ServerNotRunningYetException;
import org.apache.wasp.master.handler.ClosedEntityGroupHandler;
import org.apache.wasp.master.handler.DisableTableHandler;
import org.apache.wasp.master.handler.EnableTableHandler;
import org.apache.wasp.master.handler.OpenedEntityGroupHandler;
import org.apache.wasp.master.handler.SplitEntityGroupHandler;
import org.apache.wasp.master.metrics.MetricsMaster;
import org.apache.wasp.meta.FMetaReader;
import org.apache.wasp.meta.FMetaScanner;
import org.apache.wasp.util.KeyLocker;
import org.apache.wasp.zookeeper.ZKAssign;
import org.apache.wasp.zookeeper.ZKTable;
import org.apache.wasp.zookeeper.ZKUtil;
import org.apache.wasp.zookeeper.ZooKeeperListener;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

/**
 * Manages and performs entityGroup assignment.
 * <p>
 * Monitors ZooKeeper for events related to entityGroups in transition.
 * <p>
 * Handles existing entityGroups in transition during master failover.
 */
public class AssignmentManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(AssignmentManager.class);

  public static final ServerName HBCK_CODE_SERVERNAME = new ServerName(
      FConstants.WBCK_CODE_NAME, -1, -1L);

  protected final Server server;

  private FServerManager serverManager;

  final TimeoutMonitor timeoutMonitor;

  private TimerUpdater timerUpdater;

  private LoadBalancer balancer;

  final private KeyLocker<String> locker = new KeyLocker<String>();

  /**
   * Map of entityGroups to reopen after the schema of a table is changed. Key -
   * encoded entityGroup name, value - EntityGroupInfo
   */
  private final Map<String, EntityGroupInfo> entityGroupsToReopen;

  /*
   * Maximum times we recurse an assignment/unassignment. See below in {@link
   * #assign()} and {@link #unassign()}.
   */
  private final int maximumAttempts;

  /**
   * Plans for entityGroup movement. Key is the encoded version of a entityGroup
   * name
   */
  // TODO: When do plans get cleaned out? Ever? In server open and in server
  // shutdown processing -- St.Ack
  // All access to this Map must be synchronized.
  final NavigableMap<String, EntityGroupPlan> entityGroupPlans = new TreeMap<String, EntityGroupPlan>();

  private final ZKTable zkTable;

  /**
   * Contains the server which need to update timer, these servers will be
   * handled by {@link TimerUpdater}
   */
  private final ConcurrentSkipListSet<ServerName> serversInUpdatingTimer = new ConcurrentSkipListSet<ServerName>();

  private final ExecutorService executorService;

  // Thread pool executor service for timeout monitor
  private java.util.concurrent.ExecutorService threadPoolExecutorService;

  // A bunch of ZK events workers. Each is a single thread executor service
  private java.util.concurrent.ExecutorService[] zkEventWorkers;

  private List<EventType> ignoreStatesFSOffline = Arrays
      .asList(new EventType[] { EventType.FSERVER_ZK_ENTITYGROUP_FAILED_OPEN,
          EventType.FSERVER_ZK_ENTITYGROUP_CLOSED });

  // metrics instance to send metrics for EGITs
  MetricsMaster metricsMaster;

  private final EntityGroupStates entityGroupStates;

  /**
   * Indicator that AssignmentManager has recovered the entityGroup states so
   * that ServerShutdownHandler can be fully enabled and re-assign entityGroups
   * of dead servers. So that when re-assignment happens, AssignmentManager has
   * proper entityGroup states.
   */
  final AtomicBoolean failoverCleanupDone = new AtomicBoolean(false);

  /**
   * Constructs a new assignment manager.
   * 
   * @param server
   * @param serverManager
   * @param service
   * @param metricsMaster
   * @throws KeeperException
   * @throws IOException
   */
  public AssignmentManager(Server server, FServerManager serverManager,
      final LoadBalancer balancer, final ExecutorService service,
      MetricsMaster metricsMaster) throws KeeperException, IOException {
    super(server.getZooKeeper());
    this.server = server;
    this.serverManager = serverManager;
    this.executorService = service;
    this.entityGroupsToReopen = Collections
        .synchronizedMap(new HashMap<String, EntityGroupInfo>());
    Configuration conf = server.getConfiguration();
    this.timeoutMonitor = new TimeoutMonitor(conf.getInt(
        "wasp.master.assignment.timeoutmonitor.period", 30000), server,
        serverManager, conf.getInt(
            "wasp.master.assignment.timeoutmonitor.timeout", 600000));
    this.timerUpdater = new TimerUpdater(conf.getInt(
        "wasp.master.assignment.timerupdater.period", 10000), server);
    Threads.setDaemonThreadRunning(timerUpdater.getThread(),
        server.getServerName() + ".timerUpdater");
    this.zkTable = new ZKTable(this.watcher);
    this.maximumAttempts = this.server.getConfiguration().getInt(
        "wasp.assignment.maximum.attempts", 10);
    this.balancer = balancer;
    int maxThreads = conf.getInt("wasp.assignment.threads.max", 30);
    this.threadPoolExecutorService = Threads.getBoundedCachedThreadPool(
        maxThreads, 60L, TimeUnit.SECONDS, newDaemonThreadFactory("hbase-am"));
    this.metricsMaster = metricsMaster;// can be null only with tests.
    this.entityGroupStates = new EntityGroupStates(server, serverManager);

    int workers = conf.getInt("wasp.assignment.zkevent.workers", 5);
    zkEventWorkers = new java.util.concurrent.ExecutorService[workers];
    ThreadFactory threadFactory = newDaemonThreadFactory("am-zkevent-worker");
    for (int i = 0; i < workers; i++) {
      zkEventWorkers[i] = Threads.getBoundedCachedThreadPool(1, 60L,
          TimeUnit.SECONDS, threadFactory);
    }
  }

  void startTimeOutMonitor() {
    Threads.setDaemonThreadRunning(timeoutMonitor.getThread(),
        server.getServerName() + ".timeoutMonitor");
  }

  /**
   * Get a named {@link ThreadFactory} that just builds daemon threads
   * 
   * @param prefix
   *          name prefix for all threads created from the factory
   * @return a thread factory that creates named, daemon threads
   */
  private static ThreadFactory newDaemonThreadFactory(final String prefix) {
    final ThreadFactory namedFactory = Threads.getNamedThreadFactory(prefix);
    return new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = namedFactory.newThread(r);
        if (!t.isDaemon()) {
          t.setDaemon(true);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
          t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
      }
    };
  }

  /**
   * @return Instance of ZKTable.
   */
  public ZKTable getZKTable() {
    // These are 'expensive' to make involving trip to zk ensemble so allow
    // sharing.
    return this.zkTable;
  }

  /**
   * This SHOULD not be public. It is public now because of some unit tests.
   * 
   * TODO: make it package private and keep EntityGroupStates in the master
   * package
   */
  public EntityGroupStates getEntityGroupStates() {
    return entityGroupStates;
  }

  public EntityGroupPlan getEntityGroupReopenPlan(EntityGroupInfo egInfo) {
    return new EntityGroupPlan(egInfo, null,
        entityGroupStates.getFServerOfEntityGroup(egInfo));
  }

  /**
   * Add a entityGroupPlan for the specified entityGroup.
   * 
   * @param encodedName
   * @param plan
   */
  public void addPlan(String encodedName, EntityGroupPlan plan) {
    synchronized (entityGroupPlans) {
      entityGroupPlans.put(encodedName, plan);
    }
  }

  /**
   * Add a map of entityGroup plans.
   */
  public void addPlans(Map<String, EntityGroupPlan> plans) {
    synchronized (entityGroupPlans) {
      entityGroupPlans.putAll(plans);
    }
  }

  /**
   * Set the list of entityGroups that will be reopened because of an update in
   * table schema
   * 
   * @param entityGroups
   *          list of entityGroups that should be tracked for reopen
   */
  public void setEntityGroupsToReopen(List<EntityGroupInfo> entityGroups) {
    for (EntityGroupInfo egInfo : entityGroups) {
      entityGroupsToReopen.put(egInfo.getEncodedName(), egInfo);
    }
  }

  /**
   * Used by the client to identify if all entityGroups have the schema updates
   * 
   * @param tableName
   * @return Pair indicating the status of the alter command
   * @throws IOException
   */
  public Pair<Integer, Integer> getReopenStatus(byte[] tableName)
      throws IOException {
    List<EntityGroupInfo> egInfos = FMetaReader.getTableEntityGroups(
        server.getConfiguration(), tableName);
    Integer pending = 0;
    for (EntityGroupInfo egInfo : egInfos) {
      String name = egInfo.getEncodedName();
      // no lock concurrent access ok: sequential consistency respected.
      if (entityGroupsToReopen.containsKey(name)
          || entityGroupStates.isEntityGroupInTransition(name)) {
        pending++;
      }
    }
    return new Pair<Integer, Integer>(pending, egInfos.size());
  }

  /**
   * Used by ServerShutdownHandler to make sure AssignmentManager has completed
   * the failover cleanup before re-assigning entityGroups of dead servers. So
   * that when re-assignment happens, AssignmentManager has proper entityGroup
   * states.
   */
  public boolean isFailoverCleanupDone() {
    return failoverCleanupDone.get();
  }

  /**
   * Now, failover cleanup is completed. Notify server manager to process queued
   * up dead servers processing, if any.
   */
  void failoverCleanupDone() {
    failoverCleanupDone.set(true);
    serverManager.processQueuedDeadServers();
  }

  /**
   * Called on startup. Figures whether a fresh cluster start of we are joining
   * extant running cluster.
   * 
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  void joinCluster() throws IOException, KeeperException, InterruptedException {
    // Concurrency note: In the below the accesses on entityGroupsInTransition
    // are
    // outside of a synchronization block where usually all accesses to EGIT are
    // synchronized. The presumption is that in this case it is safe since this
    // method is being played by a single thread on startup.

    // TODO: EntityGroups that have a null location and are not in
    // entityGroupsInTransitions
    // need to be handled.

    // Scan FMETA to build list of existing entityGroups, servers, and
    // assignment
    // Returns servers who have not checked in (assumed dead) and their
    // entityGroups
    Map<ServerName, List<EntityGroupInfo>> deadServers = rebuildUserEntityGroups();

    // This method will assign all user entityGroups if a clean server startup
    // or
    // it will reconstruct master state and cleanup any leftovers from
    // previous master process.
    processDeadServersAndEntityGroupsInTransition(deadServers);

    recoverTableInDisablingState();
    recoverTableInEnablingState();
  }

  /**
   * Process all entityGroups that are in transition in zookeeper and also
   * processes the list of dead servers by scanning the FMETA. Used by master
   * joining an cluster. If we figure this is a clean cluster startup, will
   * assign all user entityGroups.
   * 
   * @param deadServers
   *          Map of dead servers and their entityGroups. Can be null.
   * @throws KeeperException
   * @throws IOException
   * @throws InterruptedException
   */
  void processDeadServersAndEntityGroupsInTransition(
      final Map<ServerName, List<EntityGroupInfo>> deadServers)
      throws KeeperException, IOException, InterruptedException {
    List<String> nodes = ZKUtil.listChildrenNoWatch(watcher,
        watcher.assignmentZNode);

    if (nodes == null) {
      String errorMessage = "Failed to get the children from ZK";
      server.abort(errorMessage, new IOException(errorMessage));
      return;
    }

    boolean failover = !serverManager.getDeadServers().isEmpty();

    if (!failover) {
      // Run through all entityGroups. If they are not assigned and not in EGIT,
      // then
      // its a clean cluster startup, else its a failover.
      Map<EntityGroupInfo, ServerName> entityGroups = entityGroupStates
          .getEntityGroupAssignments();
      for (Map.Entry<EntityGroupInfo, ServerName> e : entityGroups.entrySet()) {
        if (e.getValue() != null) {
          LOG.debug("Found " + e + " out on cluster");
          failover = true;
          break;
        }
        if (nodes.contains(e.getKey().getEncodedName())) {
          LOG.debug("Found " + e.getKey().getEntityGroupNameAsString()
              + " in EGITs");
          failover = true;
          break;
        }
      }
    }

    // If we found user entityGroups out on cluster, its a failover.
    if (failover) {
      LOG.info("Found entityGroups out on cluster or in EGIT; failover");
      // Process list of dead servers and entityGroups in EGIT.
      processDeadServersAndRecoverLostEntityGroups(deadServers, nodes);
    } else {
      // Fresh cluster startup.
      LOG.info("Clean cluster startup. Assigning userentityGroups");
      assignAllUserEntityGroups();
    }
  }

  /**
   * If entityGroup is up in zk in transition, then do fixup and block and wait
   * until the entityGroup is assigned and out of transition. Used on startup
   * for catalog entityGroups.
   * 
   * @param egInfo
   *          EntityGroup to look for.
   * @return True if we processed a entityGroup in transition else false if
   *         entityGroup was not up in zk in transition.
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   */
  boolean processEntityGroupInTransitionAndBlockUntilAssigned(
      final EntityGroupInfo egInfo) throws InterruptedException,
      KeeperException, IOException {
    boolean intransistion = processEntityGroupInTransition(
        egInfo.getEncodedName(), egInfo);
    if (!intransistion)
      return intransistion;
    LOG.debug("Waiting on " + egInfo.getEncodedName());
    while (!this.server.isStopped()
        && this.entityGroupStates.isEntityGroupInTransition(egInfo
            .getEncodedName())) {
      // We put a timeout because we may have the entityGroup getting in just
      // between the test
      // and the waitForUpdate
      this.entityGroupStates.waitForUpdate(100);
    }
    return intransistion;
  }

  /**
   * Process failover of new master for entityGroup
   * <code>encodedEntityGroupName</code> up in zookeeper.
   * 
   * @param encodedEntityGroupName
   *          EntityGroup to process failover for.
   * @param entityGroupInfo
   *          If null we'll go get it from meta table.
   * @return True if we processed <code>entityGroupInfo</code> as a EGIT.
   * @throws KeeperException
   * @throws IOException
   */
  boolean processEntityGroupInTransition(final String encodedEntityGroupName,
      final EntityGroupInfo entityGroupInfo) throws KeeperException,
      IOException {
    // We need a lock here to ensure that we will not put the same entityGroup
    // twice
    // It has no reason to be a lock shared with the other operations.
    // We can do the lock on the entityGroup only, instead of a global lock:
    // what we want to ensure
    // is that we don't have two threads working on the same entityGroup.
    Lock lock = locker.acquireLock(encodedEntityGroupName);
    try {
      Stat stat = new Stat();
      byte[] data = ZKAssign.getDataAndWatch(watcher, encodedEntityGroupName,
          stat);
      if (data == null)
        return false;
      EntityGroupTransaction rt;
      try {
        rt = EntityGroupTransaction.parseFrom(data);
      } catch (DeserializationException e) {
        LOG.warn("Failed parse znode data", e);
        return false;
      }
      EntityGroupInfo egInfo = entityGroupInfo;
      if (egInfo == null) {
        egInfo = entityGroupStates.getEntityGroupInfo(rt.getEntityGroupName());
        if (egInfo == null)
          return false;
      }
      processEntityGroupsInTransition(rt, egInfo, stat.getVersion());
      return true;
    } finally {
      lock.unlock();
    }
  }

  /**
   * This call is invoked only during failover mode startup, zk assignment node
   * processing. The locker is set in the caller.
   * 
   * It should be private but it is used by some test too.
   */
  void processEntityGroupsInTransition(
      final EntityGroupTransaction egTransition,
      final EntityGroupInfo entityGroupInfo, int expectedVersion)
      throws KeeperException {
    EventType et = egTransition.getEventType();
    // Get ServerName. Could not be null.
    ServerName sn = egTransition.getServerName();
    String encodedEntityGroupName = entityGroupInfo.getEncodedName();
    LOG.info("Processing entityGroup "
        + entityGroupInfo.getEntityGroupNameAsString() + " in state " + et);

    if (entityGroupStates.isEntityGroupInTransition(encodedEntityGroupName)) {
      // Just return
      return;
    }
    switch (et) {
    case M_ZK_ENTITYGROUP_CLOSING:
      // If zk node of the entityGroup was updated by a live server skip this
      // entityGroup and just add it into EGIT.
      if (!serverManager.isServerOnline(sn)) {
        // If was not online, its closed now. Force to OFFLINE and this
        // will get it reassigned if appropriate
        forceOffline(entityGroupInfo, egTransition);
      } else {
        // Just insert entityGroup into EGIT.
        // If this never updates the timeout will trigger new assignment
        entityGroupStates.updateEntityGroupState(egTransition,
            EntityGroupState.State.CLOSING);
      }
      break;

    case FSERVER_ZK_ENTITYGROUP_CLOSED:
    case FSERVER_ZK_ENTITYGROUP_FAILED_OPEN:
      // EntityGroup is closed, insert into EGIT and handle it
      addToEGITandCallClose(entityGroupInfo, EntityGroupState.State.CLOSED,
          egTransition);
      break;

    case M_ZK_ENTITYGROUP_OFFLINE:
      // If zk node of the entityGroup was updated by a live server skip this
      // entityGroup and just add it into EGIT.
      if (!serverManager.isServerOnline(sn)) {
        // EntityGroup is offline, insert into EGIT and handle it like a closed
        addToEGITandCallClose(entityGroupInfo, EntityGroupState.State.OFFLINE,
            egTransition);
      } else {
        // Just insert entityGroup into EGIT.
        // If this never updates the timeout will trigger new assignment
        entityGroupStates.updateEntityGroupState(egTransition,
            EntityGroupState.State.PENDING_OPEN);
      }
      break;

    case FSERVER_ZK_ENTITYGROUP_OPENING:
      if (!serverManager.isServerOnline(sn)) {
        entityGroupStates.updateEntityGroupState(egTransition,
            EntityGroupState.State.OPENING);
        // If the server is not online, it takes some time for timeout monitor
        // to kick in.
        // We know the entityGroup won't open. So we will assign the opening
        // entityGroup
        // immediately too.
        processOpeningState(entityGroupInfo);
      } else {
        // Just insert entityGroup into EGIT.
        // If this never updates the timeout will trigger new assignment
        entityGroupStates.updateEntityGroupState(egTransition,
            EntityGroupState.State.OPENING);
      }
      break;

    case FSERVER_ZK_ENTITYGROUP_OPENED:
      if (!serverManager.isServerOnline(sn)) {
        forceOffline(entityGroupInfo, egTransition);
      } else {
        // EntityGroup is opened, insert into EGIT and handle it
        entityGroupStates.updateEntityGroupState(egTransition,
            EntityGroupState.State.OPEN);
        new OpenedEntityGroupHandler(server, this, entityGroupInfo, sn,
            expectedVersion).process();
      }
      break;
    case FSERVER_ZK_ENTITYGROUP_SPLITTING:
      LOG.debug("Processed entityGroup in state : " + et);
      break;
    case FSERVER_ZK_ENTITYGROUP_SPLIT:
      LOG.debug("Processed entityGroup in state : " + et);
      break;
    default:
      throw new IllegalStateException("Received entityGroup in state :" + et
          + " is not valid");
    }
  }

  /**
   * Put the entityGroup <code>egInfo</code> into an offline state up in zk.
   * 
   * You need to have lock on the entityGroup before calling this method.
   * 
   * @param egInfo
   * @param oldEGt
   * @throws KeeperException
   */
  private void forceOffline(final EntityGroupInfo egInfo,
      final EntityGroupTransaction oldEGt) throws KeeperException {
    // If was on dead server, its closed now. Force to OFFLINE and then
    // handle it like a close; this will get it reassigned if appropriate
    LOG.debug("EGIT " + egInfo.getEncodedName() + " in state="
        + oldEGt.getEventType() + " was on deadserver; forcing offline");
    ZKAssign.createOrForceNodeOffline(this.watcher, egInfo,
        oldEGt.getServerName());
    addToEGITandCallClose(egInfo, EntityGroupState.State.OFFLINE, oldEGt);
  }

  /**
   * Add to the in-memory copy of entityGroups in transition and then call close
   * handler on passed entityGroup <code>egInfo</code>
   * 
   * @param egInfo
   * @param state
   * @param oldData
   */
  private void addToEGITandCallClose(final EntityGroupInfo egInfo,
      final EntityGroupState.State state, final EntityGroupTransaction oldData) {
    entityGroupStates.updateEntityGroupState(oldData, state);
    new ClosedEntityGroupHandler(this.server, this, egInfo).process();
  }

  /**
   * When a entityGroup is closed, it should be removed from the
   * entityGroupsToReopen
   * 
   * @param egInfo
   *          EntityGroupInfo of the entityGroup which was closed
   */
  public void removeClosedEntityGroup(EntityGroupInfo egInfo) {
    if (entityGroupsToReopen.remove(egInfo.getEncodedName()) != null) {
      LOG.debug("Removed entityGroup from reopening entityGroups because it was closed");
    }
  }

  /**
   * Handles various states an unassigned node can be in.
   * <p>
   * Method is called when a state change is suspected for an unassigned node.
   * <p>
   * This deals with skipped transitions (we got a CLOSED but didn't see CLOSING
   * yet).
   * 
   * @param egTransition
   * @param expectedVersion
   */
  private void handleEntityGroup(final EntityGroupTransaction egTransition,
      int expectedVersion) {
    if (egTransition == null) {
      LOG.warn("Unexpected NULL input " + egTransition);
      return;
    }
    final ServerName sn = egTransition.getServerName();
    // Check if this is a special HBCK transition
    if (sn.equals(HBCK_CODE_SERVERNAME)) {
      handleHBCK(egTransition);
      return;
    }
    final long createTime = egTransition.getCreateTime();
    final byte[] entityGroupName = egTransition.getEntityGroupName();
    String encodedName = EntityGroupInfo.encodeEntityGroupName(entityGroupName);
    // Verify this is a known server
    if (!serverManager.isServerOnline(sn)
        && !ignoreStatesFSOffline.contains(egTransition.getEventType())) {
      LOG.warn("Attempted to handle entityGroup transition for server but "
          + "server is not online: " + encodedName);
      return;
    }

    EntityGroupState entityGroupState = entityGroupStates
        .getEntityGroupTransitionState(encodedName);
    long startTime = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      boolean lateEvent = createTime < (startTime - 15000);
      LOG.debug("Handling transition=" + egTransition.getEventType()
          + ", server=" + sn + ", entityGroup="
          + (encodedName == null ? "null" : encodedName)
          + (lateEvent ? ", which is more than 15 seconds late" : "")
          + ", current state from entityGroup state map =" + entityGroupState);
    }
    // We don't do anything for this event,
    // so separate it out, no need to lock/unlock anything
    if (egTransition.getEventType() == EventType.M_ZK_ENTITYGROUP_OFFLINE) {
      return;
    }

    // We need a lock on the entityGroup as we could update it
    Lock lock = locker.acquireLock(encodedName);
    try {
      EntityGroupState latestState = entityGroupStates
          .getEntityGroupTransitionState(encodedName);
      if ((entityGroupState == null && latestState != null)
          || (entityGroupState != null && latestState == null)
          || (entityGroupState != null && latestState != null && latestState
              .getState() != entityGroupState.getState())) {
        LOG.warn("EntityGroup state changed from " + entityGroupState + " to "
            + latestState + ", while acquiring lock");
      }
      long waitedTime = System.currentTimeMillis() - startTime;
      if (waitedTime > 5000) {
        LOG.warn("Took " + waitedTime + "ms to acquire the lock");
      }
      entityGroupState = latestState;
      switch (egTransition.getEventType()) {
      case FSERVER_ZK_ENTITYGROUP_SPLITTING:
        if (!isInStateForSplitting(entityGroupState))
          break;
        entityGroupStates.updateEntityGroupState(egTransition,
            EntityGroupState.State.SPLITTING);
        break;

      case FSERVER_ZK_ENTITYGROUP_SPLIT:
        // EntityGroupState must be null, or SPLITTING or PENDING_CLOSE.
        if (!isInStateForSplitting(entityGroupState))
          break;
        // If null, add SPLITTING state before going to SPLIT
        if (entityGroupState == null) {
          entityGroupState = entityGroupStates.updateEntityGroupState(
              egTransition, EntityGroupState.State.SPLITTING);

          String message = "Received SPLIT for entityGroup " + encodedName
              + " from server " + sn;
          // If still null, it means we cannot find it and it was already
          // processed
          if (entityGroupState == null) {
            LOG.warn(message + " but it doesn't exist anymore,"
                + " probably already processed its split");
            break;
          }
          LOG.info(message
              + " but entityGroup was not first in SPLITTING state; continuing");
        }
        // Check it has daughters.
        byte[] payload = egTransition.getPayload();
        List<EntityGroupInfo> daughters = null;
        try {
          daughters = EntityGroupInfo.parseDelimitedFrom(payload, 0,
              payload.length);
        } catch (IOException e) {
          LOG.error("Dropped split! Failed reading split payload for "
              + encodedName);
          break;
        }
        assert daughters.size() == 2;
        // Assert that we can get a serverinfo for this server.
        if (!this.serverManager.isServerOnline(sn)) {
          LOG.error("Dropped split! ServerName=" + sn + " unknown.");
          break;
        }
        // Run handler to do the rest of the SPLIT handling.
        this.executorService.submit(new SplitEntityGroupHandler(server, this,
            entityGroupState.getEntityGroup(), sn, daughters));
        break;

      case M_ZK_ENTITYGROUP_CLOSING:
        // Should see CLOSING after we have asked it to CLOSE or additional
        // times after already being in state of CLOSING
        if (entityGroupState != null
            && !entityGroupState.isPendingCloseOrClosingOnServer(sn)) {
          LOG.warn("Received CLOSING for entityGroup " + encodedName
              + " from server " + sn + " but entityGroup was in the state "
              + entityGroupState
              + " and not in expected PENDING_CLOSE or CLOSING states,"
              + " or not on the expected server");
          return;
        }
        // Transition to CLOSING (or update stamp if already CLOSING)
        entityGroupStates.updateEntityGroupState(egTransition,
            EntityGroupState.State.CLOSING);
        break;

      case FSERVER_ZK_ENTITYGROUP_CLOSED:
        // Should see CLOSED after CLOSING but possible after PENDING_CLOSE
        if (entityGroupState != null
            && !entityGroupState.isPendingCloseOrClosingOnServer(sn)) {
          LOG.warn("Received CLOSED for entityGroup " + encodedName
              + " from server " + sn + " but entityGroup was in the state "
              + entityGroupState
              + " and not in expected PENDING_CLOSE or CLOSING states,"
              + " or not on the expected server");
          return;
        }
        // Handle CLOSED by assigning elsewhere or stopping if a disable
        // If we got here all is good. Need to update EntityGroupState -- else
        // what follows will fail because not in expected state.
        entityGroupState = entityGroupStates.updateEntityGroupState(
            egTransition, EntityGroupState.State.CLOSED);
        if (entityGroupState != null) {
          removeClosedEntityGroup(entityGroupState.getEntityGroup());
          this.executorService.submit(new ClosedEntityGroupHandler(server,
              this, entityGroupState.getEntityGroup()));
        }
        break;

      case FSERVER_ZK_ENTITYGROUP_FAILED_OPEN:
        if (entityGroupState != null
            && !entityGroupState.isPendingOpenOrOpeningOnServer(sn)) {
          LOG.warn("Received FAILED_OPEN for entityGroup " + encodedName
              + " from server " + sn + " but entityGroup was in the state "
              + entityGroupState
              + " and not in expected PENDING_OPEN or OPENING states,"
              + " or not on the expected server");
          return;
        }
        // Handle this the same as if it were opened and then closed.
        entityGroupState = entityGroupStates.updateEntityGroupState(
            egTransition, EntityGroupState.State.CLOSED);
        // When there are more than one entityGroup server a new FSERVER is
        // selected as the
        // destination and the same is updated in the entityGroupplan.
        // (HBASE-5546)
        if (entityGroupState != null) {
          getEntityGroupPlan(entityGroupState.getEntityGroup(), sn, true);
          this.executorService.submit(new ClosedEntityGroupHandler(server,
              this, entityGroupState.getEntityGroup()));
        }
        break;

      case FSERVER_ZK_ENTITYGROUP_OPENING:
        // Should see OPENING after we have asked it to OPEN or additional
        // times after already being in state of OPENING
        if (entityGroupState != null
            && !entityGroupState.isPendingOpenOrOpeningOnServer(sn)) {
          LOG.warn("Received OPENING for entityGroup " + encodedName
              + " from server " + sn + " but entityGroup was in the state "
              + entityGroupState
              + " and not in expected PENDING_OPEN or OPENING states,"
              + " or not on the expected server");
          return;
        }
        // Transition to OPENING (or update stamp if already OPENING)
        entityGroupStates.updateEntityGroupState(egTransition,
            EntityGroupState.State.OPENING);
        break;

      case FSERVER_ZK_ENTITYGROUP_OPENED:
        // Should see OPENED after OPENING but possible after PENDING_OPEN
        if (entityGroupState != null
            && !entityGroupState.isPendingOpenOrOpeningOnServer(sn)) {
          LOG.warn("Received OPENED for entityGroup " + encodedName
              + " from server " + sn + " but entityGroup was in the state "
              + entityGroupState
              + " and not in expected PENDING_OPEN or OPENING states,"
              + " or not on the expected server");
          return;
        }
        // Handle OPENED by removing from transition and deleted zk node
        entityGroupState = entityGroupStates.updateEntityGroupState(
            egTransition, EntityGroupState.State.OPEN);
        if (entityGroupState != null) {
          this.executorService.submit(new OpenedEntityGroupHandler(server,
              this, entityGroupState.getEntityGroup(), sn, expectedVersion));
        }
        break;

      default:
        throw new IllegalStateException("Received event is not valid.");
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * @return Returns true if this EntityGroupState is splittable; i.e. the
   *         EntityGroupState is currently in splitting state or pending_close
   *         or null (Anything else will return false). (Anything else will
   *         return false).
   */
  private boolean isInStateForSplitting(final EntityGroupState egState) {
    if (egState == null)
      return true;
    if (egState.isSplitting())
      return true;
    if (convertPendingCloseToSplitting(egState))
      return true;
    LOG.warn("Dropped entityGroup split! Not in state good for SPLITTING; egState="
        + egState);
    return false;
  }

  /**
   * If the passed entityGroupState is in PENDING_CLOSE, clean up PENDING_CLOSE
   * state and convert it to SPLITTING instead. This can happen in case where
   * master wants to close a entityGroup at same time a entityGroupserver starts
   * a split. The split won. Clean out old PENDING_CLOSE state.
   * 
   * @param egState
   * @return True if we converted from PENDING_CLOSE to SPLITTING
   */
  private boolean convertPendingCloseToSplitting(final EntityGroupState egState) {
    if (!egState.isPendingClose())
      return false;
    LOG.debug("Converting PENDING_CLOSE to SPLITING; egState=" + egState);
    entityGroupStates.updateEntityGroupState(egState.getEntityGroup(),
        EntityGroupState.State.SPLITTING);
    // Clean up existing state. Clear from entityGroup plans seems all we
    // have to do here by way of clean up of PENDING_CLOSE.
    clearEntityGroupPlan(egState.getEntityGroup());
    return true;
  }

  /**
   * Handle a ZK unassigned node transition triggered by HBCK repair tool.
   * <p>
   * This is handled in a separate code path because it breaks the normal rules.
   * 
   * @param egTransition
   */
  private void handleHBCK(EntityGroupTransaction egTransition) {
    String encodedName = EntityGroupInfo.encodeEntityGroupName(egTransition
        .getEntityGroupName());
    LOG.info("Handling HBCK triggered transition="
        + egTransition.getEventType() + ", server="
        + egTransition.getServerName() + ", entityGroup=" + encodedName);
    EntityGroupState entityGroupState = entityGroupStates
        .getEntityGroupTransitionState(encodedName);
    switch (egTransition.getEventType()) {
    case M_ZK_ENTITYGROUP_OFFLINE:
      EntityGroupInfo entityGroupInfo = null;
      if (entityGroupState != null) {
        entityGroupInfo = entityGroupState.getEntityGroup();
      } else {
        try {
          byte[] name = egTransition.getEntityGroupName();
          Pair<EntityGroupInfo, ServerName> p = FMetaReader
              .getEntityGroupAndLocation(server.getConfiguration(), name);
          entityGroupInfo = p.getFirst();
        } catch (IOException e) {
          LOG.info("Exception reading META doing HBCK repair operation", e);
          return;
        }
      }
      LOG.info("HBCK repair is triggering assignment of entityGroup="
          + entityGroupInfo.getEntityGroupNameAsString());
      // trigger assign, node is already in OFFLINE so don't need to update ZK
      assign(entityGroupInfo, false);
      break;

    default:
      LOG.warn("Received unexpected entityGroup state from HBCK: "
          + egTransition.toString());
      break;
    }

  }

  // ZooKeeper events

  /**
   * New unassigned node has been created.
   * 
   * <p>
   * This happens when an FSERVER begins the OPENING or CLOSING of a entityGroup
   * by creating an unassigned node.
   * 
   * <p>
   * When this happens we must:
   * <ol>
   * <li>Watch the node for further events</li>
   * <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public void nodeCreated(String path) {
    handleAssignmentEvent(path);
  }

  /**
   * Existing unassigned node has had data changed.
   * 
   * <p>
   * This happens when an FSERVER transitions from OFFLINE to OPENING, or
   * between OPENING/OPENED and CLOSING/CLOSED.
   * 
   * <p>
   * When this happens we must:
   * <ol>
   * <li>Watch the node for further events</li>
   * <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public void nodeDataChanged(String path) {
    handleAssignmentEvent(path);
  }

  @Override
  public void nodeDeleted(final String path) {
    if (path.startsWith(watcher.assignmentZNode)) {
      int wi = Math.abs(path.hashCode() % zkEventWorkers.length);
      zkEventWorkers[wi].submit(new Runnable() {
        @Override
        public void run() {
          String entityGroupName = ZKAssign.getEntityGroupName(watcher, path);
          Lock lock = locker.acquireLock(entityGroupName);
          try {
            EntityGroupState egState = entityGroupStates
                .getEntityGroupTransitionState(entityGroupName);
            if (egState == null)
              return;

            EntityGroupInfo entityGroupInfo = egState.getEntityGroup();
            if (egState.isSplit()) {
              LOG.debug("Ephemeral node deleted, entityGroupserver crashed?, "
                  + "clearing from EGIT; egState=" + egState);
              entityGroupOffline(egState.getEntityGroup());
            } else {
              LOG.debug("The znode of entityGroup "
                  + entityGroupInfo.getEntityGroupNameAsString()
                  + " has been deleted.");
              if (egState.isOpened()) {
                ServerName serverName = egState.getServerName();
                entityGroupOnline(entityGroupInfo, serverName);
                LOG.info("The master has opened the entityGroup "
                    + entityGroupInfo.getEntityGroupNameAsString()
                    + " that was online on " + serverName);
                if (getZKTable().isDisablingOrDisabledTable(
                    entityGroupInfo.getTableNameAsString())) {
                  LOG.debug("Opened entityGroup "
                      + entityGroupInfo.getEntityGroupNameAsString()
                      + " but "
                      + "this table is disabled, triggering close of entityGroup");
                  unassign(entityGroupInfo);
                }
              }
            }
          } finally {
            lock.unlock();
          }
        }
      });
    }
  }

  /**
   * New unassigned node has been created.
   * 
   * <p>
   * This happens when an FSERVER begins the OPENING, SPLITTING or CLOSING of a
   * entityGroup by creating a znode.
   * 
   * <p>
   * When this happens we must:
   * <ol>
   * <li>Watch the node for further children changed events</li>
   * <li>Watch all new children for changed events</li>
   * </ol>
   */
  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(watcher.assignmentZNode)) {
      int wi = Math.abs(path.hashCode() % zkEventWorkers.length);
      zkEventWorkers[wi].submit(new Runnable() {
        @Override
        public void run() {
          try {
            // Just make sure we see the changes for the new znodes
            List<String> children = ZKUtil.listChildrenAndWatchForNewChildren(
                watcher, watcher.assignmentZNode);
            if (children != null) {
              for (String child : children) {
                // if entityGroup is in transition, we already have a watch
                // on it, so no need to watch it again. So, as I know for now,
                // this is needed to watch splitting nodes only.
                if (!entityGroupStates.isEntityGroupInTransition(child)) {
                  ZKUtil.watchAndCheckExists(watcher,
                      ZKUtil.joinZNode(watcher.assignmentZNode, child));
                }
              }
            }
          } catch (KeeperException e) {
            server.abort("Unexpected ZK exception reading unassigned children",
                e);
          }
        }
      });
    }
  }

  /**
   * Marks the entityGroup as online. Removes it from entityGroups in transition
   * and updates the in-memory assignment information.
   * <p>
   * Used when a entityGroup has been successfully opened on a entityGroup
   * server.
   * 
   * @param entityGroupInfo
   * @param sn
   */
  void entityGroupOnline(EntityGroupInfo entityGroupInfo, ServerName sn) {
    if (!serverManager.isServerOnline(sn)) {
      LOG.warn("A entityGroup was opened on a dead server, ServerName=" + sn
          + ", entityGroup=" + entityGroupInfo.getEncodedName());
    }

    entityGroupStates.entityGroupOnline(entityGroupInfo, sn);

    // Remove plan if one.
    clearEntityGroupPlan(entityGroupInfo);
    // Add the server to serversInUpdatingTimer
    addToServersInUpdatingTimer(sn);
  }

  /**
   * Pass the assignment event to a worker for processing. Each worker is a
   * single thread executor service. The reason for just one thread is to make
   * sure all events for a given entityGroup are processed in order.
   * 
   * @param path
   */
  private void handleAssignmentEvent(final String path) {
    if (path.startsWith(watcher.assignmentZNode)) {
      int wi = Math.abs(path.hashCode() % zkEventWorkers.length);
      zkEventWorkers[wi].submit(new Runnable() {
        @Override
        public void run() {
          try {
            Stat stat = new Stat();
            byte[] data = ZKAssign.getDataAndWatch(watcher, path, stat);
            if (data == null)
              return;

            EntityGroupTransaction rt = EntityGroupTransaction.parseFrom(data);
            handleEntityGroup(rt, stat.getVersion());
          } catch (KeeperException e) {
            server.abort(
                "Unexpected ZK exception reading unassigned node data", e);
          } catch (DeserializationException e) {
            server.abort("Unexpected exception deserializing node data", e);
          }
        }
      });
    }
  }

  /**
   * Add the server to the set serversInUpdatingTimer, then {@link TimerUpdater}
   * will update timers for this server in background
   * 
   * @param sn
   */
  private void addToServersInUpdatingTimer(final ServerName sn) {
    this.serversInUpdatingTimer.add(sn);
  }

  /**
   * Touch timers for all entityGroups in transition that have the passed
   * <code>sn</code> in common. Call this method whenever a server checks in.
   * Doing so helps the case where a new entityGroupserver has joined the
   * cluster and its been given 1k entityGroups to open. If this method is
   * tickled every time the entityGroup reports in a successful open then the
   * 1k-th entityGroup won't be timed out just because its sitting behind the
   * open of 999 other entityGroups. This method is NOT used as part of bulk
   * assign -- there we have a different mechanism for extending the
   * entityGroups in transition timer (we turn it off temporarily -- because
   * there is no entityGroupplan involved when bulk assigning.
   * 
   * @param sn
   */
  private void updateTimers(final ServerName sn) {
    if (sn == null)
      return;

    // This loop could be expensive.
    // First make a copy of current entityGroupPlan rather than hold sync while
    // looping because holding sync can cause deadlock. Its ok in this loop
    // if the Map we're going against is a little stale
    List<Map.Entry<String, EntityGroupPlan>> rps;
    synchronized (this.entityGroupPlans) {
      rps = new ArrayList<Map.Entry<String, EntityGroupPlan>>(
          entityGroupPlans.entrySet());
    }

    for (Map.Entry<String, EntityGroupPlan> e : rps) {
      if (e.getValue() != null && e.getKey() != null
          && sn.equals(e.getValue().getDestination())) {
        EntityGroupState entityGroupState = entityGroupStates
            .getEntityGroupTransitionState(e.getKey());
        if (entityGroupState != null) {
          entityGroupState.updateTimestampToNow();
        }
      }
    }
  }

  /**
   * Marks the entityGroup as offline. Removes it from entityGroups in
   * transition and removes in-memory assignment information.
   * <p>
   * Used when a entityGroup has been closed and should remain closed.
   * 
   * @param entityGroupInfo
   */
  public void entityGroupOffline(final EntityGroupInfo entityGroupInfo) {
    entityGroupStates.entityGroupOffline(entityGroupInfo);

    // remove the entityGroup plan as well just in case.
    clearEntityGroupPlan(entityGroupInfo);
  }

  public void offlineDisabledEntityGroup(EntityGroupInfo entityGroupInfo) {
    // Disabling so should not be reassigned, just delete the CLOSED node
    LOG.debug("Table being disabled so deleting ZK node and removing from "
        + "entityGroups in transition, skipping assignment of entityGroup "
        + entityGroupInfo.getEntityGroupNameAsString());
    try {
      if (!ZKAssign.deleteClosedNode(watcher, entityGroupInfo.getEncodedName())) {
        // Could also be in OFFLINE mode
        ZKAssign.deleteOfflineNode(watcher, entityGroupInfo.getEncodedName());
      }
    } catch (KeeperException.NoNodeException nne) {
      LOG.debug("Tried to delete closed node for " + entityGroupInfo
          + " but it " + "does not exist so just offlining");
    } catch (KeeperException e) {
      this.server.abort("Error deleting CLOSED node in ZK", e);
    }
    entityGroupOffline(entityGroupInfo);
  }

  // Assignment methods

  /**
   * Assigns the specified entityGroup.
   * <p>
   * If a EntityGroupPlan is available with a valid destination then it will be
   * used to determine what server entityGroup is assigned to. If no
   * EntityGroupPlan is available, entityGroup will be assigned to a random
   * available server.
   * <p>
   * Updates the EntityGroupState and sends the OPEN RPC.
   * <p>
   * This will only succeed if the entityGroup is in transition and in a CLOSED
   * or OFFLINE state or not in transition (in-memory not zk), and of course,
   * the chosen server is up and running (It may have just crashed!). If the
   * in-memory checks pass, the zk node is forced to OFFLINE before assigning.
   * 
   * @param entityGroup
   *          server to be assigned
   * @param setOfflineInZK
   *          whether ZK node should be created/transitioned to an OFFLINE state
   *          before assigning the entityGroup
   */
  public void assign(EntityGroupInfo entityGroup, boolean setOfflineInZK) {
    assign(entityGroup, setOfflineInZK, false);
  }

  /**
   * Use care with forceNewPlan. It could cause double assignment.
   */
  public void assign(EntityGroupInfo entityGroup, boolean setOfflineInZK,
      boolean forceNewPlan) {
    if (!setOfflineInZK && isDisabledorDisablingEntityGroupInEGIT(entityGroup)) {
      return;
    }
    if (this.serverManager.isClusterShutdown()) {
      LOG.info("Cluster shutdown is set; skipping assign of "
          + entityGroup.getEntityGroupNameAsString());
      return;
    }
    String encodedName = entityGroup.getEncodedName();
    Lock lock = locker.acquireLock(encodedName);
    try {
      EntityGroupState state = forceEntityGroupStateToOffline(entityGroup,
          forceNewPlan);
      if (state != null) {
        assign(state, setOfflineInZK, forceNewPlan);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Bulk assign entityGroups to <code>destination</code>.
   * 
   * @param destination
   * @param entityGroups
   *          EntityGroups to assign.
   * @return true if successful
   */
  boolean assign(final ServerName destination,
      final List<EntityGroupInfo> entityGroups) {
    int entityGroupCount = entityGroups.size();
    if (entityGroupCount == 0) {
      return true;
    }
    LOG.debug("Bulk assigning " + entityGroupCount + " entityGroup(s) to "
        + destination.toString());

    Set<String> encodedNames = new HashSet<String>(entityGroupCount);
    for (EntityGroupInfo entityGroup : entityGroups) {
      encodedNames.add(entityGroup.getEncodedName());
    }

    List<EntityGroupInfo> failedToOpenEntityGroups = new ArrayList<EntityGroupInfo>();
    Map<String, Lock> locks = locker.acquireLocks(encodedNames);
    try {
      AtomicInteger counter = new AtomicInteger(0);
      Map<String, Integer> offlineNodesVersions = new ConcurrentHashMap<String, Integer>();
      OfflineCallback cb = new OfflineCallback(watcher, destination, counter,
          offlineNodesVersions);
      Map<String, EntityGroupPlan> plans = new HashMap<String, EntityGroupPlan>(
          entityGroups.size());
      List<EntityGroupState> states = new ArrayList<EntityGroupState>(
          entityGroups.size());
      for (EntityGroupInfo entityGroup : entityGroups) {
        String encodedEntityGroupName = entityGroup.getEncodedName();
        EntityGroupState state = forceEntityGroupStateToOffline(entityGroup,
            true);
        if (state != null && asyncSetOfflineInZooKeeper(state, cb, destination)) {
          EntityGroupPlan plan = new EntityGroupPlan(entityGroup,
              state.getServerName(), destination);
          plans.put(encodedEntityGroupName, plan);
          states.add(state);
        } else {
          LOG.warn("failed to force entityGroup state to offline or "
              + "failed to set it offline in ZK, will reassign later: "
              + entityGroup);
          failedToOpenEntityGroups.add(entityGroup); // assign individually
                                                     // later
          Lock lock = locks.remove(encodedEntityGroupName);
          lock.unlock();
        }
      }

      // Wait until all unassigned nodes have been put up and watchers set.
      int total = states.size();
      for (int oldCounter = 0; !server.isStopped();) {
        int count = counter.get();
        if (oldCounter != count) {
          LOG.info(destination.toString() + " unassigned znodes=" + count
              + " of total=" + total);
          oldCounter = count;
        }
        if (count >= total)
          break;
        Threads.sleep(5);
      }

      if (server.isStopped()) {
        return false;
      }

      // Add entityGroup plans, so we can updateTimers when one entityGroup is
      // opened so
      // that unnecessary timeout on EGIT is reduced.
      this.addPlans(plans);

      List<EntityGroupInfo> entityGroupOpenInfos = new ArrayList<EntityGroupInfo>(
          states.size());
      for (EntityGroupState state : states) {
        EntityGroupInfo entityGroup = state.getEntityGroup();
        String encodedEntityGroupName = entityGroup.getEncodedName();
        Integer nodeVersion = offlineNodesVersions.get(encodedEntityGroupName);
        if (nodeVersion == null || nodeVersion.intValue() == -1) {
          LOG.warn("failed to offline in zookeeper: " + entityGroup);
          failedToOpenEntityGroups.add(entityGroup); // assign individually
                                                     // later
          Lock lock = locks.remove(encodedEntityGroupName);
          lock.unlock();
        } else {
          entityGroupStates.updateEntityGroupState(entityGroup,
              EntityGroupState.State.PENDING_OPEN, destination);
          entityGroupOpenInfos.add(entityGroup);
        }
      }

      // Move on to open entityGroups.
      try {
        // Send OPEN RPC. If it fails on a IOE or RemoteException, the
        // TimeoutMonitor will pick up the pieces.
        long maxWaitTime = System.currentTimeMillis()
            + this.server.getConfiguration().getLong(
                "wasp.entityGroupserver.rpc.startup.waittime", 60000);
        for (int i = 1; i <= maximumAttempts && !server.isStopped(); i++) {
          try {
            List<EntityGroupOpeningState> entityGroupOpeningStateList = serverManager
                .sendEntityGroupsOpen(destination, entityGroupOpenInfos);
            if (entityGroupOpeningStateList == null) {
              // Failed getting RPC connection to this server
              return false;
            }
            for (int k = 0, n = entityGroupOpeningStateList.size(); k < n; k++) {
              EntityGroupOpeningState openingState = entityGroupOpeningStateList
                  .get(k);
              if (openingState != EntityGroupOpeningState.OPENED) {
                EntityGroupInfo entityGroup = entityGroupOpenInfos.get(k);
                if (openingState == EntityGroupOpeningState.ALREADY_OPENED) {
                  processAlreadyOpenedEntityGroup(entityGroup, destination);
                } else if (openingState == EntityGroupOpeningState.FAILED_OPENING) {
                  // Failed opening this entityGroup, reassign it later
                  failedToOpenEntityGroups.add(entityGroup);
                } else {
                  LOG.warn("THIS SHOULD NOT HAPPEN: unknown opening state "
                      + openingState + " in assigning entityGroup "
                      + entityGroup);
                }
              }
            }
            break;
          } catch (IOException e) {
            if (e instanceof RemoteException) {
              e = ((RemoteException) e).unwrapRemoteException();
            }
            if (e instanceof FServerStoppedException) {
              LOG.warn("The fserver was shut down, ", e);
              // No need to retry, the entityGroup server is a goner.
              return false;
            } else if (e instanceof ServerNotRunningYetException) {
              long now = System.currentTimeMillis();
              if (now < maxWaitTime) {
                LOG.debug("Server is not yet up; waiting up to "
                    + (maxWaitTime - now) + "ms", e);
                Thread.sleep(100);
                i--; // reset the try count
                continue;
              }
            } else if (e instanceof java.net.SocketTimeoutException
                && this.serverManager.isServerOnline(destination)) {
              // In case socket is timed out and the entityGroup server is still
              // online,
              // the openEntityGroup RPC could have been accepted by the server
              // and
              // just the response didn't go through. So we will retry to
              // open the entityGroup on the same server.
              if (LOG.isDebugEnabled()) {
                LOG.debug("Bulk assigner openEntityGroup() to " + destination
                    + " has timed out, but the entityGroups might"
                    + " already be opened on it.", e);
              }
              continue;
            }
            throw e;
          }
        }
      } catch (IOException e) {
        // Can be a socket timeout, EOF, NoRouteToHost, etc
        LOG.info("Unable to communicate with the fserver in order"
            + " to assign entityGroups", e);
        return false;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    } finally {
      for (Lock lock : locks.values()) {
        lock.unlock();
      }
    }

    if (!failedToOpenEntityGroups.isEmpty()) {
      for (EntityGroupInfo entityGroup : failedToOpenEntityGroups) {
        invokeAssign(entityGroup);
      }
    }
    LOG.debug("Bulk assigning done for " + destination.toString());
    return true;
  }

  /**
   * Send CLOSE RPC if the server is online, otherwise, offline the entityGroup
   */
  private void unassign(final EntityGroupInfo entityGroup,
      final EntityGroupState state, final int versionOfClosingNode,
      final ServerName dest, final boolean transitionInZK) {
    // Send CLOSE RPC
    ServerName server = state.getServerName();
    // ClosedEntityGrouphandler can remove the server from this.entityGroups
    if (!serverManager.isServerOnline(server)) {
      // delete the node. if no node exists need not bother.
      deleteClosingOrClosedNode(entityGroup);
      entityGroupOffline(entityGroup);
      return;
    }

    for (int i = 1; i <= this.maximumAttempts; i++) {
      try {
        if (serverManager.sendEntityGroupClose(server, entityGroup,
            versionOfClosingNode, dest, transitionInZK)) {
          LOG.debug("Sent CLOSE to " + server + " for entityGroup "
              + entityGroup.getEntityGroupNameAsString());
          return;
        }
        // This never happens. Currently entityGroupserver close always return
        // true.
        LOG.warn("Server " + server
            + " entityGroup CLOSE RPC returned false for "
            + entityGroup.getEntityGroupNameAsString());
      } catch (Throwable t) {
        if (t instanceof RemoteException) {
          t = ((RemoteException) t).unwrapRemoteException();
        }
        if (t instanceof NotServingEntityGroupException) {
          deleteClosingOrClosedNode(entityGroup);
          entityGroupOffline(entityGroup);
          return;
        } else if (t instanceof EntityGroupAlreadyInTransitionException) {
          // FSERVER is already processing this entityGroup, only need to update
          // the timestamp
          LOG.debug("update " + state + " the timestamp.");
          state.updateTimestampToNow();
        }
        LOG.info(
            "Server " + server + " returned " + t + " for "
                + entityGroup.getEntityGroupNameAsString() + ", try=" + i
                + " of " + this.maximumAttempts, t);
        // Presume retry or server will expire.
      }
    }
  }

  /**
   * Set entityGroup to OFFLINE unless it is opening and forceNewPlan is false.
   */
  private EntityGroupState forceEntityGroupStateToOffline(
      final EntityGroupInfo entityGroup, final boolean forceNewPlan) {
    EntityGroupState state = entityGroupStates.getEntityGroupState(entityGroup);
    if (state == null) {
      LOG.warn("Assigning a entityGroup not in entityGroup states: "
          + entityGroup);
      state = entityGroupStates.createEntityGroupState(entityGroup);
    } else {
      switch (state.getState()) {
      case OPEN:
      case OPENING:
      case PENDING_OPEN:
        if (!forceNewPlan) {
          LOG.debug("Attempting to assign entityGroup " + entityGroup
              + " but it is already in transition: " + state);
          return null;
        }
      case CLOSING:
      case PENDING_CLOSE:
        unassign(entityGroup, state, -1, null, false);
      case CLOSED:
        if (!state.isOffline()) {
          LOG.debug("Forcing OFFLINE; was=" + state);
          state = entityGroupStates.updateEntityGroupState(entityGroup,
              EntityGroupState.State.OFFLINE);
        }
      case OFFLINE:
        break;
      default:
        LOG.error("Trying to assign entityGroup " + entityGroup
            + ", which is in state " + state);
        return null;
      }
    }
    return state;
  }

  /**
   * Caller must hold lock on the passed <code>state</code> object.
   * 
   * @param state
   * @param setOfflineInZK
   * @param forceNewPlan
   */
  private void assign(EntityGroupState state, final boolean setOfflineInZK,
      final boolean forceNewPlan) {
    EntityGroupState currentState = state;
    int versionOfOfflineNode = -1;
    EntityGroupPlan plan = null;
    long maxEntityGroupServerStartupWaitTime = -1;
    EntityGroupInfo entityGroup = state.getEntityGroup();
    for (int i = 1; i <= maximumAttempts && !server.isStopped(); i++) {
      if (plan == null) { // Get a server for the entityGroup at first
        plan = getEntityGroupPlan(entityGroup, forceNewPlan);
      }
      if (plan == null) {
        LOG.debug("Unable to determine a plan to assign " + entityGroup);
        this.timeoutMonitor.setAllEntityGroupServersOffline(true);
        return; // Should get reassigned later when EGIT times out.
      }
      if (setOfflineInZK && versionOfOfflineNode == -1) {
        // get the version of the znode after setting it to OFFLINE.
        // versionOfOfflineNode will be -1 if the znode was not set to OFFLINE
        versionOfOfflineNode = setOfflineInZooKeeper(currentState,
            plan.getDestination());
        if (versionOfOfflineNode != -1) {
          if (isDisabledorDisablingEntityGroupInEGIT(entityGroup)) {
            return;
          }
          // In case of assignment from EnableTableHandler table state is
          // ENABLING. Any how
          // EnableTableHandler will set ENABLED after assigning all the table
          // entityGroups. If we
          // try to set to ENABLED directly then client API may think table is
          // enabled.
          // When we have a case such as all the entityGroups are added directly
          // into .META. and we call
          // assignEntityGroup then we need to make the table ENABLED. Hence in
          // such case the table
          // will not be in ENABLING or ENABLED state.
          String tableName = entityGroup.getTableNameAsString();
          if (!zkTable.isEnablingTable(tableName)
              && !zkTable.isEnabledTable(tableName)) {
            LOG.debug("Setting table " + tableName + " to ENABLED state.");
            setEnabledTable(tableName);
          }
        }
      }
      if (setOfflineInZK && versionOfOfflineNode == -1) {
        return;
      }
      if (this.server.isStopped()) {
        LOG.debug("Server stopped; skipping assign of " + entityGroup);
        return;
      }
      try {
        LOG.info("Assigning entityGroup "
            + entityGroup.getEntityGroupNameAsString() + " to "
            + plan.getDestination().toString());
        // Transition EntityGroupState to PENDING_OPEN
        currentState = entityGroupStates.updateEntityGroupState(entityGroup,
            EntityGroupState.State.PENDING_OPEN, plan.getDestination());
        // Send OPEN RPC. This can fail if the server on other end is is not up.
        // Pass the version that was obtained while setting the node to OFFLINE.
        EntityGroupOpeningState entityGroupOpenState = serverManager
            .sendEntityGroupOpen(plan.getDestination(), entityGroup,
                versionOfOfflineNode);
        if (entityGroupOpenState == EntityGroupOpeningState.ALREADY_OPENED) {
          processAlreadyOpenedEntityGroup(entityGroup, plan.getDestination());
        } else if (entityGroupOpenState == EntityGroupOpeningState.FAILED_OPENING) {
          // Failed opening this entityGroup
          throw new Exception("Get entityGroupOpeningState="
              + entityGroupOpenState);
        }
        break;
      } catch (Throwable t) {
        if (t instanceof RemoteException) {
          t = ((RemoteException) t).unwrapRemoteException();
        }
        boolean entityGroupAlreadyInTransitionException = false;
        boolean serverNotRunningYet = false;
        boolean socketTimedOut = false;
        if (t instanceof EntityGroupAlreadyInTransitionException) {
          entityGroupAlreadyInTransitionException = true;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failed assignment in: " + plan.getDestination()
                + " due to " + t.getMessage());
          }
        } else if (t instanceof ServerNotRunningYetException) {
          if (maxEntityGroupServerStartupWaitTime < 0) {
            maxEntityGroupServerStartupWaitTime = System.currentTimeMillis()
                + this.server.getConfiguration().getLong(
                    "wasp.entityGroupserver.rpc.startup.waittime", 60000);
          }
          try {
            long now = System.currentTimeMillis();
            if (now < maxEntityGroupServerStartupWaitTime) {
              LOG.debug("Server is not yet up; waiting up to "
                  + (maxEntityGroupServerStartupWaitTime - now) + "ms", t);
              serverNotRunningYet = true;
              Thread.sleep(100);
              i--; // reset the try count
            } else {
              LOG.debug("Server is not up for a while; try a new one", t);
            }
          } catch (InterruptedException ie) {
            LOG.warn(
                "Failed to assign " + entityGroup.getEntityGroupNameAsString()
                    + " since interrupted", ie);
            Thread.currentThread().interrupt();
            return;
          }
        } else if (t instanceof java.net.SocketTimeoutException
            && this.serverManager.isServerOnline(plan.getDestination())) {
          // In case socket is timed out and the entityGroup server is still
          // online,
          // the openEntityGroup RPC could have been accepted by the server and
          // just the response didn't go through. So we will retry to
          // open the entityGroup on the same server to avoid possible
          // double assignment.
          socketTimedOut = true;
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Call openEntityGroup() to " + plan.getDestination()
                    + " has timed out when trying to assign "
                    + entityGroup.getEntityGroupNameAsString()
                    + ", but the entityGroup might already be opened on "
                    + plan.getDestination() + ".", t);
          }
        }

        LOG.warn(
            "Failed assignment of "
                + entityGroup.getEntityGroupNameAsString()
                + " to "
                + plan.getDestination()
                + ", trying to assign "
                + (entityGroupAlreadyInTransitionException
                    || serverNotRunningYet || socketTimedOut ? "to the same entityGroup server because of EntityGroupAlreadyInTransitionException"
                    + "/ServerNotRunningYetException/SocketTimeoutException;"
                    : "elsewhere instead; ") + "try=" + i + " of "
                + this.maximumAttempts, t);

        if (i == this.maximumAttempts) {
          // Don't reset the entityGroup state or get a new plan any more.
          // This is the last try.
          continue;
        }

        // If entityGroup opened on destination of present plan, reassigning to
        // new
        // FSERVER may cause double assignments. In case of
        // EntityGroupAlreadyInTransitionException
        // reassigning to same FSERVER.
        EntityGroupPlan newPlan = plan;
        if (!(entityGroupAlreadyInTransitionException || serverNotRunningYet || socketTimedOut)) {
          // Force a new plan and reassign. Will return null if no servers.
          // The new plan could be the same as the existing plan since we don't
          // exclude the server of the original plan, which should not be
          // excluded since it could be the only server up now.
          newPlan = getEntityGroupPlan(entityGroup, true);
        }
        if (newPlan == null) {
          this.timeoutMonitor.setAllEntityGroupServersOffline(true);
          LOG.warn("Unable to find a viable location to assign entityGroup "
              + entityGroup.getEntityGroupNameAsString());
          return;
        }
        if (plan != newPlan
            && !plan.getDestination().equals(newPlan.getDestination())) {
          // Clean out plan we failed execute and one that doesn't look like
          // it'll
          // succeed anyways; we need a new plan!
          // Transition back to OFFLINE
          currentState = entityGroupStates.updateEntityGroupState(entityGroup,
              EntityGroupState.State.OFFLINE);
          versionOfOfflineNode = -1;
          plan = newPlan;
        }
      }
    }
  }

  private void processAlreadyOpenedEntityGroup(EntityGroupInfo entityGroup,
      ServerName sn) {
    // Remove entityGroup from in-memory transition and unassigned node from ZK
    // While trying to enable the table the entityGroups of the table were
    // already enabled.
    LOG.debug("ALREADY_OPENED entityGroup "
        + entityGroup.getEntityGroupNameAsString() + " to " + sn);
    String encodedEntityGroupName = entityGroup.getEncodedName();
    try {
      ZKAssign.deleteOfflineNode(watcher, encodedEntityGroupName);
    } catch (KeeperException.NoNodeException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("The unassigned node " + encodedEntityGroupName
            + " doesnot exist.");
      }
    } catch (KeeperException e) {
      server.abort(
          "Error deleting OFFLINED node in ZK for transition ZK node ("
              + encodedEntityGroupName + ")", e);
    }

    entityGroupStates.entityGroupOnline(entityGroup, sn);
  }

  private boolean isDisabledorDisablingEntityGroupInEGIT(
      final EntityGroupInfo entityGroup) {
    String tableName = entityGroup.getTableNameAsString();
    boolean disabled = this.zkTable.isDisabledTable(tableName);
    if (disabled || this.zkTable.isDisablingTable(tableName)) {
      LOG.info("Table " + tableName + (disabled ? " disabled;" : " disabling;")
          + " skipping assign of " + entityGroup.getEntityGroupNameAsString());
      offlineDisabledEntityGroup(entityGroup);
      return true;
    }
    return false;
  }

  /**
   * Set entityGroup as OFFLINED up in zookeeper
   * 
   * @param state
   * @return the version of the offline node if setting of the OFFLINE node was
   *         successful, -1 otherwise.
   */
  private int setOfflineInZooKeeper(final EntityGroupState state,
      final ServerName destination) {
    if (!state.isClosed() && !state.isOffline()) {
      String msg = "Unexpected state : " + state
          + " .. Cannot transit it to OFFLINE.";
      this.server.abort(msg, new IllegalStateException(msg));
      return -1;
    }
    entityGroupStates.updateEntityGroupState(state.getEntityGroup(),
        EntityGroupState.State.OFFLINE);
    int versionOfOfflineNode = -1;
    try {
      // get the version after setting the znode to OFFLINE
      versionOfOfflineNode = ZKAssign.createOrForceNodeOffline(watcher,
          state.getEntityGroup(), destination);
      if (versionOfOfflineNode == -1) {
        LOG.warn("Attempted to create/force node into OFFLINE state before "
            + "completing assignment but failed to do so for " + state);
        return -1;
      }
    } catch (KeeperException e) {
      server.abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      return -1;
    }
    return versionOfOfflineNode;
  }

  /**
   * @param entityGroup
   *          the entityGroup to assign
   * @return Plan for passed <code>entityGroup</code> (If none currently, it
   *         creates one or if no servers to assign, it returns null).
   */
  private EntityGroupPlan getEntityGroupPlan(final EntityGroupInfo entityGroup,
      final boolean forceNewPlan) {
    return getEntityGroupPlan(entityGroup, null, forceNewPlan);
  }

  /**
   * @param entityGroup
   *          the entityGroup to assign
   * @param serverToExclude
   *          Server to exclude (we know its bad). Pass null if all servers are
   *          thought to be assignable.
   * @param forceNewPlan
   *          If true, then if an existing plan exists, a new plan will be
   *          generated.
   * @return Plan for passed <code>entityGroup</code> (If none currently, it
   *         creates one or if no servers to assign, it returns null).
   */
  private EntityGroupPlan getEntityGroupPlan(final EntityGroupInfo entityGroup,
      final ServerName serverToExclude, final boolean forceNewPlan) {
    // Pickup existing plan or make a new one
    final String encodedName = entityGroup.getEncodedName();
    final List<ServerName> destServers = serverManager
        .createDestinationServersList(serverToExclude);

    if (destServers.isEmpty()) {
      LOG.warn("Can't move the entityGroup " + encodedName
          + ", there is no destination server available.");
      return null;
    }

    EntityGroupPlan randomPlan = null;
    boolean newPlan = false;
    EntityGroupPlan existingPlan = null;

    synchronized (this.entityGroupPlans) {
      existingPlan = this.entityGroupPlans.get(encodedName);

      if (existingPlan != null && existingPlan.getDestination() != null) {
        LOG.debug("Found an existing plan for "
            + entityGroup.getEntityGroupNameAsString()
            + " destination server is " + existingPlan.getDestination());
      }

      if (forceNewPlan || existingPlan == null
          || existingPlan.getDestination() == null
          || !destServers.contains(existingPlan.getDestination())) {
        newPlan = true;
        randomPlan = new EntityGroupPlan(entityGroup, null,
            balancer.randomAssignment(entityGroup, destServers));
        this.entityGroupPlans.put(encodedName, randomPlan);
      }
    }

    if (newPlan) {
      LOG.debug("No previous transition plan was found (or we are ignoring "
          + "an existing plan) for " + entityGroup.getEntityGroupNameAsString()
          + " so generated a random one; " + randomPlan + "; "
          + serverManager.countOfFServers() + " (online="
          + serverManager.getOnlineServers().size() + ", available="
          + destServers.size() + ") available servers");
      return randomPlan;
    }
    LOG.debug("Using pre-existing plan for entityGroup "
        + entityGroup.getEntityGroupNameAsString() + "; plan=" + existingPlan);
    return existingPlan;
  }

  /**
   * Unassign the list of entityGroups. Configuration knobs:
   * wasp.bulk.waitbetween.reopen indicates the number of milliseconds to wait
   * before unassigning another entityGroup from this entityGroup server
   * 
   * @param entityGroups
   * @throws InterruptedException
   */
  public void unassign(List<EntityGroupInfo> entityGroups) {
    int waitTime = this.server.getConfiguration().getInt(
        "wasp.bulk.waitbetween.reopen", 0);
    for (EntityGroupInfo entityGroup : entityGroups) {
      if (entityGroupStates.isEntityGroupInTransition(entityGroup))
        continue;
      unassign(entityGroup, false);
      while (entityGroupStates.isEntityGroupInTransition(entityGroup)) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          // Do nothing, continue
        }
      }
      if (waitTime > 0)
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e) {
          // Do nothing, continue
        }
    }
  }

  /**
   * Unassigns the specified entityGroup.
   * <p>
   * Updates the EntityGroupState and sends the CLOSE RPC unless entityGroup is
   * being split by entityGroupserver; then the unassign fails (silently)
   * because we presume the entityGroup being unassigned no longer exists (its
   * been split out of existence). TODO: What to do if split fails and is rolled
   * back and parent is revivified?
   * <p>
   * If a EntityGroupPlan is already set, it will remain.
   * 
   * @param entityGroup
   *          server to be unassigned
   */
  public void unassign(EntityGroupInfo entityGroup) {
    unassign(entityGroup, false);
  }

  /**
   * Unassigns the specified entityGroup.
   * <p>
   * Updates the EntityGroupState and sends the CLOSE RPC unless entityGroup is
   * being split by entityGroupserver; then the unassign fails (silently)
   * because we presume the entityGroup being unassigned no longer exists (its
   * been split out of existence). TODO: What to do if split fails and is rolled
   * back and parent is revivified?
   * <p>
   * If a EntityGroupPlan is already set, it will remain.
   * 
   * @param entityGroup
   *          server to be unassigned
   * @param force
   *          if entityGroup should be closed even if already closing
   */
  public void unassign(EntityGroupInfo entityGroup, boolean force,
      ServerName dest) {
    // TODO: Method needs refactoring. Ugly buried returns throughout. Beware!
    LOG.debug("Starting unassignment of entityGroup "
        + entityGroup.getEntityGroupNameAsString() + " (offlining)");

    String encodedName = entityGroup.getEncodedName();
    // Grab the state of this entityGroup and synchronize on it
    int versionOfClosingNode = -1;
    // We need a lock here as we're going to do a put later and we don't want
    // multiple states
    // creation
    ReentrantLock lock = locker.acquireLock(encodedName);
    EntityGroupState state = entityGroupStates
        .getEntityGroupTransitionState(encodedName);
    try {
      if (state == null) {
        // Create the znode in CLOSING state
        try {
          state = entityGroupStates.getEntityGroupState(entityGroup);
          if (state == null || state.getServerName() == null) {
            // We don't know where the entityGroup is, offline it.
            // No need to send CLOSE RPC
            entityGroupOffline(entityGroup);
            return;
          }
          versionOfClosingNode = ZKAssign.createNodeClosing(watcher,
              entityGroup, state.getServerName());
          if (versionOfClosingNode == -1) {
            LOG.debug("Attempting to unassign entityGroup "
                + entityGroup.getEntityGroupNameAsString()
                + " but ZK closing node " + "can't be created.");
            return;
          }
        } catch (KeeperException ee) {
          Exception e = ee;
          if (e instanceof NodeExistsException) {
            // Handle race between master initiated close and entityGroupserver
            // orchestrated splitting. See if existing node is in a
            // SPLITTING or SPLIT state. If so, the entityGroupserver started
            // an op on node before we could get our CLOSING in. Deal.
            NodeExistsException nee = (NodeExistsException) e;
            String path = nee.getPath();
            try {
              if (isSplitOrSplitting(path)) {
                LOG.debug(path
                    + " is SPLIT or SPLITTING; "
                    + "skipping unassign because entityGroup no longer exists -- its split");
                return;
              }
            } catch (KeeperException.NoNodeException ke) {
              LOG.warn("Failed getData on SPLITTING/SPLIT at " + path
                  + "; presuming split and that the entityGroup to unassign, "
                  + encodedName + ", no longer exists -- confirm", ke);
              return;
            } catch (KeeperException ke) {
              LOG.error("Unexpected zk state", ke);
            } catch (DeserializationException de) {
              LOG.error("Failed parse", de);
            }
          }
          // If we get here, don't understand whats going on -- abort.
          server.abort("Unexpected ZK exception creating node CLOSING", e);
          return;
        }
        state = entityGroupStates.updateEntityGroupState(entityGroup,
            EntityGroupState.State.PENDING_CLOSE);
      } else if (force && (state.isPendingClose() || state.isClosing())) {
        LOG.debug("Attempting to unassign entityGroup "
            + entityGroup.getEntityGroupNameAsString() + " which is already "
            + state.getState() + " but forcing to send a CLOSE RPC again ");
        state.updateTimestampToNow();
      } else {
        LOG.debug("Attempting to unassign entityGroup "
            + entityGroup.getEntityGroupNameAsString() + " but it is "
            + "already in transition (" + state.getState() + ", force=" + force
            + ")");
        return;
      }

      unassign(entityGroup, state, versionOfClosingNode, dest, true);
    } finally {
      lock.unlock();
    }
  }

  public void unassign(EntityGroupInfo entityGroup, boolean force) {
    unassign(entityGroup, force, null);
  }

  /**
   * 
   * @param entityGroup
   *          entityGroupinfo of znode to be deleted.
   */
  public void deleteClosingOrClosedNode(EntityGroupInfo entityGroup) {
    try {
      if (!ZKAssign.deleteNode(watcher, entityGroup.getEncodedName(),
          EventHandler.EventType.M_ZK_ENTITYGROUP_CLOSING)) {
        boolean deleteNode = ZKAssign.deleteNode(watcher,
            entityGroup.getEncodedName(),
            EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_CLOSED);
        // TODO : We don't abort if the delete node returns false. Is there any
        // such corner case?
        if (!deleteNode) {
          LOG.error("The deletion of the CLOSED node for the entityGroup "
              + entityGroup.getEncodedName() + " returned " + deleteNode);
        }
      }
    } catch (NoNodeException e) {
      LOG.debug("CLOSING/CLOSED node for the entityGroup "
          + entityGroup.getEncodedName() + " already deleted");
    } catch (KeeperException ke) {
      server.abort(
          "Unexpected ZK exception deleting node CLOSING/CLOSED for the entityGroup "
              + entityGroup.getEncodedName(), ke);
      return;
    }
  }

  /**
   * @param path
   * @return True if znode is in SPLIT or SPLITTING state.
   * @throws KeeperException
   *           Can happen if the znode went away in meantime.
   * @throws DeserializationException
   */
  private boolean isSplitOrSplitting(final String path) throws KeeperException,
      DeserializationException {
    boolean result = false;
    // This may fail if the SPLIT or SPLITTING znode gets cleaned up before we
    // can get data from it.
    byte[] data = ZKAssign.getData(watcher, path);
    if (data == null)
      return false;
    EntityGroupTransaction rt = EntityGroupTransaction.parseFrom(data);
    switch (rt.getEventType()) {
    case FSERVER_ZK_ENTITYGROUP_SPLIT:
    case FSERVER_ZK_ENTITYGROUP_SPLITTING:
      result = true;
      break;
    default:
      break;
    }
    return result;
  }

  /**
   * Waits until the specified entityGroup has completed assignment.
   * <p>
   * If the entityGroup is already assigned, returns immediately. Otherwise,
   * method blocks until the entityGroup is assigned.
   * 
   * @param entityGroupInfo
   *          entityGroup to wait on assignment for
   * @throws InterruptedException
   */
  public void waitForAssignment(EntityGroupInfo entityGroupInfo)
      throws InterruptedException {
    while (!this.server.isStopped()
        && !entityGroupStates.isEntityGroupAssigned(entityGroupInfo)) {
      // We should receive a notification, but it's
      // better to have a timeout to recheck the condition here:
      // it lowers the impact of a race condition if any
      entityGroupStates.waitForUpdate(100);
    }
  }

  /**
   * Assigns specified entityGroups retaining assignments, if any.
   * <p>
   * This is a synchronous call and will return once every entityGroup has been
   * assigned. If anything fails, an exception is thrown
   * 
   * @throws InterruptedException
   * @throws IOException
   */
  public void assign(Map<EntityGroupInfo, ServerName> entityGroups)
      throws IOException, InterruptedException {
    if (entityGroups == null || entityGroups.isEmpty()) {
      return;
    }
    List<ServerName> servers = serverManager.createDestinationServersList();
    if (servers == null || servers.isEmpty()) {
      throw new IOException(
          "Found no destination server to assign entityGroup(s)");
    }

    // Reuse existing assignment info
    Map<ServerName, List<EntityGroupInfo>> bulkPlan = balancer
        .retainAssignment(entityGroups, servers);

    LOG.info("Bulk assigning " + entityGroups.size()
        + " entityGroup(s) across " + servers.size()
        + " server(s), retainAssignment=true");
    BulkAssigner ba = new GeneralBulkAssigner(this.server, bulkPlan, this);
    ba.bulkAssign();
    LOG.info("Bulk assigning done");
  }

  /**
   * Assigns specified entityGroups round robin, if any.
   * <p>
   * This is a synchronous call and will return once every entityGroup has been
   * assigned. If anything fails, an exception is thrown
   * 
   * @throws InterruptedException
   * @throws IOException
   */
  public void assign(List<EntityGroupInfo> entityGroups) throws IOException,
      InterruptedException {
    if (entityGroups == null || entityGroups.isEmpty()) {
      return;
    }
    List<ServerName> servers = serverManager.createDestinationServersList();
    if (servers == null || servers.isEmpty()) {
      throw new IOException(
          "Found no destination server to assign entityGroup(s)");
    }

    // Generate a round-robin bulk assignment plan
    Map<ServerName, List<EntityGroupInfo>> bulkPlan = balancer
        .roundRobinAssignment(entityGroups, servers);

    LOG.info("Bulk assigning " + entityGroups.size()
        + " entityGroup(s) round-robin across " + servers.size() + " server(s)");

    // Use fixed count thread pool assigning.
    BulkAssigner ba = new GeneralBulkAssigner(this.server, bulkPlan, this);
    ba.bulkAssign();
    LOG.info("Bulk assigning done");
  }

  /**
   * Assigns all user entityGroups, if any exist. Used during cluster startup.
   * <p>
   * This is a synchronous call and will return once every entityGroup has been
   * assigned. If anything fails, an exception is thrown and the cluster should
   * be shutdown.
   * 
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  private void assignAllUserEntityGroups() throws IOException,
      InterruptedException, KeeperException {
    // Cleanup any existing ZK nodes and start watching
    ZKAssign.deleteAllNodes(watcher);
    ZKUtil.listChildrenAndWatchForNewChildren(this.watcher,
        this.watcher.assignmentZNode);
    failoverCleanupDone();

    // Skip assignment for entityGroups of tables in DISABLING state because
    // during clean cluster startup
    // no FSERVER is alive and entityGroups map also doesn't have any
    // information about the entityGroups.
    // See HBASE-6281.
    Set<String> disabledOrDisablingOrEnabling = ZKTable
        .getDisabledOrDisablingTables(watcher);
    disabledOrDisablingOrEnabling.addAll(ZKTable.getEnablingTables(watcher));
    // Scan META for all user entityGroups, skipping any disabled tables
    Map<EntityGroupInfo, ServerName> allEntityGroups = FMetaScanner.fullScan(
        server.getConfiguration(), disabledOrDisablingOrEnabling, true);
    if (allEntityGroups == null || allEntityGroups.isEmpty())
      return;

    // Determine what type of assignment to do on startup
    boolean retainAssignment = server.getConfiguration().getBoolean(
        "wasp.master.startup.retainassign", true);

    if (retainAssignment) {
      assign(allEntityGroups);
    } else {
      List<EntityGroupInfo> entityGroups = new ArrayList<EntityGroupInfo>(
          allEntityGroups.keySet());
      assign(entityGroups);
    }

    for (EntityGroupInfo egInfo : allEntityGroups.keySet()) {
      String tableName = egInfo.getTableNameAsString();
      if (!zkTable.isEnabledTable(tableName)) {
        setEnabledTable(tableName);
      }
    }
  }

  /**
   * Wait until no entityGroups in transition.
   * 
   * @param timeout
   *          How long to wait.
   * @return True if nothing in entityGroups in transition.
   * @throws InterruptedException
   */
  boolean waitUntilNoEntityGroupsInTransition(final long timeout)
      throws InterruptedException {
    // Blocks until there are no entityGroups in transition. It is possible that
    // there
    // are entityGroups in transition immediately after this returns but
    // guarantees
    // that if it returns without an exception that there was a period of time
    // with no entityGroups in transition from the point-of-view of the
    // in-memory
    // state of the Master.
    final long endTime = System.currentTimeMillis() + timeout;

    while (!this.server.isStopped()
        && entityGroupStates.isEntityGroupsInTransition()
        && endTime > System.currentTimeMillis()) {
      entityGroupStates.waitForUpdate(100);
    }

    return !entityGroupStates.isEntityGroupsInTransition();
  }

  /**
   * Rebuild the list of user entityGroups and assignment information.
   * <p>
   * Returns a map of servers that are not found to be online and the
   * entityGroups they were hosting.
   * 
   * @return map of servers not online to their assigned entityGroups, as stored
   *         in META
   * @throws IOException
   */
  Map<ServerName, List<EntityGroupInfo>> rebuildUserEntityGroups()
      throws IOException, KeeperException {
    Set<String> enablingTables = ZKTable.getEnablingTables(watcher);
    Set<String> disabledOrEnablingTables = ZKTable.getDisabledTables(watcher);
    disabledOrEnablingTables.addAll(enablingTables);
    Set<String> disabledOrDisablingOrEnabling = ZKTable
        .getDisablingTables(watcher);
    disabledOrDisablingOrEnabling.addAll(disabledOrEnablingTables);

    // EntityGroup assignment from FMETA
    List<Result> results = FMetaScanner.fullScan(server.getConfiguration());
    // Get any new but slow to checkin entityGroup server that joined the
    // cluster
    Set<ServerName> onlineServers = serverManager.getOnlineServers().keySet();
    // Map of offline servers and their entityGroups to be returned
    Map<ServerName, List<EntityGroupInfo>> offlineServers = new TreeMap<ServerName, List<EntityGroupInfo>>();
    // Iterate entityGroups in META
    if (results != null) {
      for (Result result : results) {
        Pair<EntityGroupInfo, ServerName> entityGroup = EntityGroupInfo
            .getEntityGroupInfoAndServerName(result);
        if (entityGroup == null)
          continue;
        EntityGroupInfo entityGroupInfo = entityGroup.getFirst();
        ServerName entityGroupLocation = entityGroup.getSecond();
        if (entityGroupInfo == null)
          continue;
        entityGroupStates.createEntityGroupState(entityGroupInfo);
        String tableName = entityGroupInfo.getTableNameAsString();
        if (entityGroupLocation == null) {
          // entityGroupLocation could be null if createTable didn't finish
          // properly.
          // When createTable is in progress, HMaster restarts.
          // Some entityGroups have been added to .META., but have not been
          // assigned.
          // When this happens, the entityGroup's table must be in ENABLING
          // state.
          // It can't be in ENABLED state as that is set when all entityGroups
          // are
          // assigned.
          // It can't be in DISABLING state, because DISABLING state transitions
          // from ENABLED state when application calls disableTable.
          // It can't be in DISABLED state, because DISABLED states transitions
          // from DISABLING state.
          if (!enablingTables.contains(tableName)) {
            LOG.warn("EntityGroup " + entityGroupInfo.getEncodedName()
                + " has null entityGroupLocation." + " But its table "
                + tableName + " isn't in ENABLING state.");
          }
        } else if (!onlineServers.contains(entityGroupLocation)) {
          // EntityGroup is located on a server that isn't online
          List<EntityGroupInfo> offlineEntityGroups = offlineServers
              .get(entityGroupLocation);
          if (offlineEntityGroups == null) {
            offlineEntityGroups = new ArrayList<EntityGroupInfo>(1);
            offlineServers.put(entityGroupLocation, offlineEntityGroups);
          }
          offlineEntityGroups.add(entityGroupInfo);
          // need to enable the table if not disabled or disabling or enabling
          // this will be used in rolling restarts
          if (!disabledOrDisablingOrEnabling.contains(tableName)
              && !getZKTable().isEnabledTable(tableName)) {
            setEnabledTable(tableName);
          }
        } else {
          // If entityGroup is in offline and split state check the ZKNode
          if (entityGroupInfo.isOffline() && entityGroupInfo.isSplit()) {
            String node = ZKAssign.getNodeName(this.watcher,
                entityGroupInfo.getEncodedName());
            Stat stat = new Stat();
            byte[] data = ZKUtil.getDataNoWatch(this.watcher, node, stat);
            // If znode does not exist, don't consider this entityGroup
            if (data == null) {
              LOG.debug("EntityGroup "
                  + entityGroupInfo.getEntityGroupNameAsString()
                  + " split is completed. Hence need not add to entityGroups list");
              continue;
            }
          }
          // EntityGroup is being served and on an active server
          // add only if entityGroup not in disabled or enabling table
          if (!disabledOrEnablingTables.contains(tableName)) {
            entityGroupStates.entityGroupOnline(entityGroupInfo,
                entityGroupLocation);
          }
          // need to enable the table if not disabled or disabling or enabling
          // this will be used in rolling restarts
          if (!disabledOrDisablingOrEnabling.contains(tableName)
              && !getZKTable().isEnabledTable(tableName)) {
            setEnabledTable(tableName);
          }
        }
      }
    }
    return offlineServers;
  }

  /**
   * Recover the tables that were not fully moved to DISABLED state. These
   * tables are in DISABLING state when the master restarted/switched.
   * 
   * @throws KeeperException
   * @throws TableNotFoundException
   * @throws IOException
   */
  private void recoverTableInDisablingState() throws KeeperException,
      TableNotFoundException, IOException {
    Set<String> disablingTables = ZKTable.getDisablingTables(watcher);
    if (disablingTables.size() != 0) {
      for (String tableName : disablingTables) {
        // Recover by calling DisableTableHandler
        LOG.info("The table " + tableName
            + " is in DISABLING state.  Hence recovering by moving the table"
            + " to DISABLED state.");
        new DisableTableHandler(this.server, this, tableName.getBytes(),
            (FMasterServices) this.server, true).process();
      }
    }
  }

  /**
   * Recover the tables that are not fully moved to ENABLED state. These tables
   * are in ENABLING state when the master restarted/switched
   * 
   * @throws KeeperException
   * @throws TableNotFoundException
   * @throws IOException
   */
  private void recoverTableInEnablingState() throws KeeperException,
      TableNotFoundException, IOException {
    Set<String> enablingTables = ZKTable.getEnablingTables(watcher);
    if (enablingTables.size() != 0) {
      for (String tableName : enablingTables) {
        // Recover by calling EnableTableHandler
        LOG.info("The table " + tableName
            + " is in ENABLING state.  Hence recovering by moving the table"
            + " to ENABLED state.");
        // enableTable in sync way during master startup,
        new EnableTableHandler(this.server, (FMasterServices) this.server,this, tableName.getBytes(), true)
            .process();
      }
    }
  }

  /**
   * Processes list of dead servers from result of FMETA scan and entityGroups
   * in EGIT
   * <p>
   * This is used for failover to recover the lost entityGroups that belonged to
   * EntityGroupServers which failed while there was no active master or
   * entityGroups that were in EGIT.
   * <p>
   * 
   * @param deadServers
   *          The list of dead servers which failed while there was no active
   *          master. Can be null.
   * @param nodes
   *          The entityGroups in EGIT
   * @throws IOException
   * @throws KeeperException
   */
  private void processDeadServersAndRecoverLostEntityGroups(
      Map<ServerName, List<EntityGroupInfo>> deadServers, List<String> nodes)
      throws IOException, KeeperException {
    if (deadServers != null) {
      for (Map.Entry<ServerName, List<EntityGroupInfo>> server : deadServers
          .entrySet()) {
        ServerName serverName = server.getKey();
        if (!serverManager.isServerDead(serverName)) {
          serverManager.expireServer(serverName); // Let SSH do entityGroup
                                                  // re-assign
        }
      }
    }
    nodes = ZKUtil.listChildrenAndWatchForNewChildren(this.watcher,
        this.watcher.assignmentZNode);
    if (!nodes.isEmpty()) {
      for (String encodedEntityGroupName : nodes) {
        processEntityGroupInTransition(encodedEntityGroupName, null);
      }
    }

    // Now we can safely claim failover cleanup completed and enable
    // ServerShutdownHandler for further processing. The nodes (below)
    // in transition, if any, are for entityGroups not related to those
    // dead servers at all, and can be done in parallel to SSH.
    failoverCleanupDone();
  }

  /**
   * Set EntityGroups in transitions metrics. This takes an iterator on the
   * EntityGroupInTransition map (CLSM), and is not synchronized. This iterator
   * is not fail fast, which may lead to stale read; but that's better than
   * creating a copy of the map for metrics computation, as this method will be
   * invoked on a frequent interval.
   */
  public void updateEntityGroupsInTransitionMetrics() {
    long currentTime = System.currentTimeMillis();
    int totalEGITs = 0;
    int totalEGITsOverThreshold = 0;
    long oldestEGITTime = 0;
    int egitThreshold = this.server.getConfiguration().getInt(
        FConstants.METRICS_EGIT_STUCK_WARNING_THRESHOLD, 60000);
    for (EntityGroupState state : entityGroupStates
        .getEntityGroupsInTransition().values()) {
      totalEGITs++;
      long egitTime = currentTime - state.getStamp();
      if (egitTime > egitThreshold) { // more than the threshold
        totalEGITsOverThreshold++;
      }
      if (oldestEGITTime < egitTime) {
        oldestEGITTime = egitTime;
      }
    }
    if (this.metricsMaster != null) {
      this.metricsMaster.updateEGITOldestAge(oldestEGITTime);
      this.metricsMaster.updateEGITCount(totalEGITs);
      this.metricsMaster.updateEGITCountOverThreshold(totalEGITsOverThreshold);
    }
  }

  /**
   * @param entityGroup
   *          EntityGroup whose plan we are to clear.
   */
  void clearEntityGroupPlan(final EntityGroupInfo entityGroup) {
    synchronized (this.entityGroupPlans) {
      this.entityGroupPlans.remove(entityGroup.getEncodedName());
    }
  }

  /**
   * Wait on entityGroup to clear entityGroups-in-transition.
   * 
   * @param egInfo
   *          EntityGroup to wait on.
   * @throws IOException
   */
  public void waitOnEntityGroupToClearEntityGroupsInTransition(
      final EntityGroupInfo egInfo) throws IOException, InterruptedException {
    if (!entityGroupStates.isEntityGroupInTransition(egInfo))
      return;
    EntityGroupState egState = null;
    // There is already a timeout monitor on entityGroups in transition so I
    // should not have to have one here too?
    while (!this.server.isStopped()
        && entityGroupStates.isEntityGroupInTransition(egInfo)) {
      LOG.info("Waiting on " + egState + " to clear entityGroups-in-transition");
      entityGroupStates.waitForUpdate(100);
    }
    if (this.server.isStopped()) {
      LOG.info("Giving up wait on entityGroups in "
          + "transition because stoppable.isStopped is set");
    }
  }

  /**
   * Update timers for all entityGroups in transition going against the server
   * in the serversInUpdatingTimer.
   */
  public class TimerUpdater extends Chore {

    public TimerUpdater(final int period, final Stoppable stopper) {
      super("AssignmentTimerUpdater", period, stopper);
    }

    @Override
    protected void chore() {
      ServerName serverToUpdateTimer = null;
      while (!serversInUpdatingTimer.isEmpty() && !stopper.isStopped()) {
        if (serverToUpdateTimer == null) {
          serverToUpdateTimer = serversInUpdatingTimer.first();
        } else {
          serverToUpdateTimer = serversInUpdatingTimer
              .higher(serverToUpdateTimer);
        }
        if (serverToUpdateTimer == null) {
          break;
        }
        updateTimers(serverToUpdateTimer);
        serversInUpdatingTimer.remove(serverToUpdateTimer);
      }
    }
  }

  /**
   * Monitor to check for time outs on entityGroup transition operations
   */
  public class TimeoutMonitor extends Chore {
    private boolean allEntityGroupServersOffline = false;
    private FServerManager serverManager;
    private final int timeout;

    /**
     * Creates a periodic monitor to check for time outs on entityGroup
     * transition operations. This will deal with retries if for some reason
     * something doesn't happen within the specified timeout.
     * 
     * @param period
     * @param stopper
     *          When {@link Stoppable#isStopped()} is true, this thread will
     *          cleanup and exit cleanly.
     * @param timeout
     */
    public TimeoutMonitor(final int period, final Stoppable stopper,
        FServerManager serverManager, final int timeout) {
      super("AssignmentTimeoutMonitor", period, stopper);
      this.timeout = timeout;
      this.serverManager = serverManager;
    }

    private synchronized void setAllEntityGroupServersOffline(
        boolean allEntityGroupServersOffline) {
      this.allEntityGroupServersOffline = allEntityGroupServersOffline;
    }

    @Override
    protected void chore() {
      boolean noFSERVERAvailable = this.serverManager
          .createDestinationServersList().isEmpty();

      // Iterate all entityGroups in transition checking for time outs
      long now = System.currentTimeMillis();
      // no lock concurrent access ok: we will be working on a copy, and it's
      // java-valid to do
      // a copy while another thread is adding/removing items
      for (String entityGroupName : entityGroupStates
          .getEntityGroupsInTransition().keySet()) {
        EntityGroupState entityGroupState = entityGroupStates
            .getEntityGroupTransitionState(entityGroupName);
        if (entityGroupState == null)
          continue;

        if (entityGroupState.getStamp() + timeout <= now) {
          // decide on action upon timeout
          actOnTimeOut(entityGroupState);
        } else if (this.allEntityGroupServersOffline && !noFSERVERAvailable) {
          EntityGroupPlan existingPlan = entityGroupPlans.get(entityGroupName);
          if (existingPlan == null
              || !this.serverManager.isServerOnline(existingPlan
                  .getDestination())) {
            // if some FSERVERs just came back online, we can start the
            // assignment
            // right away
            actOnTimeOut(entityGroupState);
          }
        }
      }
      setAllEntityGroupServersOffline(noFSERVERAvailable);
    }

    private void actOnTimeOut(EntityGroupState entityGroupState) {
      EntityGroupInfo entityGroupInfo = entityGroupState.getEntityGroup();
      LOG.info("EntityGroups in transition timed out:  " + entityGroupState);
      // Expired! Do a retry.
      switch (entityGroupState.getState()) {
      case CLOSED:
        LOG.info("EntityGroup " + entityGroupInfo.getEncodedName()
            + " has been CLOSED for too long, waiting on queued "
            + "ClosedEntityGroupHandler to run or server shutdown");
        // Update our timestamp.
        entityGroupState.updateTimestampToNow();
        break;
      case OFFLINE:
        LOG.info("EntityGroup has been OFFLINE for too long, " + "reassigning "
            + entityGroupInfo.getEntityGroupNameAsString()
            + " to a random server");
        invokeAssign(entityGroupInfo);
        break;
      case PENDING_OPEN:
        LOG.info("EntityGroup has been PENDING_OPEN for too "
            + "long, reassigning entityGroup="
            + entityGroupInfo.getEntityGroupNameAsString());
        invokeAssign(entityGroupInfo);
        break;
      case OPENING:
        processOpeningState(entityGroupInfo);
        break;
      case OPEN:
        LOG.error("EntityGroup has been OPEN for too long, "
            + "we don't know where entityGroup was opened so can't do anything");
        entityGroupState.updateTimestampToNow();
        break;

      case PENDING_CLOSE:
        LOG.info("EntityGroup has been PENDING_CLOSE for too "
            + "long, running forced unassign again on entityGroup="
            + entityGroupInfo.getEntityGroupNameAsString());
        invokeUnassign(entityGroupInfo);
        break;
      case CLOSING:
        LOG.info("EntityGroup has been CLOSING for too "
            + "long, this should eventually complete or the server will "
            + "expire, send RPC again");
        invokeUnassign(entityGroupInfo);
        break;

      case SPLIT:
      case SPLITTING:
        break;

      default:
        throw new IllegalStateException("Received event is not valid.");
      }
    }
  }

  private void processOpeningState(EntityGroupInfo entityGroupInfo) {
    LOG.info("EntityGroup has been OPENING for too long, reassigning entityGroup="
        + entityGroupInfo.getEntityGroupNameAsString());
    // Should have a ZK node in OPENING state
    try {
      String node = ZKAssign.getNodeName(watcher,
          entityGroupInfo.getEncodedName());
      Stat stat = new Stat();
      byte[] data = ZKAssign.getDataNoWatch(watcher, node, stat);
      if (data == null) {
        LOG.warn("Data is null, node " + node + " no longer exists");
        return;
      }
      EntityGroupTransaction rt = EntityGroupTransaction.parseFrom(data);
      EventType et = rt.getEventType();
      if (et == EventType.FSERVER_ZK_ENTITYGROUP_OPENED) {
        LOG.debug("EntityGroup has transitioned to OPENED, allowing "
            + "watched event handlers to process");
        return;
      } else if (et != EventType.FSERVER_ZK_ENTITYGROUP_OPENING
          && et != EventType.FSERVER_ZK_ENTITYGROUP_FAILED_OPEN) {
        LOG.warn("While timing out a entityGroup, found ZK node in unexpected state: "
            + et);
        return;
      }
      invokeAssign(entityGroupInfo);
    } catch (KeeperException ke) {
      LOG.error("Unexpected ZK exception timing out CLOSING entityGroup", ke);
      return;
    } catch (DeserializationException e) {
      LOG.error("Unexpected exception parsing CLOSING entityGroup", e);
      return;
    }
    return;
  }

  void invokeAssign(EntityGroupInfo entityGroupInfo) {
    threadPoolExecutorService.submit(new AssignCallable(this, entityGroupInfo));
  }

  private void invokeUnassign(EntityGroupInfo entityGroupInfo) {
    threadPoolExecutorService
        .submit(new UnAssignCallable(this, entityGroupInfo));
  }

  /**
   * Check if the shutdown server carries the specific entityGroup. We have a
   * bunch of places that store entityGroup location Those values aren't
   * consistent. There is a delay of notification. The location from zookeeper
   * unassigned node has the most recent data; but the node could be deleted
   * after the entityGroup is opened by AM. The AM's info could be old when
   * OpenedEntityGroupHandler processing hasn't finished yet when server
   * shutdown occurs.
   * 
   * @return whether the serverName currently hosts the entityGroup
   */
  private boolean isCarryingEntityGroup(ServerName serverName,
      EntityGroupInfo egInfo) {
    EntityGroupTransaction rt = null;
    try {
      byte[] data = ZKAssign.getData(watcher, egInfo.getEncodedName());
      // This call can legitimately come by null
      rt = data == null ? null : EntityGroupTransaction.parseFrom(data);
    } catch (KeeperException e) {
      server.abort("Exception reading unassigned node for entityGroup="
          + egInfo.getEncodedName(), e);
    } catch (DeserializationException e) {
      server.abort("Exception parsing unassigned node for entityGroup="
          + egInfo.getEncodedName(), e);
    }

    ServerName addressFromZK = rt != null ? rt.getServerName() : null;
    if (addressFromZK != null) {
      // if we get something from ZK, we will use the data
      boolean matchZK = (addressFromZK != null && addressFromZK
          .equals(serverName));
      LOG.debug("based on ZK, current entityGroup="
          + egInfo.getEntityGroupNameAsString() + " is on server="
          + addressFromZK + " server being checked=: " + serverName);
      return matchZK;
    }

    ServerName addressFromAM = entityGroupStates
        .getFServerOfEntityGroup(egInfo);
    boolean matchAM = (addressFromAM != null && addressFromAM
        .equals(serverName));
    LOG.debug("based on AM, current entityGroup="
        + egInfo.getEntityGroupNameAsString() + " is on server="
        + (addressFromAM != null ? addressFromAM : "null")
        + " server being checked: " + serverName);

    return matchAM;
  }

  /**
   * Process shutdown server removing any assignments.
   * 
   * @param sn
   *          Server that went down.
   * @return list of entityGroups in transition on this server
   */
  public List<EntityGroupState> processServerShutdown(final ServerName sn) {
    // Clean out any existing assignment plans for this server
    synchronized (this.entityGroupPlans) {
      for (Iterator<Map.Entry<String, EntityGroupPlan>> i = this.entityGroupPlans
          .entrySet().iterator(); i.hasNext();) {
        Map.Entry<String, EntityGroupPlan> e = i.next();
        ServerName otherSn = e.getValue().getDestination();
        // The name will be null if the entityGroup is planned for a random
        // assign.
        if (otherSn != null && otherSn.equals(sn)) {
          // Use iterator's remove else we'll get CME
          i.remove();
        }
      }
    }
    return entityGroupStates.serverOffline(sn);
  }

  /**
   * Update inmemory structures.
   * 
   * @param sn
   *          Server that reported the split
   * @param parent
   *          Parent entityGroup that was split
   * @param a
   *          Daughter entityGroup A
   * @param b
   *          Daughter entityGroup B
   */
  public void handleSplitReport(final ServerName sn,
      final EntityGroupInfo parent, final EntityGroupInfo a,
      final EntityGroupInfo b) {
    entityGroupOffline(parent);
    entityGroupOnline(a, sn);
    entityGroupOnline(b, sn);

    // There's a possibility that the entityGroup was splitting while a user
    // asked
    // the master to disable, we need to make sure we close those entityGroups
    // in
    // that case. This is not racing with the entityGroup server itself since
    // FSERVER
    // report is done after the split transaction completed.
    if (this.zkTable.isDisablingOrDisabledTable(parent.getTableNameAsString())) {
      unassign(a);
      unassign(b);
    }
  }

  /**
   * @param plan
   *          Plan to execute.
   */
  void balance(final EntityGroupPlan plan) {
    synchronized (this.entityGroupPlans) {
      this.entityGroupPlans.put(plan.getEntityGroupName(), plan);
    }
    unassign(plan.getEntityGroupInfo(), false, plan.getDestination());
  }

  public void stop() {
    this.timeoutMonitor.interrupt();
    this.timerUpdater.interrupt();
  }

  /**
   * Shutdown the threadpool executor service
   */
  public void shutdown() {
    threadPoolExecutorService.shutdownNow();
    for (int i = 0, n = zkEventWorkers.length; i < n; i++) {
      zkEventWorkers[i].shutdownNow();
    }
  }

  protected void setEnabledTable(String tableName) {
    try {
      this.zkTable.setEnabledTable(tableName);
    } catch (KeeperException e) {
      // here we can abort as it is the start up flow
      String errorMsg = "Unable to ensure that the table " + tableName
          + " will be" + " enabled because of a ZooKeeper issue";
      LOG.error(errorMsg);
      this.server.abort(errorMsg, e);
    }
  }

  /**
   * Set entityGroup as OFFLINED up in zookeeper asynchronously.
   * 
   * @param state
   * @return True if we succeeded, false otherwise (State was incorrect or
   *         failed updating zk).
   */
  private boolean asyncSetOfflineInZooKeeper(final EntityGroupState state,
      final AsyncCallback.StringCallback cb, final ServerName destination) {
    if (!state.isClosed() && !state.isOffline()) {
      this.server.abort("Unexpected state trying to OFFLINE; " + state,
          new IllegalStateException());
      return false;
    }
    entityGroupStates.updateEntityGroupState(state.getEntityGroup(),
        EntityGroupState.State.OFFLINE);
    try {
      ZKAssign.asyncCreateNodeOffline(watcher, state.getEntityGroup(),
          destination, cb, state);
    } catch (KeeperException e) {
      if (e instanceof NodeExistsException) {
        LOG.warn("Node for " + state.getEntityGroup() + " already exists");
      } else {
        server
            .abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      }
      return false;
    }
    return true;
  }
}

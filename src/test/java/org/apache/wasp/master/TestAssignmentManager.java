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
package org.apache.wasp.master;

import static org.junit.Assert.assertNotSame;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.wasp.DeserializationException;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.FConstants;
import org.apache.wasp.Server;
import org.apache.wasp.ServerLoad;
import org.apache.wasp.ServerName;
import org.apache.wasp.ZooKeeperConnectionException;
import org.apache.wasp.executor.EventHandler.EventType;
import org.apache.wasp.executor.ExecutorService;
import org.apache.wasp.executor.ExecutorService.ExecutorType;
import org.apache.wasp.fserver.EntityGroupOpeningState;
import org.apache.wasp.master.balancer.LoadBalancerFactory;
import org.apache.wasp.zookeeper.ZKAssign;
import org.apache.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test {@link AssignmentManager}
 */

public class TestAssignmentManager {
  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();

  private static final EntityGroupInfo ENTITYGROUPINFO = new EntityGroupInfo(
      Bytes.toBytes("t"), FConstants.EMPTY_START_ROW,
      FConstants.EMPTY_START_ROW);
  private static final ServerName SERVERNAME_A = new ServerName("example.org",
      1234, 5678);
  private static final ServerName SERVERNAME_B = new ServerName("example.org",
      0, 5678);

  // Mocked objects or; get redone for each test.
  private Server server;
  private FServerManager serverManager;
  private ZooKeeperWatcher watcher;
  private LoadBalancer balancer;
  private FMaster master;

  @BeforeClass
  public static void beforeClass() throws Exception {
    HTU.startMiniZKCluster();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    HTU.shutdownMiniZKCluster();
  }

  @Before
  public void before() throws ZooKeeperConnectionException, IOException {
    // TODO: Make generic versions of what we do below and put up in a mocking
    // utility class or move up into HBaseTestingUtility.

    // Mock a Server. Have it return a legit Configuration and ZooKeeperWatcher.
    // If abort is called, be sure to fail the test (don't just swallow it
    // silently as is mockito default).
    this.server = Mockito.mock(Server.class);
    Mockito.when(server.getServerName()).thenReturn(
        new ServerName("master,1,1"));
    Mockito.when(server.getConfiguration()).thenReturn(HTU.getConfiguration());
    HTU.getConfiguration().set(FConstants.ZOOKEEPER_QUORUM,
        HTU.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM));
    HTU.getConfiguration().set(FConstants.ZOOKEEPER_CLIENT_PORT,
        HTU.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT));
    this.watcher = new ZooKeeperWatcher(HTU.getConfiguration(), "mockedServer",
        this.server, true);
    Mockito.when(server.getZooKeeper()).thenReturn(this.watcher);
    Mockito.doThrow(new RuntimeException("Aborted")).when(server)
        .abort(Mockito.anyString(), (Throwable) Mockito.anyObject());

    // Mock a ServerManager. Say server SERVERNAME_{A,B} are online. Also
    // make it so if close or open, we return 'success'.
    this.serverManager = Mockito.mock(FServerManager.class);
    Mockito.when(this.serverManager.isServerOnline(SERVERNAME_A)).thenReturn(
        true);
    Mockito.when(this.serverManager.isServerOnline(SERVERNAME_B)).thenReturn(
        true);
    final Map<ServerName, ServerLoad> onlineServers = new HashMap<ServerName, ServerLoad>();
    onlineServers.put(SERVERNAME_B, ServerLoad.EMPTY_SERVERLOAD);
    onlineServers.put(SERVERNAME_A, ServerLoad.EMPTY_SERVERLOAD);
    Mockito.when(this.serverManager.getOnlineServersList()).thenReturn(
        new ArrayList<ServerName>(onlineServers.keySet()));
    Mockito.when(this.serverManager.getOnlineServers()).thenReturn(
        onlineServers);

    List<ServerName> avServers = new ArrayList<ServerName>();
    avServers.addAll(onlineServers.keySet());
    Mockito.when(this.serverManager.createDestinationServersList()).thenReturn(
        avServers);
    Mockito.when(this.serverManager.createDestinationServersList(null))
        .thenReturn(avServers);

    Mockito.when(
        this.serverManager.sendEntityGroupClose(SERVERNAME_A, ENTITYGROUPINFO,
            -1)).thenReturn(true);
    Mockito.when(
        this.serverManager.sendEntityGroupClose(SERVERNAME_B, ENTITYGROUPINFO,
            -1)).thenReturn(true);
    // Ditto on open.
    Mockito.when(
        this.serverManager.sendEntityGroupOpen(SERVERNAME_A, ENTITYGROUPINFO,
            -1)).thenReturn(EntityGroupOpeningState.OPENED);
    Mockito.when(
        this.serverManager.sendEntityGroupOpen(SERVERNAME_B, ENTITYGROUPINFO,
            -1)).thenReturn(EntityGroupOpeningState.OPENED);
    this.master = Mockito.mock(FMaster.class);

    Mockito.when(this.master.getFServerManager()).thenReturn(serverManager);
  }

  @After
  public void after() throws KeeperException {
    if (this.watcher != null) {
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
      this.watcher.close();
    }
  }

  /**
   * Tests AssignmentManager balance function. Runs a balance moving a
   * entityGroup from one server to another mocking fserver responding over zk.
   * 
   * @throws IOException
   * @throws KeeperException
   * @throws DeserializationException
   */
  @Test
  public void testBalance() throws IOException, KeeperException,
      DeserializationException, InterruptedException {
    // Create and startup an executor. This is used by AssignmentManager
    // handling zk callbacks.
    ExecutorService executor = startupMasterExecutor("testBalanceExecutor");

    // We need a mocked catalog tracker.
    LoadBalancer balancer = LoadBalancerFactory.getLoadBalancer(server
        .getConfiguration());
    // Create an AM.
    AssignmentManager am = new AssignmentManager(this.server,
        this.serverManager, balancer, executor, null);
    am.failoverCleanupDone.set(true);
    try {
      // Make sure our new AM gets callbacks; once registered, can't unregister.
      // Thats ok because we make a new zk watcher for each test.
      this.watcher.registerListenerFirst(am);
      // Call the balance function but fake the entityGroup being online first
      // at
      // SERVERNAME_A. Create a balance plan.
      am.entityGroupOnline(ENTITYGROUPINFO, SERVERNAME_A);
      // Balance entityGroup from A to B.
      EntityGroupPlan plan = new EntityGroupPlan(ENTITYGROUPINFO, SERVERNAME_A,
          SERVERNAME_B);
      am.balance(plan);

      // Now fake the entityGroup closing successfully over on the fserver; the
      // fserver will have set the entityGroup in CLOSED state. This will
      // trigger callback into AM. The below zk close call is from the FSERVER
      // close
      // entityGroup handler duplicated here because its down deep in a private
      // method hard to expose.
      int versionid = ZKAssign.transitionNodeClosed(this.watcher,
          ENTITYGROUPINFO, SERVERNAME_A, -1);
      assertNotSame(versionid, -1);
      // AM is going to notice above CLOSED and queue up a new assign. The
      // assign will go to open the entityGroup in the new location set by the
      // balancer. The zk node will be OFFLINE waiting for fserver to
      // transition it through OPENING, OPENED. Wait till we see the OFFLINE
      // zk node before we proceed.
      Mocking.waitForEntityGroupPendingOpenInRIT(am,
          ENTITYGROUPINFO.getEncodedName());

      // Get current versionid else will fail on transition from OFFLINE to
      // OPENING below
      versionid = ZKAssign.getVersion(this.watcher, ENTITYGROUPINFO);
      assertNotSame(-1, versionid);
      // This uglyness below is what the openentityGrouphandler on FSERVER side
      // does.
      versionid = ZKAssign.transitionNode(server.getZooKeeper(),
          ENTITYGROUPINFO, SERVERNAME_A, EventType.M_ZK_ENTITYGROUP_OFFLINE,
          EventType.FSERVER_ZK_ENTITYGROUP_OPENING, versionid);
      assertNotSame(-1, versionid);
      // Move znode from OPENING to OPENED as FSERVER does on successful open.
      versionid = ZKAssign.transitionNodeOpened(this.watcher, ENTITYGROUPINFO,
          SERVERNAME_B, versionid);
      assertNotSame(-1, versionid);
      // Wait on the handler removing the OPENED znode.
      while (am.getEntityGroupStates().isEntityGroupInTransition(
          ENTITYGROUPINFO))
        Threads.sleep(1);
    } finally {
      executor.shutdown();
      am.shutdown();
      // Clean up all znodes
      ZKAssign.deleteAllNodes(this.watcher);
    }
  }

  /**
   * Create and startup executor pools. Start same set as master does (just run
   * a few less).
   * 
   * @param name
   *          Name to give our executor
   * @return Created executor (be sure to call shutdown when done).
   */
  private ExecutorService startupMasterExecutor(final String name) {
    // TODO: Move up into HBaseTestingUtility? Generally useful.
    ExecutorService executor = new ExecutorService(name);
    executor.startExecutorService(ExecutorType.MASTER_OPEN_ENTITYGROUP, 3);
    executor.startExecutorService(ExecutorType.MASTER_CLOSE_ENTITYGROUP, 3);
    executor.startExecutorService(ExecutorType.MASTER_SERVER_OPERATIONS, 3);
    return executor;
  }
}

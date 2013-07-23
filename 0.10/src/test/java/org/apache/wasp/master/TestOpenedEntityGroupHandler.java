/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.wasp.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.FConstants;
import org.apache.wasp.MiniWaspCluster;
import org.apache.wasp.Server;
import org.apache.wasp.WaspTestingUtility;
import org.apache.wasp.ZooKeeperConnectionException;
import org.apache.wasp.executor.EventHandler.EventType;
import org.apache.wasp.fserver.EntityGroup;
import org.apache.wasp.fserver.FServer;
import org.apache.wasp.fserver.redo.MemRedoLog;
import org.apache.wasp.fserver.redo.Redo;
import org.apache.wasp.master.handler.OpenedEntityGroupHandler;
import org.apache.wasp.meta.FMetaTestUtil;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.util.MockServer;
import org.apache.wasp.zookeeper.ZKAssign;
import org.apache.wasp.zookeeper.ZKTable;
import org.apache.wasp.zookeeper.ZKUtil;
import org.apache.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestOpenedEntityGroupHandler {

  private static final Log LOG = LogFactory
      .getLog(TestOpenedEntityGroupHandler.class);

  private WaspTestingUtility TEST_UTIL;
  private final int NUM_MASTERS = 1;
  private final int NUM_FServer = 1;
  private ZooKeeperWatcher zkw;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL = new WaspTestingUtility();
    TEST_UTIL.getConfiguration().setClass(FConstants.REDO_IMPL,
        MemRedoLog.class, Redo.class);
  }

  @After
  public void tearDown() throws Exception {
    // Stop the cluster
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL = new WaspTestingUtility();
  }

  @Test
  public void testOpenedEntityGroupHandlerOnMasterRestart() throws Exception {
    // Start the cluster
    log("Starting cluster");
    TEST_UTIL.getConfiguration().setInt(
        "wasp.master.assignment.timeoutmonitor.period", 2000);
    TEST_UTIL.getConfiguration().setInt(
        "wasp.master.assignment.timeoutmonitor.timeout", 5000);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_FServer);
    String tableName = "testOpenedEntityGroupHandlerOnMasterRestart";
    MiniWaspCluster cluster = createEntityGroups(tableName);
    abortMaster(cluster);

    FServer entityGroupServer = cluster.getFServer(0);
    EntityGroup entityGroup = getEntityGroupBeingServed(cluster,
        entityGroupServer);

    // forcefully move a entityGroup to OPENED state in zk
    // Create a ZKW to use in the test
    zkw = WaspTestingUtility.createAndForceNodeToOpenedState(TEST_UTIL,
        entityGroup, entityGroupServer.getServerName());

    // Start up a new master
    log("Starting up a new master");
    cluster.startMaster().getMaster();
    log("Waiting for master to be ready");
    cluster.waitForActiveAndReadyMaster();
    log("Master is ready");

    // Failover should be completed, now wait for no RIT
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);
  }

  @Test
  public void testShouldNotCompeleteOpenedEntityGroupSuccessfullyIfVersionMismatches()
      throws Exception {
    EntityGroup entityGroup = null;
    try {
      int testIndex = 0;
      TEST_UTIL.getHBaseTestingUtility().startMiniZKCluster();
      TEST_UTIL.getConfiguration().set(FConstants.ZOOKEEPER_QUORUM,
          TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM));
      TEST_UTIL.getConfiguration().set(FConstants.ZOOKEEPER_CLIENT_PORT,
          TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT));
      final Server server = new MockServer(TEST_UTIL);
      FTable htd = FMetaTestUtil
          .makeTable("testShouldNotCompeleteOpenedEntityGroupSuccessfullyIfVersionMismatches");
      EntityGroupInfo egi = new EntityGroupInfo(Bytes.toBytes(htd
          .getTableName()), Bytes.toBytes(testIndex),
          Bytes.toBytes(testIndex + 1));
      entityGroup = EntityGroup.createEntityGroup(egi,
          TEST_UTIL.getConfiguration(), htd, null);
      assertNotNull(entityGroup);
      AssignmentManager am = Mockito.mock(AssignmentManager.class);
      EntityGroupStates rsm = Mockito.mock(EntityGroupStates.class);
      Mockito.doReturn(rsm).when(am).getEntityGroupStates();
      when(rsm.isEntityGroupInTransition(egi)).thenReturn(false);
      when(rsm.getEntityGroupState(egi)).thenReturn(
          new EntityGroupState(entityGroup.getEntityGroupInfo(),
              EntityGroupState.State.OPEN, System.currentTimeMillis(), server
                  .getServerName()));
      // create a node with OPENED state
      zkw = WaspTestingUtility.createAndForceNodeToOpenedState(TEST_UTIL,
          entityGroup, server.getServerName());
      when(am.getZKTable()).thenReturn(new ZKTable(zkw));
      Stat stat = new Stat();
      String nodeName = ZKAssign.getNodeName(zkw, entityGroup
          .getEntityGroupInfo().getEncodedName());
      ZKUtil.getDataAndWatch(zkw, nodeName, stat);

      // use the version for the OpenedEntityGroupHandler
      OpenedEntityGroupHandler handler = new OpenedEntityGroupHandler(server,
          am, entityGroup.getEntityGroupInfo(), server.getServerName(),
          stat.getVersion());
      // Once again overwrite the same znode so that the version changes.
      ZKAssign.transitionNode(zkw, entityGroup.getEntityGroupInfo(),
          server.getServerName(), EventType.FSERVER_ZK_ENTITYGROUP_OPENED,
          EventType.FSERVER_ZK_ENTITYGROUP_OPENED, stat.getVersion());

      // Should not invoke assignmentmanager.entityGroupOnline. If it is
      // invoked as per current mocking it will throw null pointer exception.
      boolean expectedException = false;
      try {
        handler.process();
      } catch (Exception e) {
        expectedException = true;
      }
      assertFalse("The process method should not throw any exception.",
          expectedException);
      List<String> znodes = ZKUtil.listChildrenAndWatchForNewChildren(zkw,
          zkw.assignmentZNode);
      String entityGroupName = znodes.get(0);
      assertEquals("The entityGroup should not be opened successfully.",
          entityGroupName, entityGroup.getEntityGroupInfo().getEncodedName());
    } finally {
      EntityGroup.closeEntityGroup(entityGroup);
      TEST_UTIL.getHBaseTestingUtility().shutdownMiniZKCluster();
    }
  }

  private MiniWaspCluster createEntityGroups(String tableName)
      throws InterruptedException, ZooKeeperConnectionException, IOException,
      KeeperException {
    MiniWaspCluster cluster = TEST_UTIL.getWaspCluster();
    log("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();
    zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "testOpenedEntityGroupHandler", null);

    // Create a table with entityGroups
    byte[] table = Bytes.toBytes(tableName);
    byte[] family = Bytes.toBytes("family");
    TEST_UTIL.createTable(table);

    // wait till the entityGroups are online
    log("Waiting for no more RIT");
    ZKAssign.blockUntilNoRIT(zkw);

    return cluster;
  }

  private void abortMaster(MiniWaspCluster cluster) {
    // Stop the master
    log("Aborting master");
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    log("Master has aborted");
  }

  private EntityGroup getEntityGroupBeingServed(MiniWaspCluster cluster,
      FServer entityGroupServer) {
    Collection<EntityGroup> onlineEntityGroupsLocalContext = entityGroupServer
        .getOnlineEntityGroupsLocalContext();
    Iterator<EntityGroup> iterator = onlineEntityGroupsLocalContext.iterator();
    EntityGroup entityGroup = null;
    while (iterator.hasNext()) {
      entityGroup = iterator.next();
      break;
    }
    return entityGroup;
  }

  private void log(String msg) {
    LOG.debug("\n\nTRR: " + msg + "\n");
  }

}

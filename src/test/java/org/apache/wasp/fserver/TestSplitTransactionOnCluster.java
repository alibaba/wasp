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
package org.apache.wasp.fserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.DeserializationException;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.EntityGroupTransaction;
import org.apache.wasp.FConstants;
import org.apache.wasp.MiniWaspCluster;
import org.apache.wasp.ServerName;
import org.apache.wasp.WaspTestingUtility;
import org.apache.wasp.client.WaspAdmin;
import org.apache.wasp.executor.EventHandler;
import org.apache.wasp.master.EntityGroupPlan;
import org.apache.wasp.master.FMaster;
import org.apache.wasp.master.handler.SplitEntityGroupHandler;
import org.apache.wasp.meta.FMetaReader;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.meta.TableSchemaCacheReader;
import org.apache.wasp.protobuf.ProtobufUtil;
import org.apache.wasp.zookeeper.ZKAssign;
import org.apache.wasp.zookeeper.ZKUtil;
import org.apache.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.ServiceException;

/**
 * Like {@link TestSplitTransaction} in that we're testing
 * {@link SplitTransaction} only the below tests are against a running cluster
 * where {@link TestSplitTransaction} is tests against a bare
 * {@link EntityGroup}.
 */
public class TestSplitTransactionOnCluster {
  private static final Log LOG = LogFactory
      .getLog(TestSplitTransactionOnCluster.class);
  private static WaspAdmin admin = null;
  private static MiniWaspCluster cluster = null;
  private static final int NB_SERVERS = 5;
  private static ZooKeeperWatcher zkw;

  private static final WaspTestingUtility TESTING_UTIL = new WaspTestingUtility();

  @BeforeClass
  public static void before() throws Exception {
    TESTING_UTIL.getConfiguration().setInt("wasp.balancer.period", 60000);
    // Needed because some tests have splits happening on FS that are killed
    // We don't want to wait 3min for the master to figure it out
    TESTING_UTIL.getConfiguration().setInt(
        "wasp.master.assignment.timeoutmonitor.timeout", 4000);
    TESTING_UTIL.getConfiguration().setBoolean(
        "wasp.online.schema.update.enable", true);
    TESTING_UTIL.getConfiguration().set(FConstants.REDO_IMPL,
        "org.apache.wasp.fserver.redo.MemRedoLog");
    TESTING_UTIL.getConfiguration().setInt("wasp.fserver.msginterval", 100);
    TESTING_UTIL.getConfiguration().setInt("wasp.client.pause", 250);
    TESTING_UTIL.getConfiguration().setInt("wasp.client.retries.number", 6);
    TESTING_UTIL.getConfiguration().setBoolean(
        "wasp.master.enabletable.roundrobin", true);
    TESTING_UTIL.getConfiguration().setBoolean(
        "wasp.ipc.server.blacklist.enable", true);
    TESTING_UTIL.getConfiguration().setBoolean("wasp.testing.nocluster", false);
    TESTING_UTIL.startMiniCluster(NB_SERVERS);
    TableSchemaCacheReader.getInstance(TESTING_UTIL.getConfiguration())
        .clearCache();
    admin = new WaspAdmin(TESTING_UTIL.getConfiguration());
    cluster = TESTING_UTIL.getMiniWaspCluster();
    zkw = WaspTestingUtility.getZooKeeperWatcher(TESTING_UTIL);
  }

  @AfterClass
  public static void after() throws Exception {
    TESTING_UTIL.shutdownMiniCluster();
  }

  private EntityGroupInfo getAndCheckSingleTableEntityGroup(
      final List<EntityGroup> entityGroups) {
    assertEquals(1, entityGroups.size());
    return entityGroups.get(0).getEntityGroupInfo();
  }

  /**
   * A test that intentionally has master fail the processing of the split
   * message. Tests that the fserver split ephemeral node gets cleaned up if it
   * crashes and that after we process server shutdown, the daughters are up on
   * line.
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws NodeExistsException
   * @throws KeeperException
   * @throws DeserializationException
   */
  @Test
  public void testFSSplitEphemeralsDisappearButDaughtersAreOnlinedAfterShutdownHandling()
      throws IOException, InterruptedException, NodeExistsException,
      KeeperException, DeserializationException, ServiceException {
    final byte[] tableName = Bytes.toBytes("ephemeral");

    // Create table then get the single entityGroup for our new table.
    FTable t = TESTING_UTIL.createTable(tableName);

    List<EntityGroup> entityGroups = cluster.getEntityGroups(tableName);
    EntityGroupInfo egi = getAndCheckSingleTableEntityGroup(entityGroups);

    int tableEntityGroupIndex = getTableEntityGroupIndex(admin, egi);

    // Turn off balancer so it doesn't cut in and mess up our placements.
    admin.setBalancerRunning(false, true);
    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t);
      // Get entityGroup pre-split.
      FServer server = cluster.getFServer(tableEntityGroupIndex);
      printOutEntityGroups(server, "Initial entityGroups: ");
      int entityGroupCount = ProtobufUtil.getOnlineEntityGroups(server).size();
      // Now, before we split, set special flag in master, a flag that has
      // it FAIL the processing of split.
      SplitEntityGroupHandler.TEST_SKIP = true;
      // Now try splitting and it should work.
      split(egi, "1", server, entityGroupCount);
      // Get daughters
      List<EntityGroup> daughters = checkAndGetDaughters(tableName);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(zkw, egi.getEncodedName());
      Stat stats = zkw.getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats="
          + stats);
      EntityGroupTransaction rtd = EntityGroupTransaction.parseFrom(ZKAssign
          .getData(zkw, egi.getEncodedName()));
      // State could be SPLIT or SPLITTING.
      assertTrue(rtd.getEventType().equals(
          EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLIT)
          || rtd.getEventType().equals(
              EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLITTING));
      // Now crash the server
      cluster.abortFServer(tableEntityGroupIndex);
      waitUntilFServerDead();
      awaitDaughters(tableName, daughters.size());

      // Assert daughters are online.
      entityGroups = cluster.getEntityGroups(tableName);
      for (EntityGroup r : entityGroups) {
        assertTrue(daughters.contains(r));
      }
      // Finally assert that the ephemeral SPLIT znode was cleaned up.
      for (int i = 0; i < 100; i++) {
        // wait a bit (10s max) for the node to disappear
        stats = zkw.getRecoverableZooKeeper().exists(path, false);
        if (stats == null)
          break;
        Thread.sleep(100);
      }
      LOG.info("EPHEMERAL NODE AFTER SERVER ABORT, path=" + path + ", stats="
          + stats);
      assertTrue(stats == null);
    } finally {
      // Set this flag back.
      SplitEntityGroupHandler.TEST_SKIP = false;
      admin.setBalancerRunning(true, false);
    }
  }

  @Test
  public void testExistingZnodeBlocksSplitAndWeRollback() throws IOException,
      InterruptedException, NodeExistsException, KeeperException {
    final byte[] tableName = Bytes
        .toBytes("testExistingZnodeBlocksSplitAndWeRollback");

    // Create table then get the single entityGroup for our new table.
    FTable t = TESTING_UTIL.createTable(tableName);
    // TESTING_UTIL.waitTableEnabled(tableName, 5000);
    List<EntityGroup> entityGroups = cluster.getEntityGroups(tableName);
    EntityGroupInfo egi = getAndCheckSingleTableEntityGroup(entityGroups);

    int tableEntityGroupIndex = getTableEntityGroupIndex(admin, egi);

    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t);
      // Get entityGroup pre-split.
      FServer server = cluster.getFServer(tableEntityGroupIndex);
      printOutEntityGroups(server, "Initial entityGroups: ");
      int entityGroupCount = server.getOnlineEntityGroups().size();
      // Insert into zk a blocking znode, a znode of same name as entityGroup
      // so it gets in way of our splitting.

      ZooKeeperWatcher zkw = WaspTestingUtility
          .getZooKeeperWatcher(TESTING_UTIL);
      ZKAssign.createNodeClosing(zkw, egi, new ServerName("any.old.server",
          1234, -1));
      // Now try splitting.... should fail. And each should successfully
      // rollback.
      admin.split(egi.getEntityGroupNameAsString(), "3");
      admin.split(egi.getEntityGroupNameAsString(), "2");
      admin.split(egi.getEntityGroupNameAsString(), "1");
      // Wait around a while and assert count of entityGroups remains constant.
      for (int i = 0; i < 10; i++) {
        Thread.sleep(100);
        assertEquals(entityGroupCount, server.getOnlineEntityGroups().size());
      }
      // Now clear the zknode
      ZKAssign.deleteClosingNode(zkw, egi);
      // Now try splitting and it should work.
      split(egi, "0", server, entityGroupCount);
      // Get daughters
      List<EntityGroup> daughters = cluster.getEntityGroups(tableName);
      assertTrue(daughters.size() >= 2);
      // OK, so split happened after we cleared the blocking node.
    } finally {
      //
    }
  }

  /**
   * Messy test that simulates case where SplitTransactions fails to add one of
   * the daughters up into the .META. table before crash. We're testing fact
   * that the shutdown handler will fixup the missing daughter entityGroup
   * adding it back into .META.
   * 
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  @Test
  public void testShutdownSimpleFixup() throws IOException,
      InterruptedException {
    final byte[] tableName = Bytes.toBytes("testShutdownSimpleFixup");

    // Create table then get the single entityGroup for our new table.
    FTable t = TESTING_UTIL.createTable(tableName);
    TESTING_UTIL.waitTableEnabled(tableName, 5000);
    List<EntityGroup> entityGroups = cluster.getEntityGroups(tableName);
    EntityGroupInfo egi = getAndCheckSingleTableEntityGroup(entityGroups);

    int tableEntityGroupIndex = getTableEntityGroupIndex(admin, egi);

    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t);
      // Get entityGroup pre-split.
      FServer server = cluster.getFServer(tableEntityGroupIndex);
      printOutEntityGroups(server, "Initial entityGroups: ");
      int entityGroupCount = server.getOnlineEntityGroups().size();
      // Now split.
      split(egi, "1", server, entityGroupCount);
      // Get daughters
      List<EntityGroup> daughters = cluster.getEntityGroups(tableName);
      assertTrue(daughters.size() >= 2);
      // Remove one of the daughters from .META. to simulate failed insert of
      // daughter entityGroup up into .META.
      // removeDaughterFromMeta(daughters.get(0).getEntityGroupInfo());
      // Now crash the server
      cluster.abortFServer(tableEntityGroupIndex);
      waitUntilFServerDead();
      // Wait till entityGroups are back on line again.
      while (cluster.getEntityGroups(tableName).size() < daughters.size()) {
        LOG.info("Waiting for repair to happen");
        Thread.sleep(1000);
      }
      // Assert daughters are online.
      entityGroups = cluster.getEntityGroups(tableName);
      for (EntityGroup r : entityGroups) {
        assertTrue(daughters.contains(r));
      }
    } finally {
      //
    }
  }

  /**
   * Test that if daughter split on us, we won't do the shutdown handler fixup
   * just because we can't find the immediate daughter of an offlined parent.
   * 
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  @Test
  public void testShutdownFixupWhenDaughterHasSplit() throws IOException,
      InterruptedException {
    final byte[] tableName = Bytes
        .toBytes("testShutdownFixupWhenDaughterHasSplit");

    // Create table then get the single entityGroup for our new table.
    FTable t = TESTING_UTIL.createTable(tableName);
    TESTING_UTIL.waitTableEnabled(tableName, 5000);
    List<EntityGroup> entityGroups = cluster.getEntityGroups(tableName);
    EntityGroupInfo egi = getAndCheckSingleTableEntityGroup(entityGroups);

    int tableEntityGroupIndex = getTableEntityGroupIndex(admin, egi);

    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t);
      // Get entityGroup pre-split.
      FServer server = cluster.getFServer(tableEntityGroupIndex);
      printOutEntityGroups(server, "Initial entityGroups: ");
      int entityGroupCount = server.getOnlineEntityGroups().size();
      // Now split.
      split(egi, "2", server, entityGroupCount);
      // Get daughters
      List<EntityGroup> daughters = cluster.getEntityGroups(tableName);
      assertTrue(daughters.size() >= 2);
      // Now split one of the daughters.
      entityGroupCount = server.getOnlineEntityGroups().size();
      EntityGroupInfo daughter = null;
      for (EntityGroup entityGroup : daughters) {
        if (!entityGroup.getEntityGroupInfo().isSplit()) {
          try {
            entityGroup.checkRow(Bytes.toBytes("1"), "find eg");
            daughter = entityGroup.getEntityGroupInfo();
            break;
          } catch (WrongEntityGroupException e) {
          }
        }
      }
      assertTrue(daughter != null);
      daughters = cluster.getEntityGroups(tableName);
      EntityGroup daughterEntityGroup = null;
      for (EntityGroup eg : daughters) {
        if (eg.getEntityGroupInfo().equals(daughter)) {
          daughterEntityGroup = eg;
        }
      }
      assertTrue(daughterEntityGroup != null);

      split(daughter, "1", server, entityGroupCount);
      // Get list of daughters
      daughters = cluster.getEntityGroups(tableName);
      // Now crash the server
      cluster.abortFServer(tableEntityGroupIndex);
      waitUntilFServerDead();
      // Wait till entityGroups are back on line again.
      while (cluster.getEntityGroups(tableName).size() < daughters.size()) {
        LOG.info("Waiting for repair to happen");
        Thread.sleep(1000);
      }
      // Assert daughters are online and ONLY the original daughters -- that
      // fixup didn't insert one during server shutdown recover.
      entityGroups = cluster.getEntityGroups(tableName);
      assertEquals(daughters.size(), entityGroups.size());
      for (EntityGroup r : entityGroups) {
        assertTrue(daughters.contains(r));
      }
    } finally {
      // ;
    }
  }

  private int getTableEntityGroupIndex(WaspAdmin admin, EntityGroupInfo egi) {
    MiniWaspCluster cluster = TESTING_UTIL.getMiniWaspCluster();
    int tableEntityGroupIndex = cluster.getServerWith(egi.getEntityGroupName());
    assertTrue(tableEntityGroupIndex != -1);
    cluster.getFServer(tableEntityGroupIndex);

    tableEntityGroupIndex = cluster.getServerWith(egi.getEntityGroupName());
    assertTrue(tableEntityGroupIndex != -1);
    return tableEntityGroupIndex;
  }

  /**
   * When splitting is partially done and the master goes down when the SPLIT
   * node is in either SPLIT or SPLITTING state.
   * 
   * @throws java.io.IOException
   * @throws InterruptedException
   * @throws org.apache.zookeeper.KeeperException.NodeExistsException
   * @throws org.apache.zookeeper.KeeperException
   */
  @Test
  public void testMasterRestartWhenSplittingIsPartial() throws IOException,
      InterruptedException, NodeExistsException, KeeperException,
      DeserializationException {
    final byte[] tableName = Bytes
        .toBytes("testMasterRestartWhenSplittingIsPartial");

    // Create table then get the single entityGroup for our new table.
    FTable t = TESTING_UTIL.createTable(tableName);
    TESTING_UTIL.waitTableEnabled(tableName, 5000);
    List<EntityGroup> entityGroups = cluster.getEntityGroups(tableName);
    EntityGroupInfo egi = getAndCheckSingleTableEntityGroup(entityGroups);

    int tableEntityGroupIndex = getTableEntityGroupIndex(admin, egi);

    try {
      // Add a bit of load up into the table so splittable.
      TESTING_UTIL.loadTable(t);
      // Get entityGroup pre-split.
      FServer server = cluster.getFServer(tableEntityGroupIndex);
      printOutEntityGroups(server, "Initial entityGroups: ");
      int entityGroupCount = server.getOnlineEntityGroups().size();
      // Now, before we split, set special flag in master, a flag that has
      // it FAIL the processing of split.
      SplitEntityGroupHandler.TEST_SKIP = true;
      // Now try splitting and it should work.
      split(egi, "1", server, entityGroupCount);
      // Get daughters
      List<EntityGroup> daughters = cluster.getEntityGroups(tableName);
      assertTrue(daughters.size() >= 2);
      // Assert the ephemeral node is up in zk.
      String path = ZKAssign.getNodeName(zkw, egi.getEncodedName());
      Stat stats = zkw.getRecoverableZooKeeper().exists(path, false);
      LOG.info("EPHEMERAL NODE BEFORE SERVER ABORT, path=" + path + ", stats="
          + stats);
      EntityGroupTransaction rtd = EntityGroupTransaction.parseFrom(ZKAssign
          .getData(zkw, egi.getEncodedName()));
      // State could be SPLIT or SPLITTING.
      assertTrue(rtd.getEventType().equals(
          EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLIT)
          || rtd.getEventType().equals(
              EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLITTING));

      // abort and wait for new master.
      FMaster master = abortAndWaitForMaster();

      admin = new WaspAdmin(TESTING_UTIL.getConfiguration());

      // update the egi to be offlined and splitted.
      egi.setOffline(true);
      egi.setSplit(true);
      EntityGroupPlan egPlan = master.getAssignmentManager()
          .getEntityGroupReopenPlan(egi);
      assertTrue(egPlan != null);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // Set this flag back.
      SplitEntityGroupHandler.TEST_SKIP = false;
    }
  }

  /**
   * While transitioning node from FS_ZK_ENTITYGROUP_SPLITTING to
   * FS_ZK_ENTITYGROUP_SPLITTING during entityGroup split,if zookeper went down
   * split always fails for the entityGroup.This test case is to test the znode
   * is deleted(if created) or not in roll back.
   * 
   * @throws java.io.IOException
   * @throws InterruptedException
   * @throws org.apache.zookeeper.KeeperException
   */
  @Test
  public void testSplitBeforeSettingSplittingInZK() throws IOException,
      InterruptedException, KeeperException {
    final byte[] tableName = Bytes
        .toBytes("testSplitBeforeSettingSplittingInZK");
    TESTING_UTIL.createTable(tableName);
    TESTING_UTIL.waitTableEnabled(tableName, 5000);
    testSplitBeforeSettingSplittingInZK(tableName, true, false);
    testSplitBeforeSettingSplittingInZK(tableName, false, true);
  }

  private void testSplitBeforeSettingSplittingInZK(byte[] tableName,
      boolean nodeCreated, boolean delete) throws IOException, KeeperException,
      InterruptedException {

    try {
      // Create table then get the single entityGroup for our new table.
      List<EntityGroup> entityGroups = cluster.getEntityGroups(tableName);
      int fserverIndex = cluster.getServerWith(entityGroups.get(0)
          .getEntityGroupName());
      FServer fserver = cluster.getFServer(fserverIndex);
      SplitTransaction st = null;
      if (nodeCreated) {
        st = new MockedSplitTransaction(entityGroups.get(0), null) {
          // @Override
          int transitionNodeSplitting(ZooKeeperWatcher zkw,
              EntityGroupInfo parent, ServerName serverName, int version)
              throws KeeperException, IOException {
            throw new IOException();
          }
        };
      } else {
        st = new MockedSplitTransaction(entityGroups.get(0), null) {
          @Override
          int createNodeSplitting(ZooKeeperWatcher zkw,
              EntityGroupInfo entityGroup, ServerName serverName)
              throws KeeperException, IOException {
            throw new IOException();
          }
        };
      }
      try {
        st.execute(fserver, fserver);
      } catch (IOException e) {
        String node = ZKAssign.getNodeName(fserver.getZooKeeper(), entityGroups
            .get(0).getEntityGroupInfo().getEncodedName());
        if (nodeCreated) {
          assertFalse(ZKUtil.checkExists(fserver.getZooKeeper(), node) == -1);
        } else {
          assertTrue(ZKUtil.checkExists(fserver.getZooKeeper(), node) == -1);
        }
        assertTrue(st.rollback(fserver, fserver));
        assertTrue(ZKUtil.checkExists(fserver.getZooKeeper(), node) == -1);
      }
    } finally {
      if (delete) {
        if (admin.isTableAvailable(tableName)
            && admin.isTableEnabled(tableName)) {
          admin.disableTable(tableName);
          admin.deleteTable(tableName);
        }
      }
    }
  }

  public static class MockedSplitTransaction extends SplitTransaction {

    public MockedSplitTransaction(EntityGroup r, byte[] splitrow)
        throws IOException {
      super(r, splitrow, cluster.getConfiguration());
    }

  }

  private FMaster abortAndWaitForMaster() throws IOException,
      InterruptedException {
    cluster.abortMaster(0);
    cluster.waitOnMaster(0);
    cluster.getConfiguration().setClass(FConstants.MASTER_IMPL, FMaster.class,
        FMaster.class);
    FMaster master = cluster.startMaster().getMaster();
    cluster.waitForActiveAndReadyMaster();
    return master;
  }

  private void split(final EntityGroupInfo egi, final String splitPoint,
      final FServer server, final int entityGroupCount) throws IOException,
      InterruptedException {
    admin.split(egi.getEntityGroupNameAsString(), splitPoint);
    while (server.getOnlineEntityGroups().size() <= entityGroupCount) {
      LOG.debug("Waiting on entityGroup to split");
      Thread.sleep(100);
    }
  }

  @Test
  public void testTableExistsIfTheSpecifiedTableEntityGroupIsSplitParent()
      throws Exception {
    final byte[] tableName = Bytes
        .toBytes("testTableExistsIfTheSpecifiedTableEntityGroupIsSplitParent");
    FServer fServer = null;
    List<EntityGroup> entityGroups = null;
    WaspAdmin admin = new WaspAdmin(TESTING_UTIL.getConfiguration());
    try {
      // Create table then get the single entityGroup for our new table.
      FTable t = TESTING_UTIL.createTable(tableName);
      TESTING_UTIL.waitTableEnabled(tableName, 5000);
      entityGroups = cluster.getEntityGroups(tableName);
      int fserverIndex = cluster.getServerWith(entityGroups.get(0)
          .getEntityGroupName());
      fServer = cluster.getFServer(fserverIndex);
      insertData(tableName, admin, t);
      // Turn off balancer so it doesn't cut in and mess up our placements.
      boolean tableExists = FMetaReader.tableExists(
          TESTING_UTIL.getConfiguration(), Bytes.toString(tableName));
      assertEquals("The specified table should present.", true, tableExists);
      SplitTransaction st = new SplitTransaction(entityGroups.get(0),
          Bytes.toBytes("row2"), TESTING_UTIL.getConfiguration());
      try {
        st.prepare();
        st.createDaughters(fServer, fServer);
      } catch (IOException e) {

      }
      tableExists = FMetaReader.tableExists(TESTING_UTIL.getConfiguration(),
          Bytes.toString(tableName));
      assertEquals("The specified table should present.", true, tableExists);
    } finally {
      admin.close();
    }
  }

  private void insertData(final byte[] tableName, WaspAdmin admin, FTable t)
      throws IOException, InterruptedException {

  }

  private void printOutEntityGroups(final FServer fs, final String prefix)
      throws IOException {
    for (EntityGroup entityGroup : fs.getOnlineEntityGroups()) {
      LOG.info(prefix + entityGroup.getEntityGroupNameAsString());
    }
  }

  private List<EntityGroup> checkAndGetDaughters(byte[] tableName)
      throws InterruptedException {
    List<EntityGroup> daughters = null;
    // try up to 10s
    for (int i = 0; i < 100; i++) {
      daughters = cluster.getEntityGroups(tableName);
      if (daughters.size() >= 2)
        break;
      Thread.sleep(100);
    }
    assertTrue(daughters.size() >= 2);
    return daughters;
  }

  private void waitUntilFServerDead() throws InterruptedException {
    // Wait until the master processes the FS shutdown
    // while (cluster.getMaster().getClusterStatus().getServers().size() ==
    // NB_SERVERS) {
    // LOG.info("Waiting on server to go down");
    // Thread.sleep(100);
    // }
    for (int i = 0; cluster.getMaster().getClusterStatus().getServers().size() == NB_SERVERS
        && i < 100; i++) {
      LOG.info("Waiting on server to go down");
      Thread.sleep(100);
    }
    assertFalse("Waited too long for FS to die", cluster.getMaster()
        .getClusterStatus().getServers().size() == NB_SERVERS);
  }

  private void awaitDaughters(byte[] tableName, int numDaughters)
      throws InterruptedException {
    // Wait till entityGroups are back on line again.
    for (int i = 0; cluster.getEntityGroups(tableName).size() < numDaughters
        && i < 60; i++) {
      LOG.info("Waiting for repair to happen");
      Thread.sleep(1000);
    }
    if (cluster.getEntityGroups(tableName).size() < numDaughters) {
      fail("Waiting too long for daughter entityGroups");
    }
  }
}
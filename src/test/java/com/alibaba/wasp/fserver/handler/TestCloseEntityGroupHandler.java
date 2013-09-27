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
package com.alibaba.wasp.fserver.handler;

import com.alibaba.wasp.DeserializationException;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupTransaction;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.executor.EventHandler.EventType;
import com.alibaba.wasp.fserver.EntityGroup;
import com.alibaba.wasp.fserver.FServerServices;
import com.alibaba.wasp.meta.FMetaTestUtil;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.util.MockFServerServices;
import com.alibaba.wasp.util.MockServer;
import com.alibaba.wasp.zookeeper.ZKAssign;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test of the {@link CloseEntityGroupHandler}.
 */
public class TestCloseEntityGroupHandler {
  static final Log LOG = LogFactory.getLog(TestCloseEntityGroupHandler.class);
  private final static WaspTestingUtility WTU = new WaspTestingUtility();
  private static final FTable TEST_FTD = FMetaTestUtil
      .makeTable("TestCloseEntityGroupHandler");
  private EntityGroupInfo TEST_EGI;
  private int testIndex = 0;

  @BeforeClass
  public static void before() throws Exception {
    WTU.getConfiguration().set(FConstants.REDO_IMPL,
        "com.alibaba.wasp.fserver.redo.MemRedoLog");
    WTU.getHBaseTestingUtility().startMiniZKCluster();
    WTU.getConfiguration().set(FConstants.ZOOKEEPER_QUORUM,
        WTU.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM));
    WTU.getConfiguration().set(FConstants.ZOOKEEPER_CLIENT_PORT,
        WTU.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT));
  }

  @AfterClass
  public static void after() throws IOException {
    WTU.getHBaseTestingUtility().shutdownMiniZKCluster();
  }

  /**
   * Before each test, use a different HRI, so the different tests don't
   * interfere with each other. This allows us to use just a single ZK cluster
   * for the whole suite.
   */
  @Before
  public void setupEGI() {
    TEST_EGI = new EntityGroupInfo(Bytes.toBytes(TEST_FTD.getTableName()),
        Bytes.toBytes(testIndex), Bytes.toBytes(testIndex + 1));
    testIndex++;
  }

  /**
   * Test that if we fail a commit, abort gets set on close.
   *
   *
   * @throws java.io.IOException
   * @throws org.apache.zookeeper.KeeperException.NodeExistsException
   * @throws org.apache.zookeeper.KeeperException
   */
  @Test
  public void testFailedCommitAborts() throws IOException, NodeExistsException,
      KeeperException {
    final Server server = new MockServer(WTU, false);
    final FServerServices rss = new MockFServerServices();
    FTable wtd = TEST_FTD;
    final EntityGroupInfo egi = new EntityGroupInfo(wtd.getTableName(),
        FConstants.EMPTY_END_ROW, FConstants.EMPTY_END_ROW);
    EntityGroup entityGroup = EntityGroup.createEntityGroup(egi,
        WTU.getConfiguration(), wtd, rss);
    try {
      assertNotNull(entityGroup);
      // Spy on the entityGroup so can throw exception when close is called.
      EntityGroup spy = Mockito.spy(entityGroup);
      final boolean abort = false;
      Mockito.when(spy.close(abort)).thenThrow(
          new RuntimeException("Mocked failed close!"));
      rss.addToOnlineEntityGroups(spy);
      assertFalse(server.isStopped());
      CloseEntityGroupHandler handler = new CloseEntityGroupHandler(server,
          rss, egi, false, false, -1, EventType.M_FSERVER_CLOSE_ENTITYGROUP);
      boolean throwable = false;
      try {
        handler.process();
      } catch (Throwable t) {
        throwable = true;
      } finally {
        assertTrue(throwable);
        // Abort calls stop so stopped flag should be set.
        assertTrue(server.isStopped());
      }
    } finally {
      EntityGroup.closeEntityGroup(entityGroup);
    }
  }

  /**
   * Test if close entityGroup can handle ZK closing node version mismatch
   *
   * @throws java.io.IOException
   * @throws org.apache.zookeeper.KeeperException.NodeExistsException
   * @throws org.apache.zookeeper.KeeperException
   * @throws com.alibaba.wasp.DeserializationException
   */
  @Test
  public void testZKClosingNodeVersionMismatch() throws IOException,
      NodeExistsException, KeeperException, DeserializationException {
    final Server server = new MockServer(WTU);
    final MockFServerServices rss = new MockFServerServices();

    FTable ftd = TEST_FTD;
    final EntityGroupInfo egi = TEST_EGI;

    // open a entityGroup first so that it can be closed later
    openEntityGroup(server, rss, ftd, egi);

    int versionOfClosingNode = ZKAssign.createNodeClosing(
        server.getZooKeeper(), egi, server.getServerName());

    CloseEntityGroupHandler handler = new CloseEntityGroupHandler(server, rss,
        egi, false, true, versionOfClosingNode + 1,
        EventType.M_FSERVER_CLOSE_ENTITYGROUP);
    handler.process();

    EntityGroupTransaction rt = EntityGroupTransaction.parseFrom(ZKAssign
        .getData(server.getZooKeeper(), egi.getEncodedName()));
    assertTrue(rt.getEventType().equals(EventType.M_ZK_ENTITYGROUP_CLOSING));
  }

  /**
   * Test if the entityGroup can be closed properly
   *
   * @throws java.io.IOException
   * @throws org.apache.zookeeper.KeeperException.NodeExistsException
   * @throws org.apache.zookeeper.KeeperException
   * @throws com.alibaba.wasp.DeserializationException
   */
  @Test
  public void testCloseEntityGroup() throws IOException, NodeExistsException,
      KeeperException, DeserializationException {
    final Server server = new MockServer(WTU);
    final MockFServerServices rss = new MockFServerServices();
    FTable htd = TEST_FTD;
    EntityGroupInfo egi = TEST_EGI;
    openEntityGroup(server, rss, htd, egi);
    int versionOfClosingNode = ZKAssign.createNodeClosing(
        server.getZooKeeper(), egi, server.getServerName());
    CloseEntityGroupHandler handler = new CloseEntityGroupHandler(server, rss,
        egi, false, true, versionOfClosingNode,
        EventType.M_FSERVER_CLOSE_ENTITYGROUP);
    handler.process();
    EntityGroupTransaction rt = EntityGroupTransaction.parseFrom(ZKAssign
        .getData(server.getZooKeeper(), egi.getEncodedName()));
    assertTrue(rt.getEventType()
        .equals(EventType.FSERVER_ZK_ENTITYGROUP_CLOSED));
  }

  private void openEntityGroup(Server server, FServerServices rss, FTable ftd,
      EntityGroupInfo egi) throws IOException, NodeExistsException,
      KeeperException, DeserializationException {
    // Create it OFFLINE node, which is what Master set before sending OPEN RPC
    ZKAssign.createNodeOffline(server.getZooKeeper(), egi,
        server.getServerName());
    OpenEntityGroupHandler openHandler = new OpenEntityGroupHandler(server,
        rss, egi, ftd);
    openHandler.process();
    // This parse is not used?
    EntityGroupTransaction.parseFrom(ZKAssign.getData(server.getZooKeeper(),
        egi.getEncodedName()));
    // delete the node, which is what Master do after the entityGroup is opened
    ZKAssign.deleteNode(server.getZooKeeper(), egi.getEncodedName(),
        EventType.FSERVER_ZK_ENTITYGROUP_OPENED);
  }
}
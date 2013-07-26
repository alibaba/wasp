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
package com.alibaba.wasp.fserver.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

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
import com.alibaba.wasp.zookeeper.ZKUtil;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test of the {@link OpenEntityGroupHandler}.
 */
public class TestOpenEntityGroupHandler {
  static final Log LOG = LogFactory.getLog(TestOpenEntityGroupHandler.class);
  private final static WaspTestingUtility WTU = new WaspTestingUtility();
  private static FTable TEST_FTD;
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
    TEST_FTD = FMetaTestUtil.makeTable("TestOpenEntityGroupHandler");
  }

  @AfterClass
  public static void after() throws IOException {
    TEST_FTD = null;
    WTU.getHBaseTestingUtility().shutdownMiniZKCluster();
  }

  /**
   * Before each test, use a different EGI, so the different tests don't
   * interfere with each other. This allows us to use just a single ZK cluster
   * for the whole suite.
   */
  @Before
  public void setupEGI() {
    TEST_EGI = new EntityGroupInfo(TEST_FTD.getTableName(),
        Bytes.toBytes(testIndex), Bytes.toBytes(testIndex + 1));
    testIndex++;
  }

  /**
   * Test the open entityGroup handler can deal with its znode being yanked out
   * from under it.
   * 
   * @throws IOException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test
  public void testYankingEntityGroupFromUnderIt() throws IOException,
      NodeExistsException, KeeperException {
    final Server server = new MockServer(WTU);
    final FServerServices rss = new MockFServerServices();

    FTable htd = TEST_FTD;
    final EntityGroupInfo egi = TEST_EGI;
    EntityGroup entityGroup = EntityGroup.createEntityGroup(egi,
        WTU.getConfiguration(), htd, rss);
    assertNotNull(entityGroup);
    try {
      OpenEntityGroupHandler handler = new OpenEntityGroupHandler(server, rss,
          egi, htd) {
        @Override
        EntityGroup openEntityGroup() {
          // Open entityGroup first, then remove znode as though it'd been
          // hijacked.
          EntityGroup entityGroup = super.openEntityGroup();

          // Don't actually open entityGroup BUT remove the znode as though it'd
          // been hijacked on us.
          ZooKeeperWatcher zkw = this.server.getZooKeeper();
          String node = ZKAssign.getNodeName(zkw, egi.getEncodedName());
          try {
            ZKUtil.deleteNodeFailSilent(zkw, node);
          } catch (KeeperException e) {
            throw new RuntimeException("Ugh failed delete of " + node, e);
          }
          return entityGroup;
        }
      };
      // Call process without first creating OFFLINE entityGroup in zk, see if
      // exception or just quiet return (expected).
      handler.process();
      ZKAssign.createNodeOffline(server.getZooKeeper(), egi,
          server.getServerName());
      // Call process again but this time yank the zk znode out from under it
      // post OPENING; again will expect it to come back w/o NPE or exception.
      handler.process();
    } finally {
      EntityGroup.closeEntityGroup(entityGroup);
    }
  }

  @Test
  public void testFailedOpenEntityGroup() throws Exception {
    Server server = new MockServer(WTU);
    FServerServices rsServices = new MockFServerServices();

    // Create it OFFLINE, which is what it expects
    ZKAssign.createNodeOffline(server.getZooKeeper(), TEST_EGI,
        server.getServerName());

    // Create the handler
    OpenEntityGroupHandler handler = new OpenEntityGroupHandler(server,
        rsServices, TEST_EGI, TEST_FTD) {
      @Override
      EntityGroup openEntityGroup() {
        // Fake failure of opening a entityGroup due to an IOE, which is caught
        return null;
      }
    };
    handler.process();

    // Handler should have transitioned it to FAILED_OPEN
    EntityGroupTransaction rt = EntityGroupTransaction.parseFrom(ZKAssign
        .getData(server.getZooKeeper(), TEST_EGI.getEncodedName()));
    assertEquals(EventType.FSERVER_ZK_ENTITYGROUP_FAILED_OPEN,
        rt.getEventType());
  }

  @Test
  public void testFailedUpdateMeta() throws Exception {
    Server server = new MockServer(WTU);
    FServerServices rsServices = new MockFServerServices();

    // Create it OFFLINE, which is what it expects
    ZKAssign.createNodeOffline(server.getZooKeeper(), TEST_EGI,
        server.getServerName());

    // Create the handler
    OpenEntityGroupHandler handler = new OpenEntityGroupHandler(server,
        rsServices, TEST_EGI, TEST_FTD) {
      @Override
      boolean updateMeta(final EntityGroup e) {
        // Fake failure of updating META
        return false;
      }
    };
    handler.process();

    // Handler should have transitioned it to FAILED_OPEN
    EntityGroupTransaction rt = EntityGroupTransaction.parseFrom(ZKAssign
        .getData(server.getZooKeeper(), TEST_EGI.getEncodedName()));
    assertEquals(EventType.FSERVER_ZK_ENTITYGROUP_FAILED_OPEN,
        rt.getEventType());
  }
}
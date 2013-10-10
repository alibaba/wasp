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
package com.alibaba.wasp.master;

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.client.WaspAdmin;
import com.alibaba.wasp.fserver.FServer;
import com.alibaba.wasp.fserver.redo.MemRedoLog;
import com.alibaba.wasp.fserver.redo.Redo;
import com.alibaba.wasp.meta.FMetaEditor;
import com.alibaba.wasp.meta.FMetaTestUtil;
import com.alibaba.wasp.meta.FTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestAssignmentManagerOnCluster {
  private final static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  private final static Configuration conf = TEST_UTIL.getConfiguration();
  private static WaspAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setClass(FConstants.REDO_IMPL,
        MemRedoLog.class, Redo.class);
    TEST_UTIL.startMiniCluster(3);
    admin = TEST_UTIL.getWaspAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This tests entityGroup assignment
   */
  @Test(timeout = 120000)
  public void testAssignEntityGroup() throws Exception {
    String table = "testAssignEntityGroup";
    try {
      FTable desc = FMetaTestUtil.makeTable(table);
      admin.createTable(desc);
      EntityGroupInfo egInfo = new EntityGroupInfo(Bytes.toBytes(desc
          .getTableName()), Bytes.toBytes("A"), Bytes.toBytes("Z"));
      FMetaEditor.addEntityGroupToMeta(conf, egInfo);
      FMaster master = TEST_UTIL.getWaspCluster().getMaster();
      master.assignEntityGroup(egInfo);
      master.getAssignmentManager().waitForAssignment(egInfo);
      ServerName serverName = master.getAssignmentManager()
          .getEntityGroupStates().getFServerOfEntityGroup(egInfo);
      TEST_UTIL.assertEntityGroupOnServer(egInfo, serverName, 200);
    } finally {
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * This tests offlining a entityGroup
   */
  @Test(timeout = 120000)
  public void testOfflineEntityGroup() throws Exception {
    String table = "testOfflineEntityGroup";
    try {
      EntityGroupInfo egInfo = createTableAndGetOneEntityGroup(table);

      EntityGroupStates entityGroupStates = TEST_UTIL.getWaspCluster()
          .getMaster().getAssignmentManager().getEntityGroupStates();
      ServerName serverName = entityGroupStates.getFServerOfEntityGroup(egInfo);
      TEST_UTIL.assertEntityGroupOnServer(egInfo, serverName, 200);
      admin.offline(egInfo.getEntityGroupName());

      long timeoutTime = System.currentTimeMillis() + 800;
      while (true) {
        List<EntityGroupInfo> entityGroups = entityGroupStates
            .getEntityGroupsOfTable(Bytes.toBytes(table));
        if (!entityGroups.contains(egInfo))
          break;
        long now = System.currentTimeMillis();
        if (now > timeoutTime) {
          fail("Failed to offline the entityGroup in time");
          break;
        }
        Thread.sleep(10);
      }
      EntityGroupState entityGroupState = entityGroupStates
          .getEntityGroupState(egInfo);
      assertTrue(entityGroupState.isOffline());
    } finally {
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  /**
   * This tests moving a entityGroup
   */
  @Test(timeout = 120000)
  public void testMoveEntityGroup() throws Exception {
    String table = "testMoveEntityGroup";
    try {
      EntityGroupInfo egInfo = createTableAndGetOneEntityGroup(table);

      EntityGroupStates entityGroupStates = TEST_UTIL.getWaspCluster()
          .getMaster().getAssignmentManager().getEntityGroupStates();
      ServerName serverName = entityGroupStates.getFServerOfEntityGroup(egInfo);
      ServerName destServerName = null;
      for (int i = 0; i < 3; i++) {
        FServer destServer = TEST_UTIL.getWaspCluster().getFServer(i);
        if (!destServer.getServerName().equals(serverName)) {
          destServerName = destServer.getServerName();
          break;
        }
      }
      assertTrue(destServerName != null && !destServerName.equals(serverName));
      TEST_UTIL.getWaspAdmin().move(egInfo.getEncodedNameAsBytes(),
          Bytes.toBytes(destServerName.getServerName()));

      long timeoutTime = System.currentTimeMillis() + 5000;
      while (true) {
        ServerName sn = entityGroupStates.getFServerOfEntityGroup(egInfo);
        if (sn != null && sn.equals(destServerName)) {
          TEST_UTIL.assertEntityGroupOnServer(egInfo, sn, 2000);
          break;
        }
        long now = System.currentTimeMillis();
        if (now > timeoutTime) {
          fail("Failed to move the entityGroup in time");
        }
        entityGroupStates.waitForUpdate(50);
      }

    } finally {
      TEST_UTIL.deleteTable(Bytes.toBytes(table));
    }
  }

  EntityGroupInfo createTableAndGetOneEntityGroup(final String tableName)
      throws IOException, InterruptedException {
    FTable desc = FMetaTestUtil.makeTable(tableName);
    admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 5);

    // wait till the table is assigned
    FMaster master = TEST_UTIL.getWaspCluster().getMaster();
    long timeoutTime = System.currentTimeMillis() + 100;
    while (true) {
      List<EntityGroupInfo> entityGroups = master.getAssignmentManager()
          .getEntityGroupStates()
          .getEntityGroupsOfTable(Bytes.toBytes(tableName));
      if (entityGroups.size() > 3) {
        return entityGroups.get(2);
      }
      long now = System.currentTimeMillis();
      if (now > timeoutTime) {
        fail("Could not find an online entityGroup");
      }
      Thread.sleep(10);
    }
  }
}

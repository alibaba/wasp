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
import com.alibaba.wasp.MiniWaspCluster;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.executor.EventHandler.EventHandlerListener;
import com.alibaba.wasp.executor.EventHandler.EventType;
import com.alibaba.wasp.fserver.redo.MemRedoLog;
import com.alibaba.wasp.fserver.redo.Redo;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FTable;
import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMaster {
  private static final WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestMaster.class);
  private static final byte[] TABLENAME = Bytes.toBytes("TestMaster");

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    TEST_UTIL.getConfiguration().setClass(FConstants.REDO_IMPL,
        MemRedoLog.class, Redo.class);
    // Start a cluster of two fservers.
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMasterOpsWhileSplitting() throws Exception {
    MiniWaspCluster cluster = TEST_UTIL.getWaspCluster();
    FMaster m = cluster.getMaster();

    FTable ft = TEST_UTIL.createTable(TABLENAME);
    assertTrue(m.assignmentManager.getZKTable().isEnabledTable(
        Bytes.toString(TABLENAME)));
    TEST_UTIL.loadTable(ft);

    List<Pair<EntityGroupInfo, ServerName>> tableEntityGroups = FMetaReader
        .getTableEntityGroupsAndLocations(TEST_UTIL.getConfiguration(),
            TABLENAME);
    LOG.info("EntityGroups after load: "
        + Joiner.on(',').join(tableEntityGroups));
    assertEquals(1, tableEntityGroups.size());
    assertArrayEquals(FConstants.EMPTY_START_ROW, tableEntityGroups.get(0)
        .getFirst().getStartKey());
    assertArrayEquals(FConstants.EMPTY_END_ROW, tableEntityGroups.get(0)
        .getFirst().getEndKey());

    // Now trigger a split and stop when the split is in progress
    CountDownLatch split = new CountDownLatch(1);
    CountDownLatch proceed = new CountDownLatch(1);
    EntityGroupSplitListener list = new EntityGroupSplitListener(split, proceed);
    cluster.getMaster().executorService.registerListener(
        EventType.FSERVER_ZK_ENTITYGROUP_SPLIT, list);

    LOG.info("Splitting table");
    TEST_UTIL.getWaspAdmin().split(TABLENAME, Bytes.toBytes("1234"));
    LOG.info("Waiting for split result to be about to open");
    split.await(60, TimeUnit.SECONDS);
    try {
      LOG.info("Making sure we can call getTableEntityGroups while opening");
      tableEntityGroups = FMetaReader.getTableEntityGroupsAndLocations(
          TEST_UTIL.getConfiguration(), TABLENAME, false);

      LOG.info("EntityGroups: " + Joiner.on(',').join(tableEntityGroups));
      // We have three entitygroups because one is split-in-progress
      assertEquals(3, tableEntityGroups.size());
      LOG.info("Making sure we can call getTableEntityGroupClosest while opening");
      Pair<EntityGroupInfo, ServerName> pair = m.getTableEntityGroupForRow(
          TABLENAME, Bytes.toBytes("cde"));
      LOG.info("Result is: " + pair);
      Pair<EntityGroupInfo, ServerName> tableEntityGroupFromName = FMetaReader
          .getEntityGroup(TEST_UTIL.getConfiguration(), pair.getFirst()
              .getEntityGroupName());
      assertEquals(tableEntityGroupFromName.getFirst(), pair.getFirst());
    } finally {
      proceed.countDown();
    }
  }

  static class EntityGroupSplitListener implements EventHandlerListener {
    CountDownLatch split, proceed;

    public EntityGroupSplitListener(CountDownLatch split, CountDownLatch proceed) {
      this.split = split;
      this.proceed = proceed;
    }

    @Override
    public void afterProcess(EventHandler event) {
      if (event.getEventType() != EventType.FSERVER_ZK_ENTITYGROUP_SPLIT) {
        return;
      }
      try {
        split.countDown();
        proceed.await(60, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
      return;
    }

    @Override
    public void beforeProcess(EventHandler event) {
    }
  }
}
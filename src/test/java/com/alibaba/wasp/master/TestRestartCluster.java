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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.ipc.RemoteException;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.fserver.redo.MemRedoLog;
import com.alibaba.wasp.fserver.redo.Redo;
import com.alibaba.wasp.meta.FMetaScanner;
import org.junit.Before;
import org.junit.Test;

public class TestRestartCluster {
  private static final Log LOG = LogFactory.getLog(TestRestartCluster.class);
  private WaspTestingUtility UTIL = new WaspTestingUtility();

  private static final byte[] TABLENAME = Bytes.toBytes("master_transitions");
  private static final byte[][] TABLES = { Bytes.toBytes("restartTableOne"),
      Bytes.toBytes("restartTableTwo"), Bytes.toBytes("restartTableThree") };

  @Before
  public void setUp() throws Exception {
    WaspTestingUtility.adjustLogLevel();
    UTIL.getConfiguration().setClass(FConstants.REDO_IMPL, MemRedoLog.class,
        Redo.class);
  }

  @Test(timeout = 100000)
  public void testRestartClusterAfterKill() throws Exception {
    // start the wasp cluster
    LOG.info("Starting Wasp cluster...");
    UTIL.startMiniCluster(2);
    while (!UTIL.getMiniWaspCluster().getMaster().isInitialized()) {
      Threads.sleep(1);
    }
    try {
      UTIL.createTable(TABLENAME);
      UTIL.waitTableEnabled(TABLENAME, 30000);
      LOG.info("Created a table, waiting for table to be available...");
      UTIL.waitTableAvailable(TABLENAME, 30000);
      LOG.info("Master deleted unassigned entityGroup and started up successfully.");
    } catch (Exception e) {
      throw e;
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  @Test(timeout = 300000)
  public void testClusterRestart() throws Exception {
    UTIL.startMiniCluster(3);
    while (!UTIL.getMiniWaspCluster().getMaster().isInitialized()) {
      Threads.sleep(1);
    }
    LOG.info("\n\nCreating tables");
    for (byte[] TABLE : TABLES) {
      UTIL.createTable(TABLE);
    }
    for (byte[] TABLE : TABLES) {
      UTIL.waitTableAvailable(TABLE, 30000);
    }

    List<EntityGroupInfo> allEntityGroups = FMetaScanner
        .listAllEntityGroups(UTIL.getConfiguration());
    assertEquals(3, allEntityGroups.size());

    LOG.info("\n\nShutting down cluster");
    UTIL.shutdownMiniWaspCluster();

    LOG.info("\n\nSleeping a bit");
    Thread.sleep(2000);

    LOG.info("\n\nStarting cluster the second time");
    UTIL.restartWaspCluster(3);

    // Need to use a new 'Configuration' so we make a new FConnection.
    // Otherwise we're reusing an FConnection that has gone stale because
    // the shutdown of the cluster also called shut of the connection.
    allEntityGroups = FMetaScanner.listAllEntityGroups(new Configuration(UTIL
        .getConfiguration()));
    assertEquals(3, allEntityGroups.size());

    LOG.info("\n\nWaiting for tables to be available");
    for (byte[] TABLE : TABLES) {
      try {
        UTIL.createTable(TABLE);
        assertTrue("Able to create table that should already exist", false);
      } catch (Exception t) {
        if (t instanceof RemoteException) {
          t = ((RemoteException) t).unwrapRemoteException();
        }
        if (t.getClass().getName()
            .equals("com.alibaba.wasp.TableExistsException")) {
          LOG.info("Table already exists as expected");
        } else {
          throw t;
        }
      }
      UTIL.waitTableAvailable(TABLE, 30000);
    }
    UTIL.shutdownMiniCluster();
  }
}
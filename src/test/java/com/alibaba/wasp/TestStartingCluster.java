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
package com.alibaba.wasp;

import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.fserver.redo.MemRedoLog;
import com.alibaba.wasp.fserver.redo.Redo;
import org.junit.Test;

public class TestStartingCluster {
  private static final Log LOG = LogFactory.getLog(TestStartingCluster.class);

  @Test(timeout = 300000)
  public void testStartAndShutdownCluster() throws Exception {
    WaspTestingUtility waspTestingUtility = new WaspTestingUtility();
    waspTestingUtility.getConfiguration().setClass(FConstants.REDO_IMPL,
        MemRedoLog.class, Redo.class);
    try {
      waspTestingUtility.startMiniCluster(1);
      byte[] tableName = Bytes.toBytes("testStartAndShutdownCluster");
      waspTestingUtility.createTable(tableName);
      waspTestingUtility.waitTableEnabled(tableName, 60 * 1000);
      waspTestingUtility.getWaspCluster().getEntityGroups(tableName);
    } catch (Throwable t) {
      LOG.warn("Failed starting mini cluster", t);
      fail("Failed starting cluster");
    } finally {
      waspTestingUtility.shutdownMiniCluster();
    }
    
    try {
      waspTestingUtility.startMiniCluster(1);
      byte[] tableName = Bytes.toBytes("testStartAndShutdownCluster");
      waspTestingUtility.createTable(tableName);
      waspTestingUtility.waitTableEnabled(tableName, 60 * 1000);
      waspTestingUtility.getWaspCluster().getEntityGroups(tableName);
    } catch (Throwable t) {
      LOG.warn("Failed starting mini cluster", t);
      fail("Failed starting cluster");
    } finally {
      waspTestingUtility.shutdownMiniCluster();
    }
  }
}
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
package com.alibaba.wasp.client;

/**
 * Test client load balance.First client connects a random server, then the 
 * client communicates with server by using the connection until session expired.
 */

import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.UnknownSessionException;
import com.alibaba.wasp.WaspTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertNotNull;

public class TestFClientLoadBalance {

  private static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();

  public static final String TABLE_NAME = "TEST_TABLE";

  public static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);

  private static FClient client;

  public static String sessionId;

  public static final int fetchSize = 20;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("wasp.online.schema.update.enable",
        true);
    TEST_UTIL.getConfiguration().setInt("wasp.fserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("wasp.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("wasp.client.retries.number", 6);
    TEST_UTIL.getConfiguration().setBoolean(
        "wasp.master.enabletable.roundrobin", true);
    TEST_UTIL.getConfiguration().setBoolean("wasp.ipc.server.blacklist.enable",
        true);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.createTable(TABLE);

    TEST_UTIL.getWaspAdmin().disableTable(TABLE);
    client = new FClient(TEST_UTIL.getConfiguration());
    client.execute("create index test_index on " + TABLE_NAME + "(column3);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    TEST_UTIL.getWaspAdmin().enableTable(TABLE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testExecute() throws IOException {
    Pair<String, Pair<Boolean, List<ExecuteResult>>> pair = client.execute(
        "select column1 from " + TABLE_NAME + " where column3='2f'",
        ReadModel.SNAPSHOT, fetchSize);
    sessionId = pair.getFirst();
    assertNotNull(pair);
  }

  @Test(expected = UnknownSessionException.class)
  public void testNextWithWrongSession() throws IOException {
    sessionId = "testSessionId";
    client.next(sessionId);
  }

  @Test(expected = RetriesExhaustedException.class)
  public void testExecuteWhenWaspClose() throws IOException {
    TEST_UTIL.shutdownMiniWaspCluster();
    client.execute("select column1 from " + TABLE_NAME + " where column3='2f'",
        ReadModel.SNAPSHOT, fetchSize);
  }
}
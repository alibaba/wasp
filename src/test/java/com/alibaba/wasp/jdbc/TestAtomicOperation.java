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
package com.alibaba.wasp.jdbc;

import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.client.FClient;
import com.alibaba.wasp.fserver.EntityGroup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
@Category(MediumTests.class)
// Starts 100 threads
public class TestAtomicOperation extends TestJdbcBase {
  static final Log LOG = LogFactory.getLog(TestAtomicOperation.class);

  EntityGroup entityGroup = null;

  private static Connection conn;
  private static Statement stat;

  private final static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  private static FClient client;

  // Test names
  static final String TABLE_NAME = "testtable";
  static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("wasp.client.retries.number", 3);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.createTable(TABLE);
    TEST_UTIL.getWaspAdmin().disableTable(TABLE);
    client = new FClient(TEST_UTIL.getConfiguration());
    client.execute("create index test_index on " + TABLE_NAME + "(column3);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    client.execute("create index test_index2 on " + TABLE_NAME + "(column2);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    client.execute("create index test_index3 on " + TABLE_NAME
        + "(column1,column3);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    TEST_UTIL.getWaspAdmin().enableTable(TABLE);

    Class.forName("com.alibaba.wasp.jdbc.Driver");
    conn = getConnection("test", TEST_UTIL.getConfiguration());
    conn.setClientInfo(FConstants.READ_MODEL, ReadModel.CURRENT.name());
    stat = conn.createStatement();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    stat.close();
    conn.close();
  }

  /**
   * Test multi-threaded row mutations.
   */
  @Test
  public void testRowMutationMultiThreads() throws IOException {

    LOG.info("Starting test testRowMutationMultiThreads");

    // create 100 threads, each will alternate between adding and
    // removing a column
    int numThreads = 10;
    int opsPerThread = 50;
    AtomicOperation[] all = new AtomicOperation[numThreads];

    AtomicInteger failures = new AtomicInteger(0);
    // create all threads
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i] = new AtomicOperation(entityGroup, opsPerThread, conn,
            failures) {
          @Override
          public void run() {
            for (int i = 0; i < numOps; i++) {
              try {
                int lines = stmt.executeUpdate("insert into " + TABLE_NAME + " (column1,column2,column3) values(1,1,'wasptest')");
                if (lines != 1) {
                  LOG.debug(r);
                  failures.incrementAndGet();
                  fail();
                }
              } catch (SQLException e) {
                failures.incrementAndGet();
              }
            }
          }
        };
      } catch (SQLException e) {
      }
    }

    // run all threads
    for (int i = 0; i < numThreads; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i].join();
      } catch (InterruptedException e) {
      }
    }
    System.out.println(failures.get());
    assertEquals(opsPerThread * numThreads - 1, failures.get());
  }

  public static class AtomicOperation extends Thread {
    protected final EntityGroup entityGroup;
    protected final int numOps;
    protected final Connection conn;
    protected final AtomicInteger failures;
    protected final Random r = new Random();
    protected final Statement stmt;


    public AtomicOperation(EntityGroup entityGroup, int numOps,
        Connection conn, AtomicInteger failures) throws SQLException {
      this.entityGroup = entityGroup;
      this.numOps = numOps;
      this.conn = conn;
      this.failures = failures;
      this.stmt = conn.createStatement();
    }
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu = new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

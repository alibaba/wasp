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
package com.alibaba.wasp.fserver.redo;

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.meta.FMetaTestUtil;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.StorageTableNameBuilder;
import com.alibaba.wasp.plan.action.Primary;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Test of the {@link RedoLog}.
 */
public class TestRedoLog {
  static final Log LOG = LogFactory.getLog(TestRedoLog.class);
  private final static WaspTestingUtility WTU = new WaspTestingUtility();
  private static final FTable TEST_FTD = FMetaTestUtil.makeTable("TestRedoLog");
  private EntityGroupInfo TEST_EGI;
  private int testIndex = 0;

  @BeforeClass
  public static void before() throws Exception {
    WaspTestingUtility.adjustLogLevel();
    WTU.startMiniCluster(3);
  }

  @AfterClass
  public static void after() throws Exception {
    WTU.shutdownMiniCluster();
  }

  /**
   * Before each test, use a different EGI, so the different tests don't
   * interfere with each other. This allows us to use just a single ZK cluster
   * for the whole suite.
   */
  @Before
  public void setupEGI() {
    TEST_EGI = new EntityGroupInfo(Bytes.toBytes(TEST_FTD.getTableName()),
        Bytes.toBytes(testIndex), Bytes.toBytes(testIndex + 1));
    testIndex++;
  }

  @Test
  public void testInstanceAndInitlize() throws IOException {
    RedoLog redo = new RedoLog(TEST_EGI, WTU.getConfiguration());
    redo.initlize();
    String tTableName = StorageTableNameBuilder
        .buildTransactionTableName(TEST_EGI.getEncodedName());
    try {
      HBaseAdmin admin = new HBaseAdmin(WTU.getConfiguration());
      boolean isAva = admin.isTableAvailable(tTableName);
      Assert.assertTrue(isAva);
      redo.close();
      admin.disableTable(tTableName);
      admin.deleteTable(tTableName);
      admin.close();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testRedoLog() throws IOException {
    try {
      RedoLog redo = new RedoLog(TEST_EGI, WTU.getConfiguration());
      long transactionID = 1000;
      Transaction start = new Transaction(transactionID++);
      boolean f = false;

      Primary p = null;
      // before initlize
      try {
        redo.append(p, start);
      } catch (Exception e) {
        f = true;
      }
      Assert.assertTrue(f);

      f = false;
      
      try {
        WALEdit edit = redo.lastUnCommitedTransaction();
        redo.commit(edit);
      } catch (Exception e) {
        f = true;
      }
      Assert.assertTrue(f);

      // initlize
      redo.initlize();

      WALEdit lastUnCommitedTransaction = redo.peekLastUnCommitedTransaction();
      Assert.assertTrue(lastUnCommitedTransaction == null);
      // lastCommitedTransaction
      WALEdit lastCommitedTransaction = redo.lastCommitedTransaction();
      Assert.assertTrue(lastCommitedTransaction == null);

      // before append, commit it first
      f = redo.commit(new WALEdit(p, start));
      Assert.assertFalse(f);

      // append
      redo.append(p, start);
      
      // lastUnCommitedTransaction
      lastUnCommitedTransaction = redo.peekLastUnCommitedTransaction();
      Assert.assertEquals(start.getTransactionID(), lastUnCommitedTransaction
          .getT().getTransactionID());
      Assert.assertTrue(lastCommitedTransaction == null);
      WALEdit edit = redo.lastUnCommitedTransaction();
      // lastCommitedTransaction
      lastCommitedTransaction = redo.lastCommitedTransaction();
      Assert.assertTrue(lastCommitedTransaction == null);

      f = false;
      // append again
      try {
        // failed
        redo.append(p, start);
      } catch (Exception e) {
        f = true;
      }
      Assert.assertTrue(f);

      // commit
      boolean s = redo.commit(edit);
      Assert.assertTrue(s);
      f = false;
      // commit again, failed
      f = redo.commit(edit);
      Assert.assertFalse(f);

      // lastUnCommitedTransaction
      lastUnCommitedTransaction = redo.peekLastUnCommitedTransaction();
      Assert.assertTrue(lastUnCommitedTransaction == null);
      // lastCommitedTransaction
      lastCommitedTransaction = redo.lastCommitedTransaction();
      Assert.assertEquals(start.getTransactionID(), lastCommitedTransaction
          .getT().getTransactionID());

      // append an old Transaction
      Transaction old = new Transaction(transactionID - 100);
      f = false;
      try {
        redo.append(p, old);
      } catch (Exception e) {
        f = true;
      }
      Assert.assertTrue(f);

      String tTableName = StorageTableNameBuilder
          .buildTransactionTableName(TEST_EGI.getEncodedName());
      try {
        HBaseAdmin admin = new HBaseAdmin(WTU.getConfiguration());
        boolean isAva = admin.isTableAvailable(tTableName);
        Assert.assertTrue(isAva);
        redo.close();
        admin.disableTable(tTableName);
        admin.deleteTable(tTableName);
        admin.close();
      } catch (Exception e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }

}
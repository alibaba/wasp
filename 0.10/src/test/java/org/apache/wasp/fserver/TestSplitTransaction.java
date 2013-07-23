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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.FConstants;
import org.apache.wasp.MetaException;
import org.apache.wasp.Server;
import org.apache.wasp.WaspTestingUtility;
import org.apache.wasp.meta.FMetaEditor;
import org.apache.wasp.meta.FMetaReader;
import org.apache.wasp.meta.FMetaTestUtil;
import org.apache.wasp.meta.FTable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test the {@link org.apache.wasp.fserver.SplitTransaction} class against an
 * EntityGroup (as opposed to running cluster).
 */

public class TestSplitTransaction {
  private static WaspTestingUtility TEST_UTIL;
  private EntityGroup parent;
  private static final byte[] STARTROW = new byte[] { 'a', 'a', 'a' };
  // '{' is next ascii after 'z'.
  private static final byte[] ENDROW = new byte[] { '{', '{', '{' };
  private static final byte[] GOOD_SPLIT_ROW = new byte[] { 'd', 'd', 'd' };
  public static final String TABLE_NAME = "table";

  @BeforeClass
  public static void before() throws Exception {
    TEST_UTIL = new WaspTestingUtility();
    TEST_UTIL.getConfiguration().set(FConstants.REDO_IMPL,
        "org.apache.wasp.fserver.redo.MemRedoLog");
    TEST_UTIL.startMiniCluster(1);
  }

  @Before
  public void setup() throws Exception {
    EntityGroupInfo entityGroupInfo = new EntityGroupInfo(
        Bytes.toBytes(TABLE_NAME), STARTROW, ENDROW);
    this.parent = createEntityGroup(entityGroupInfo);
    TEST_UTIL.getConfiguration().setBoolean("wasp.testing.nocluster", true);
  }

  @AfterClass
  public static void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void teardown() throws Exception {
    if (this.parent != null && !this.parent.isClosed())
      this.parent.close();
    if (FMetaReader.exists(TEST_UTIL.getConfiguration(),
        this.parent.getEntityGroupInfo())) {
      try {
        FMetaEditor.deleteEntityGroup(TEST_UTIL.getConfiguration(),
            this.parent.getEntityGroupInfo());
      } catch (MetaException me) {
        throw new IOException("Failed delete of "
            + this.parent.getEntityGroupNameAsString());
      }
    }
  }

  /**
   * Test straight prepare works. Tries to split on {@link #GOOD_SPLIT_ROW}
   * 
   * @throws java.io.IOException
   */
  @Test
  public void testPrepare() throws IOException {
    prepareGOOD_SPLIT_ROW();
  }

  private SplitTransaction prepareGOOD_SPLIT_ROW() throws IOException {
    SplitTransaction st = new SplitTransaction(this.parent, GOOD_SPLIT_ROW, TEST_UTIL.getConfiguration());
    assertTrue(st.prepare());
    return st;
  }

  /**
   * Pass an unreasonable split row.
   */
  @Test
  public void testPrepareWithBadSplitRow() throws IOException {
    // Pass start row as split key.
    SplitTransaction st = new SplitTransaction(this.parent, STARTROW, TEST_UTIL.getConfiguration());
    assertFalse(st.prepare());
    st = new SplitTransaction(this.parent, FConstants.EMPTY_BYTE_ARRAY, TEST_UTIL.getConfiguration());
    assertFalse(st.prepare());
    st = new SplitTransaction(this.parent, new byte[] { 'A', 'A', 'A' }, TEST_UTIL.getConfiguration());
    assertFalse(st.prepare());
    st = new SplitTransaction(this.parent, ENDROW, TEST_UTIL.getConfiguration());
    assertFalse(st.prepare());
  }

  @Test
  public void testPrepareWithClosedEntityGroup() throws IOException {
    this.parent.close();
    SplitTransaction st = new SplitTransaction(this.parent, GOOD_SPLIT_ROW, TEST_UTIL.getConfiguration());
    assertFalse(st.prepare());
  }

  @Test
  public void testWholesomeSplit() throws IOException {
    // Start transaction.
    SplitTransaction st = prepareGOOD_SPLIT_ROW();

    // Run the execute. Look at what it returns.
    Server mockServer = Mockito.mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    PairOfSameType<EntityGroup> daughters = st.execute(mockServer, null);
    // Do some assertions about execution.
    assertTrue(this.parent.isClosed());

    // Check daughters have correct key span.
    assertTrue(Bytes.equals(this.parent.getStartKey(), daughters.getFirst()
        .getStartKey()));
    assertTrue(Bytes.equals(GOOD_SPLIT_ROW, daughters.getFirst().getEndKey()));
    assertTrue(Bytes
        .equals(daughters.getSecond().getStartKey(), GOOD_SPLIT_ROW));
    assertTrue(Bytes.equals(this.parent.getEndKey(), daughters.getSecond()
        .getEndKey()));
    // Count rows.
    for (EntityGroup entityGroup : daughters) {
      // Open so can count its content.
      EntityGroup openEntityGroup = EntityGroup.openEntityGroup(
          entityGroup.getEntityGroupInfo(), entityGroup.getTableDesc(),
          entityGroup.getConf());
      try {
        countRows(openEntityGroup);
      } finally {
        openEntityGroup.close();
        openEntityGroup.getLog().close();
      }
    }

    // Assert the write lock is no longer held on parent
    assertTrue(!this.parent.lock.writeLock().isHeldByCurrentThread());
  }

  @Test
  public void testRollback() throws IOException {
    int parentRowCount = countRows(this.parent);

    // Start transaction.
    SplitTransaction st = prepareGOOD_SPLIT_ROW();
    SplitTransaction spiedUponSt = spy(st);
    when(
        spiedUponSt.createDaughterEntityGroup(spiedUponSt.getSecondDaughter(),
            null)).thenThrow(new MockedFailedDaughterCreation());
    // Run the execute. Look at what it returns.
    boolean expectedException = false;
    Server mockServer = Mockito.mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    try {
      spiedUponSt.execute(mockServer, null);
    } catch (MockedFailedDaughterCreation e) {
      expectedException = true;
    }
    assertTrue(expectedException);
    // Run rollback
    assertTrue(spiedUponSt.rollback(null, null));

    // Assert I can scan parent.
    int parentRowCount2 = countRows(this.parent);
    assertEquals(parentRowCount, parentRowCount2);

    // Assert rollback cleaned up stuff in .FMETA.
    assertTrue(!FMetaReader.exists(TEST_UTIL.getConfiguration(),
        st.getFirstDaughter()));
    assertTrue(!FMetaReader.exists(TEST_UTIL.getConfiguration(),
        st.getSecondDaughter()));
    assertTrue(!this.parent.lock.writeLock().isHeldByCurrentThread());

    // Now retry the split but do not throw an exception this time.
    assertTrue(st.prepare());
    PairOfSameType<EntityGroup> daughters = st.execute(mockServer, null);
    // Count rows.
    for (EntityGroup entityGroup : daughters) {
      // Open so can count its content.
      EntityGroup openEntityGroup = EntityGroup.openEntityGroup(
          entityGroup.getEntityGroupInfo(), entityGroup.getTableDesc(),
          entityGroup.getConf());
      try {
        countRows(openEntityGroup);
      } finally {
        openEntityGroup.close();
        openEntityGroup.getLog().close();
      }
    }
    // assertEquals(rowcount, daughtersRowCount);
    // Assert the write lock is no longer held on parent
    assertTrue(!this.parent.lock.writeLock().isHeldByCurrentThread());
  }

  /**
   * Exception used in this class only.
   */
  private class MockedFailedDaughterCreation extends IOException {
    private static final long serialVersionUID = -5455992718284501115L;
  }

  private int countRows(final EntityGroup entityGroup) throws IOException {
    int rowcount = 0;
    return rowcount;
  }

  EntityGroup createEntityGroup(final EntityGroupInfo egi) throws IOException {
    FTable table = FMetaTestUtil.makeTable("TestSplitTransaction");
    return EntityGroup.createEntityGroup(egi, TEST_UTIL.getConfiguration(), table, null);
  }
}
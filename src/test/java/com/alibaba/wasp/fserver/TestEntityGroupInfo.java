/**
 * Copyright 2007 The Apache Software Foundation
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
package com.alibaba.wasp.fserver;

import com.alibaba.wasp.meta.FTable;import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.meta.FTable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestEntityGroupInfo {
  @Test
  public void testCreateEntityGroupInfoName() throws Exception {
    String tableName = "tablename";
    final byte[] tn = Bytes.toBytes(tableName);
    String startKey = "startkey";
    final byte[] sk = Bytes.toBytes(startKey);
    String id = "id";

    // old format entityGroup name
    byte[] name = EntityGroupInfo.createEntityGroupName(tn, sk, id, false);
    String nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id, nameStr);

    // new format entityGroup name.
    String md5HashInHex = MD5Hash.getMD5AsHex(name);
    assertEquals(EntityGroupInfo.MD5_HEX_LENGTH, md5HashInHex.length());
    name = EntityGroupInfo.createEntityGroupName(tn, sk, id, true);
    nameStr = Bytes.toString(name);
    assertEquals(tableName + "," + startKey + "," + id + "." + md5HashInHex
        + ".", nameStr);
  }

  @Test
  public void testContainsRange() {
    FTable table = new FTable();
    table.setTableName("testtable");
    EntityGroupInfo egi = new EntityGroupInfo(Bytes.toBytes(table
        .getTableName()), Bytes.toBytes("a"), Bytes.toBytes("g"));
    // Single row range at start of entityGroup
    assertTrue(egi.containsRange(Bytes.toBytes("a"), Bytes.toBytes("a")));
    // Fully contained range
    assertTrue(egi.containsRange(Bytes.toBytes("b"), Bytes.toBytes("c")));
    // Range overlapping start of entityGroup
    assertTrue(egi.containsRange(Bytes.toBytes("a"), Bytes.toBytes("c")));
    // Fully contained single-row range
    assertTrue(egi.containsRange(Bytes.toBytes("c"), Bytes.toBytes("c")));
    // Range that overlaps end key and hence doesn't fit
    assertFalse(egi.containsRange(Bytes.toBytes("a"), Bytes.toBytes("g")));
    // Single row range on end key
    assertFalse(egi.containsRange(Bytes.toBytes("g"), Bytes.toBytes("g")));
    // Single row range entirely outside
    assertFalse(egi.containsRange(Bytes.toBytes("z"), Bytes.toBytes("z")));

    // Degenerate range
    try {
      egi.containsRange(Bytes.toBytes("z"), Bytes.toBytes("a"));
      fail("Invalid range did not throw IAE");
    } catch (IllegalArgumentException iae) {
    }
  }

  @Test
  public void testLastEntityGroupCompare() {
    FTable table = new FTable();
    table.setTableName("testtable");
    EntityGroupInfo egip = new EntityGroupInfo(Bytes.toBytes(table
        .getTableName()), Bytes.toBytes("a"), new byte[0]);
    EntityGroupInfo egic = new EntityGroupInfo(Bytes.toBytes(table
        .getTableName()), Bytes.toBytes("a"), Bytes.toBytes("b"));
    assertTrue(egip.compareTo(egic) > 0);
  }

  @Test
  public void testComparator() {
    byte[] tablename = Bytes.toBytes("comparatorTablename");
    byte[] empty = new byte[0];
    EntityGroupInfo older = new EntityGroupInfo(tablename, empty, empty, false,
        0L);
    EntityGroupInfo newer = new EntityGroupInfo(tablename, empty, empty, false,
        1L);
    assertTrue(older.compareTo(newer) < 0);
    assertTrue(newer.compareTo(older) > 0);
    assertTrue(older.compareTo(older) == 0);
    assertTrue(newer.compareTo(newer) == 0);
  }
}
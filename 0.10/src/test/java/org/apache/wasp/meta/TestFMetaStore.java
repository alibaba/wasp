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

package org.apache.wasp.meta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.wasp.DeserializationException;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.EntityGroupLocation;
import org.apache.wasp.FConstants;
import org.apache.wasp.MetaException;
import org.apache.wasp.ServerName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFMetaStore {

  private static final Log LOG = LogFactory.getLog(TestFMetaStore.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static FMetaStore fMetaService;
  private Configuration conf = HBaseConfiguration.create(TEST_UTIL
      .getConfiguration());
  private String metaTable = conf.get(FConstants.METASTORE_TABLE,
      FConstants.DEFAULT_METASTORE_TABLE);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.info.port", -1);
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", -1);
    TEST_UTIL.startMiniCluster(1);
    Configuration conf = HBaseConfiguration
        .create(TEST_UTIL.getConfiguration());
    FMetaUtil.checkAndInit(conf);
    fMetaService = new FMetaStore(conf);
    LOG.info("Check and Init FMetaStore");
    fMetaService.connect();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCheckAndInit() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    assertTrue(admin.tableExists(metaTable));
    assertTrue(admin.isTableEnabled(metaTable));
    admin.close();
  }

  @Test
  public void testClean() throws IOException {
    boolean success = FMetaUtil.clean(conf);
    assertTrue(success);
    HBaseAdmin admin = new HBaseAdmin(conf);
    assertFalse(admin.tableExists(metaTable));
    admin.close();
    FMetaUtil.checkAndInit(conf);
  }

  @Test
  public void testCreateTable() throws MetaException {
    FTable User = FMetaTestUtil.User;
    FTable Photo = FMetaTestUtil.Photo;
    fMetaService.createTable(User);
    assertTrue(fMetaService.tableExists(User.getTableName()));
    FTable table = fMetaService.getTable(User.getTableName());
    compare(table, User);
    try {
      fMetaService.createTable(User);
      assertTrue(false);
    } catch (MetaException e) {
      assertTrue(true);
    }
    assertTrue(!fMetaService.tableExists(Photo.getTableName()));
    fMetaService.createTable(Photo);
    assertTrue(fMetaService.tableExists(Photo.getTableName()));
    table = fMetaService.getTable(Photo.getTableName());
    compare(table, Photo);
    fMetaService.dropTable(User.getTableName());
    fMetaService.dropTable(Photo.getTableName());
  }

  @Test
  public void testDropTable() throws MetaException {
    FTable User = FMetaTestUtil.User;
    if (fMetaService.tableExists(User.getTableName())) {
      fMetaService.dropTable(User.getTableName());
      assertFalse(fMetaService.tableExists(User.getTableName()));
    } else {
      fMetaService.createTable(User);
      assertTrue(fMetaService.tableExists(User.getTableName()));
      fMetaService.dropTable(User.getTableName());
      assertFalse(fMetaService.tableExists(User.getTableName()));
    }
  }

  @Test
  public void testAlterTable() throws MetaException {
    FTable User = FMetaTestUtil.User;

    if (fMetaService.tableExists(User.getTableName())) {
      fMetaService.dropTable(User.getTableName());
      assertFalse(fMetaService.tableExists(User.getTableName()));
    }
    fMetaService.createTable(User);
    assertTrue(fMetaService.tableExists(User.getTableName()));

    FTable newTable = clone(User);
    newTable.setOwner("ROOT");
    assertFalse(User.getOwner().equals(newTable.getOwner()));

    fMetaService.alterTable(User.getTableName(), newTable);
    FTable newGetTable = fMetaService.getTable(User.getTableName());
    assertFalse(User.getOwner().equals(newGetTable.getOwner()));
    assertTrue(newTable.getOwner().equals(newGetTable.getOwner()));
    fMetaService.dropTable(User.getTableName());
  }

  @Test
  public void testGetAllTables() throws MetaException {
    restoreAndInitFMETA();
    fMetaService.createTable(FMetaTestUtil.User);
    fMetaService.createTable(FMetaTestUtil.Photo);
    List<FTable> allTables = fMetaService.getAllTables();
    assertEquals(allTables.size(), 2);
    fMetaService.dropTable(FMetaTestUtil.User.getTableName());
    fMetaService.dropTable(FMetaTestUtil.Photo.getTableName());
  }

  @Test
  public void testGetChildTables() throws MetaException {
    restoreAndInitFMETA();
    fMetaService.createTable(FMetaTestUtil.User);
    fMetaService.createTable(FMetaTestUtil.Photo);
    List<FTable> childTables = fMetaService.getChildTables(FMetaTestUtil.User
        .getTableName());
    assertEquals(childTables.size(), 1);
    assertEquals(childTables.get(0).getTableName(),
        FMetaTestUtil.Photo.getTableName());
    fMetaService.dropTable(FMetaTestUtil.User.getTableName());
    fMetaService.dropTable(FMetaTestUtil.Photo.getTableName());
  }

  @Test
  public void testIndex() throws MetaException {
    FTable Photo = FMetaTestUtil.Photo;
    restoreAndInitFMETA();
    fMetaService.createTable(FMetaTestUtil.User);
    fMetaService.createTable(FMetaTestUtil.Photo);
    // addIndex
    fMetaService.addIndex(Photo.getTableName(), FMetaTestUtil.PhotosByTime);
    fMetaService.addIndex(Photo.getTableName(), FMetaTestUtil.PhotosByTag);
    // getAllIndex
    LinkedHashMap<String, Index> allIndexs = fMetaService.getAllIndex(Photo
        .getTableName());
    assertEquals(allIndexs.size(), 2);
    // getIndex
    Index PBT = fMetaService.getIndex(Photo.getTableName(),
        FMetaTestUtil.PhotosByTime.getIndexName());
    compare(PBT, FMetaTestUtil.PhotosByTime);
    // getIndex
    Index PBTAG = fMetaService.getIndex(Photo.getTableName(),
        FMetaTestUtil.PhotosByTag.getIndexName());
    compare(PBTAG, FMetaTestUtil.PhotosByTag);
    // deleteIndex
    fMetaService.deleteIndex(Photo.getTableName(), FMetaTestUtil.PhotosByTag);
    allIndexs = fMetaService.getAllIndex(Photo.getTableName());
    assertEquals(allIndexs.size(), 1);
    fMetaService.dropTable(FMetaTestUtil.User.getTableName());
    fMetaService.dropTable(FMetaTestUtil.Photo.getTableName());
  }

  @Test
  public void testAddEntityGroup() throws MetaException {
    FTable User = FMetaTestUtil.User;
    String tableName = User.getTableName();
    FTable Photo = FMetaTestUtil.Photo;
    restoreAndInitFMETA();
    fMetaService.createTable(FMetaTestUtil.User);
    fMetaService.createTable(FMetaTestUtil.Photo);

    List<EntityGroupInfo> egis = getEntityGroup(tableName);
    // add EntityGroup
    fMetaService.addEntityGroup(egis);
    // get EntityGroup
    List<EntityGroupInfo> uEgis = fMetaService.getTableEntityGroups(tableName);
    assertEquals(egis.size(), uEgis.size());

    List<EntityGroupInfo> pEgis = fMetaService.getTableEntityGroups(Photo
        .getTableName());
    assertEquals(pEgis.size(), uEgis.size());

    for (int i = 0; i < pEgis.size(); i++) {
      assertEquals(pEgis.get(i).compareTo(uEgis.get(i)), 0);
    }

    for (EntityGroupInfo egi : uEgis) {
      boolean has = false;
      for (EntityGroupInfo eg : egis) {
        if (egi.equals(eg)) {
          has = true;
          break;
        }
      }
      assertTrue(has);
    }
    // test exists
    assertTrue(fMetaService.exists(egis.get(1)));
    fMetaService.deleteEntityGroup(egis.get(1));
    assertFalse(fMetaService.exists(egis.get(1)));

    // delete one EntityGroup
    fMetaService.deleteEntityGroup(egis.get(1));
    uEgis = fMetaService.getTableEntityGroups(tableName);
    assertEquals(egis.size() - 1, uEgis.size());

    fMetaService.deleteEntityGroups(Photo.getTableName());
    uEgis = fMetaService.getTableEntityGroups(tableName);
    assertEquals(egis.size() - 1, uEgis.size());

    // delete all EntityGroup
    fMetaService.deleteEntityGroups(tableName);
    uEgis = fMetaService.getTableEntityGroups(tableName);
    assertEquals(0, uEgis.size());
    fMetaService.dropTable(FMetaTestUtil.User.getTableName());
    fMetaService.dropTable(FMetaTestUtil.Photo.getTableName());
  }

  @Test
  public void testEntityGroup() throws MetaException, DeserializationException {
    FTable User = FMetaTestUtil.User;
    String tableName = User.getTableName();

    restoreAndInitFMETA();
    fMetaService.createTable(FMetaTestUtil.User);

    List<EntityGroupInfo> egis = getEntityGroup(tableName);
    // add EntityGroup
    fMetaService.addEntityGroup(egis);

    for (int i = 0; i < egis.size(); i++) {
      fMetaService.updateEntityGroupLocation(egis.get(i), egLocations[i]);
    }
    for (int i = 0; i < egis.size(); i++) {
      // getEntityGroupLocation
      ServerName sn = fMetaService.getEntityGroupLocation(egis.get(i));
      assertEquals(sn.compareTo(egLocations[i]), 0);
    }

    for (int i = 0; i < egis.size(); i++) {
      // getEntityGroupsAndLocations
      Pair<EntityGroupInfo, ServerName> pair = fMetaService
          .getEntityGroupAndLocation(egis.get(i).getEntityGroupName());
      assertEquals(pair.getFirst().compareTo(egis.get(i)), 0);
      assertEquals(pair.getSecond().compareTo(egLocations[i]), 0);
    }

    Set<String> disabledTables = new HashSet<String>();
    // fullScan
    Map<EntityGroupInfo, ServerName> entityGroups = fMetaService.fullScan(
        disabledTables, false);
    assertEquals(entityGroups.size(), egis.size());
    for (int i = 0; i < egis.size(); i++) {
      ServerName sn = entityGroups.get(egis.get(i));
      assertEquals(sn.compareTo(egLocations[i]), 0);
    }

    // getOfflineSplitParents
    Map<EntityGroupInfo, Result> offlineSplitParents = fMetaService
        .getOfflineSplitParents();
    assertEquals(offlineSplitParents.size(), 0);

    // getServerUserEntityGroups
    NavigableMap<EntityGroupInfo, Result> egR = fMetaService
        .getServerUserEntityGroups(egLocations[0]);
    assertEquals(1, egR.size());
    {
      Result r = egR.get(egis.get(0));
      byte[] infoValue = r.getValue(FConstants.CATALOG_FAMILY,
          FConstants.EGINFO);
      byte[] locationValue = r.getValue(FConstants.CATALOG_FAMILY,
          FConstants.EGLOCATION);
      EntityGroupInfo key = EntityGroupInfo.parseFromOrNull(infoValue);
      ServerName value = ServerName.convert(locationValue);
      assertEquals(key.compareTo(egis.get(0)), 0);
      assertEquals(value.compareTo(egLocations[0]), 0);
    }

    // getTableEntityGroupsAndLocations
    List<Pair<EntityGroupInfo, ServerName>> entityGroupInfos = fMetaService
        .getTableEntityGroupsAndLocations(Bytes.toBytes(tableName), false);
    entityGroups = new TreeMap<EntityGroupInfo, ServerName>();
    for (Pair<EntityGroupInfo, ServerName> pair : entityGroupInfos) {
      entityGroups.put(pair.getFirst(), pair.getSecond());
    }
    for (int i = 0; i < egis.size(); i++) {
      ServerName sn = entityGroups.get(egis.get(i));
      assertEquals(sn.compareTo(egLocations[i]), 0);
    }
    fMetaService.dropTable(FMetaTestUtil.User.getTableName());
  }

  @Test
  public void testFullScan() throws MetaException, DeserializationException {
    FTable User = FMetaTestUtil.User;
    String tableName = User.getTableName();

    List<EntityGroupInfo> egis = getEntityGroup(tableName);
    Map<EntityGroupInfo, ServerName> entityGroups = null;
    restoreAndInitFMETA();
    fMetaService.createTable(FMetaTestUtil.User);
    fMetaService.createTable(FMetaTestUtil.Photo);
    try {
      // add EntityGroup
      fMetaService.addEntityGroup(egis);

      for (int i = 0; i < egis.size(); i++) {
        fMetaService.updateEntityGroupLocation(egis.get(i), egLocations[i]);
      }

      // fullScan
      List<Result> results = fMetaService.fullScan();
      assertEquals(egis.size(), results.size());
      entityGroups = new TreeMap<EntityGroupInfo, ServerName>();
      for (Result r : results) {
        byte[] infoValue = r.getValue(FConstants.CATALOG_FAMILY,
            FConstants.EGINFO);
        byte[] locationValue = r.getValue(FConstants.CATALOG_FAMILY,
            FConstants.EGLOCATION);
        EntityGroupInfo key = EntityGroupInfo.parseFromOrNull(infoValue);
        ServerName value = ServerName.convert(locationValue);
        entityGroups.put(key, value);
      }
      for (int i = 0; i < egis.size(); i++) {
        ServerName sn = entityGroups.get(egis.get(i));
        assertEquals(sn.compareTo(egLocations[i]), 0);
      }

      // scanEntityGroupLocation
      byte[] row = Bytes.toBytes("20001");
      EntityGroupLocation egLocation = fMetaService.scanEntityGroupLocation(
          Bytes.toBytes(tableName), row);
      assertEquals(egLocation.getHostname(), egLocations[2].getHostname());
      assertEquals(egLocation.getPort(), egLocations[2].getPort());
      assertEquals(egLocation.getEntityGroupInfo().compareTo(egis.get(2)), 0);

      row = Bytes.toBytes(1);
      egLocation = fMetaService.scanEntityGroupLocation(
          Bytes.toBytes(tableName), row);
      assertEquals(egLocation.getHostname(), egLocations[0].getHostname());
      assertEquals(egLocation.getPort(), egLocations[0].getPort());
      assertEquals(egLocation.getEntityGroupInfo().compareTo(egis.get(0)), 0);

      List<EntityGroupLocation> egls = fMetaService
          .getEntityGroupLocations(Bytes.toBytes(tableName));
      assertEquals(egis.size(), egls.size());

      for (EntityGroupLocation egl : egls) {
        EntityGroupInfo eginfo = egl.getEntityGroupInfo();
        for (int i = 0; i < egis.size(); i++) {
          if (eginfo.compareTo(egis.get(i)) == 0) {
            assertEquals(egl.getHostname(), egLocations[i].getHostname());
            assertEquals(egl.getPort(), egLocations[i].getPort());
            break;
          }
        }
      }
    } finally {
      fMetaService.dropTable(FMetaTestUtil.User.getTableName());
      fMetaService.dropTable(FMetaTestUtil.Photo.getTableName());
    }
  }

  @Test
  public void testModifyEntityGroup() throws MetaException {
    FTable User = FMetaTestUtil.User;
    String tableName = User.getTableName();

    List<EntityGroupInfo> egis = getEntityGroup(tableName);
    restoreAndInitFMETA();
    fMetaService.createTable(FMetaTestUtil.User);
    // add EntityGroup
    fMetaService.addEntityGroup(egis);

    for (int i = 0; i < egis.size(); i++) {
      fMetaService.updateEntityGroupLocation(egis.get(i), egLocations[i]);
    }
    EntityGroupInfo egi = egis.get(1);
    assertFalse(egi.isOffline());
    egi.setOffline(true);
    fMetaService.modifyEntityGroupInfo(egi);

    EntityGroupInfo egi2 = fMetaService.getEntityGroupInfo(egi);
    assertEquals(egi.compareTo(egi2), 0);
    fMetaService.dropTable(FMetaTestUtil.User.getTableName());
  }

  @Test
  public void testIsTableAvailable() throws IOException {
    FTable User = FMetaTestUtil.User;
    String tableName = User.getTableName();

    List<EntityGroupInfo> egis = getEntityGroup(tableName);
    restoreAndInitFMETA();
    fMetaService.createTable(FMetaTestUtil.User);
    // add EntityGroup
    fMetaService.addEntityGroup(egis);

    for (int i = 0; i < egis.size(); i++) {
      fMetaService.updateEntityGroupLocation(egis.get(i), egLocations[i]);
    }
    boolean isTableAvailable = fMetaService.isTableAvailable(Bytes
        .toBytes(tableName));
    assertTrue(isTableAvailable);
    fMetaService.dropTable(FMetaTestUtil.User.getTableName());
  }

  public void restoreAndInitFMETA() throws MetaException {
    boolean success = FMetaUtil.clean(conf);
    assertTrue(success);
    FMetaUtil.checkAndInit(conf);
  }

  public static List<EntityGroupInfo> getEntityGroup(String tableName) {
    byte[][] splitKeys = { null, Bytes.toBytes("10000"),
        Bytes.toBytes("20000"), Bytes.toBytes("30000"), Bytes.toBytes("40000"),
        Bytes.toBytes("50000"), Bytes.toBytes("60000"), Bytes.toBytes("70000"),
        Bytes.toBytes("80000"), Bytes.toBytes("90000"), null };
    List<EntityGroupInfo> egis = new ArrayList<EntityGroupInfo>();
    for (int i = 0; i < splitKeys.length - 2; i++) {
      EntityGroupInfo egi = new EntityGroupInfo(Bytes.toBytes(tableName),
          splitKeys[i], splitKeys[i + 1]);
      egis.add(egi);
    }
    return egis;
  }

  ServerName[] egLocations = { new ServerName("host1", 100, 1000),
      new ServerName("host2", 100, 1000), new ServerName("host3", 100, 1000),
      new ServerName("host4", 100, 1000), new ServerName("host5", 100, 1000),
      new ServerName("host6", 100, 1000), new ServerName("host7", 100, 1000),
      new ServerName("host8", 100, 1000), new ServerName("host9", 100, 1000),
      new ServerName("host10", 100, 1000) };

  public static FTable clone(FTable table) {
    FTable clone = new FTable();
    clone.setTableName(table.getTableName());
    clone.setOwner(table.getOwner());
    clone.setCreateTime(table.getCreateTime());
    clone.setLastAccessTime(table.getLastAccessTime());
    clone.setColumns(clone(table.getColumns()));
    clone.setPrimaryKeys(clone(table.getPrimaryKeys()));
    clone.setTableType(table.getTableType());
    clone.setEntityGroupKey(clone(table.getEntityGroupKey()));
    clone.setParentName(table.getParentName());
    return clone;
  }

  public static Field clone(Field field) {
    if (field == null) {
      return null;
    }
    Field clone = new Field();
    clone.setComment(field.getComment());
    clone.setFamily(field.getFamily());
    clone.setKeyWord(field.getKeyWord());
    clone.setName(field.getName());
    clone.setType(field.getType());
    return clone;
  }

  public static Map<String, String> clone(Map<String, String> parameters) {
    Map<String, String> clone = new HashMap<String, String>();
    clone.putAll(parameters);
    return clone;
  }

  public static LinkedHashMap<String, Field> clone(
      LinkedHashMap<String, Field> fields) {
    if (fields == null) {
      return null;
    }

    LinkedHashMap<String, Field> clone = new LinkedHashMap<String, Field>();
    for (Field field : fields.values()) {
      Field cloneField = clone(field);
      clone.put(cloneField.getName(), cloneField);
    }
    return clone;
  }

  public static void compare(FTable left, FTable right) {
    if (compareNull(left, right)) {
      assertTrue(false);
      return;
    }
    if (left != null && right != null) {
      assertTrue(left.getTableName().equals(right.getTableName()));
      assertTrue(left.getOwner().equals(right.getOwner()));
      assertTrue(left.getCreateTime() == right.getCreateTime());
      assertTrue(left.getLastAccessTime() == right.getLastAccessTime());
      assertTrue(left.getTableType().equals(right.getTableType()));
      compare(left.getColumns(), right.getColumns());
      compare(left.getPrimaryKeys(), right.getPrimaryKeys());
      assertTrue(left.getTableType().equals(right.getTableType()));
      compare(left.getEntityGroupKey(), right.getEntityGroupKey());
      compare(left.getParentName(), right.getParentName());
      compare(left.getParameters(), right.getParameters());
    }
  }

  public static void compare(LinkedHashMap<String, Field> left,
      LinkedHashMap<String, Field> right) {
    if (compareNull(left, right)) {
      assertTrue(false);
      return;
    }
    if (left != null && right != null) {
      assertTrue(left.size() == right.size());
      Iterator<Entry<String, Field>> leftIter = left.entrySet().iterator();
      Iterator<Entry<String, Field>> rightIter = right.entrySet().iterator();
      while (leftIter.hasNext()) {
        Field leftField = leftIter.next().getValue();
        Field rightField = rightIter.next().getValue();
        compare(rightField, leftField);
      }
    }
  }

  public static void compare(Field left, Field right) {
    if (compareNull(left, right)) {
      assertTrue(false);
      return;
    }
    if (left != null && right != null) {
      assertTrue(left.getKeyWord().equals(right.getKeyWord()));
      assertTrue(left.getName().equals(right.getName()));
      assertTrue(left.getFamily().equals(right.getFamily()));
      assertTrue(left.getType().equals(right.getType()));
      compare(left.getComment(), right.getComment());
    }
  }

  public static void compare(Map<String, String> left, Map<String, String> right) {
    if (compareNull(left, right)) {
      return;
    }
    if (left != null && right != null) {
      assertTrue(left.size() == right.size());
      Iterator<Map.Entry<String, String>> iter = left.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String, String> entry = (Map.Entry<String, String>) iter
            .next();
        String key = entry.getKey();
        String leftVal = entry.getValue();
        String rightVal = right.get(key);
        compare(leftVal, rightVal);
      }
    }
  }

  public static void compare(String left, String right) {
    if (compareNull(left, right)) {
      return;
    }
    if (left != null && right != null) {
      assertTrue(left.equals(right));
    }
  }

  public static void compare(Index left, Index right) {
    if (compareNull(left, right)) {
      assertTrue(false);
      return;
    }
    if (left != null && right != null) {
      assertTrue(left.getIndexName().equals(right.getIndexName()));
      compare(left.getIndexKeys(), right.getIndexKeys());
      compare(left.getParameters(), right.getParameters());
    }
  }

  public static boolean compareNull(Object left, Object right) {
    if (left == null && right == null) {
      return true;
    }
    if (left == null && right != null) {
      return false;
    }
    if (left != null && right == null) {
      return false;
    }
    return false;
  }
}
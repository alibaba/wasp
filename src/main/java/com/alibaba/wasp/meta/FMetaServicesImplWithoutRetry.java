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
package com.alibaba.wasp.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import com.alibaba.wasp.DeserializationException;import com.alibaba.wasp.EntityGroupLocation;import com.alibaba.wasp.storage.StorageActionManager;import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import com.alibaba.wasp.DeserializationException;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupLocation;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.TableNotFoundException;
import com.alibaba.wasp.meta.FMetaScanner.MetaScannerVisitor;
import com.alibaba.wasp.meta.FMetaScanner.MetaScannerVisitorBase;
import com.alibaba.wasp.storage.StorageActionManager;
import com.alibaba.wasp.storage.StorageTableNotFoundException;

/**
 * implement FMetaServices, we use many scan to get TableInfo and EGinfo Without
 * Retry
 * 
 */
public class FMetaServicesImplWithoutRetry extends FMetaServices {

  public static final Log LOG = LogFactory
      .getLog(FMetaServicesImplWithoutRetry.class);

  protected String metaTable;

  StorageActionManager hbaseActionManager;

  public FMetaServicesImplWithoutRetry() throws MetaException {
  }

  public FMetaServicesImplWithoutRetry(Configuration conf) throws MetaException {
    this.setConf(conf);
    this.connect();
  }

  /**
   * Use HTable to connect to the meta table, will use the HTable to
   * create\drop\alter\get Table and so on.
   */
  @Override
  public boolean connect() {
    try {
      Configuration conf = getConf();
      metaTable = conf.get(FConstants.METASTORE_TABLE,
          FConstants.DEFAULT_METASTORE_TABLE);
      hbaseActionManager = new StorageActionManager(conf);
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  @Override
  public void close() {
    hbaseActionManager.close();
  }

  private HTableInterface getHTable() throws MetaException {
    try {
      return hbaseActionManager.getTable(metaTable);
    } catch (StorageTableNotFoundException e) {
      throw new MetaException(e);
    }
  }

  private void closeHTable(HTableInterface htable) throws MetaException {
    if (htable == null) {
      return;
    }
    try {
      htable.close();
    } catch (IOException e) {
      throw new MetaException(e);
    }
  }

  @Override
  public HTableDescriptor getStorageTableDesc(FTable tbl) {
    String htablename = StorageTableNameBuilder.buildEntityTableName(tbl
        .getTableName());
    HTableDescriptor desc = new HTableDescriptor(htablename);
    Set<String> set = RowBuilder.buildFamilyName(tbl);
    Iterator<String> iterator = set.iterator();
    while (iterator.hasNext()) {
      String fami = iterator.next();
      HColumnDescriptor hcolumn = new HColumnDescriptor(fami);
      hcolumn.setBloomFilterType(BloomType.ROW);
      hcolumn.setCompressionType(Algorithm.GZ);
      desc.addFamily(hcolumn);
    }
    return desc;
  }

  @Override
  public HTableDescriptor getStorageTableDesc(String storageTable)
      throws MetaException {
    try {
      return hbaseActionManager.getStorageTableDesc(storageTable);
    } catch (IOException e) {
      LOG.error("getStorageTableDesc ", e);
      throw new MetaException(e);
    }
  }

  @Override
  public HTableDescriptor[] listStorageTables() throws MetaException {
    try {
      return hbaseActionManager.listStorageTables();
    } catch (IOException e) {
      LOG.error("listStorageTables ", e);
      throw new MetaException(e);
    }
  }

  // create table in HBase
  @Override
  public void createStorageTable(HTableDescriptor desc) throws MetaException {
    try {
      hbaseActionManager.createStorageTable(desc);
    } catch (IOException e) {
      LOG.error("listStorageTables ", e);
      throw new MetaException(e);
    }
  }

  // delete tables in HBase
  @Override
  public void deleteStorageTables(List<HTableDescriptor> deleteEntityTables)
      throws MetaException {
    for (HTableDescriptor htable : deleteEntityTables) {
      try {
        hbaseActionManager.deleteStorageTable(htable.getName());
      } catch (IOException e) {
        LOG.error("deleteStorageTables " + htable.getNameAsString(), e);
      }
    }
  }

  // delete table in HBase
  @Override
  public void deleteStorageTable(String deleteStorageTable)
      throws MetaException {
    try {
      hbaseActionManager.deleteStorageTable(Bytes.toBytes(deleteStorageTable));
    } catch (IOException e) {
      LOG.error("deleteStorageTables " + deleteStorageTable, e);
    }
  }

  // Check table exists in HBase
  @Override
  public boolean storageTableExists(String deleteStorageTable)
      throws MetaException {
    try {
      return hbaseActionManager.storageTableExists(deleteStorageTable);
    } catch (IOException e) {
      LOG.error("storageTableExists ", e);
      throw new MetaException(e);
    }
  }
  
  @Override
  public void createTable(FTable tbl) throws MetaException {
    byte[] rowKey = Bytes.toBytes(FConstants.TABLEROW_PREFIX_STR
        + tbl.getTableName());
    Put put = new Put(rowKey);
    put.add(FConstants.CATALOG_FAMILY, FConstants.TABLEINFO, tbl.toByte());
    boolean a = checkAndPut(FConstants.CATALOG_FAMILY, FConstants.TABLEINFO,
        null, put);
    if (!a) {
      throw new MetaException(tbl.getTableName() + " is already exists.");
    }
  }

  @Override
  public void dropTable(String tableName) throws MetaException {
    byte[] rowKey = Bytes.toBytes(FConstants.TABLEROW_PREFIX_STR + tableName);
    Get get = new Get(rowKey);
    if (!exists(get)) {
      throw new MetaException(tableName + " is not exists.");
    }
    Delete delete = new Delete(rowKey);
    delete(delete);
  }

  @Override
  public void alterTable(String tableName, FTable newFTable)
      throws MetaException {
    byte[] rowKey = Bytes.toBytes(FConstants.TABLEROW_PREFIX_STR + tableName);
    Get get = new Get(rowKey);
    if (!exists(get)) {
      throw new MetaException(tableName + " is not exists.");
    }
    if (tableName.equals(newFTable.getTableName())) {
      Put put = new Put(rowKey);
      put.add(FConstants.CATALOG_FAMILY, FConstants.TABLEINFO,
          newFTable.toByte());
      put(put);
    } else {
      // rename (1) tableName exists (2)newFTable.getTableName() is not exists
      rowKey = Bytes.toBytes(FConstants.TABLEROW_PREFIX_STR
          + newFTable.getTableName());
      get = new Get(rowKey);
      if (exists(get)) {
        throw new MetaException(tableName + " is already exists.");
      }
      // put into a new row
      Put put = new Put(rowKey);
      put.add(FConstants.CATALOG_FAMILY, FConstants.TABLEINFO,
          newFTable.toByte());
      put(put);
      // delete the former row
      rowKey = Bytes.toBytes(FConstants.TABLEROW_PREFIX_STR + tableName);
      Delete delete = new Delete(rowKey);
      delete(delete);
    }
  }

  @Override
  public FTable getTable(String tableName) throws MetaException {
    byte[] rowKey = Bytes.toBytes(FConstants.TABLEROW_PREFIX_STR + tableName);
    return getTableByRow(rowKey);
  }

  public FTable getTable(byte[] tableName) throws MetaException {
    byte[] rowKey = Bytes.add(FConstants.TABLEROW_PREFIX, tableName);
    return getTableByRow(rowKey);
  }

  public FTable getTableByRow(byte[] rowKey) throws MetaException {
    Get get = new Get(rowKey);
    Result rs = get(get);
    byte[] value = rs.getValue(FConstants.CATALOG_FAMILY, FConstants.TABLEINFO);
    FTable ftable = FTable.convert(value);
    if (ftable == null) {
      return null;
    }
    LinkedHashMap<String, Index> indexs = parseIndex(rs);
    ftable.setIndex(indexs);
    return ftable;
  }

  @Override
  public List<FTable> getTables(String regex) throws MetaException {
    List<FTable> allTables = getAllTables();
    Pattern pattern = Pattern.compile(regex);
    List<FTable> matched = new LinkedList<FTable>();
    for (FTable table : allTables) {
      if (pattern.matcher(table.getTableName()).matches()) {
        matched.add(table);
      }
    }
    return matched;
  }

  @Override
  public List<FTable> getAllTables() throws MetaException {
    List<FTable> tables = new LinkedList<FTable>();
    Scan scan = new Scan(FConstants.TABLEROW_PREFIX);
    scan.addFamily(FConstants.CATALOG_FAMILY);
    int rows = getConf().getInt(HConstants.HBASE_META_SCANNER_CACHING,
        HConstants.DEFAULT_HBASE_META_SCANNER_CACHING);
    scan.setCaching(rows);
    FilterList allFilters = new FilterList();
    allFilters.addFilter(new PrefixFilter(FConstants.TABLEROW_PREFIX));
    scan.setFilter(allFilters);
    HTableInterface htable = null;
    try {
      htable = getHTable();
      ResultScanner scanner = htable.getScanner(scan);
      for (Result r = scanner.next(); r != null; r = scanner.next()) {
        byte[] value = r.getValue(FConstants.CATALOG_FAMILY,
            FConstants.TABLEINFO);
        FTable ftable = FTable.convert(value);
        if (ftable == null) {
          continue;
        }
        LinkedHashMap<String, Index> indexs = parseIndex(r);
        ftable.setIndex(indexs);
        tables.add(ftable);
      }
    } catch (IOException e) {
      throw new MetaException(e);
    } finally {
      closeHTable(htable);
    }
    return tables;
  }

  @Override
  public List<FTable> getChildTables(String tableName) throws MetaException {
    List<FTable> allTables = getAllTables();
    List<FTable> childTables = new LinkedList<FTable>();
    for (FTable t : allTables) {
      if (t.getParentName() != null && t.getParentName().equals(tableName)) {
        childTables.add(t);
      }
    }
    return childTables;
  }

  @Override
  public boolean tableExists(String tableName) throws MetaException {
    byte[] rowKey = Bytes.toBytes(FConstants.TABLEROW_PREFIX_STR + tableName);
    Get get = new Get(rowKey);
    return exists(get);
  }

  // ///////////////////////////////////////////////////
  // FTable operation
  // ///////////////////////////////////////////////////

  @Override 
  public void addIndex(String tableName, Index index) throws MetaException {
    byte[] rowKey = Bytes.toBytes(FConstants.TABLEROW_PREFIX_STR + tableName);
    Get get = new Get(rowKey);
    // check if the table exists
    if (!exists(get)) {
      throw new MetaException(tableName + " is not exists.");
    }
    Put put = new Put(rowKey);
    byte[] cq = Bytes.toBytes(FConstants.INDEXQUALIFIER_PREFIX_STR
        + index.getIndexName());
    put.add(FConstants.CATALOG_FAMILY, cq, index.toByte());
    put(put);
  }

  @Override
  public void deleteIndex(String tableName, Index index) throws MetaException {
    byte[] rowKey = Bytes.toBytes(FConstants.TABLEROW_PREFIX_STR + tableName);
    Get get = new Get(rowKey);
    // check if the table exists
    if (!exists(get)) {
      throw new MetaException(tableName + " is not exists.");
    }
    Delete delete = new Delete(rowKey);
    byte[] cq = Bytes.toBytes(FConstants.INDEXQUALIFIER_PREFIX_STR
        + index.getIndexName());
    delete.deleteColumns(FConstants.CATALOG_FAMILY, cq);
    delete(delete);
  }

  @Override
  public LinkedHashMap<String, Index> getAllIndex(String tableName)
      throws MetaException {
    byte[] rowKey = Bytes.toBytes(FConstants.TABLEROW_PREFIX_STR + tableName);
    Get get = new Get(rowKey);
    get.addFamily(FConstants.CATALOG_FAMILY);
    // check if the table exists
    if (!exists(get)) {
      throw new MetaException(tableName + " is not exists.");
    }
    Result rs = get(get);
    return parseIndex(rs);
  }

  public LinkedHashMap<String, Index> parseIndex(Result rs) {
    LinkedHashMap<String, Index> indexs = new LinkedHashMap<String, Index>();
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> familyMap = rs
        .getNoVersionMap();

    if (familyMap == null) {
      return indexs;
    }
    NavigableMap<byte[], byte[]> kvs = familyMap.get(FConstants.CATALOG_FAMILY);
    for (Map.Entry<byte[], byte[]> kv : kvs.entrySet()) {
      byte[] cq = kv.getKey();
      byte[] value = kv.getValue();
      if (Bytes.startsWith(cq, FConstants.INDEXQUALIFIER_PREFIX)) {
        Index index = Index.convert(value);
        indexs.put(index.getIndexName(), index);
      }
    }
    return indexs;
  }

  @Override
  public Index getIndex(String tableName, String indexName)
      throws MetaException {
    byte[] rowKey = Bytes.toBytes(FConstants.TABLEROW_PREFIX_STR + tableName);
    Get get = new Get(rowKey);
    // check if the table exists
    if (!exists(get)) {
      throw new MetaException(tableName + " is not exists.");
    }
    byte[] cq = Bytes.toBytes(FConstants.INDEXQUALIFIER_PREFIX_STR + indexName);
    get.addColumn(FConstants.CATALOG_FAMILY, cq);
    Result rs = get(get);
    byte[] value = rs.getValue(FConstants.CATALOG_FAMILY, cq);
    return Index.convert(value);
  }

  public static byte[] getRowKey(EntityGroupInfo entityGroupInfo) {
    return entityGroupInfo.getEntityGroupName();
  }

  // ///////////////////////////////////////////////////
  // EntityGroup operation
  // ///////////////////////////////////////////////////
  @Override
  public void addEntityGroup(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    byte[] rowKey = getRowKey(entityGroupInfo);
    Put put = new Put(rowKey);
    put.add(FConstants.CATALOG_FAMILY, FConstants.EGINFO,
        entityGroupInfo.toByte());
    put(put);
  }

  @Override
  public void addEntityGroup(List<EntityGroupInfo> entityGroupInfos)
      throws MetaException {
    List<Put> allPut = new LinkedList<Put>();
    for (EntityGroupInfo egi : entityGroupInfos) {
      byte[] rowKey = getRowKey(egi);
      LOG.debug(" Put rowKey : " + Bytes.toString(rowKey));
      Put put = new Put(rowKey);
      put.add(FConstants.CATALOG_FAMILY, FConstants.EGINFO, egi.toByte());
      allPut.add(put);
    }
    put(allPut);
  }

  @Override
  public boolean exists(EntityGroupInfo entityGroupInfo) throws MetaException {
    byte[] rowKey = getRowKey(entityGroupInfo);
    Get get = new Get(rowKey);
    return exists(get);
  }

  @Override
  public void deleteEntityGroup(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    byte[] rowKey = getRowKey(entityGroupInfo);
    Delete delete = new Delete(rowKey);
    delete(delete);
  }

  @Override
  public void deleteEntityGroups(String tableName) throws MetaException {
    // parent and child share EntityGroup, we just delete EntityGroup for root
    // table
    FTable table = getTable(tableName);
    if (table.isChildTable()) {
      LOG.info("Delete a child's EntityGroups, do nothing " + tableName);
      return;
    }
    List<EntityGroupInfo> entityGroupInfos = getTableEntityGroups(tableName);
    List<Delete> allDelete = new LinkedList<Delete>();
    for (EntityGroupInfo egi : entityGroupInfos) {
      byte[] rowKey = getRowKey(egi);
      Delete delete = new Delete(rowKey);
      allDelete.add(delete);
    }
    if (allDelete != null && allDelete.size() > 0) {
      delete(allDelete);
    }
  }

  public FTable getRootTable(String tableName) throws MetaException {
    return getRootTable(Bytes.toBytes(tableName));
  }

  public FTable getRootTable(byte[] tableName) throws MetaException {
    FTable table = getTable(tableName);
    if (table.isRootTable()) {
      return table;
    } else {
      return getTable(table.getParentName());
    }
  }

  public List<EntityGroupInfo> getTableEntityGroups(String tableName,
      boolean root) throws MetaException {
    if (root) {
      return getTableEntityGroups(Bytes.toBytes(tableName));
    } else {
      FTable table = getTable(tableName);
      return getTableEntityGroups(Bytes.toBytes(table.getParentName()));
    }
  }

  @Override
  public List<EntityGroupInfo> getTableEntityGroups(String tableName)
      throws MetaException {
    FTable table = getTable(tableName);
    if (table.isChildTable()) {
      return getTableEntityGroups(Bytes.toBytes(table.getParentName()));
    } else if (table.isRootTable()) {
      return getTableEntityGroups(Bytes.toBytes(tableName));
    }
    return null;
  }

  @Override
  public List<EntityGroupInfo> getTableEntityGroups(final byte[] tableByte)
      throws MetaException {
    final List<EntityGroupInfo> entityGroupInfos = new LinkedList<EntityGroupInfo>();
    final byte[] startKey = tableByte;
    FilterList allFilters = new FilterList();
    allFilters.addFilter(new PrefixFilter(tableByte));

    FMetaVisitor visitor = new FMetaVisitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r == null || r.isEmpty()) {
          return true;
        }
        byte[] value = r.getValue(FConstants.CATALOG_FAMILY, FConstants.EGINFO);
        EntityGroupInfo eginfo = EntityGroupInfo.parseFromOrNull(value);
        if (eginfo == null) {
          return true;
        }
        if (!Bytes.equals(eginfo.getTableName(), tableByte)) {
          // this is another table, we can exit search.
          return false;
        }
        entityGroupInfos.add(eginfo);
        // Returning true means "keep scanning"
        return true;
      }
    };
    fullScan(visitor, startKey, null, allFilters);
    return entityGroupInfos;
  }

  @Override
  public void modifyEntityGroupInfo(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    addEntityGroup(entityGroupInfo);
  }

  @Override
  public EntityGroupInfo getEntityGroupInfo(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    byte[] rowKey = getRowKey(entityGroupInfo);
    Get get = new Get(rowKey);
    get.addColumn(FConstants.CATALOG_FAMILY, FConstants.EGINFO);
    Result rs = get(get);
    byte[] value = rs.getValue(FConstants.CATALOG_FAMILY, FConstants.EGINFO);
    return EntityGroupInfo.parseFromOrNull(value);
  }

  @Override
  public ServerName getEntityGroupLocation(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    byte[] rowKey = getRowKey(entityGroupInfo);
    Get get = new Get(rowKey);
    get.addColumn(FConstants.CATALOG_FAMILY, FConstants.EGLOCATION);
    Result rs = get(get);
    byte[] value = rs
        .getValue(FConstants.CATALOG_FAMILY, FConstants.EGLOCATION);
    try {
      return ServerName.convert(value);
    } catch (DeserializationException de) {
      throw new MetaException(de);
    }
  }

  @Override
  public Pair<EntityGroupInfo, ServerName> getEntityGroupAndLocation(
      byte[] entityGroupName) throws MetaException {
    Get get = new Get(entityGroupName);
    get.addColumn(FConstants.CATALOG_FAMILY, FConstants.EGINFO);
    get.addColumn(FConstants.CATALOG_FAMILY, FConstants.EGLOCATION);
    Result rs = get(get);
    byte[] infoValue = rs
        .getValue(FConstants.CATALOG_FAMILY, FConstants.EGINFO);
    byte[] locationValue = rs.getValue(FConstants.CATALOG_FAMILY,
        FConstants.EGLOCATION);
    try {
      return new Pair<EntityGroupInfo, ServerName>(
          EntityGroupInfo.parseFromOrNull(infoValue),
          ServerName.convert(locationValue));
    } catch (DeserializationException de) {
      throw new MetaException(de);
    }
  }

  @Override
  public Map<EntityGroupInfo, ServerName> fullScan(
      final Set<String> disabledTables,
      final boolean excludeOfflinedSplitParents) throws MetaException {
    final Map<EntityGroupInfo, ServerName> entityGroups = new TreeMap<EntityGroupInfo, ServerName>();
    FMetaVisitor v = new FMetaVisitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        try {
          if (r == null || r.isEmpty()) {
            return true;
          }
          byte[] infoValue = r.getValue(FConstants.CATALOG_FAMILY,
              FConstants.EGINFO);
          byte[] locationValue = r.getValue(FConstants.CATALOG_FAMILY,
              FConstants.EGLOCATION);
          Pair<EntityGroupInfo, ServerName> eginfoSN = new Pair<EntityGroupInfo, ServerName>(
              EntityGroupInfo.parseFromOrNull(infoValue),
              ServerName.convert(locationValue));
          EntityGroupInfo eginfo = eginfoSN.getFirst();
          if (eginfo == null) {
            return true;
          }
          if (eginfo.getTableNameAsString() == null) {
            return true;
          }
          if (disabledTables.contains(eginfo.getTableNameAsString())) {
            return true;
          }
          // Are we to include split parents in the list?
          if (excludeOfflinedSplitParents && eginfo.isSplitParent()) {
            return true;
          }
          entityGroups.put(eginfo, eginfoSN.getSecond());
          return true;
        } catch (DeserializationException de) {
          LOG.warn("Failed parse " + r, de);
          return true;
        }
      }
    };
    fullScan(v);
    return entityGroups;
  }

  @Override
  public List<EntityGroupInfo> getAllEntityGroupInfos() throws MetaException {
    final List<EntityGroupInfo> entityGroups = new ArrayList<EntityGroupInfo>();
    FMetaVisitor v = new FMetaVisitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r == null || r.isEmpty()) {
          return true;
        }
        EntityGroupInfo eginfo = EntityGroupInfo.getEntityGroupInfo(r);
        if (eginfo == null) {
          return true;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("EntityGroupInfo : " + eginfo.toString());
        }
        entityGroups.add(eginfo);
        return true;
      }
    };
    fullScan(v);
    return entityGroups;
  }

  @Override
  public List<Result> fullScan() throws MetaException {
    CollectAllVisitor v = new CollectAllVisitor();
    fullScan(v);
    return v.getResults();
  }

  @Override
  public Map<EntityGroupInfo, Result> getOfflineSplitParents()
      throws MetaException {
    final Map<EntityGroupInfo, Result> offlineSplitParents = new HashMap<EntityGroupInfo, Result>();
    // This visitor collects offline split parents in the .FMETA. table
    FMetaVisitor visitor = new FMetaVisitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r == null || r.isEmpty()) {
          return true;
        }
        EntityGroupInfo info = EntityGroupInfo.getEntityGroupInfo(r);
        if (info == null) {
          return true; // Keep scanning
        }
        if (info.isOffline() && info.isSplit()) {
          offlineSplitParents.put(info, r);
        }
        // Returning true means "keep scanning"
        return true;
      }
    };
    // Run full scan of .FMETA. catalog table passing in our custom visitor
    fullScan(visitor);
    return offlineSplitParents;
  }

  @Override
  public NavigableMap<EntityGroupInfo, Result> getServerUserEntityGroups(
      final ServerName serverName) throws MetaException {
    final NavigableMap<EntityGroupInfo, Result> egis = new TreeMap<EntityGroupInfo, Result>();
    // Fill the above egis map with entries from .FMETA. that have the passed
    // servername.
    CollectingVisitor<Result> v = new CollectingVisitor<Result>() {
      @Override
      void add(Result r) {
        if (r == null || r.isEmpty())
          return;
        ServerName sn = ServerName.getServerName(r);
        if (sn != null && sn.equals(serverName))
          this.results.add(r);
      }
    };
    fullScan(v);
    List<Result> results = v.getResults();
    if (results != null && !results.isEmpty()) {
      // Convert results to Map keyed by HRI
      for (Result r : results) {
        Pair<EntityGroupInfo, ServerName> p = EntityGroupInfo
            .getEntityGroupInfoAndServerName(r);
        if (p != null && p.getFirst() != null)
          egis.put(p.getFirst(), r);
      }
    }
    return egis;
  }

  @Override
  public void fullScan(final FMetaVisitor visitor) throws MetaException {
    fullScan(visitor, null, null);
  }

  @Override
  public void fullScan(final FMetaVisitor visitor, final byte[] startrow,
      final byte[] endrow) throws MetaException {
    fullScan(visitor, startrow, endrow, null);
  }

  @Override
  public void fullScan(final FMetaVisitor visitor, final byte[] startrow,
      final byte[] endrow, FilterList allFilters) throws MetaException {
    Scan scan = new Scan();
    if (startrow != null) {
      scan.setStartRow(startrow);
    }
    if (endrow != null) {
      scan.setStopRow(endrow);
    }
    int caching = getConf().getInt(HConstants.HBASE_META_SCANNER_CACHING,
        HConstants.DEFAULT_HBASE_META_SCANNER_CACHING);
    scan.setCaching(caching);
    if (allFilters != null) {
      scan.setFilter(allFilters);
    }
    scan.addFamily(FConstants.CATALOG_FAMILY);
    HTableInterface metaTable = getHTable();
    try {
      ResultScanner scanner = metaTable.getScanner(scan);
      try {
        Result data;
        while ((data = scanner.next()) != null) {
          if (data.isEmpty()) {
            continue;
          }
          if (Bytes.startsWith(data.getRow(), FConstants.TABLEROW_PREFIX)) {
            continue;
          }
          // Break if visit returns false.
          if (!visitor.visit(data))
            break;
        }
      } finally {
        scanner.close();
        metaTable.close();
      }
    } catch (IOException e) {
      throw new MetaException(e);
    }
    return;
  }

  @Override
  public void updateEntityGroupLocation(EntityGroupInfo entityGroupInfo,
      ServerName sn) throws MetaException {
    byte[] rowKey = getRowKey(entityGroupInfo);
    Put put = new Put(rowKey);
    addLocation(put, sn);
    put(put);
  }

  @Override
  @SuppressWarnings("deprecation")
  public EntityGroupLocation scanEntityGroupLocation(final byte[] tableName,
      final byte[] row) throws MetaException {
    FTable root = getRootTable(tableName);
    final byte[] rootTableName = Bytes.toBytes(root.getTableName());

    byte[] metaKey = EntityGroupInfo.createEntityGroupName(rootTableName, row,
        FConstants.NINES, false);
    try {
      Result r = this.getHTable().getRowOrBefore(metaKey,
          FConstants.CATALOG_FAMILY);
      String rowString = Bytes.toString(r.getRow());
      if (!rowString.startsWith(Bytes.toString(rootTableName))) {
        final List<EntityGroupLocation> results = new ArrayList<EntityGroupLocation>();
        FMetaVisitor visitor = new FMetaVisitor() {
          @Override
          public boolean visit(Result r) throws IOException {
            if (r == null || r.isEmpty()) {
              return true;
            }
            EntityGroupInfo info = EntityGroupInfo.getEntityGroupInfo(r);
            if (info == null) {
              return true; // Keep scanning
            }
            if (info.isOffline()) {
              return true; // Keep scanning
            }
            if (info.isSplit()) {
              return true; // Keep scanning
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("EntityGroupInfo : " + info.toString());
            }
            if (Bytes.equals(info.getTableName(), rootTableName)) {
              // find it, so end search
              ServerName sn = ServerName.getServerName(r);
              EntityGroupLocation egLoc = new EntityGroupLocation(info,
                  sn.getHostname(), sn.getPort());
              results.add(egLoc);
              return false;
            }
            return true;
          }
        };
        // Run full scan of _FMETA_ catalog table passing in our custom visitor
        fullScan(visitor, rootTableName, null);
        return results.size() == 0 ? null : results.get(0);
      } else {
        EntityGroupInfo info = EntityGroupInfo.getEntityGroupInfo(r);
        if (info == null) {
          throw new TableNotFoundException(Bytes.toString(tableName));
        }
        ServerName sn = ServerName.getServerName(r);
        EntityGroupLocation egLoc = new EntityGroupLocation(info,
            sn.getHostname(), sn.getPort());
        return egLoc;
      }
    } catch (IOException e) {
      throw new MetaException(e);
    }
  }

  @Override
  public List<EntityGroupLocation> getEntityGroupLocations(
      final byte[] tableName) throws MetaException {
    final List<EntityGroupLocation> egLocations = new LinkedList<EntityGroupLocation>();
    final byte[] startKey = tableName;
    FilterList allFilters = new FilterList();
    allFilters.addFilter(new PrefixFilter(tableName));

    FMetaVisitor visitor = new FMetaVisitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        if (r == null || r.isEmpty()) {
          return true;
        }
        byte[] value = r.getValue(FConstants.CATALOG_FAMILY, FConstants.EGINFO);
        EntityGroupInfo eginfo = EntityGroupInfo.parseFromOrNull(value);
        if (eginfo == null) {
          return true;
        }
        if (!Bytes.equals(eginfo.getTableName(), tableName)) {
          // this is another table, we can exit search.
          return false;
        }
        ServerName sn = ServerName.getServerName(r);
        EntityGroupLocation egLoc = new EntityGroupLocation(eginfo,
            sn.getHostname(), sn.getPort());
        egLocations.add(egLoc);
        // Returning true means "keep scanning"
        return true;
      }
    };
    fullScan(visitor, startKey, null, allFilters);
    return egLocations;
  }

  /**
   * @param tableName
   * @return Return list of EntityGroupInfos and server addresses.
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  public List<Pair<EntityGroupInfo, ServerName>> getTableEntityGroupsAndLocations(
      final byte[] tableName, final boolean excludeOfflinedSplitParents)
      throws MetaException {
    byte[] startrow = tableName;
    FilterList allFilters = new FilterList();
    allFilters.addFilter(new PrefixFilter(tableName));

    final List<Pair<EntityGroupInfo, ServerName>> entityGroupInfos = new ArrayList<Pair<EntityGroupInfo, ServerName>>();
    FMetaVisitor visitor = new FMetaVisitor() {
      @Override
      public boolean visit(Result r) throws IOException {
        byte[] value = r.getValue(FConstants.CATALOG_FAMILY, FConstants.EGINFO);
        EntityGroupInfo egi = EntityGroupInfo.parseFromOrNull(value);
        if (egi == null) {
          LOG.warn("No serialized EntityGroupInfo in " + r);
          return true; // Keep scanning
        }
        if (excludeOfflinedSplitParents && egi.isSplitParent()) {
          return true; // Keep scanning
        }
        if (!Bytes.equals(egi.getTableName(), tableName)) {
          // this is another table, we can exit search.
          return false;
        }
        ServerName sn = ServerName.getServerName(r);
        entityGroupInfos.add(new Pair<EntityGroupInfo, ServerName>(egi, sn));
        // Returning true means "keep scanning"
        return true;
      }
    };
    // Run full scan of .FMETA. catalog table passing in our custom visitor
    fullScan(visitor, startrow, null, allFilters);
    return entityGroupInfos;
  }

  public void offlineParentInMeta(EntityGroupInfo parent,
      final EntityGroupInfo a, final EntityGroupInfo b) throws MetaException {
    EntityGroupInfo copyOfParent = new EntityGroupInfo(parent);
    copyOfParent.setOffline(true);
    copyOfParent.setSplit(true);
    addEntityGroupToMeta(copyOfParent, a, b);
    LOG.info("Offlined parent entityGroup "
        + parent.getEntityGroupNameAsString() + " in META");
  }

  /**
   * Adds a (single) META row for the specified new entityGroup and its
   * daughters. Note that this does not add its daughter's as different rows,
   * but adds information about the daughters in the same row as the parent. Use
   * {@link #offlineParentInMeta(EntityGroupInfo, EntityGroupInfo, EntityGroupInfo)}
   * and {@link #addDaughter(EntityGroupInfo, ServerName)} if you want to do
   * that.
   * 
   * @param entityGroupInfo
   *          EntityGroupInfo information
   * @param splitA
   *          first split daughter of the parent EntityGroupInfo
   * @param splitB
   *          second split daughter of the parent EntityGroupInfo
   * @throws IOException
   *           if problem connecting or updating meta
   */
  public void addEntityGroupToMeta(EntityGroupInfo entityGroupInfo,
      EntityGroupInfo splitA, EntityGroupInfo splitB) throws MetaException {
    Put put = makePutFromEntityGroupInfo(entityGroupInfo);
    addDaughtersToPut(put, splitA, splitB);
    put(put);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Added entityGroup "
          + entityGroupInfo.getEntityGroupNameAsString() + " to META");
    }
  }

  /**
   * @param entityGroupInfo
   * @param sn
   * @throws MetaException
   */
  @Override
  public void addDaughter(final EntityGroupInfo entityGroupInfo,
      final ServerName sn) throws MetaException {
    Put put = new Put(getRowKey(entityGroupInfo));
    addEntityGroupInfo(put, entityGroupInfo);
    if (sn != null)
      addLocation(put, sn);
    put(put);
    LOG.info("Added daughter "
        + entityGroupInfo.getEntityGroupNameAsString()
        + (sn == null ? ", entityGroupLocation=null" : ", entityGroupLocation="
            + sn.toString()));
  }

  /**
   * Adds split daughters to the Put
   */
  public Put addDaughtersToPut(Put put, EntityGroupInfo splitA,
      EntityGroupInfo splitB) {
    if (splitA != null) {
      put.add(FConstants.CATALOG_FAMILY, FConstants.SPLITA_QUALIFIER,
          splitA.toByte());
    }
    if (splitB != null) {
      put.add(FConstants.CATALOG_FAMILY, FConstants.SPLITB_QUALIFIER,
          splitB.toByte());
    }
    return put;
  }

  /**
   * Deletes daughters references in offlined split parent.
   * 
   * @param parent
   *          Parent row we're to remove daughter reference from
   * @throws MetaException
   */
  public void deleteDaughtersReferencesInParent(final EntityGroupInfo parent)
      throws MetaException {
    Delete delete = new Delete(getRowKey(parent));
    delete
        .deleteColumns(FConstants.CATALOG_FAMILY, FConstants.SPLITA_QUALIFIER);
    delete
        .deleteColumns(FConstants.CATALOG_FAMILY, FConstants.SPLITB_QUALIFIER);
    delete(delete);
    LOG.info("Deleted daughters references, qualifier="
        + Bytes.toStringBinary(FConstants.SPLITA_QUALIFIER) + " and qualifier="
        + Bytes.toStringBinary(FConstants.SPLITB_QUALIFIER) + ", from parent "
        + parent.getEntityGroupNameAsString());
  }

  /**
   * Generates and returns a Put containing the EntityGroupInfo into for the
   * catalog table
   */
  public static Put makePutFromEntityGroupInfo(EntityGroupInfo entityGroupInfo) {
    Put put = new Put(getRowKey(entityGroupInfo));
    addEntityGroupInfo(put, entityGroupInfo);
    return put;
  }

  private static Put addEntityGroupInfo(final Put p,
      final EntityGroupInfo entityGroupInfo) {
    p.add(FConstants.CATALOG_FAMILY, FConstants.EGINFO,
        entityGroupInfo.toByte());
    return p;
  }

  private static Put addLocation(final Put p, final ServerName sn) {
    p.add(FConstants.CATALOG_FAMILY, FConstants.EGLOCATION, sn.toByte());
    return p;
  }

  private void put(final Put p) throws MetaException {
    HTableInterface meta = getHTable();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("FMeta Put " + p);
      }
      meta.put(p);
    } catch (IOException e) {
      throw new MetaException(e);
    } finally {
      closeHTable(meta);
    }
  }

  private boolean checkAndPut(final byte[] family, final byte[] qualifier,
      final byte[] value, final Put p) throws MetaException {
    HTableInterface meta = getHTable();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("FMeta checkAndPut " + p);
      }
      return meta.checkAndPut(p.getRow(), family, qualifier, value, p);
    } catch (IOException e) {
      throw new MetaException(e);
    } finally {
      closeHTable(meta);
    }
  }

  private void put(final List<Put> p) throws MetaException {
    HTableInterface meta = getHTable();
    try {
      meta.put(p);
      if (LOG.isDebugEnabled()) {
        LOG.debug("FMeta Put " + p);
      }
    } catch (IOException e) {
      throw new MetaException(e);
    } finally {
      closeHTable(meta);
    }
  }

  private Result get(final Get get) throws MetaException {
    HTableInterface meta = getHTable();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("FMeta Get " + get);
      }
      return meta.get(get);
    } catch (IOException e) {
      throw new MetaException(e);
    } finally {
      closeHTable(meta);
    }
  }

  private void delete(final Delete delete) throws MetaException {
    HTableInterface meta = getHTable();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("FMeta Delete " + delete);
      }
      meta.delete(delete);
    } catch (IOException e) {
      throw new MetaException(e);
    } finally {
      closeHTable(meta);
    }
  }

  private void delete(final List<Delete> allDelete) throws MetaException {
    HTableInterface meta = getHTable();
    try {
      meta.delete(allDelete);
      if (LOG.isDebugEnabled()) {
        LOG.debug("FMeta Delete " + allDelete);
      }
    } catch (IOException e) {
      throw new MetaException(e);
    } finally {
      closeHTable(meta);
    }
  }

  private boolean exists(final Get get) throws MetaException {
    HTableInterface meta = getHTable();
    try {
      boolean ex = meta.exists(get);
      if (LOG.isDebugEnabled()) {
        LOG.debug("FMeta " + get + " exists " + "=" + ex);
      }
      return ex;
    } catch (IOException e) {
      throw new MetaException(e);
    } finally {
      closeHTable(meta);
    }
  }

  public boolean isTableAvailable(final byte[] tableName) throws IOException {
    final AtomicBoolean available = new AtomicBoolean(true);
    final AtomicInteger entityGroupCount = new AtomicInteger(0);
    FMetaScanner.MetaScannerVisitor visitor = new FMetaScanner.MetaScannerVisitorBase() {
      @Override
      public boolean processRow(Result row) throws IOException {
        EntityGroupInfo info = FMetaScanner.getEntityGroupInfo(row);
        if (info != null) {
          if (Bytes.equals(tableName, info.getTableName())) {
            ServerName sn = ServerName.getServerName(row);
            if (sn == null) {
              available.set(false);
              return false;
            }
            entityGroupCount.incrementAndGet();
          }
        }
        return true;
      }
    };
    FMetaScanner.metaScan(getConf(), visitor);
    return available.get() && (entityGroupCount.get() > 0);
  }

  public void updateLocation(Configuration conf,
      EntityGroupInfo entityGroupInfo, ServerName serverNameFromMasterPOV)
      throws MetaException {
    updateEntityGroupLocation(entityGroupInfo, serverNameFromMasterPOV);
  }

}

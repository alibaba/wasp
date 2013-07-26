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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupLocation;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.ServerName;

/**
 * implement FMetaServices
 **/
class FMetaStore extends FMetaServices {

  public static final Log LOG = LogFactory.getLog(FMetaStore.class);

  protected FMetaServicesImplWithoutRetry fMetaServices;
  private int maxRetries;
  private long pause;

  public FMetaStore(Configuration conf) throws MetaException {
    this.setConf(conf);
    fMetaServices = new FMetaServicesImplWithoutRetry(conf);
    maxRetries = conf.getInt(FConstants.WASP_FMETA_RETRIES_NUMBER,
        FConstants.DEFAULT_WASP_FMETA_RETRIES_NUMBER);
    pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
  }

  /**
   * Use HTable to connect to the meta table, will use the HTable to
   * create\drop\alter\get Table and so on.
   */
  @Override
  public boolean connect() {
    return fMetaServices.connect();
  }

  @Override
  public void close() {
    fMetaServices.close();
  }

  @Override
  public HTableDescriptor[] listStorageTables() throws MetaException {
    return fMetaServices.listStorageTables();
  }

  @Override
  public HTableDescriptor getStorageTableDesc(String storageTable)
      throws MetaException {
    return fMetaServices.getStorageTableDesc(storageTable);
  }

  @Override
  public void deleteStorageTables(List<HTableDescriptor> deleteEntityTables)
      throws MetaException {
    fMetaServices.deleteStorageTables(deleteEntityTables);
  }

  @Override
  public void deleteStorageTable(String deleteStorageTable)
      throws MetaException {
    fMetaServices.deleteStorageTable(deleteStorageTable);
  }

  @Override
  public boolean storageTableExists(String deleteStorageTable)
      throws MetaException {
    return fMetaServices.storageTableExists(deleteStorageTable);
  }

  @Override
  public void createTable(FTable tbl) throws MetaException {
    fMetaServices.createTable(tbl);
    try {
      // This method will be concurrent called
      // current we thought this is might be a right place to create the hbase
      // table
      HTableDescriptor desc = getStorageTableDesc(tbl);
      createStorageTable(desc);
    } catch (IOException e) {
      LOG.error("Failed to create table.", e);
      throw new MetaException(e);
    }
  }

  @Override
  public void dropTable(String tableName) throws MetaException {
    LinkedHashMap<String, Index> indexs = fMetaServices.getAllIndex(tableName);
    if (indexs != null) {
      for (Index index : indexs.values()) {
        String htable = StorageTableNameBuilder.buildIndexTableName(index);
        deleteTable(htable);
      }
    }
    String htablename = StorageTableNameBuilder.buildEntityTableName(tableName);
    deleteTable(htablename);
    fMetaServices.dropTable(tableName);
  }

  private void deleteTable(String tableName) throws MetaException {
    try {
      // This method will be concurrent called
      // current we thought this is might be a right place to create the hbase
      // table
      HBaseAdmin admin = new HBaseAdmin(getConf());
      if (!admin.tableExists(tableName)) {
        LOG.warn(tableName + " have been already deleted.");
        admin.close();
        return;
      }
      admin.disableTable(tableName);
      try {
        admin.deleteTable(tableName);
      } finally {
        admin.close();
      }
    } catch (MasterNotRunningException e) {
      LOG.error("Failed to delete table.", e);
      throw new MetaException(e);
    } catch (ZooKeeperConnectionException e) {
      LOG.error("Failed to delete table.", e);
      throw new MetaException(e);
    } catch (IOException e) {
      LOG.error("Failed to delete table.", e);
      throw new MetaException(e);
    }
  }

  @Override
  public void alterTable(String tableName, FTable newFTable)
      throws MetaException {
    fMetaServices.alterTable(tableName, newFTable);
  }

  @Override
  public FTable getTable(String tableName) throws MetaException {
    return fMetaServices.getTable(tableName);
  }

  @Override
  public List<FTable> getTables(String regex) throws MetaException {
    return fMetaServices.getTables(regex);
  }

  @Override
  public List<FTable> getAllTables() throws MetaException {
    return fMetaServices.getAllTables();
  }

  @Override
  public List<FTable> getChildTables(String tableName) throws MetaException {
    return fMetaServices.getChildTables(tableName);
  }

  @Override
  public boolean tableExists(String tableName) throws MetaException {
    return fMetaServices.tableExists(tableName);
  }

  // ///////////////////////////////////////////////////
  // FTable operation
  // ///////////////////////////////////////////////////

  @Override
  public void addIndex(String tableName, Index index) throws MetaException {
    fMetaServices.addIndex(tableName, index);
    try {
      // This method will be concurrent called
      // current we thought this is might be a right place to create the hbase
      // table
      HBaseAdmin admin = new HBaseAdmin(getConf());
      try {
        String htable = StorageTableNameBuilder.buildIndexTableName(index);
        HTableDescriptor desc = new HTableDescriptor(htable);
        HColumnDescriptor hcolumn = new HColumnDescriptor(
            FConstants.INDEX_STORING_FAMILY_STR);
        hcolumn.setBloomFilterType(BloomType.ROW);
        hcolumn.setCompressionType(Algorithm.GZ);
        desc.addFamily(hcolumn);
        admin.createTable(desc);
      } finally {
        admin.close();
      }
    } catch (MasterNotRunningException e) {
      LOG.error("Failed to create index table.", e);
      throw new MetaException(e);
    } catch (ZooKeeperConnectionException e) {
      LOG.error("Failed to create index table.", e);
      throw new MetaException(e);
    } catch (IOException e) {
      LOG.error("Failed to create index table.", e);
      throw new MetaException(e);
    }
  }

  @Override
  public void deleteIndex(String tableName, Index index) throws MetaException {
    fMetaServices.deleteIndex(tableName, index);
    String htable = StorageTableNameBuilder.buildIndexTableName(index);
    deleteTable(htable);
  }

  @Override
  public LinkedHashMap<String, Index> getAllIndex(String tableName)
      throws MetaException {
    return fMetaServices.getAllIndex(tableName);
  }

  @Override
  public Index getIndex(String tableName, String indexName)
      throws MetaException {
    return fMetaServices.getIndex(tableName, indexName);
  }

  // ///////////////////////////////////////////////////
  // EntityGroup operation
  // ///////////////////////////////////////////////////
  @Override
  public void addEntityGroup(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    fMetaServices.addEntityGroup(entityGroupInfo);
  }

  @Override
  public void addEntityGroup(List<EntityGroupInfo> entityGroupInfos)
      throws MetaException {
    fMetaServices.addEntityGroup(entityGroupInfos);
  }

  @Override
  public boolean exists(EntityGroupInfo entityGroupInfo) throws MetaException {
    return fMetaServices.exists(entityGroupInfo);
  }

  @Override
  public void deleteEntityGroup(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    fMetaServices.deleteEntityGroup(entityGroupInfo);
  }

  @Override
  public void deleteEntityGroups(String tableName) throws MetaException {
    fMetaServices.deleteEntityGroups(tableName);
  }

  @Override
  public List<EntityGroupInfo> getTableEntityGroups(String tableName)
      throws MetaException {
    FTable root = fMetaServices.getRootTable(tableName);
    byte[] rootName = Bytes.toBytes(root.getTableName());
    // use scan
    for (int tries = 0; tries < maxRetries; ++tries) {
      try {
        return fMetaServices.getTableEntityGroups(rootName);
      } catch (MetaException me) {
        if (tries == maxRetries - 1) {
          throw new MetaException(" RetriesExhaustedException ", me);
        }
        LOG.info("RetryCount " + tries, me);
      }
      try {
        Thread.sleep(ConnectionUtils.getPauseTime(pause, tries));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetaException("Giving up after tries=" + tries, e);
      }
    }
    return new ArrayList<EntityGroupInfo>(0);
  }

  @Override
  public List<EntityGroupInfo> getTableEntityGroups(final byte[] tableByte)
      throws MetaException {
    FTable root = fMetaServices.getRootTable(tableByte);
    byte[] rootName = Bytes.toBytes(root.getTableName());
    // use scan
    for (int tries = 0; tries < maxRetries; ++tries) {
      try {
        return fMetaServices.getTableEntityGroups(rootName);
      } catch (MetaException me) {
        if (tries == maxRetries - 1) {
          throw new MetaException(" RetriesExhaustedException ", me);
        }
        LOG.info("RetryCount " + tries, me);
      }
      try {
        Thread.sleep(ConnectionUtils.getPauseTime(pause, tries));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetaException("Giving up after tries=" + tries, e);
      }
    }
    return new ArrayList<EntityGroupInfo>(0);
  }

  @Override
  public void modifyEntityGroupInfo(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    fMetaServices.modifyEntityGroupInfo(entityGroupInfo);
  }

  @Override
  public EntityGroupInfo getEntityGroupInfo(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    return fMetaServices.getEntityGroupInfo(entityGroupInfo);
  }

  @Override
  public ServerName getEntityGroupLocation(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    return fMetaServices.getEntityGroupLocation(entityGroupInfo);
  }

  @Override
  public Pair<EntityGroupInfo, ServerName> getEntityGroupAndLocation(
      byte[] entityGroupName) throws MetaException {
    return fMetaServices.getEntityGroupAndLocation(entityGroupName);
  }

  @Override
  public Map<EntityGroupInfo, ServerName> fullScan(
      final Set<String> disabledTables,
      final boolean excludeOfflinedSplitParents) throws MetaException {
    // use scan
    for (int tries = 0; tries < maxRetries; ++tries) {
      try {
        return fMetaServices.fullScan(disabledTables,
            excludeOfflinedSplitParents);
      } catch (MetaException me) {
        if (tries == maxRetries - 1) {
          throw new MetaException(" RetriesExhaustedException ", me);
        }
        LOG.info("RetryCount " + tries, me);
      }
      try {
        Thread.sleep(ConnectionUtils.getPauseTime(pause, tries));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetaException("Giving up after tries=" + tries, e);
      }
    }
    return new HashMap<EntityGroupInfo, ServerName>(0);
  }

  @Override
  public List<Result> fullScan() throws MetaException {
    // use scan
    for (int tries = 0; tries < maxRetries; ++tries) {
      try {
        return fMetaServices.fullScan();
      } catch (MetaException me) {
        if (tries == maxRetries - 1) {
          throw new MetaException("RetriesExhaustedException ", me);
        }
        LOG.info("RetryCount " + tries, me);
      }
      try {
        Thread.sleep(ConnectionUtils.getPauseTime(pause, tries));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetaException("Giving up after tries=" + tries, e);
      }
    }
    return new ArrayList<Result>(0);
  }

  @Override
  public Map<EntityGroupInfo, Result> getOfflineSplitParents()
      throws MetaException {
    // use scan
    for (int tries = 0; tries < maxRetries; ++tries) {
      try {
        return fMetaServices.getOfflineSplitParents();
      } catch (MetaException me) {
        if (tries == maxRetries - 1) {
          throw new MetaException(" RetriesExhaustedException ", me);
        }
        LOG.info("RetryCount " + tries, me);
      }
      try {
        Thread.sleep(ConnectionUtils.getPauseTime(pause, tries));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetaException("Giving up after tries=" + tries, e);
      }
    }

    return new HashMap<EntityGroupInfo, Result>(0);
  }

  @Override
  public NavigableMap<EntityGroupInfo, Result> getServerUserEntityGroups(
      final ServerName serverName) throws MetaException {
    // use scan
    for (int tries = 0; tries < maxRetries; ++tries) {
      try {
        return fMetaServices.getServerUserEntityGroups(serverName);
      } catch (MetaException me) {
        if (tries == maxRetries - 1) {
          throw new MetaException(" RetriesExhaustedException ", me);
        }
        LOG.info("RetryCount " + tries, me);
      }
      try {
        Thread.sleep(ConnectionUtils.getPauseTime(pause, tries));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetaException("Giving up after tries=" + tries, e);
      }
    }
    return new TreeMap<EntityGroupInfo, Result>();
  }

  @Override
  public List<EntityGroupInfo> getAllEntityGroupInfos() throws MetaException {
    return fMetaServices.getAllEntityGroupInfos();
  }

  @Override
  public void fullScan(final FMetaVisitor visitor) throws MetaException {
    // use scan
    fMetaServices.fullScan(visitor);
  }

  @Override
  public void fullScan(final FMetaVisitor visitor, final byte[] startrow,
      final byte[] endrow) throws MetaException {
    // use scan
    fMetaServices.fullScan(visitor, startrow, endrow);
  }

  @Override
  public void fullScan(final FMetaVisitor visitor, final byte[] startrow,
      final byte[] endrow, FilterList allFilters) throws MetaException {
    // use scan
    fMetaServices.fullScan(visitor, startrow, endrow, allFilters);
  }

  @Override
  public void updateEntityGroupLocation(EntityGroupInfo entityGroupInfo,
      ServerName sn) throws MetaException {
    fMetaServices.updateEntityGroupLocation(entityGroupInfo, sn);
  }

  @Override
  public EntityGroupLocation scanEntityGroupLocation(final byte[] tableName,
      final byte[] row) throws MetaException {
    // use scan
    for (int tries = 0; tries < maxRetries; ++tries) {
      try {
        return fMetaServices.scanEntityGroupLocation(tableName, row);
      } catch (MetaException me) {
        if (tries == maxRetries - 1) {
          LOG.error("RetriesExhaustedException", me);
          throw new MetaException("RetriesExhaustedException ", me);
        }
        LOG.info("RetryCount " + tries, me);
      }
      try {
        Thread.sleep(ConnectionUtils.getPauseTime(pause, tries));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetaException("Giving up after tries=" + tries, e);
      }
    }
    return null;
  }

  @Override
  public List<EntityGroupLocation> getEntityGroupLocations(
      final byte[] tableName) throws MetaException {
    FTable root = fMetaServices.getRootTable(tableName);
    byte[] rootName = Bytes.toBytes(root.getTableName());
    // use scan
    for (int tries = 0; tries < maxRetries; ++tries) {
      try {
        return fMetaServices.getEntityGroupLocations(rootName);
      } catch (MetaException me) {
        if (tries == maxRetries - 1) {
          throw new MetaException(" RetriesExhaustedException ", me);
        }
        LOG.info(" RetryCount " + tries, me);
      }
      try {
        Thread.sleep(ConnectionUtils.getPauseTime(pause, tries));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetaException("Giving up after tries=" + tries, e);
      }
    }
    return new ArrayList<EntityGroupLocation>(0);
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
    FTable root = fMetaServices.getRootTable(tableName);
    byte[] rootName = Bytes.toBytes(root.getTableName());
    // use scan
    for (int tries = 0; tries < maxRetries; ++tries) {
      try {
        return fMetaServices.getTableEntityGroupsAndLocations(rootName,
            excludeOfflinedSplitParents);
      } catch (MetaException me) {
        if (tries == maxRetries - 1) {
          throw new MetaException(" RetriesExhaustedException ", me);
        }
        LOG.info(" RetryCount " + tries, me);
      }
      try {
        Thread.sleep(ConnectionUtils.getPauseTime(pause, tries));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetaException("Giving up after tries=" + tries, e);
      }
    }
    return new ArrayList<Pair<EntityGroupInfo, ServerName>>(0);
  }

  public void offlineParentInMeta(EntityGroupInfo parent,
      final EntityGroupInfo a, final EntityGroupInfo b) throws MetaException {
    fMetaServices.offlineParentInMeta(parent, a, b);
  }

  /**
   * @param entityGroupInfo
   * @param sn
   * @throws MetaException
   */
  @Override
  public void addDaughter(final EntityGroupInfo entityGroupInfo,
      final ServerName sn) throws MetaException {
    fMetaServices.addDaughter(entityGroupInfo, sn);
  }

  /**
   * Generates and returns a Put containing the EntityGroupInfo into for the
   * catalog table
   */
  public static Put makePutFromEntityGroupInfo(EntityGroupInfo entityGroupInfo) {
    return FMetaServicesImplWithoutRetry
        .makePutFromEntityGroupInfo(entityGroupInfo);
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
    fMetaServices.deleteDaughtersReferencesInParent(parent);
  }

  public boolean isTableAvailable(final byte[] tableName) throws IOException {
    return fMetaServices.isTableAvailable(tableName);
  }

  public void updateLocation(Configuration conf,
      EntityGroupInfo entityGroupInfo, ServerName serverNameFromMasterPOV)
      throws MetaException {
    fMetaServices
        .updateLocation(conf, entityGroupInfo, serverNameFromMasterPOV);
  }

  /**
   * @see com.alibaba.wasp.meta.FMetaServices#getStorageTableDesc(com.alibaba.wasp.meta.FTable)
   */
  @Override
  public HTableDescriptor getStorageTableDesc(FTable tbl) {
    return fMetaServices.getStorageTableDesc(tbl);
  }

  /**
   * @see com.alibaba.wasp.meta.FMetaServices#createStorageTable(org.apache.hadoop.hbase.HTableDescriptor)
   */
  @Override
  public void createStorageTable(HTableDescriptor desc) throws MetaException {
    fMetaServices.createStorageTable(desc);
  }
}
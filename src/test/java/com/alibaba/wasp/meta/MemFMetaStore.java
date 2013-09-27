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

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupLocation;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.ServerName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Only for single process test.
 */
public class MemFMetaStore extends FMetaServices {

  private Map<String, FTable> tableNameToFTable = new ConcurrentHashMap<String, FTable>();

  @Override
  public boolean connect() {
    return true;
  }

  @Override
  public void close() {
    tableNameToFTable.clear();
  }

  @Override
  public HTableDescriptor[] listStorageTables() throws MetaException {
    
    return null;
  }

  @Override
  public void deleteStorageTables(List<HTableDescriptor> deleteEntityTables)
      throws MetaException {
    
  }

  @Override
  public void deleteStorageTable(String deleteStorageTable)
      throws MetaException {
    
  }

  @Override
  public void createTable(FTable tbl) throws MetaException {
    tableNameToFTable.put(tbl.getTableName(), tbl);
  }

  @Override
  public void dropTable(String tableName) throws MetaException {
    tableNameToFTable.remove(tableName);
  }

  @Override
  public void alterTable(String tableName, FTable newFTable)
      throws MetaException {
    tableNameToFTable.put(tableName, newFTable);
  }

  @Override
  public FTable getTable(String tableName) throws MetaException {
    return tableNameToFTable.get(tableName);
  }

  @Override
  public List<FTable> getTables(String pattern) throws MetaException {
    return null;
  }

  @Override
  public List<FTable> getAllTables() throws MetaException {
    List<FTable> allTables = new ArrayList<FTable>();
    for (Map.Entry<String, FTable> entry : tableNameToFTable.entrySet()) {
      allTables.add(entry.getValue());
    }
    return allTables;
  }

  @Override
  public List<FTable> getChildTables(String tableName) throws MetaException {
    
    return null;
  }

  @Override
  public boolean tableExists(String tableName) throws MetaException {
    
    return false;
  }

  @Override
  public void addIndex(String tableName, Index index) throws MetaException {
    FTable ftable = tableNameToFTable.get(tableName);
    ftable.addIndex(index);
  }

  @Override
  public void deleteIndex(String tableName, Index index) throws MetaException {
    FTable ftable = tableNameToFTable.get(tableName);
    ftable.removeIndex(index.getIndexName());
  }

  @Override
  public LinkedHashMap<String, Index> getAllIndex(String tableName)
      throws MetaException {
    FTable ftable = tableNameToFTable.get(tableName);
    return ftable.getIndex();
  }

  @Override
  public Index getIndex(String tableName, String indexName)
      throws MetaException {
    FTable ftable = tableNameToFTable.get(tableName);
    return ftable.getIndex(indexName);
  }

  @Override
  public void offlineParentInMeta(EntityGroupInfo parent, EntityGroupInfo a,
      EntityGroupInfo b) throws MetaException {
    

  }

  @Override
  public void addEntityGroup(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    

  }

  @Override
  public void addEntityGroup(List<EntityGroupInfo> entityGroupInfos)
      throws MetaException {
    

  }

  @Override
  public boolean exists(EntityGroupInfo entityGroupInfo) throws MetaException {
    
    return false;
  }

  @Override
  public void deleteDaughtersReferencesInParent(EntityGroupInfo parent)
      throws MetaException {
    

  }

  @Override
  public void deleteEntityGroup(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    

  }

  @Override
  public void deleteEntityGroups(String tableName) throws MetaException {
    

  }

  @Override
  public List<EntityGroupInfo> getTableEntityGroups(byte[] tableName)
      throws MetaException {
    
    return null;
  }

  @Override
  public List<EntityGroupInfo> getTableEntityGroups(String tableName)
      throws MetaException {
    
    return null;
  }

  @Override
  public void modifyEntityGroupInfo(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    

  }

  @Override
  public EntityGroupInfo getEntityGroupInfo(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    
    return null;
  }

  @Override
  public ServerName getEntityGroupLocation(EntityGroupInfo entityGroupInfo)
      throws MetaException {
    
    return null;
  }

  @Override
  public Pair<EntityGroupInfo, ServerName> getEntityGroupAndLocation(
      byte[] entityGroupName) throws MetaException {
    
    return null;
  }

  @Override
  public void updateEntityGroupLocation(EntityGroupInfo entityGroupInfo,
      ServerName sn) throws MetaException {
    

  }

  @Override
  public List<Pair<EntityGroupInfo, ServerName>> getTableEntityGroupsAndLocations(
      byte[] tableName, boolean excludeOfflinedSplitParents)
      throws MetaException {
    
    return null;
  }

  @Override
  public List<EntityGroupInfo> getAllEntityGroupInfos() throws MetaException {
    
    return null;
  }

  @Override
  public void addDaughter(EntityGroupInfo entityGroupInfo, ServerName sn)
      throws MetaException {
    

  }

  @Override
  public Map<EntityGroupInfo, Result> getOfflineSplitParents()
      throws MetaException {
    
    return null;
  }

  @Override
  public NavigableMap<EntityGroupInfo, Result> getServerUserEntityGroups(
      ServerName serverName) throws MetaException {
    
    return null;
  }

  @Override
  public Map<EntityGroupInfo, ServerName> fullScan(Set<String> disabledTables,
      boolean excludeOfflinedSplitParents) throws MetaException {
    
    return null;
  }

  @Override
  public List<Result> fullScan() throws MetaException {
    
    return null;
  }

  @Override
  public void fullScan(FMetaVisitor visitor) throws MetaException {
    

  }

  @Override
  public void fullScan(FMetaVisitor visitor, byte[] startrow, byte[] endrow)
      throws MetaException {
    

  }

  @Override
  public void fullScan(FMetaVisitor visitor, byte[] startrow, byte[] endrow,
      FilterList allFilters) throws MetaException {
    

  }

  @Override
  public EntityGroupLocation scanEntityGroupLocation(byte[] tableName,
      byte[] row) throws MetaException {
    
    return null;
  }

  @Override
  public List<EntityGroupLocation> getEntityGroupLocations(byte[] tableName)
      throws MetaException {
    
    return null;
  }

  /**
   * @see com.alibaba.wasp.meta.FMetaServices#getStorageTableDesc(com.alibaba.wasp.meta.FTable)
   */
  @Override
  public HTableDescriptor getStorageTableDesc(FTable tbl) {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * @see com.alibaba.wasp.meta.FMetaServices#createStorageTable(org.apache.hadoop.hbase.HTableDescriptor)
   */
  @Override
  public void createStorageTable(HTableDescriptor desc) throws MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public HTableDescriptor getStorageTableDesc(String storageTable)
      throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean storageTableExists(String storageTable) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }
}
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import com.alibaba.wasp.EntityGroupLocation;import com.alibaba.wasp.ServerName;import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Pair;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupLocation;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.ServerName;

/**
 * Meta Store operation services interface, including create\drop\alter\get
 * Table, alter table including column\index\EntityGroup related operation
 * */
public abstract class FMetaServices extends Configured implements Configurable {

  // connect to the FMETA
  public abstract boolean connect();

  // close connection
  public abstract void close();

  // list all Storage tables in HBase
  public abstract HTableDescriptor[] listStorageTables() throws MetaException;

  // get table desc from HBase
  public abstract HTableDescriptor getStorageTableDesc(String storageTable)
      throws MetaException;

  // get table desc from FTable
  public abstract HTableDescriptor getStorageTableDesc(FTable tbl);

  // create table in HBase
  public abstract void createStorageTable(HTableDescriptor desc)
      throws MetaException;

  // delete tables in HBase
  public abstract void deleteStorageTables(
      List<HTableDescriptor> deleteEntityTables) throws MetaException;

  // delete table in HBase
  public abstract void deleteStorageTable(String deleteStorageTable)
      throws MetaException;

  // Check table exists in HBase
  public abstract boolean storageTableExists(String storageTable)
      throws MetaException;

  // Add FTable info into FMETA
  public abstract void createTable(FTable tbl) throws MetaException;

  // Drop table's FTable info from FMETA
  public abstract void dropTable(String tableName) throws MetaException;

  // Alter table's FTable in FMETA
  public abstract void alterTable(String tableName, FTable newFTable)
      throws MetaException;

  // Get table's FTable from FMETA
  public abstract FTable getTable(String tableName) throws MetaException;

  // Get all tables's FTable from FMETA according to pattern
  public abstract List<FTable> getTables(String pattern) throws MetaException;

  // Get all tables's FTable from FMETA
  public abstract List<FTable> getAllTables() throws MetaException;

  // Get a table's all children FTable
  public abstract List<FTable> getChildTables(String tableName)
      throws MetaException;

  // Check if the table exists
  public abstract boolean tableExists(String tableName) throws MetaException;

  // ///////////////////////////////////////////////////
  // FTable operation
  // ///////////////////////////////////////////////////

  // Add a index for table
  public abstract void addIndex(String tableName, Index index)
      throws MetaException;

  // Delete a table's index
  public abstract void deleteIndex(String tableName, Index index)
      throws MetaException;

  // Get a table's all indexes
  public abstract LinkedHashMap<String, Index> getAllIndex(String tableName)
      throws MetaException;

  // Get a table's index
  public abstract Index getIndex(String tableName, String indexName)
      throws MetaException;

  // ///////////////////////////////////////////////////
  // EntityGroup operation
  // ///////////////////////////////////////////////////
  public abstract void offlineParentInMeta(EntityGroupInfo parent,
      final EntityGroupInfo a, final EntityGroupInfo b) throws MetaException;

  // Add a EntityGroupInfo
  public abstract void addEntityGroup(EntityGroupInfo entityGroupInfo)
      throws MetaException;

  // Add some EntityGroupInfo
  public abstract void addEntityGroup(List<EntityGroupInfo> entityGroupInfos)
      throws MetaException;

  // Check if a EntityGroupInfo exists
  public abstract boolean exists(EntityGroupInfo entityGroupInfo)
      throws MetaException;

  public abstract void deleteDaughtersReferencesInParent(
      final EntityGroupInfo parent) throws MetaException;

  // Delete a EntityGroupInfo
  public abstract void deleteEntityGroup(EntityGroupInfo entityGroupInfo)
      throws MetaException;

  // Delete a table's EntityGroupInfo. Root and Child table share the same
  // EntityGroups, so delete a child table' EntityGroupInfo we will do nothing
  public abstract void deleteEntityGroups(String tableName)
      throws MetaException;

  // Get a table's all EntityGroupInfos
  public abstract List<EntityGroupInfo> getTableEntityGroups(byte[] tableName)
      throws MetaException;

  // Get a table's all EntityGroupInfos
  public abstract List<EntityGroupInfo> getTableEntityGroups(String tableName)
      throws MetaException;

  // Modify the EntityGroupInfo
  public abstract void modifyEntityGroupInfo(EntityGroupInfo entityGroupInfo)
      throws MetaException;

  // Get EntityGroupInfo
  public abstract EntityGroupInfo getEntityGroupInfo(
      EntityGroupInfo entityGroupInfo) throws MetaException;

  // Get the EntityGroup's location
  public abstract ServerName getEntityGroupLocation(
      EntityGroupInfo entityGroupInfo) throws MetaException;

  // Get the EntityGroup's EntityGroupInfo and Location
  public abstract Pair<EntityGroupInfo, ServerName> getEntityGroupAndLocation(
      final byte[] entityGroupName) throws MetaException;

  // Update EntityGroup's Location
  public abstract void updateEntityGroupLocation(
      EntityGroupInfo entityGroupInfo, ServerName sn) throws MetaException;

  // Get table's EntityGroupInfo and Location
  public abstract List<Pair<EntityGroupInfo, ServerName>> getTableEntityGroupsAndLocations(
      final byte[] tableName, final boolean excludeOfflinedSplitParents)
      throws MetaException;

  // Get all EntityGroupInfos
  public abstract List<EntityGroupInfo> getAllEntityGroupInfos()
      throws MetaException;

  public abstract void addDaughter(final EntityGroupInfo entityGroupInfo,
      final ServerName sn) throws MetaException;

  public abstract Map<EntityGroupInfo, Result> getOfflineSplitParents()
      throws MetaException;

  // Get all EntityGroup's EntityGroupInfos and Result in a FServer
  public abstract NavigableMap<EntityGroupInfo, Result> getServerUserEntityGroups(
      ServerName serverName) throws MetaException;

  public abstract Map<EntityGroupInfo, ServerName> fullScan(
      Set<String> disabledTables, boolean excludeOfflinedSplitParents)
      throws MetaException;

  public abstract List<Result> fullScan() throws MetaException;

  public abstract void fullScan(final FMetaVisitor visitor)
      throws MetaException;

  public abstract void fullScan(final FMetaVisitor visitor,
      final byte[] startrow, final byte[] endrow) throws MetaException;

  public abstract void fullScan(final FMetaVisitor visitor,
      final byte[] startrow, final byte[] endrow, FilterList allFilters)
      throws MetaException;

  // access control should to be done

  // Get the row belong to which EntityGroup in the table, return
  // EntityGroupInfo and Location
  public abstract EntityGroupLocation scanEntityGroupLocation(byte[] tableName,
      byte[] row) throws MetaException;

  // Get the table's EntityGroupInfo and Location
  public abstract List<EntityGroupLocation> getEntityGroupLocations(
      byte[] tableName) throws MetaException;

  /**
   * A {@link Visitor} that collects content out of passed {@link Result}.
   */
  static abstract class CollectingVisitor<T> implements FMetaVisitor {
    final List<T> results = new ArrayList<T>();

    @Override
    public boolean visit(Result r) throws IOException {
      if (r == null || r.isEmpty())
        return true;
      add(r);
      return true;
    }

    abstract void add(Result r);

    /**
     * @return Collected results; wait till visits complete to collect all
     *         possible results
     */
    List<T> getResults() {
      return this.results;
    }
  }

  /**
   * Collects all returned.
   */
  static class CollectAllVisitor extends CollectingVisitor<Result> {
    @Override
    void add(Result r) {
      this.results.add(r);
    }
  }
}
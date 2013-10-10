/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.wasp.meta;

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupLocation;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.ServerName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class FMetaReader extends AbstractMetaService {

  /**
   * Checks if the specified table exists. Looks at the FMETA table hosted on
   * the specified server.
   * 
   * @param tableName
   *          table to check
   * @return true if the table exists in meta, false if not
   * @throws com.alibaba.wasp.MetaException
   * @throws java.io.IOException
   */
  public static boolean tableExists(final Configuration conf, String tableName)
      throws MetaException {
    return getService(conf).tableExists(tableName);
  }

  /**
   * Gets the entityGroup info and assignment for the specified entityGroup.
   *
   * @param conf
   * @param entityGroupname
   *          EntityGroup to lookup.
   * @return Location and EntityGroupInfo for <code>entityGroupname</code>
   * @throws com.alibaba.wasp.MetaException
   * @throws java.io.IOException
   */
  public static Pair<EntityGroupInfo, ServerName> getEntityGroup(
      final Configuration conf, byte[] entityGroupname) throws MetaException {
    return getService(conf).getEntityGroupAndLocation(entityGroupname);
  }

  /**
   * Gets all of the entityGroupinfos of the specified table.
   * @param conf
   * @param tableName
   * @return Ordered list of {@link com.alibaba.wasp.EntityGroupInfo}.
   * @throws com.alibaba.wasp.MetaException
   */
  public static List<EntityGroupInfo> getTableEntityGroups(
      final Configuration conf, final byte[] tableName) throws MetaException {
    return getTableEntityGroups(conf, tableName, false);
  }

  /**
   * Gets all of the entityGroupinfos of the specified table.
   *
   * @param conf
   * @param tableName
   * @param excludeOfflinedSplitParents
   *          If true, do not include offlined split parents in the return.
   * @return Ordered list of {@link com.alibaba.wasp.EntityGroupInfo}.
   * @throws com.alibaba.wasp.MetaException
   * @throws java.io.IOException
   */
  public static List<EntityGroupInfo> getTableEntityGroups(
      final Configuration conf, final byte[] tableName,
      final boolean excludeOfflinedSplitParents) throws MetaException {
    return getService(conf).getTableEntityGroups(tableName);
  }

  /**
   * @param conf
   * @param tableName
   * @return Return list of entityGroupinfos and server addresses.
   * @throws com.alibaba.wasp.MetaException
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  public static List<Pair<EntityGroupInfo, ServerName>> getTableEntityGroupsAndLocations(
      final Configuration conf, final byte[] tableName) throws MetaException {
    return getService(conf).getTableEntityGroupsAndLocations(tableName, true);
  }

  /**
   *
   * @param conf
   * @param tableName
   * @param excludeOfflinedSplitParents
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static List<Pair<EntityGroupInfo, ServerName>> getTableEntityGroupsAndLocations(
      final Configuration conf, final byte[] tableName,
      final boolean excludeOfflinedSplitParents) throws MetaException {
    return getService(conf).getTableEntityGroupsAndLocations(tableName,
        excludeOfflinedSplitParents);
  }

  /**
   *
   * @param tableName
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static FTable getTable(final Configuration conf, String tableName)
      throws MetaException {
    return getService(conf).getTable(tableName);
  }

  /**
   *
   * @param tableName
   * @return child tables
   * @throws com.alibaba.wasp.MetaException
   */
  public static List<FTable> getChildTable(final Configuration conf,
      String tableName) throws MetaException {
    return getService(conf).getChildTables(tableName);
  }

  /**
   *
   * @param tableName
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static byte[] getRootTable(final Configuration conf,
      String tableName) throws MetaException {
    FTable fTable = getService(conf).getTable(tableName);
    if (fTable == null) {
      return null;
    }
    byte[] rootTableName = fTable.isRootTable() ? Bytes.toBytes(tableName)
        : Bytes.toBytes(fTable.getParentName());
    return rootTableName;
  }

  /**
   *
   * @param tableName
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static byte[] getRootTable(final Configuration conf, byte[] tableName)
      throws MetaException {
    return getRootTable(conf, Bytes.toString(tableName));
  }

  /**
   *
   *
   * @param entityGroupInfo
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static ServerName getEntityGroupLocation(final Configuration conf,
      EntityGroupInfo entityGroupInfo) throws MetaException {
    return getService(conf).getEntityGroupLocation(entityGroupInfo);
  }

  /**
   *
   * @param conf
   * @param entityGroupName
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static Pair<EntityGroupInfo, ServerName> getEntityGroupAndLocation(
      final Configuration conf, final byte[] entityGroupName)
      throws MetaException {
    return getService(conf).getEntityGroupAndLocation(entityGroupName);
  }

  /**
   *
   * @param tableName
   * @param row
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static EntityGroupLocation scanEntityGroupLocation(
      final Configuration conf, byte[] tableName, byte[] row)
      throws MetaException {
    return getService(conf).scanEntityGroupLocation(tableName, row);
  }

  /**
   *
   * @param tableName
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static List<EntityGroupLocation> getEntityGroupLocations(
      final Configuration conf, byte[] tableName) throws MetaException {
    return getService(conf).getEntityGroupLocations(tableName);
  }

  /**
   * @param configuration
   * @param serverName
   * @return
   */
  public static NavigableMap<EntityGroupInfo, Result> getServerUserEntityGroups(
      final Configuration conf, final ServerName serverName)
      throws MetaException {
    return getService(conf).getServerUserEntityGroups(serverName);
  }

  /**
   *
   * @param conf
   * @param tableName
   * @param indexName
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static Index getIndex(final Configuration conf,
      final String tableName, final String indexName) throws MetaException {
    return getService(conf).getIndex(tableName, indexName);
  }

  /**
   * @param conf
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static Map<EntityGroupInfo, Result> getOfflineSplitParents(
      final Configuration conf) throws MetaException {
    return getService(conf).getOfflineSplitParents();
  }

  /**
   * @param conf
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static List<FTable> getAllTables(final Configuration conf)
      throws MetaException {
    return getService(conf).getAllTables();
  }

  /**
   * @param conf
   * @param entityGroupInfo
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static boolean exists(final Configuration conf,
      EntityGroupInfo entityGroupInfo) throws MetaException {
    return getService(conf).exists(entityGroupInfo);
  }
}
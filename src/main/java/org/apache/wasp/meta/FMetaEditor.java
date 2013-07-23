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

package org.apache.wasp.meta;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.MetaException;
import org.apache.wasp.ServerName;

/**
 * Writes entityGroup and assignment information to <code>.FMETA.</code>.
 */
public class FMetaEditor extends AbstractMetaService {

  public static void addDaughter(final Configuration conf,
      final EntityGroupInfo daughter, final ServerName sn) throws IOException {
    getService(conf).addDaughter(daughter, sn);
  }

  public static void updateLocation(final Configuration conf,
      EntityGroupInfo entityGroupInfo, ServerName serverNameFromMasterPOV)
      throws IOException {
    getService(conf).updateEntityGroupLocation(entityGroupInfo,
        serverNameFromMasterPOV);
  }

  public static void offlineParentInMeta(Configuration conf,
      EntityGroupInfo parent, EntityGroupInfo a, EntityGroupInfo b)
      throws IOException {
    getService(conf).offlineParentInMeta(parent, a, b);
  }

  /**
   * Deletes daughters references in offlined split parent.
   * 
   * @param parent
   *          Parent row we're to remove daughter reference from
   * @throws IOException
   */
  public static void deleteDaughtersReferencesInParent(
      final Configuration conf, final EntityGroupInfo parent)
      throws IOException {
    getService(conf).deleteDaughtersReferencesInParent(parent);
  }

  /**
   * @param conf
   * @param egInfo
   * @throws MetaException
   */
  public static void addEntityGroupToMeta(final Configuration conf,
      final EntityGroupInfo egInfo) throws MetaException {
    getService(conf).addEntityGroup(egInfo);
  }

  /**
   * 
   * @param conf
   * @param egInfos
   * @throws MetaException
   */
  public static void addEntityGroupsToMeta(final Configuration conf,
      final List<EntityGroupInfo> egInfos) throws MetaException {
    getService(conf).addEntityGroup(egInfos);
  }

  /**
   * @param table
   * @throws MetaException
   */
  public static void createTable(final Configuration conf, final FTable table)
      throws MetaException {
    getService(conf).createTable(table);
  }

  /**
   * 
   * @param conf
   * @param tableNameStr
   * @param newTable
   * @throws MetaException
   */
  public static void alterTable(final Configuration conf,
      final String tableNameStr, final FTable newTable) throws MetaException {
    getService(conf).alterTable(tableNameStr, newTable);

  }

  /**
   * 
   * @param conf
   * @param tableNameStr
   * @param index
   * @throws MetaException
   */
  public static void addIndex(final Configuration conf,
      final String tableNameStr, final Index index) throws MetaException {
    getService(conf).addIndex(tableNameStr, index);
  }

  /**
   * 
   * @param conf
   * @param entityGroupInfo
   * @throws MetaException
   */
  public static void deleteEntityGroup(final Configuration conf,
      final EntityGroupInfo entityGroupInfo) throws MetaException {
    getService(conf).deleteEntityGroup(entityGroupInfo);
  }

  /**
   * 
   * @param conf
   * @param tableNameStr
   */
  public static void dropTable(final Configuration conf,
      final String tableNameStr) throws MetaException {
    getService(conf).dropTable(tableNameStr);
  }

  /**
   * 
   * @param conf
   * @param tableName
   * @param index
   * @throws MetaException
   */
  public static void deleteIndex(final Configuration conf,
      final String tableName, final Index index) throws MetaException {
    getService(conf).deleteIndex(tableName, index);
  }

  /**
   * 
   * @param conf
   * @param deleteStorageTable
   * @throws MetaException
   */
  public static void deleteStorageTable(final Configuration conf,
      String deleteStorageTable) throws MetaException {
    getService(conf).deleteStorageTable(deleteStorageTable);
  }
}

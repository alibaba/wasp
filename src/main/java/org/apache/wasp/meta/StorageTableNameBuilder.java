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

import org.apache.wasp.FConstants;

public class StorageTableNameBuilder {

  private StorageTableNameBuilder instance = new StorageTableNameBuilder();

  public StorageTableNameBuilder build() {
    return instance;
  }

  /**
   * Get Transaction table.
   * 
   * @param entityGroupName
   * @return
   */
  public static String buildTransactionTableName(String entityGroupName) {
    return FConstants.WASP_TABLE_TRANSACTION_PREFIX + entityGroupName;
  }

  /**
   * To build a variety of index table.
   * 
   * @param index
   * @return
   */
  public static String buildIndexTableName(Index index) {
    String indexName = index.getIndexName();
    String tableName = index.getDependentTableName();
    return FConstants.WASP_TABLE_INDEX_PREFIX + tableName
        + FConstants.TABLE_ROW_SEP + indexName;
  }

  /**
   * Convert wasp table name to Storage table name.
   * 
   * @param tableName
   * @return
   */
  public static String buildEntityTableName(String tableName) {
    return FConstants.WASP_TABLE_ENTITY_PREFIX + tableName;
  }

  /**
   * Convert Storage table name to wasp table name.
   * 
   * @param tableName
   * @return
   */
  public static String getFTableNameFromEntityTableName(String tableName) {
    int beginIndex = FConstants.WASP_TABLE_ENTITY_PREFIX.length();
    return tableName.substring(beginIndex);
  }
}
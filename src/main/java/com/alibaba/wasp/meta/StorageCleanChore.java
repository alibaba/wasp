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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Stoppable;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FConstants;

/**
 * The Class StorageCleanChore for running FMETA checker regularly. Find which
 * tables and indexes have been deleted but not clean and which EntityGroups are
 * useless, delete those deleted table and index and EntityGroups from FMETA and
 * their storage table In HBase
 * 
 */
public class StorageCleanChore extends Chore {

  private static final Log LOG = LogFactory.getLog(StorageCleanChore.class
      .getName());

  private final FMetaServices fMetaServices;

  /**
   * @param name
   * @param p
   * @param stopper
   */
  public StorageCleanChore(int sleepTime, Stoppable stopper,
      Configuration conf, FMetaServices fMetaServices) {
    super("StorageClean", sleepTime, stopper);
    this.fMetaServices = fMetaServices;
  }

  /**
   * @see org.apache.hadoop.hbase.Chore#chore()
   */
  @Override
  protected void chore() {
    try {
      // Get all table from HBase
      HTableDescriptor[] hbaseTables = fMetaServices.listStorageTables();
      // EntityTable store table data
      List<HTableDescriptor> entityTables = new ArrayList<HTableDescriptor>();
      // IndexTable store index data
      List<HTableDescriptor> indexTables = new ArrayList<HTableDescriptor>();
      // TransactionTable store EntityGroup Transaction data
      List<HTableDescriptor> transactionTables = new ArrayList<HTableDescriptor>();
      for (HTableDescriptor htable : hbaseTables) {
        if (htable.getNameAsString().startsWith(
            FConstants.WASP_TABLE_ENTITY_PREFIX)) {
          entityTables.add(htable);
        } else if (htable.getNameAsString().startsWith(
            FConstants.WASP_TABLE_INDEX_PREFIX)) {
          indexTables.add(htable);
        } else if (htable.getNameAsString().startsWith(
            FConstants.WASP_TABLE_TRANSACTION_PREFIX)) {
          transactionTables.add(htable);
        } else {
          LOG.debug("Other tables in HBase " + htable.getNameAsString());
        }
      }

      // Get all Wasp Table
      List<FTable> allFTables = fMetaServices.getAllTables();

      // Clean FTable Storage
      cleanFTableStorage(allFTables, entityTables);

      // Clean Index Storage
      cleanIndexStorage(allFTables, indexTables);

      // Clean Transaction Storage
      cleanTransactionStorage(transactionTables);
    } catch (Exception e) {
      LOG.warn("Failed in StorageCleanChore", e);
    }
  }

  public void cleanFTableStorage(List<FTable> allFTables,
      List<HTableDescriptor> entityTables) {
    try {
      // Get all Wasp Tables's Storage Table Name in HBase
      Set<String> allTableNames = new HashSet<String>(allFTables.size());
      for (FTable ftable : allFTables) {
        allTableNames.add(StorageTableNameBuilder.buildEntityTableName(ftable
            .getTableName()));
      }
      List<HTableDescriptor> needDeleteEntityTables = new ArrayList<HTableDescriptor>();
      for (HTableDescriptor entityTable : entityTables) {
        String tableName = entityTable.getNameAsString();
        if (!allTableNames.contains(tableName)) {
          // This is a deleted Wasp Table's Storage Table
          needDeleteEntityTables.add(entityTable);
          LOG.info("CleanFTableStorage Table-HTableName:"
              + entityTable.getNameAsString());
        }
      }

      // Delete wasp tables's storage table in HBase, which have been deleted
      fMetaServices.deleteStorageTables(needDeleteEntityTables);
    } catch (Exception e) {
      LOG.warn("Failed in StorageCleanChore", e);
    }
  }

  public void cleanIndexStorage(List<FTable> allFTables,
      List<HTableDescriptor> indexTables) {
    try {
      // Get all Indexes's Storage Table Name in HBase
      Set<String> allIndexTableNames = new HashSet<String>();
      for (FTable ftable : allFTables) {
        for (Index index : ftable.getIndex().values()) {
          allIndexTableNames.add(StorageTableNameBuilder
              .buildIndexTableName(index));
        }
      }

      List<HTableDescriptor> needDeleteIndexTables = new ArrayList<HTableDescriptor>();
      for (HTableDescriptor indexTable : indexTables) {
        String tableName = indexTable.getNameAsString();
        if (!allIndexTableNames.contains(tableName)) {
          // This is a deleted index's Storage Table
          needDeleteIndexTables.add(indexTable);
          LOG.info("cleanIndexStorage Index-HTableName:"
              + indexTable.getNameAsString());
        }
      }
      // Delete Index's storage table in HBase, which have been deleted
      fMetaServices.deleteStorageTables(needDeleteIndexTables);
    } catch (Exception e) {
      LOG.warn("Failed in StorageCleanChore", e);
    }
  }

  public void cleanTransactionStorage(List<HTableDescriptor> transactionTables) {
    try {
      // Scan FMeta and get all EntityGroups
      List<EntityGroupInfo> entityGroups = fMetaServices
          .getAllEntityGroupInfos();
      Set<String> allTransactionTableNames = new HashSet<String>();
      for (EntityGroupInfo eninfo : entityGroups) {
        allTransactionTableNames.add(StorageTableNameBuilder
            .buildTransactionTableName(eninfo.getEncodedName()));
      }

      List<HTableDescriptor> needDeleteTransactionTables = new ArrayList<HTableDescriptor>();
      for (HTableDescriptor transactionTable : transactionTables) {
        String tableName = transactionTable.getNameAsString();
        if (!allTransactionTableNames.contains(tableName)) {
          // This is a deleted EntityGroup's Transaction Table
          needDeleteTransactionTables.add(transactionTable);
          LOG.info("cleanTransactionStorage Transaction-HTableName:"
              + tableName);
        }
      }
      // Delete EntityGroups's Transaction table in HBase, which have been
      // deleted
      fMetaServices.deleteStorageTables(needDeleteTransactionTables);
    } catch (Exception e) {
      LOG.warn("Failed in StorageCleanChore", e);
    }
  }
}

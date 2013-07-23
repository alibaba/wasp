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
package org.apache.wasp.storage;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.FConstants;

/**
 * 
 * A manager that is used to execute the hbase actions, such as put,delete,get
 * and scan.
 * 
 */
public class StorageActionManager {

  public static final Log LOG = LogFactory.getLog(StorageActionManager.class);

  private HBaseAdmin admin;

  private HTablePool pool;

  private Configuration hbc;

  private static final int MAX_TABLE_COUNT = 20;

  /**
   * default constructor
   * 
   * @param conf
   */
  public StorageActionManager(Configuration conf) throws IOException {
    try {
      hbc = HBaseConfiguration.create(conf);
      pool = new HTablePool(hbc, conf.getInt(
          FConstants.WASP_FSERVER_MAX_TABLE_COUNT, MAX_TABLE_COUNT));
      admin = new HBaseAdmin(hbc);
    } catch (MasterNotRunningException e) {
      LOG.error("HBaseActionManager initlized failed.HMaster is not running.",
          e);
      throw new IOException(e);
    } catch (ZooKeeperConnectionException e) {
      LOG.error(
          "HBaseActionManager initlized failed.Cann't connect zookeeper.", e);
      throw new IOException(e);
    }
  }

  // Get all table in HBase
  public HTableDescriptor[] listStorageTables() throws IOException {
    return admin.listTables();
  }

  // Get Table Desc from HBase
  public HTableDescriptor getStorageTableDesc(String storageTable)
      throws IOException {
    return admin.getTableDescriptor(Bytes.toBytes(storageTable));
  }

  // Create table in HBase
  public void createStorageTable(HTableDescriptor desc) throws IOException {
    getHBaseAdmin().createTable(desc);
  }

  // Delete table in HBase
  public void deleteStorageTable(byte[] tableName) throws IOException {
    try {
      getHBaseAdmin().disableTable(tableName);
    } catch (TableNotEnabledException e) {
      LOG.debug("Table: " + Bytes.toString(tableName)
          + " already disabled, so just deleting it.");
    }
    getHBaseAdmin().deleteTable(tableName);
  }

  // Check table exists in HBase
  public boolean storageTableExists(String tableName) throws IOException {
    return getHBaseAdmin().tableExists(tableName);
  }

  // Get HBaseAdmin
  public synchronized HBaseAdmin getHBaseAdmin() throws IOException {
    if (admin == null) {
      admin = new HBaseAdmin(hbc);
    }
    return admin;
  }

  /**
   * 
   * get a HTableInterface from hTable pool.
   * 
   * 
   * @param tableName
   *          name of table
   * @return Null if there isn't the table. else the HTableInterface
   * @throws StorageTableNotFoundException
   */
  public HTableInterface getTable(String tableName)
      throws StorageTableNotFoundException {
    return pool.getTable(tableName);
  }

  /**
   * 
   * process the put operator by the hbase table
   * 
   * @param tableName
   *          name of table
   * @param put
   *          Put operator of hbase
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  public void put(String tableName, Put put) throws IOException,
      StorageTableNotFoundException {
    HTableInterface tableInterface = getTable(tableName);

    try {
      tableInterface.put(put);
    } finally {
      // return table to hTable pool
      tableInterface.close();
    }
  }

  /**
   * 
   * process the delete operator by the hbase table
   * 
   * @param tableName
   *          name of table
   * @param delete
   *          Delete operator of hbase
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  public void delete(String tableName, Delete delete) throws IOException,
      StorageTableNotFoundException {
    HTableInterface tableInterface = getTable(tableName);

    try {
      tableInterface.delete(delete);
    } finally {
      // return table to hTable pool
      tableInterface.close();
    }
  }

  /**
   * process the get operator by the hbase table
   * 
   * @param tableName
   * @param get
   * @return
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  public Result get(String tableName, Get get) throws IOException,
      StorageTableNotFoundException {
    HTableInterface tableInterface = getTable(tableName);

    try {
      return tableInterface.get(get);
    } finally {
      // return table to hTable pool
      tableInterface.close();
    }
  }

  /**
   * process the get operator by the hbase table
   * 
   * @param tableName
   * @param get
   * @return
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  public boolean exits(String tableName, Get get) throws IOException,
      StorageTableNotFoundException {
    HTableInterface tableInterface = getTable(tableName);

    try {
      return tableInterface.exists(get);
    } finally {
      // return table to hTable pool
      tableInterface.close();
    }
  }

  /**
   * batch gets.
   * 
   * @param tableName
   * @param gets
   * @return
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  public Result[] get(String tableName, List<Get> gets) throws IOException,
      StorageTableNotFoundException {
    HTableInterface tableInterface = getTable(tableName);

    try {
      return tableInterface.get(gets);
    } finally {
      // return table to hTable pool
      tableInterface.close();
    }
  }

  /**
   * 
   * process the actions by the hbase table
   * 
   * @param tableName
   *          name of table
   * @param actions
   *          a action list
   * @throws IOException
   * @throws StorageTableNotFoundException
   * @throws InterruptedException
   */
  public void batch(String tableName, final List<? extends Row> actions)
      throws IOException, StorageTableNotFoundException, InterruptedException {
    HTableInterface tableInterface = getTable(tableName);

    try {
      tableInterface.batch(actions);
    } finally {
      // return table to hTable pool
      tableInterface.close();
    }
  }

  /**
   * get scanner.
   * 
   * @param tableName
   * @param scan
   * @return
   * @throws StorageTableNotFoundException
   * @throws IOException
   */
  public ResultScanner scan(String tableName, Scan scan)
      throws StorageTableNotFoundException, IOException {
    HTableInterface tableInterface = getTable(tableName);
    try {
      return tableInterface.getScanner(scan);
    } finally {
      // return table to hTable pool
      tableInterface.close();
    }
  }

  public void close() {
    if (pool != null) {
      try {
        pool.close();
      } catch (IOException e) {
        LOG.error("Close HTable failed ", e);
      }
    }
    if (admin != null) {
      try {
        admin.close();
      } catch (IOException e) {
        LOG.error("Close HBaseAdmin failed ", e);
      }
    }
  }
}

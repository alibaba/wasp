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
package com.alibaba.wasp.master.handler;

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.TableLockedException;
import com.alibaba.wasp.TableNotDisabledException;
import com.alibaba.wasp.TableNotEnabledException;
import com.alibaba.wasp.TableNotFoundException;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.master.AssignmentManager;
import com.alibaba.wasp.master.FMasterServices;
import com.alibaba.wasp.master.TableLockManager;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FMetaServices;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Index;
import com.alibaba.wasp.meta.StorageTableNameBuilder;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Handler to truncate a table.
 */
public class TruncateTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(TruncateTableHandler.class);
  // private final AssignmentManager assignmentManager;
  private final String tableNameStr;
  private final Configuration conf;
  private TableLockManager tableLockManager;
  private byte[] table;

  public TruncateTableHandler(final byte[] tableName, FMasterServices server,
      AssignmentManager assignmentManager) throws TableNotFoundException,
      TableNotEnabledException, IOException {
    super(server, EventType.C_M_TRUNCATE_TABLE);
    this.table = tableName;
    this.tableNameStr = Bytes.toString(tableName);
    // this.assignmentManager = assignmentManager;
    this.conf = server.getConfiguration();
    this.tableLockManager = server.getTableLockManager();
    // Check if table exists
    if (!FMetaReader.tableExists(conf, tableNameStr)) {
      throw new TableNotFoundException(tableNameStr);
    }

    try {
      server.checkTableModifiable(tableName);
    } catch (TableNotDisabledException ex) {
      throw ex;
    }
    if (tableLockManager.lockTable(tableNameStr)) {
      LOG.info("lock table '" + tableNameStr + "' by TruncateTableHandler");
    } else {
      throw new TableLockedException(tableNameStr + " has been locked. ");
    }
  }

  @Override
  public void process() throws IOException {
    try {
      LOG.info("Attempting to truncate the table " + tableNameStr);
      handleTruncateTable(tableNameStr);
    } catch (IOException e) {
      LOG.error("Error trying to truncate the table " + tableNameStr, e);
    } catch (ServiceException e) {
      LOG.error("Error trying to truncate the table " + tableNameStr, e);
    } catch (KeeperException e) {
      LOG.error("Error trying to truncate the table " + tableNameStr, e);
    } finally {
      tableLockManager.unlockTable(tableNameStr);
    }
  }

  private void handleTruncateTable(String tableName) throws IOException,
      ServiceException, KeeperException {
    FMetaServices fmetaServices = FMetaReader.getService(server
        .getConfiguration());
    FTable ftable = FMetaReader.getTable(server.getConfiguration(),
        tableNameStr);
    // 1. delete storage table in HBase
    String htablename = StorageTableNameBuilder.buildEntityTableName(ftable
        .getTableName());
    fmetaServices.deleteStorageTable(htablename);

    // 2. create storage table in HBase
    HTableDescriptor desc = fmetaServices.getStorageTableDesc(ftable);
    fmetaServices.createStorageTable(desc);

    // 3. delete Transaction table in HBase
    List<EntityGroupInfo> eginfos = FMetaReader.getTableEntityGroups(
        server.getConfiguration(), table);
    for (EntityGroupInfo eginfo : eginfos) {
      String tTableName = StorageTableNameBuilder
          .buildTransactionTableName(eginfo.getEncodedName());
      if (fmetaServices.storageTableExists(tTableName)) {
        fmetaServices.deleteStorageTable(tTableName);
      }
    }

    // 4. clean Index table in HBase
    LinkedHashMap<String, Index> indexs = ftable.getIndex();
    Iterator<Index> iter = indexs.values().iterator();
    while (iter.hasNext()) {
      Index index = iter.next();
      String htable = StorageTableNameBuilder.buildIndexTableName(index);
      HTableDescriptor htableDesc = fmetaServices.getStorageTableDesc(htable);
      fmetaServices.deleteStorageTable(htable);
      fmetaServices.createStorageTable(htableDesc);
    }

    // unlock table
    tableLockManager.unlockTable(tableNameStr);
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if (server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-"
        + tableNameStr;
  }

}

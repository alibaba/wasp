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

import com.alibaba.wasp.TableExistsException;import com.alibaba.wasp.master.AssignmentManager;import com.alibaba.wasp.master.FServerManager;import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.TableExistsException;
import com.alibaba.wasp.TableLockedException;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.master.AssignmentManager;
import com.alibaba.wasp.master.FMasterServices;
import com.alibaba.wasp.master.FServerManager;
import com.alibaba.wasp.master.TableLockManager;
import com.alibaba.wasp.meta.FMetaEditor;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FTable;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Handler to create a table.
 */
public class CreateTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(CreateTableHandler.class);
  private final AssignmentManager assignmentManager;
  private final FTable table;
  private EntityGroupInfo[] newEntityGroups;
  private final Configuration conf;
  private TableLockManager tableLockManager;

  public CreateTableHandler(Server server, FMasterServices fMasterServices,
      AssignmentManager assignmentManager, FTable table,
      EntityGroupInfo[] newEntityGroups) throws TableExistsException,
      IOException {
    super(server, EventType.C_M_CREATE_TABLE);
    this.table = table;
    this.newEntityGroups = newEntityGroups;
    this.assignmentManager = assignmentManager;
    String tableName = this.table.getTableName();
    this.conf = server.getConfiguration();
    // Check if table exists
    if (FMetaReader.tableExists(conf, tableName)) {
      throw new TableExistsException(tableName);
    }

    // If we have multiple client threads trying to create the table at the
    // same time, given the async nature of the operation, the table
    // could be in a state where .META. table hasn't been updated yet in
    // the process() function.
    // Use enabling state to tell if there is already a request for the same
    // table in progress. This will introduce a new zookeeper call. Given
    // createTable isn't a frequent operation, that should be ok.
    try {
      this.tableLockManager = fMasterServices.getTableLockManager();
      if (tableLockManager.lockTable(tableName)) {
        LOG.info("lock table '" + tableName + "' by CreateIndexHandler");
      } else {
        throw new TableLockedException(tableName + " has been locked. ");
      }
      try {
        if (!this.assignmentManager.getZKTable().checkAndSetEnablingTable(
            tableName))
          throw new TableExistsException(tableName);
      } catch (KeeperException e) {
        tableLockManager.unlockTable(tableName);
        throw new IOException("Unable to ensure that the table will be"
            + " enabling because of a ZooKeeper issue", e);
      }
    } catch (TableExistsException e) {
      tableLockManager.unlockTable(tableName);
      throw e;
    }
  }

  @Override
  public void process() throws IOException {
    String tableName = this.table.getTableName();
    try {
      LOG.info("Attempting to create the table " + tableName);
      handleCreateTable(tableName);
    } catch (IOException e) {
      LOG.error("Error trying to create the table " + tableName, e);
    } catch (ServiceException e) {
      LOG.error("Error trying to create the table " + tableName, e);
    } finally {
      tableLockManager.unlockTable(tableName);
    }
  }

  private void handleCreateTable(String tableName) throws IOException,
      ServiceException {
    // 1. Add Table Info to FMETA
    FMetaEditor.createTable(this.conf, table);
    // 2. Add Table's EntityGroupInfos to FMETA
    List<EntityGroupInfo> entityGroupInfos = Arrays.asList(newEntityGroups);
    if (entityGroupInfos.size() > 0) {
      FMetaEditor.addEntityGroupsToMeta(this.conf, entityGroupInfos);
    }

    // 3. Trigger immediate assignment of the entityGroups in round-robin
    // fashion
    try {
      List<EntityGroupInfo> entityGroups = Arrays.asList(newEntityGroups);
      if (entityGroups.size() > 0) {
        assignmentManager.getEntityGroupStates().createEntityGroupStates(
            entityGroups);
        assignmentManager.assign(entityGroups);
      }
    } catch (InterruptedException ie) {
      LOG.error("Caught " + ie + " during round-robin assignment");
      throw new IOException(ie);
    }

    // 4. Set table enabled flag up in zk.
    try {
      // notify server to refresh the table's meta cache.
      FServerManager serverManager = ((FMasterServices) server)
          .getFServerManager();
      for (ServerName serverName : serverManager.getOnlineServersList()) {
        serverManager.sendEnableTable(serverName, tableName);
      }
      assignmentManager.getZKTable().setEnabledTable(this.table.getTableName());
    } catch (KeeperException e) {
      throw new IOException("Unable to ensure that the table will be"
          + " enabled because of a ZooKeeper issue", e);
    }
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if (server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-"
        + this.table.getTableName();
  }
}
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

import com.alibaba.wasp.ChildrenExistException;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.TableLockedException;
import com.alibaba.wasp.TableNotDisabledException;
import com.alibaba.wasp.TableNotFoundException;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.master.AssignmentManager;
import com.alibaba.wasp.master.FMasterServices;
import com.alibaba.wasp.master.FServerManager;
import com.alibaba.wasp.master.TableLockManager;
import com.alibaba.wasp.meta.FMetaEditor;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Handler to delete a table.
 */
public class DeleteTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(DeleteTableHandler.class);
  private final byte[] tableName;
  private final String tableNameStr;
  private final AssignmentManager assignmentManager;
  private FTable ftable;
  private TableLockManager tableLockManager;

  public DeleteTableHandler(byte[] tableName, FMasterServices server,
      AssignmentManager assignmentManager) throws TableNotFoundException,
      IOException {
    super(server, EventType.C_M_DELETE_TABLE);
    this.tableName = tableName;
    this.tableNameStr = Bytes.toString(tableName);
    this.assignmentManager = assignmentManager;
    this.tableLockManager = server.getTableLockManager();

    // Check if table exists
    if (!FMetaReader.tableExists(server.getConfiguration(), this.tableNameStr)) {
      throw new TableNotFoundException(tableNameStr);
    }
    if (!this.assignmentManager.getZKTable().isDisabledTable(tableNameStr)) {
      throw new TableNotDisabledException(tableNameStr);
    }
    ftable = FMetaReader.getTable(server.getConfiguration(), tableNameStr);
    if (ftable.isRootTable()) {
      List<FTable> childTables = FMetaReader.getChildTable(
          server.getConfiguration(), tableNameStr);
      if (childTables != null && childTables.size() > 0) {
        throw new ChildrenExistException(tableNameStr + " has child");
      }
    }
    if (tableLockManager.lockTable(tableNameStr)) {
      LOG.info("lock table '" + tableNameStr + "' by DeleteTableHandler");
    } else {
      throw new TableLockedException(tableNameStr + " has been locked. ");
    }
  }

  @Override
  public void process() throws IOException {
    try {
      LOG.info("Attempting to delete the table " + this.tableNameStr);
      handleDeleteTable();
    } catch (IOException e) {
      LOG.error("Error trying to delete the table " + this.tableNameStr, e);
    } catch (KeeperException e) {
      LOG.error("Error trying to delete the table " + this.tableNameStr, e);
    } finally {
      tableLockManager.unlockTable(tableNameStr);
    }
  }

  protected void handleDeleteTable()
      throws IOException, KeeperException {
    List<EntityGroupInfo> egis = null;
    if (ftable.isRootTable()) {
      egis = FMetaReader.getTableEntityGroups(
          server.getConfiguration(), Bytes.toBytes(tableNameStr));
    } else {
      egis = new ArrayList<EntityGroupInfo>();
    }

    long waitTime = server.getConfiguration().getLong(
        "wasp.master.wait.on.entityGroup", 5 * 60 * 1000);
    for (EntityGroupInfo entityGroup : egis) {
      long done = System.currentTimeMillis() + waitTime;
      while (System.currentTimeMillis() < done) {
        if (!assignmentManager.getEntityGroupStates().isEntityGroupInTransition(entityGroup))
          break;
        Threads.sleep(waitingTimeForEvents);
        LOG.debug("Waiting on entityGroup to clear entityGroups in transition; "
            + assignmentManager.getEntityGroupStates().getEntityGroupTransitionState(entityGroup));
      }
      if (assignmentManager.getEntityGroupStates().isEntityGroupInTransition(entityGroup)) {
        throw new IOException("Waited wasp.master.wait.on.entityGroup (" + waitTime
            + "ms) for entityGroup to leave entityGroup "
            + entityGroup.getEntityGroupNameAsString() + " in transitions");
      }
      LOG.debug("Deleting entityGroup "
          + entityGroup.getEntityGroupNameAsString() + " from FMETA and FS");
      // Remove entityGroup from FMETA
      FMetaEditor.deleteEntityGroup(server.getConfiguration(), entityGroup);
      // Delete entityGroup from FS

    }
    LOG.debug("Deleting table" + tableNameStr + " from fmeta");
    // Delete table from FMETA
    FMetaEditor.dropTable(server.getConfiguration(), tableNameStr);

    // notify server to clean the table's meta cache.
    FServerManager serverManager = ((FMasterServices) server)
        .getFServerManager();
    for (ServerName serverName : serverManager.getOnlineServersList()) {
      serverManager.sendDisableTable(serverName, tableNameStr);
    }

    LOG.debug("Setting table" + tableNameStr + " deleted on ZK");
    // If entry for this table in zk, and up in AssignmentManager, remove it.
    assignmentManager.getZKTable().setDeletedTable(Bytes.toString(tableName));
    tableLockManager.unlockTable(tableNameStr);
    LOG.debug("Successfully delete table" + tableNameStr);
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

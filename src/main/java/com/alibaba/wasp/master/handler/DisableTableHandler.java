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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.NotAllChildTableDisableException;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.TableLockedException;
import com.alibaba.wasp.TableNotEnabledException;
import com.alibaba.wasp.TableNotFoundException;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.master.AssignmentManager;
import com.alibaba.wasp.master.BulkAssigner;
import com.alibaba.wasp.master.EntityGroupStates;
import com.alibaba.wasp.master.FMasterServices;
import com.alibaba.wasp.master.FServerManager;
import com.alibaba.wasp.master.TableLockManager;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FTable;
import org.apache.zookeeper.KeeperException;
import org.cloudera.htrace.Trace;

/**
 * Handler to run disable of a table.
 */
public class DisableTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(DisableTableHandler.class);
  private final byte[] tableName;
  private final String tableNameStr;
  private final AssignmentManager assignmentManager;
  private TableLockManager tableLockManager;

  public DisableTableHandler(Server server,
      AssignmentManager assignmentManager, byte[] tableName,
      FMasterServices fMasterServices, boolean skipTableStateCheck)
      throws TableNotFoundException, TableNotEnabledException, IOException {
    super(server, EventType.C_M_DISABLE_TABLE);
    this.tableName = tableName;
    this.tableNameStr = Bytes.toString(this.tableName);
    this.assignmentManager = assignmentManager;

    // Check if table exists
    // do we want to keep this in-memory as well? i guess this is
    // part of old master rewrite, schema to zk to check for table
    // existence and such
    if (!FMetaReader.tableExists(server.getConfiguration(), this.tableNameStr)) {
      throw new TableNotFoundException(tableNameStr);
    }
    // There could be multiple client requests trying to disable or enable
    // the table at the same time. Ensure only the first request is honored
    // After that, no other requests can be accepted until the table reaches
    // DISABLED or ENABLED.
    try {
      this.tableLockManager = fMasterServices.getTableLockManager();
      if (tableLockManager.lockTable(tableNameStr)) {
        LOG.info("lock table '" + tableNameStr + "' by DisableTableHandler");
      } else {
        throw new TableLockedException(tableNameStr + " has been locked. ");
      }
      if (!skipTableStateCheck) {
        try {
          if (!this.assignmentManager.getZKTable()
              .checkEnabledAndSetDisablingTable(this.tableNameStr)) {
            LOG.info("Table " + tableNameStr
                + " isn't enabled; skipping disable");
            throw new TableNotEnabledException(this.tableNameStr);
          }
        } catch (KeeperException e) {
          tableLockManager.unlockTable(tableNameStr);
          throw new IOException("Unable to ensure that the table will be"
              + " disabling because of a ZooKeeper issue", e);
        }
      }
    } catch (TableNotEnabledException e) {
      tableLockManager.unlockTable(tableNameStr);
      throw e;
    }
  }

  @Override
  public void process() throws IOException {
    try {
      LOG.info("Attempting to disable table " + this.tableNameStr);
      handleDisableTable();
    } catch (IOException e) {
      LOG.error("Error trying to disable table " + this.tableNameStr, e);
    } catch (KeeperException e) {
      LOG.error("Error trying to disable table " + this.tableNameStr, e);
    } finally {
      tableLockManager.unlockTable(tableNameStr);
    }
  }

  private void handleDisableTable() throws IOException, KeeperException {
    // Set table disabling flag up in zk.
    this.assignmentManager.getZKTable().setDisablingTable(this.tableNameStr);
    boolean done = false;
    FTable ftable = FMetaReader.getTable(server.getConfiguration(),
        tableNameStr);
    // Child Table
    if (ftable.isChildTable()) {
      done = true;
    } else {
      List<FTable> childs = FMetaReader.getChildTable(
          server.getConfiguration(), tableNameStr);
      if (childs != null && childs.size() > 0) {
        Set<String> disableTables = this.assignmentManager.getZKTable()
            .getDisabledTables();
        StringBuilder tableNames = new StringBuilder();
        for (FTable child : childs) {
          if (!disableTables.contains(child.getTableName())) {
            tableNames.append(child.getTableName()).append(",");
          }
        }
        if (tableNames.length() > 0) {
          throw new NotAllChildTableDisableException(tableNames.toString()
              .replaceAll("$,", ""));
        }
      }
    }
    while (true) {
      if (done) {
        break;
      }
      // Get list of online entityGroups that are of this table. EntityGroups
      // that are
      // already closed will not be included in this list; i.e. the returned
      // list is not ALL entityGroups in a table, its all online entityGroups
      // according
      // to the in-memory state on this master.
      final List<EntityGroupInfo> entityGroups = this.assignmentManager
          .getEntityGroupStates().getEntityGroupsOfTable(tableName);
      if (entityGroups.size() == 0) {
        done = true;
        break;
      }
      LOG.info("Offlining " + tableNameStr + "'s " + entityGroups.size()
          + " entityGroups.");
      BulkDisabler bd = new BulkDisabler(this.server, entityGroups);
      try {
        if (bd.bulkAssign()) {
          done = true;
          break;
        }
      } catch (InterruptedException e) {
        LOG.warn("Disable was interrupted");
        // Preserve the interrupt.
        Thread.currentThread().interrupt();
        break;
      }
    }

    // notify server to clean the table's meta cache.
    FServerManager serverManager = ((FMasterServices) server)
        .getFServerManager();
    for (ServerName serverName : serverManager.getOnlineServersList()) {
      serverManager.sendDisableTable(serverName, Bytes.toString(tableName));
    }
    // Flip the table to disabled if success.
    if (done) {
      this.assignmentManager.getZKTable().setDisabledTable(this.tableNameStr);
    } else {
      this.assignmentManager.getZKTable().setEnabledTable(this.tableNameStr);
    }
    LOG.info("Disabled table '" + tableNameStr + "' is done=" + done);
  }

  /**
   * Run bulk disable.
   */
  class BulkDisabler extends BulkAssigner {
    private final List<EntityGroupInfo> entityGroups;

    BulkDisabler(final Server server, final List<EntityGroupInfo> entityGroups) {
      super(server);
      this.entityGroups = entityGroups;
    }

    @Override
    protected void populatePool(ExecutorService pool) {
      EntityGroupStates entityGroupStates = assignmentManager
          .getEntityGroupStates();
      for (EntityGroupInfo entityGroup : entityGroups) {
        if (entityGroupStates.isEntityGroupInTransition(entityGroup))
          continue;
        final EntityGroupInfo egi = entityGroup;
        pool.execute(Trace.wrap(new Runnable() {
          public void run() {
            assignmentManager.unassign(egi);
          }
        }));
      }
    }

    @Override
    protected boolean waitUntilDone(long timeout) throws InterruptedException {
      long startTime = System.currentTimeMillis();
      long remaining = timeout;
      List<EntityGroupInfo> entityGroups = null;
      while (!server.isStopped() && remaining > 0) {
        Thread.sleep(waitingTimeForEvents);
        entityGroups = assignmentManager.getEntityGroupStates()
            .getEntityGroupsOfTable(tableName);
        if (entityGroups.isEmpty())
          break;
        remaining = timeout - (System.currentTimeMillis() - startTime);
      }
      return entityGroups != null && entityGroups.isEmpty();
    }
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

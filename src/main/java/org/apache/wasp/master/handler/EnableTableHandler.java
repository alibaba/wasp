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
package org.apache.wasp.master.handler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.Server;
import org.apache.wasp.ServerName;
import org.apache.wasp.TableLockedException;
import org.apache.wasp.TableNotDisabledException;
import org.apache.wasp.TableNotFoundException;
import org.apache.wasp.executor.EventHandler;
import org.apache.wasp.master.AssignmentManager;
import org.apache.wasp.master.BulkAssigner;
import org.apache.wasp.master.EntityGroupPlan;
import org.apache.wasp.master.EntityGroupStates;
import org.apache.wasp.master.FMaster;
import org.apache.wasp.master.FMasterServices;
import org.apache.wasp.master.FServerManager;
import org.apache.wasp.master.TableLockManager;
import org.apache.wasp.meta.FMetaReader;
import org.apache.wasp.meta.FTable;
import org.apache.zookeeper.KeeperException;
import org.cloudera.htrace.Trace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Handler to run enable of a table.
 */
public class EnableTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(EnableTableHandler.class);
  private final byte[] tableName;
  private final String tableNameStr;
  private final AssignmentManager assignmentManager;
  private boolean retainAssignment = false;
  private boolean isChild = false;
  private TableLockManager tableLockManager;

  public EnableTableHandler(Server server, FMasterServices fMasterServices,
      AssignmentManager assignmentManager, byte[] tableName,
      boolean skipTableStateCheck) throws TableNotFoundException,
      TableNotDisabledException, IOException {
    super(server, EventType.C_M_ENABLE_TABLE);
    this.tableName = tableName;
    this.tableNameStr = Bytes.toString(tableName);
    this.assignmentManager = assignmentManager;
    this.retainAssignment = skipTableStateCheck;
    // Check if table exists
    if (!FMetaReader.tableExists(server.getConfiguration(), this.tableNameStr)) {
      throw new TableNotFoundException(tableNameStr);
    }
    FTable ftable = FMetaReader.getTable(server.getConfiguration(),
        tableNameStr);
    // Child Table
    if (ftable.isChildTable()) {
      isChild = true;
    }
    // There could be multiple client requests trying to disable or enable
    // the table at the same time. Ensure only the first request is honored
    // After that, no other requests can be accepted until the table reaches
    // DISABLED or ENABLED.
    try {
      this.tableLockManager = fMasterServices.getTableLockManager();
      if (tableLockManager.lockTable(tableNameStr)) {
        LOG.info("lock table '" + tableNameStr + "' by EnableTableHandler");
      } else {
        throw new TableLockedException(tableNameStr + " has been locked. ");
      }
      if (!skipTableStateCheck) {
        try {
          if (!this.assignmentManager.getZKTable()
              .checkDisabledAndSetEnablingTable(this.tableNameStr)) {
            LOG.info("Table " + tableNameStr
                + " isn't disabled; skipping enable");
            throw new TableNotDisabledException(this.tableNameStr);
          }
        } catch (KeeperException e) {
          tableLockManager.unlockTable(tableNameStr);
          throw new IOException("Unable to ensure that the table will be"
              + " enabling because of a ZooKeeper issue", e);
        }
      }
    } catch (TableNotDisabledException e) {
      tableLockManager.unlockTable(tableNameStr);
      throw e;
    }
  }

  @Override
  public void process() throws IOException {
    try {
      LOG.info("Attempting to enable the table " + this.tableNameStr);
      handleEnableTable();
    } catch (IOException e) {
      LOG.error("Error trying to enable the table " + this.tableNameStr, e);
    } catch (KeeperException e) {
      LOG.error("Error trying to enable the table " + this.tableNameStr, e);
    } catch (InterruptedException e) {
      LOG.error("Error trying to enable the table " + this.tableNameStr, e);
    } finally {
      tableLockManager.unlockTable(tableNameStr);
    }
  }

  private void handleEnableTable() throws IOException, KeeperException,
      InterruptedException {
    // I could check table is disabling and if so, not enable but require
    // that user first finish disabling but that might be obnoxious.

    // Set table enabling flag up in zk.
    this.assignmentManager.getZKTable().setEnablingTable(this.tableNameStr);
    boolean done = false;
    if (isChild) {
      done = true;
    } else {
      // Get the entityGroups of this table. We're done when all listed
      // tables are onlined.
      List<Pair<EntityGroupInfo, ServerName>> tableEntityGroupsAndLocations = FMetaReader
          .getTableEntityGroupsAndLocations(server.getConfiguration(),
              tableName, true);
      int countOfEntityGroupsInTable = tableEntityGroupsAndLocations.size();
      List<EntityGroupInfo> entityGroups = entityGroupsToAssignWithServerName(tableEntityGroupsAndLocations);
      int entityGroupsCount = entityGroups.size();
      if (entityGroupsCount == 0) {
        done = true;
      }
      LOG.info("Table '" + this.tableNameStr + "' has "
          + countOfEntityGroupsInTable + " entityGroups, of which "
          + entityGroupsCount + " are offline.");
      BulkEnabler bd = new BulkEnabler(this.server, entityGroups,
          countOfEntityGroupsInTable, this.retainAssignment);
      try {
        if (bd.bulkAssign()) {
          done = true;
        }
      } catch (InterruptedException e) {
        LOG.warn("Enable operation was interrupted when enabling table '"
            + this.tableNameStr + "'");
        // Preserve the interrupt.
        Thread.currentThread().interrupt();
      }
    }

    // notify server to refresh the table's meta cache.
    FServerManager serverManager = ((FMasterServices) server)
        .getFServerManager();
    for (ServerName serverName : serverManager.getOnlineServersList()) {
      serverManager.sendEnableTable(serverName, Bytes.toString(tableName));
    }

    if (done) {
      // Flip the table to enabled.
      this.assignmentManager.getZKTable().setEnabledTable(this.tableNameStr);
      LOG.info("Table '" + this.tableNameStr
          + "' was successfully enabled. Status: done=" + done);
    } else {
      LOG.warn("Table '" + this.tableNameStr
          + "' wasn't successfully enabled. Status: done=" + done);
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

  /**
   * @param entityGroupsInMeta
   * @return List of entityGroups neither in transition nor assigned.
   * @throws IOException
   */
  private List<EntityGroupInfo> entityGroupsToAssignWithServerName(
      final List<Pair<EntityGroupInfo, ServerName>> entityGroupsInMeta)
      throws IOException {
    FServerManager serverManager = ((FMaster) this.server).getFServerManager();
    List<EntityGroupInfo> entityGroups = new ArrayList<EntityGroupInfo>();
    EntityGroupStates entityGroupStates = this.assignmentManager
        .getEntityGroupStates();
    for (Pair<EntityGroupInfo, ServerName> entityGroupLocation : entityGroupsInMeta) {
      EntityGroupInfo egi = entityGroupLocation.getFirst();
      ServerName sn = entityGroupLocation.getSecond();
      if (!entityGroupStates.isEntityGroupInTransition(egi)
          && !entityGroupStates.isEntityGroupAssigned(egi)) {
        if (this.retainAssignment && sn != null
            && serverManager.isServerOnline(sn)) {
          this.assignmentManager.addPlan(egi.getEncodedName(),
              new EntityGroupPlan(egi, null, sn));
        }
        entityGroups.add(egi);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping assign for the entityGroup " + egi
              + " during enable table " + egi.getTableNameAsString()
              + " because its already in tranition or assigned.");
        }
      }
    }
    return entityGroups;
  }

  /**
   * Run bulk enable.
   */
  class BulkEnabler extends BulkAssigner {
    private final List<EntityGroupInfo> entityGroups;
    // Count of entityGroups in table at time this assign was launched.
    private final int countOfEntityGroupsInTable;
    private final boolean retainAssignment;

    BulkEnabler(final Server server, final List<EntityGroupInfo> entityGroups,
        final int countOfEntityGroupsInTable, boolean retainAssignment) {
      super(server);
      this.entityGroups = entityGroups;
      this.countOfEntityGroupsInTable = countOfEntityGroupsInTable;
      this.retainAssignment = retainAssignment;
    }

    @Override
    protected void populatePool(ExecutorService pool) throws IOException {
      boolean roundRobinAssignment = this.server.getConfiguration().getBoolean(
          "wasp.master.enabletable.roundrobin", false);

      // In case of masterRestart always go with single assign. Going thro
      // roundRobinAssignment will use bulkassign which may lead to double
      // assignment.
      if (retainAssignment || !roundRobinAssignment) {
        for (EntityGroupInfo entityGroup : entityGroups) {
          if (assignmentManager.getEntityGroupStates()
              .isEntityGroupInTransition(entityGroup)) {
            continue;
          }
          final EntityGroupInfo egi = entityGroup;
          pool.execute(Trace.wrap(new Runnable() {
            public void run() {
              assignmentManager.assign(egi, true);
            }
          }));
        }
      } else {
        try {
          assignmentManager.assign(entityGroups);
        } catch (InterruptedException e) {
          LOG.warn("Assignment was interrupted");
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    protected boolean waitUntilDone(long timeout) throws InterruptedException {
      long startTime = System.currentTimeMillis();
      long remaining = timeout;
      List<EntityGroupInfo> entityGroups = null;
      int lastNumberOfEntityGroups = 0;
      while (!server.isStopped() && remaining > 0) {
        Thread.sleep(waitingTimeForEvents);
        entityGroups = assignmentManager.getEntityGroupStates()
            .getEntityGroupsOfTable(tableName);
        if (isDone(entityGroups))
          break;

        // Punt on the timeout as long we make progress
        if (entityGroups.size() > lastNumberOfEntityGroups) {
          lastNumberOfEntityGroups = entityGroups.size();
          timeout += waitingTimeForEvents;
        }
        remaining = timeout - (System.currentTimeMillis() - startTime);
      }
      return isDone(entityGroups);
    }

    private boolean isDone(final List<EntityGroupInfo> entityGroups) {
      return entityGroups != null
          && entityGroups.size() >= this.countOfEntityGroupsInTable;
    }
  }

}

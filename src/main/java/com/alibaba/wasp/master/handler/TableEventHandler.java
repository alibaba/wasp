/**
 *
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

import com.alibaba.wasp.EntityGroupInfo;import com.alibaba.wasp.Server;import com.alibaba.wasp.ServerName;import com.alibaba.wasp.TableNotDisabledException;import com.alibaba.wasp.executor.EventHandler;import com.alibaba.wasp.master.BulkReOpen;import com.alibaba.wasp.meta.FMetaReader;import com.alibaba.wasp.meta.FMetaScanner;import com.alibaba.wasp.meta.FTable;import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.TableNotDisabledException;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.master.BulkReOpen;
import com.alibaba.wasp.master.FMasterServices;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FMetaScanner;
import com.alibaba.wasp.meta.FTable;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Base class for performing operations against tables. Checks on whether the
 * process can go forward are done in constructor rather than later on in
 * {@link #process()}. The idea is to fail fast rather than later down in an
 * async invocation of {@link #process()} (which currently has no means of
 * reporting back issues once started).
 */
public abstract class TableEventHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(TableEventHandler.class);
  protected final byte[] tableName;
  protected final String tableNameStr;
  protected boolean persistedToZk = false;
  protected final FMasterServices fMasterServices;

  public TableEventHandler(final byte[] tableName, Server server,
      FMasterServices fMasterServices, EventType eventType) throws IOException {
    super(server, eventType);
    this.tableName = tableName;
    this.tableNameStr = Bytes.toString(this.tableName);
    this.fMasterServices = fMasterServices;
    try {
      this.fMasterServices.checkTableModifiable(tableName);
    } catch (TableNotDisabledException ex) {
      if (isOnlineSchemaChangeAllowed()
          && eventType.isOnlineSchemaChangeSupported()) {
        LOG.debug("Ignoring table not disabled exception "
            + "for supporting online schema changes.");
      } else {
        throw ex;
      }
    }
  }

  private boolean isOnlineSchemaChangeAllowed() {
    return this.server.getConfiguration().getBoolean(
        "wasp.online.schema.update.enable", false);
  }

  @Override
  public void process() {
    try {
      LOG.info("Handling table operation " + eventType + " on table "
          + tableNameStr);
      List<EntityGroupInfo> egis = FMetaReader.getTableEntityGroups(
          server.getConfiguration(), Bytes.toBytes(tableNameStr));
      handleTableOperation(egis);
      if (eventType.isOnlineSchemaChangeSupported()
          && this.fMasterServices.getAssignmentManager().getZKTable()
              .isEnabledTable(tableNameStr)) {
        if (reOpenAllEntityGroups(egis)) {
          LOG.info("Completed table operation " + eventType + " on table "
              + tableNameStr);
        } else {
          LOG.warn("Error on reopening the entityGroups");
        }
      }
    } catch (IOException e) {
      LOG.error("Error manipulating table " + tableNameStr, e);
    } catch (KeeperException e) {
      LOG.error("Error manipulating table " + tableNameStr, e);
    } finally {
      // notify the waiting thread that we're done persisting the request
      setPersist();
    }
  }

  public boolean reOpenAllEntityGroups(List<EntityGroupInfo> entityGroups)
      throws IOException {
    boolean done = false;
    LOG.info("Bucketing entityGroups by entityGroup server...");
    TreeMap<ServerName, List<EntityGroupInfo>> serverToEntityGroups = Maps
        .newTreeMap();
    NavigableMap<EntityGroupInfo, ServerName> egiHserverMapping = FMetaScanner
        .allTableEntityGroups(server.getConfiguration(), tableName, false);

    List<EntityGroupInfo> reEntityGroups = new ArrayList<EntityGroupInfo>();
    for (EntityGroupInfo egi : entityGroups) {
      ServerName egLocation = egiHserverMapping.get(egi);

      // Skip the offlined split parent EntityGroup
      if (null == egLocation) {
        LOG.info("Skip " + egi);
        continue;
      }
      if (!serverToEntityGroups.containsKey(egLocation)) {
        LinkedList<EntityGroupInfo> egiList = Lists.newLinkedList();
        serverToEntityGroups.put(egLocation, egiList);
      }
      reEntityGroups.add(egi);
      serverToEntityGroups.get(egLocation).add(egi);
    }

    LOG.info("Reopening " + reEntityGroups.size() + " entityGroups on "
        + serverToEntityGroups.size() + " fservers.");
    this.fMasterServices.getAssignmentManager().setEntityGroupsToReopen(
        reEntityGroups);
    BulkReOpen bulkReopen = new BulkReOpen(this.server, serverToEntityGroups,
        this.fMasterServices.getAssignmentManager());
    while (true) {
      try {
        if (bulkReopen.bulkReOpen()) {
          done = true;
          break;
        } else {
          LOG.warn("Timeout before reopening all entityGroups");
        }
      } catch (InterruptedException e) {
        LOG.warn("Reopen was interrupted");
        // Preserve the interrupt.
        Thread.currentThread().interrupt();
        break;
      }
    }
    return done;
  }

  /**
   * Table modifications are processed asynchronously, but provide an API for
   * you to query their status.
   * 
   * @throws IOException
   */
  public synchronized void waitForPersist() throws IOException {
    if (!persistedToZk) {
      try {
        wait();
      } catch (InterruptedException ie) {
        throw (IOException) new InterruptedIOException().initCause(ie);
      }
      assert persistedToZk;
    }
  }

  private synchronized void setPersist() {
    if (!persistedToZk) {
      persistedToZk = true;
      notify();
    }
  }

  /**
   * @return Table Info for this table
   * @throws IOException
   */
  FTable getTable() throws IOException {
    FTable table = FMetaReader
        .getTable(server.getConfiguration(), tableNameStr);
    if (table == null) {
      throw new IOException("Table missing for " + tableNameStr);
    }
    return table;
  }

  protected abstract void handleTableOperation(
      List<EntityGroupInfo> entityGroups) throws IOException, KeeperException;
}

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
import com.alibaba.wasp.Server;
import com.alibaba.wasp.TableLockedException;
import com.alibaba.wasp.master.FMasterServices;
import com.alibaba.wasp.master.TableLockManager;
import com.alibaba.wasp.meta.FMetaEditor;
import com.alibaba.wasp.meta.FTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

/**
 * Handler to modify a table.
 */
public class ModifyTableHandler extends TableEventHandler {
  private static final Log LOG = LogFactory.getLog(ModifyTableHandler.class);
  private final FTable table;
  private TableLockManager tableLockManager;

  public ModifyTableHandler(final byte[] tableName, final FTable table,
      Server server, FMasterServices fMasterServices) throws IOException {
    super(tableName, server, fMasterServices, EventType.C_M_MODIFY_TABLE);
    // Check table exists.
    getTable();
    // This is the new schema we are going to write out as this modification.
    this.table = table;
    this.tableLockManager = fMasterServices.getTableLockManager();
    if (tableLockManager.lockTable(tableNameStr)) {
      LOG.info("lock table '" + tableNameStr + "' by ModifyTableHandler");
    } else {
      throw new TableLockedException(tableNameStr + " has been locked. ");
    }
  }

  @Override
  protected void handleTableOperation(List<EntityGroupInfo> egis)
      throws IOException {
    try {
      // Update table info
      FMetaEditor.alterTable(server.getConfiguration(), tableNameStr,
          this.table);
    } finally {
      tableLockManager.unlockTable(tableNameStr);
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
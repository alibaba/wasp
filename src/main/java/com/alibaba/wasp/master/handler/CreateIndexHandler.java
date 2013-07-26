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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.TableLockedException;
import com.alibaba.wasp.master.FMasterServices;
import com.alibaba.wasp.master.TableLockManager;
import com.alibaba.wasp.meta.FMetaEditor;
import com.alibaba.wasp.meta.Index;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

/**
 * Handler to create a index.
 */
public class CreateIndexHandler extends TableEventHandler {
  private static final Log LOG = LogFactory.getLog(CreateIndexHandler.class);
  private Index index;
  private TableLockManager tableLockManager;

  /**
   * @param tableName
   * @param server
   * @param fMasterServices
   * @param eventType
   * @throws IOException
   */
  public CreateIndexHandler(byte[] tableName, Index index, Server server,
      FMasterServices fMasterServices, EventType eventType) throws IOException {
    super(tableName, server, fMasterServices, eventType);
    this.tableLockManager = fMasterServices.getTableLockManager();
    if (tableLockManager.lockTable(tableNameStr)) {
      LOG.info("lock table '" + tableNameStr + "' by CreateIndexHandler");
    } else {
      throw new TableLockedException(tableNameStr + " has been locked. ");
    }
    this.index = index;
  }

  /**
   * @see com.alibaba.wasp.master.handler.TableEventHandler#handleTableOperation(java.util.List)
   */
  @Override
  protected void handleTableOperation(List<EntityGroupInfo> entityGroups)
      throws IOException, KeeperException {
    try {
      FMetaEditor.addIndex(server.getConfiguration(),
          Bytes.toString(tableName), index);
    } finally {
      tableLockManager.unlockTable(tableNameStr);
    }
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "CreateIndexHandler [index=" + index + "]";
  }
}
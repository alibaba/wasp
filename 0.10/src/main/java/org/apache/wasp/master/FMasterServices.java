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
package org.apache.wasp.master;

import java.io.IOException;

import org.apache.wasp.Server;
import org.apache.wasp.TableNotDisabledException;
import org.apache.wasp.TableNotFoundException;
import org.apache.wasp.executor.ExecutorService;
import org.apache.wasp.meta.FTable;

/**
 * Services FMaster supplies
 */
public interface FMasterServices extends Server {
  /**
   * @return Master's instance of the {@link AssignmentManager}
   */
  public AssignmentManager getAssignmentManager();

  /**
   * @return Master's {@link FServerManager} instance.
   */
  public FServerManager getFServerManager();

  /**
   * @return Master's instance of {@link ExecutorService}
   */
  public ExecutorService getExecutorService();

  /**
   * Check table is modifiable; i.e. exists and is offline.
   * 
   * @param tableName
   *          Name of table to check.
   * @throws TableNotDisabledException
   * @throws TableNotFoundException
   */
  public void checkTableModifiable(final byte[] tableName) throws IOException;

  /**
   * Create a table using the given table definition.
   * @param desc The table definition
   * @param splitKeys Starting row keys for the initial table entityGroups. If null a
   *          single entityGroup is created.
   */
  public void createTable(FTable desc, byte[][] splitKeys)
      throws IOException;

  /**
   * @return Master's instance of the {@link TableLockManager}
   */
  public TableLockManager getTableLockManager();
}

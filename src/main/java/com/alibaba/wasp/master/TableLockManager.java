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
package com.alibaba.wasp.master;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * 
 * Lock table when do some DDL
 * 
 */
public class TableLockManager {

  private Set<String> lockTables = Collections
      .synchronizedSet(new HashSet<String>());
  
  public TableLockManager() {
  }

  /**
   * lock table, do not wait for ever
   * 
   */
  public synchronized boolean lockTable(String table) {
    if (lockTables.contains(table)) {
      return false;
    } else {
      lockTables.add(table);
      return true;
    }
  }

  /**
   * unlock table
   */
  public synchronized void unlockTable(String table) {
    lockTables.remove(table);
  }

  public boolean isTableLocked(String table) {
    if (lockTables.contains(table)) {
      return true;
    } else {
      return false;
    }
  }

}

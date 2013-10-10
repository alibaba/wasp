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
package com.alibaba.wasp.plan;

import java.util.List;

/**
 * Drop table;Use case is 'DROP TABLE IF EXISTS B,C,A;';
 * 
 */
public class DropTablePlan extends DDLPlan {

  private List<String> tableNames;

  private boolean ifExists = false;

  public DropTablePlan(List<String> tableNames) {
    this.tableNames = tableNames;
  }

  public List<String> getTableNames() {
    return tableNames;
  }

  public void setTableNames(List<String> tableNames) {
    this.tableNames = tableNames;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  public void setIfExists(boolean ifExists) {
    this.ifExists = ifExists;
  }

  /**
   * @see Object#toString()
   */
  @Override
  public String toString() {
    return "DropTablePlan [tableNames=" + tableNames + ", ifExists=" + ifExists
        + "]";
  }
}
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
package org.apache.wasp.plan;

import org.apache.wasp.meta.FTable;

/**
 * The plan to alter table. Use case<DELETE FROM t1 WHERE id = 'value'>
 * 
 */
public class AlterTablePlan extends DDLPlan {

  private FTable oldTable;
  private FTable newTable;

  public AlterTablePlan(FTable oldTable, FTable newTable) {
    this.oldTable = oldTable;
    this.newTable = newTable;
  }

  public FTable getOldTable() {
    return oldTable;
  }

  public void setOldTable(FTable oldTable) {
    this.oldTable = oldTable;
  }

  public FTable getNewTable() {
    return newTable;
  }

  public void setNewTable(FTable newTable) {
    this.newTable = newTable;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "AlterTablePlan [oldTable=" + oldTable + ", newTable=" + newTable
        + "]";
  }
}
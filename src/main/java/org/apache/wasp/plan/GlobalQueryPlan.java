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

/**
 * Driver parses sql to plan which includes table's global query plan normal
 * is "select * from t where a=1;".A global index spans entity groups. It is used to find
 * entities without knowing in advance the entity groups that contain them. Global index scans can 
 * read data owned by many entity groups but are not guaranteed to reflect all recent updates.
 */
import org.apache.wasp.meta.FTable;
import org.apache.wasp.plan.action.ScanAction;

public class GlobalQueryPlan extends DQLPlan {

  private ScanAction action;

  private FTable tableDesc;

  public GlobalQueryPlan(ScanAction action, FTable table) {
    this.action = action;
    this.tableDesc = table;
  }

  /**
   * @return the action
   */
  public ScanAction getAction() {
    return action;
  }

  /**
   * @param action
   *          the action to set
   */
  public void setAction(ScanAction action) {
    this.action = action;
  }

  /**
   * @return the tableDesc
   */
  public FTable getTableDesc() {
    return tableDesc;
  }

  /**
   * @param tableDesc
   *          the tableDesc to set
   */
  public void setTableDesc(FTable tableDesc) {
    this.tableDesc = tableDesc;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "GlobalQueryPlan [action=" + action + ", tableDesc=" + tableDesc
        + ", fetchRows=" + this.getFetchRows() + "]";
  }
}
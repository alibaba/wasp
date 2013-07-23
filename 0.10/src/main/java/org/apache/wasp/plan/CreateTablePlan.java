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

import java.util.Arrays;

import org.apache.wasp.meta.FTable;

/**
 * The plan to create table.
 * 
 */
public class CreateTablePlan extends DDLPlan {

  private FTable table;

  private byte[][] splitKeys;

  public CreateTablePlan(FTable table, byte[][] splitKeys) {
    this.table = table;
    this.splitKeys = splitKeys;
  }

  public FTable getTable() {
    return table;
  }

  public void setTable(FTable table) {
    this.table = table;
  }

  /**
   * @return the splitKeys
   */
  public byte[][] getSplitKeys() {
    return splitKeys;
  }

  /**
   * @param splitKeys
   *          the splitKeys to set
   */
  public void setSplitKeys(byte[][] splitKeys) {
    this.splitKeys = splitKeys;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "CreateTablePlan [table=" + table + ", splitKeys="
        + Arrays.toString(splitKeys) + "]";
  }
}
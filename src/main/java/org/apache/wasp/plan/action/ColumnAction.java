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
package org.apache.wasp.plan.action;

import org.apache.wasp.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ColumnAction extends Action {

  private List<ColumnStruct> columns = new ArrayList<ColumnStruct>();
  private final Map<String, ColumnStruct> name2Column = new HashMap<String, ColumnStruct>();

  /**
   * 
   * @return
   */
  public List<ColumnStruct> getColumns() {
    return columns;
  }

  /**
   * Method for checking if any columns have been inserted into this Get
   * 
   * @return true if columns is non empty false otherwise
   */
  public boolean hasColumns() {
    return !this.columns.isEmpty();
  }

  /**
   * 
   * @param columns
   */
  public void setColumns(List<ColumnStruct> columns) {
    this.columns = columns;
    for (ColumnStruct col : columns) {
      name2Column.put(col.getColumnName(), col);
    }
  }

  /**
   * @return the name2Column
   */
  public Map<String, ColumnStruct> getName2Column() {
    return name2Column;
  }

  /**
   * Generate ColumnStruct instance and then add it to list.
   * 
   * @param tableName
   * @param familyName
   * @param columnName
   * @param value
   * @param cols
   */
  public void addEntityColumn(String tableName, String familyName,
      String columnName, DataType dataType, byte[] value) {
    ColumnStruct col = new ColumnStruct(tableName, familyName, columnName,
       dataType, value);
    columns.add(col);
    name2Column.put(col.getColumnName(), col);
  }

  /**
   * Generate ColumnAction instance and then add it to list.
   * 
   * @param fTableName
   * @param familyName
   * @param columnName
   * @param value
   * @param cols
   */
  public void addEntityColumn(ColumnStruct col) {
    columns.add(col);
    name2Column.put(col.getColumnName(), col);
  }
}
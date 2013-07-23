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
package org.apache.wasp.client;

import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.wasp.DataType;
import org.apache.wasp.fserver.OperationStatus;

public class QueryResult implements ExecuteResult {

  private final Map<String, Pair<DataType, byte[]>> cloumnValues;

  /**
   * @param results
   */
  public QueryResult(Map<String, Pair<DataType, byte[]>> cloumnValues) {
    this.cloumnValues = cloumnValues;
  }


  @Override
  public OperationStatus getStatus() {
    throw new UnsupportedOperationException("Unsupported");
  }

  /**
   * It is a query result
   * @return true
   */
  @Override
  public boolean isQuery() {
    return true;
  }

  /**
   * Get result of the given column
   * @param columnName
   * @return pair of data type and value
   */
  @Override
  public Pair<DataType, byte[]> getColumn(String columnName) {
    return cloumnValues.get(columnName);
  }

  /**
   * Get the byte data of given column
   * @param columnName
   * @return byte data of column
   */
  @Override
  public byte[] getValue(String columnName) {
    return cloumnValues.get(columnName).getSecond();
  }

  /**
   * @return true if empty
   */
  @Override
  public boolean isEmpty() {
    return cloumnValues == null || cloumnValues.isEmpty();
  }

  /**
   * Map of columns to pair of its data type and value
   * @return map from column to pair of its data type and value
   */
  @Override
  public Map<String, Pair<DataType, byte[]>> getMap() {
    return this.cloumnValues;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "QueryResult [results=" + cloumnValues + "]";
  }
}
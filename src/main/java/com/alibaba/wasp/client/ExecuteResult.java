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
package com.alibaba.wasp.client;

import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;
import com.alibaba.wasp.DataType;
import com.alibaba.wasp.fserver.OperationStatus;

/**
 * Supper class of QueryResult and WriteResult.
 * 
 */
public interface ExecuteResult {
  /**
   * @return the status
   */
  public OperationStatus getStatus();

  /**
   * Whether it is a query execute
   * @return true if it is an execute result for query
   */
  public boolean isQuery();

  /**
   * Get result of the given column
   * @param columnName
   * @return pair of data type and value
   */
  public Pair<DataType, byte[]> getColumn(String columnName);

  /**
   * Get the byte data of given column
   * @param columnName
   * @return byte data of column
   */
  public byte[] getValue(String columnName);

  /**
   * @return true if empty
   */
  public boolean isEmpty();

  /**
   * Map of columns to pair of its data type and value
   * @return map from column to pair of its data type and value
   */
  public Map<String, Pair<DataType, byte[]>> getMap();
}
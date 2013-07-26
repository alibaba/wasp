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

public class WriteResult implements ExecuteResult {

  private OperationStatus status;

  /**
   * @param status
   */
  public WriteResult(OperationStatus status) {
    this.status = status;
  }

  /**
   * @return the status
   */
  @Override
  public OperationStatus getStatus() {
    return status;
  }

  @Override
  public boolean isQuery() {
    return false;
  }

  @Override
  public Pair<DataType, byte[]> getColumn(String columnName) {
    throw new UnsupportedOperationException("Unsupported");
  }

  @Override
  public byte[] getValue(String columnName) {
    throw new UnsupportedOperationException("Unsupported");
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException("Unsupported");
  }

  @Override
  public Map<String, Pair<DataType, byte[]>> getMap() {
    throw new UnsupportedOperationException("Unsupported");
  }
}
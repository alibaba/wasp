/**
 * Copyright 2011 The Apache Software Foundation
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

package com.alibaba.wasp.coprocessor;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;

/**
 * Defines the aggregation functions that are to be supported in this
 * Coprocessor. For each method, it takes a Scan object and a columnInterpreter.
 * The scan object should have a column family (else an exception will be
 * thrown), and an optional column qualifier. In the current implementation
 * {@link org.apache.hadoop.hbase.coprocessor.AggregateImplementation}, only one column family and column qualifier
 * combination is served. In case there are more than one, only first one will
 * be picked. Refer to {@link org.apache.hadoop.hbase.client.coprocessor.AggregationClient} for some general conditions on
 * input parameters.
 */
public interface WaspAggregateProtocol extends CoprocessorProtocol {
  public static final long VERSION = 1L;



  /**
   *
   * @param ci
   * @param scan
   * @return sum of values as defined by the column interpreter
   * @throws java.io.IOException
   */
  <T, S> S getSum(ColumnInterpreter<T, S> ci, Scan scan, byte[] qualifier) throws IOException;

  /**
   * @param ci
   * @param scan
   * @return Row count for the given column family and column qualifier, in
   * the given row range as defined in the Scan object.
   * @throws java.io.IOException
   */
  <T, S> long getRowNum(ColumnInterpreter<T, S> ci, Scan scan)
      throws IOException;



}

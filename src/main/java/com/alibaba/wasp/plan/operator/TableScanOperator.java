/**
 * Copyright 2010 The Apache Software Foundation
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
package com.alibaba.wasp.plan.operator;

import org.apache.hadoop.hbase.util.Bytes;

public class TableScanOperator extends Operator {

  private final String table;

  private final byte[] startKey;

  private final byte[] endKey;

  public TableScanOperator(String table, byte[] startKey, byte[] endKey) {
    super();
    this.table = table;
    this.startKey = startKey;
    this.endKey = endKey;
  }

  @Override
  public String getName() {
    return "TableScanOperator," + table + "," + Bytes.toString(startKey) + ","
        + Bytes.toString(endKey);
  }

  @Override
  public Object forward(Object obj) {
    // TODO Auto-generated method stub
    return null;
  }

}

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
package com.alibaba.wasp.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class WaspAggregateImplementation extends BaseEndpointCoprocessor implements
    WaspAggregateProtocol {

  protected static Log log = LogFactory.getLog(WaspAggregateImplementation.class);

  @Override
  public ProtocolSignature getProtocolSignature(
      String protocol, long version, int clientMethodsHashCode)
  throws IOException {
    if (WaspAggregateProtocol.class.getName().equals(protocol)) {
      return new ProtocolSignature(WaspAggregateProtocol.VERSION, null);
    }
    throw new IOException("Unknown protocol: " + protocol);
  }

  @Override
  public <T, S> S getSum(ColumnInterpreter<T, S> ci, Scan scan, byte[] qualifier) throws IOException {
     long sum = 0l;
    S sumVal = null;
    T temp;
    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    byte[] colFamily = scan.getFamilies()[0];
    List<KeyValue> results = new ArrayList<KeyValue>();
    try {
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        log.info("results size is " + results.size());
        for (KeyValue kv : results) {
          if(!Bytes.equals(kv.getQualifier(), qualifier)) {
            continue;
          }
          temp = ci.getValue(colFamily, qualifier, kv);
          log.info("WaspAggregateImplementation log : qualifier="
              + Bytes.toString(qualifier) + " value is :" + Bytes.toStringBinary(kv.getValue()) + " sum is :" + sumVal
          + " temp is :" + temp + " row is :" + Bytes.toStringBinary(kv.getRow()));
          if (temp != null)
            sumVal = ci.add(sumVal, ci.castToReturnType(temp));
        }
        results.clear();
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    log.debug("Sum from this region is "
        + ((RegionCoprocessorEnvironment) getEnvironment()).getRegion()
            .getRegionNameAsString() + ": " + sum);
    return sumVal;
  }

  @Override
  public <T, S> long getRowNum(ColumnInterpreter<T, S> ci, Scan scan)
      throws IOException {
    long counter = 0l;
    List<KeyValue> results = new ArrayList<KeyValue>();
    byte[] colFamily = scan.getFamilies()[0];
    byte[] qualifier = scan.getFamilyMap().get(colFamily).pollFirst();
    if (scan.getFilter() == null && qualifier == null)
      scan.setFilter(new FirstKeyOnlyFilter());
    InternalScanner scanner = ((RegionCoprocessorEnvironment) getEnvironment())
        .getRegion().getScanner(scan);
    try {
      boolean hasMoreRows = false;
      int j = 0;
      do {
        j++;
        hasMoreRows = scanner.next(results);
        log.info("results size is " + results.size());
        int i = 1;
        for (KeyValue result : results) {
          log.info("j = " + j + "; i = " + i + "; column = " + Bytes.toString(result.getQualifier()) + "; value = "
              + Bytes.toStringBinary(result.getValue()) );

        }
        if (results.size() > 0) {
          counter++;
        }
        results.clear();
      } while (hasMoreRows);
    } finally {
      scanner.close();
    }
    log.info("Row counter from this region is "
        + ((RegionCoprocessorEnvironment) getEnvironment()).getRegion()
            .getRegionNameAsString() + ": " + counter);
    return counter;
  }
}

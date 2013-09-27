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
package com.alibaba.wasp.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class WaspAggregationClient extends AggregationClient {


  Configuration conf;

  /**
   * Constructor with Conf object
   *
   * @param cfg
   */
  public WaspAggregationClient(Configuration cfg) {
    super(cfg);
    this.conf = cfg;
  }


   /**
   * It gives the row count, by summing up the individual results obtained from
   * regions. In case the qualifier is null, FirstKEyValueFilter is used to
   * optimised the operation. In case qualifier is provided, I can't use the
   * filter as it may set the flag to skip to next row, but the value read is
   * not of the given filter: in this case, this particular row will not be
   * counted ==> an error.
   * @param tableName
   * @param ci
   * @param scan
   * @return <R, S>
   * @throws Throwable
   */
  public <R, S> long count(final byte[] tableName,
      final ColumnInterpreter<R, S> ci, final Scan scan) throws Throwable {
    validateParameters(scan);
    class RowNumCallback implements Batch.Callback<Long> {
      private final AtomicLong rowCountL = new AtomicLong(0);

      public long getRowNumCount() {
        return rowCountL.get();
      }

      @Override
      public void update(byte[] region, byte[] row, Long result) {
        rowCountL.addAndGet(result.longValue());
      }
    }
    RowNumCallback rowNum = new RowNumCallback();
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
      table.coprocessorExec(WaspAggregateProtocol.class, scan.getStartRow(),
          scan.getStopRow(), new Batch.Call<WaspAggregateProtocol, Long>() {
            @Override
            public Long call(WaspAggregateProtocol instance) throws IOException {
              return instance.getRowNum(ci, scan);
            }
          }, rowNum);
    } finally {
      if (table != null) {
        table.close();
      }
    }
    return rowNum.getRowNumCount();
  }


   /**
   * It sums up the value returned from various regions. In case qualifier is
   * null, summation of all the column qualifiers in the given family is done.
   * @param tableName
   * @param ci
   * @param scan
   * @return sum <S>
   * @throws Throwable
   */
  public <R, S> S sum(final byte[] tableName, final ColumnInterpreter<R, S> ci,
      final Scan scan, final byte[] qualifier) throws Throwable {
    validateParameters(scan);
    class SumCallBack implements Batch.Callback<S> {
      S sumVal = null;

      public S getSumResult() {
        return sumVal;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, S result) {
        sumVal = ci.add(sumVal, result);
      }
    }
    SumCallBack sumCallBack = new SumCallBack();
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
      table.coprocessorExec(WaspAggregateProtocol.class, scan.getStartRow(),
          scan.getStopRow(), new Batch.Call<WaspAggregateProtocol, S>() {
            @Override
            public S call(WaspAggregateProtocol instance) throws IOException {
              return instance.getSum(ci, scan, qualifier);
            }
          }, sumCallBack);
    } finally {
      if (table != null) {
        table.close();
      }
    }
    return sumCallBack.getSumResult();
  }

  private void validateParameters(Scan scan) throws IOException {
    if (scan == null
        || (Bytes.equals(scan.getStartRow(), scan.getStopRow()) && !Bytes
            .equals(scan.getStartRow(), HConstants.EMPTY_START_ROW))
        || ((Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0) &&
        	!Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW))) {
      throw new IOException(
          "Agg client Exception: Startrow should be smaller than Stoprow");
    } else if (scan.getFamilyMap().size() != 1) {
      throw new IOException("There must be only one family.");
    }
  }
}

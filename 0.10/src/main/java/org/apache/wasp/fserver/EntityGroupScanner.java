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
package org.apache.wasp.fserver;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.FConstants;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.meta.RowBuilder;
import org.apache.wasp.meta.StorageTableNameBuilder;
import org.apache.wasp.plan.action.ColumnStruct;
import org.apache.wasp.plan.action.ScanAction;
import org.apache.wasp.protobuf.ProtobufUtil;
import org.apache.wasp.protobuf.generated.ClientProtos.QueryResultProto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * EntityGroupScanner describes iterators over rows in an EntityGroup.
 */
public class EntityGroupScanner implements InternalScanner {

  /** EntityGroup instance **/
  private final EntityGroupServices entityGroup;

  /** max time stamp **/
  private final long timeStamp;

  /** computer action **/
  private final ScanAction action;

  /** unique id **/
  private Long scannerID;

  private FTable tableDesc;

  private int batch;
  private int caching;
  private List<QueryResultProto> results = new ArrayList<QueryResultProto>();
  private ResultScanner indexScanner;
  private ResultScanner entityScanner;

  public EntityGroupScanner(EntityGroupServices entityGroup, ScanAction action,
      long timeStamp) throws IOException {
    super();
    this.action = action;
    this.entityGroup = entityGroup;
    this.timeStamp = timeStamp + 1; // as a result of the maximum timestamp
                                    // value is exclusive, but we need the
                                    // inclusive one
    this.batch = action.getBatch();
    if (this.batch == -1) {
      this.batch = this.entityGroup.getConf().getInt(
          FConstants.ENTITYGROUP_SCANNER_LIMIT,
          FConstants.DEFAULT_ENTITYGROUP_SCANNER_LIMIT);
    }
    this.caching = action.getLimit();
    if (this.caching == -1) {
      this.caching = this.entityGroup.getConf().getInt(
          FConstants.ENTITYGROUP_SCANNER_CACHING,
          FConstants.DEFAULT_ENTITYGROUP_SCANNER_CACHING);
    }
    initlize();
  }

  /**
   * Initialization
   * 
   * @throws IOException
   * @throws org.apache.wasp.storage.StorageTableNotFoundException
   **/
  private void initlize() throws IOException {
    Scan scan = new Scan();
    scan.setStartRow(this.action.getStartKey());
    scan.setStopRow(this.action.getEndKey());
    scan.setTimeRange(0, this.timeStamp);
    scan.setMaxVersions(1);
    scan.setBatch(batch);
    scan.setCaching(caching);
    if (this.action.getIndexTableName() != null) {
      scan.addColumn(FConstants.INDEX_STORING_FAMILY_BYTES,
          FConstants.INDEX_STORE_ROW_QUALIFIER);
      for (ColumnStruct col : this.action.getStoringColumns()) {
        scan.addColumn(FConstants.INDEX_STORING_FAMILY_BYTES,
            Bytes.toBytes(col.getColumnName()));
      }
      indexScanner = this.entityGroup.getFServerServices().getActionManager()
          .scan(this.action.getIndexTableName(), scan);
    }
  }

  /**
   * @param scannerID
   *          the scannerID to set
   */
  public void setScannerID(Long scannerID) {
    this.scannerID = scannerID;
  }

  /**
   * @return the scannerID
   */
  public Long getScannerID() {
    return scannerID;
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

  @Override
  public EntityGroupInfo getEntityGroupInfo() {
    return entityGroup.getEntityGroupInfo();
  }

  @Override
  public boolean next(List<QueryResultProto> outResults) throws IOException {
    return next(outResults, null);
  }

  @Override
  public boolean next(List<QueryResultProto> outResults, String metric)
      throws IOException {
    results.clear();
    boolean returnResult = nextInternal(this.caching, metric);
    outResults.addAll(results);
    return returnResult;
  }

  private boolean nextInternal(int limit, String metric) throws IOException {
    List<Get> gets = new ArrayList<Get>();
    RowBuilder builder = RowBuilder.build();
    Get get = null;
    Result[] indexResults = indexScanner.next(limit);
    for (Result result : indexResults) {
      if (action.getFTableName() != null) {
        get = new Get(builder.buildEntityRowKey(result));
        get.setTimeRange(0, this.timeStamp);
        get.setMaxVersions(1);
        for (ColumnStruct col : this.action.getColumns()) {
          get.addColumn(Bytes.toBytes(col.getFamilyName()),
              Bytes.toBytes(col.getColumnName()));
        }
        gets.add(get);
      } else {
        this.results.add(ProtobufUtil.toQeuryResultProto(result));
      }
    }

    if (action.getEntityTableName() != null) {
      Result[] entityResults = this.entityGroup
          .getFServerServices()
          .getActionManager()
          .get(
              StorageTableNameBuilder.buildEntityTableName(this.action
                  .getEntityTableName()), gets);
      for (int i = 0; i < entityResults.length; i++) {
        this.results.add(ProtobufUtil.toQeuryResultProto(entityResults[i],
            indexResults[i]));
      }
    }

    return results.size() == limit ? true : false;
  }

  @Override
  public void close() throws IOException {
    if (indexScanner != null) {
      indexScanner.close();
    }
    if (entityScanner != null) {
      entityScanner.close();
    }
  }

  /**
   * @return the batch
   */
  public int getBatch() {
    return batch;
  }

  /**
   * @return the caching
   */
  public int getCaching() {
    return caching;
  }

  /**
   * @return the timeStamp
   */
  public long getTimeStamp() {
    return timeStamp;
  }

  /**
   * @return the action
   */
  public ScanAction getAction() {
    return action;
  }

  /**
   * @return the results
   */
  public List<QueryResultProto> getResults() {
    return results;
  }
}

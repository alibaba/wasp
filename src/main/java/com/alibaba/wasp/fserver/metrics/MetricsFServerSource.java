/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.wasp.fserver.metrics;

import com.alibaba.wasp.metrics.BaseSource;

/**
 * Interface for classes that expose metrics about the fserver.
 */
public interface MetricsFServerSource extends BaseSource {

  /**
   * The name of the metrics
   */
  static final String METRICS_NAME = "Server";

  /**
   * The name of the metrics context that metrics will be under.
   */
  static final String METRICS_CONTEXT = "fserver";

  /**
   * Description
   */
  static final String METRICS_DESCRIPTION = "Metrics about Wasp FServer";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  static final String METRICS_JMX_CONTEXT = "FServer,sub=" + METRICS_NAME;

  /**
   * Update the Insert time histogram
   * 
   * @param t time it took
   */
  void updateInsert(long t);

  /**
   * Increment the number of slow Inserts that have happened.
   */
  void incrSlowInsert();

  /**
   * Update the Transaction time histogram
   *
   * @param t time it took
   */
  void updateTransaction(long t);

  /**
   * Increment the number of slow Transaction that have happened.
   */
  void incrSlowTransaction();

  /**
   * Update the GenPlan time histogram
   * 
   * @param t
   *          time it took
   */
  void updateGenPlan(long t);

  /**
   * Increment the number of slow GenPlans that have happened.
   */
  void incrSlowGenPlan();

  /**
   * Update the Update time histogram
   * 
   * @param t
   *          time it took
   */
  void updateUpdate(long t);

  /**
   * Increment the number of slow Updates that have happened.
   */
  void incrSlowUpdate();

  /**
   * Update the Delete time histogram
   * 
   * @param t
   *          time it took
   */
  void updateDelete(long t);

  /**
   * Increment the number of slow Deletes that have happened.
   */
  void incrSlowDelete();

  /**
   * Update the Get time histogram
   * 
   * @param t
   *          time it took
   */
  void updateGet(long t);

  /**
   * Increment the number of slow Gets that have happened.
   */
  void incrSlowGet();

  /**
   * Update the Scan time histogram
   * 
   * @param t
   *          time it took
   */
  void updateScan(long t);

  /**
   * Increment the number of slow Scans that have happened.
   */
  void incrSlowScan();

  /**
   * Update the count time histogram
   *
   * @param t
   *          time it took
   */
  void updateCount(long t);

  /**
   * Increment the number of slow count that have happened.
   */
  void incrSlowCount();

   /**
   * Update the sum time histogram
   *
   * @param t
   *          time it took
   */
  void updateSum(long t);

  /**
   * Increment the number of slow sum that have happened.
   */
  void incrSlowSum();

  /**
   * Update the SqlExecute time histogram
   * 
   * @param t
   *          time it took
   */
  void updateSqlExecute(long t);

  /**
   * Increment the number of slow SqlExecute that have happened.
   */
  void incrSlowSqlExecute();

  // Strings used for exporting to metrics system.
  static final String ENTITYGROUP_COUNT = "entityGroupCount";
  static final String ENTITYGROUP_COUNT_DESC = "Number of entity groups";
  static final String READ_REQUEST_COUNT = "readRequestCount";
  static final String READ_REQUEST_COUNT_DESC = "Number of read requests this fserver has answered.";
  static final String WRITE_REQUEST_COUNT = "writeRequestCount";
  static final String WRITE_REQUEST_COUNT_DESC = "Number of mutation requests this fserver has answered.";
  static final String TRANSACTION_LOG_SIZE = "transactionLogSize";
  static final String TRANSACTION_LOG_SIZE_DESC = "Number of uncommitted redo log size of this EntityGroup.";
  static final String FS_START_TIME_NAME = "fserverStartTime";
  static final String ZOOKEEPER_QUORUM_NAME = "zookeeperQuorum";
  static final String SERVER_NAME_NAME = "serverName";
  static final String CLUSTER_ID_NAME = "clusterId";
  static final String FS_START_TIME_DESC = "FServer Start Time";
  static final String ZOOKEEPER_QUORUM_DESC = "Zookeeper Quorum";
  static final String SERVER_NAME_DESC = "Server Name";
  static final String CLUSTER_ID_DESC = "Cluster Id";
  static final String TRANSACTION_KEY = "transaction";
  static final String SLOW_TRANSACTION_KEY = "slowTransactionCount";
  static final String SLOW_TRANSACTION_DESC = "The number of Transactions that took over 1000ms to complete";
  static final String INSERT_KEY = "insert";
  static final String SLOW_INSERT_KEY = "slowInsertCount";
  static final String SLOW_INSERT_DESC = "The number of Inserts that took over 1000ms to complete";
  static final String DELETE_KEY = "delete";
  static final String SLOW_DELETE_KEY = "slowDeleteCount";
  static final String SLOW_DELETE_DESC = "The number of Deletes that took over 1000ms to complete";
  static final String UPDATE_KEY = "update";
  static final String SLOW_UPDATE_KEY = "slowUpdateCount";
  static final String SLOW_UPDATE_DESC = "The number of Updates that took over 1000ms to complete";
  static final String GET_KEY = "get";
  static final String SLOW_GET_KEY = "slowGetCount";
  static final String SLOW_GET_DESC = "The number of Gets that took over 1000ms to complete";
  static final String SCAN_KEY = "scan";
  static final String SLOW_SCAN_KEY = "slowScanCount";
  static final String SLOW_SCAN_DESC = "The number of Scans that took over 1000ms to complete";
  static final String GENPLAN_KEY = "genPlan";
  static final String SLOW_GENPLAN_KEY = "slowGenPlanCount";
  static final String SLOW_GENPLAN_DESC = "The number of genPlans that took over 1000ms to complete";
  static final String COUNT_KEY = "count";
  static final String SLOW_COUNT_KEY = "slowCountCount";
  static final String SLOW_COUNT_DESC = "The number of count that took over 1000ms to complete";
  static final String SUM_KEY = "sum";
  static final String SLOW_SUM_KEY = "slowSumCount";
  static final String SLOW_SUM_DESC = "The number of sum that took over 1000ms to complete";
  static final String SQLEXECUTE_KEY = "sqlExecute";
  static final String SLOW_SQLEXECUTE_KEY = "slowSqlExecuteCount";
  static final String SLOW_SQLEXECUTE_DESC = "The number of sqlExecutes that took over 1000ms to complete";
  // select -> get and scan
  static final String SELECT_KEY = "select";
}

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
package com.alibaba.wasp.fserver;

import com.alibaba.wasp.fserver.metrics.MetricsFServerSource;
import com.alibaba.wasp.fserver.metrics.MetricsFServerWrapper;
import com.alibaba.wasp.metrics.BaseSourceImpl;
import com.alibaba.wasp.metrics.lib.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;

public class MetricsFServerSourceImpl extends BaseSourceImpl implements
    MetricsFServerSource {

  final MetricsFServerWrapper fsWrap;
  private final MetricHistogram transactionHisto;
  private final MetricMutableCounterLong slowTransaction;
  private final MetricHistogram insertHisto;
  private final MetricMutableCounterLong slowInsert;
  private final MetricHistogram updateHisto;
  private final MetricMutableCounterLong slowUpdate;
  private final MetricHistogram deleteHisto;
  private final MetricMutableCounterLong slowDelete;
  private final MetricHistogram getHisto;
  private final MetricMutableCounterLong slowGet;
  private final MetricHistogram scanHisto;
  private final MetricMutableCounterLong slowScan;
  private final MetricHistogram genPlanHisto;
  private final MetricMutableCounterLong slowGenPlan;
  private final MetricHistogram sqlExecuteHisto;
  private final MetricMutableCounterLong slowSqlExecute;
  private final MetricHistogram countHisto;
  private final MetricMutableCounterLong slowCount;
  private final MetricHistogram sumHisto;
  private final MetricMutableCounterLong slowSum;

  public MetricsFServerSourceImpl(MetricsFServerWrapper fsWrap) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT,
        METRICS_JMX_CONTEXT, fsWrap);
  }

  public MetricsFServerSourceImpl(String metricsName,
      String metricsDescription, String metricsContext,
      String metricsJmxContext, MetricsFServerWrapper fsWrap) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.fsWrap = fsWrap;

    transactionHisto = getMetricsRegistry().newHistogram(TRANSACTION_KEY);
    slowTransaction = getMetricsRegistry().newCounter(SLOW_TRANSACTION_KEY,
        SLOW_TRANSACTION_DESC, 0l);
    insertHisto = getMetricsRegistry().newHistogram(INSERT_KEY);
    slowInsert = getMetricsRegistry().newCounter(SLOW_INSERT_KEY,
        SLOW_INSERT_DESC, 0l);
    updateHisto = getMetricsRegistry().newHistogram(UPDATE_KEY);
    slowUpdate = getMetricsRegistry().newCounter(SLOW_UPDATE_KEY,
        SLOW_UPDATE_DESC, 0l);
    deleteHisto = getMetricsRegistry().newHistogram(DELETE_KEY);
    slowDelete = getMetricsRegistry().newCounter(SLOW_DELETE_KEY,
        SLOW_DELETE_DESC, 0l);
    getHisto = getMetricsRegistry().newHistogram(GET_KEY);
    slowGet = getMetricsRegistry().newCounter(SLOW_GET_KEY, SLOW_GET_DESC, 0l);
    scanHisto = getMetricsRegistry().newHistogram(SCAN_KEY);
    slowScan = getMetricsRegistry().newCounter(SLOW_SCAN_KEY, SLOW_SCAN_DESC,
        0l);
    genPlanHisto = getMetricsRegistry().newHistogram(GENPLAN_KEY);
    slowGenPlan = getMetricsRegistry().newCounter(SLOW_GENPLAN_KEY,
        SLOW_GENPLAN_DESC, 0l);
    sqlExecuteHisto = getMetricsRegistry().newHistogram(SQLEXECUTE_KEY);
    slowSqlExecute = getMetricsRegistry().newCounter(SLOW_SQLEXECUTE_KEY,
        SLOW_SQLEXECUTE_DESC, 0l);
    countHisto = getMetricsRegistry().newHistogram(COUNT_KEY);
    slowCount = getMetricsRegistry().newCounter(SLOW_COUNT_KEY, SLOW_COUNT_DESC, 0l);
    sumHisto = getMetricsRegistry().newHistogram(SUM_KEY);
    slowSum = getMetricsRegistry().newCounter(SLOW_SUM_KEY, SLOW_SUM_DESC, 0l);
  }

  @Override
  public void updateInsert(long t) {
    insertHisto.add(t);
  }

  @Override
  public void incrSlowInsert() {
    slowInsert.incr();
  }

  @Override
  public void updateTransaction(long t) {
    transactionHisto.add(t);
  }

  @Override
  public void incrSlowTransaction() {
    slowTransaction.incr();
  }

  @Override
  public void updateUpdate(long t) {
    updateHisto.add(t);
  }

  @Override
  public void incrSlowUpdate() {
    slowUpdate.incr();
  }

  @Override
  public void updateDelete(long t) {
    deleteHisto.add(t);
  }

  @Override
  public void incrSlowDelete() {
    slowDelete.incr();
  }

  @Override
  public void updateGet(long t) {
    getHisto.add(t);
  }

  @Override
  public void incrSlowGet() {
    slowGet.incr();
  }

  @Override
  public void updateScan(long t) {
    scanHisto.add(t);
  }

  @Override
  public void incrSlowScan() {
    slowScan.incr();
  }

  @Override
  public void updateCount(long t) {
    countHisto.add(t);
  }

  @Override
  public void incrSlowCount() {
    slowCount.incr();
  }

  @Override
  public void updateSum(long t) {
    sumHisto.add(t);
  }

  @Override
  public void incrSlowSum() {
    slowSum.incr();
  }

  @Override
  public void updateGenPlan(long t) {
    genPlanHisto.add(t);
  }

  @Override
  public void incrSlowGenPlan() {
    slowGenPlan.incr();
  }

  @Override
  public void updateSqlExecute(long t) {
    sqlExecuteHisto.add(t);
  }

  @Override
  public void incrSlowSqlExecute() {
    slowSqlExecute.incr();
  }

  /**
   * Yes this is a get function that doesn't return anything. Thanks Hadoop for
   * breaking all expectations of java programmers. Instead of returning
   * anything Hadoop metrics expects getMetrics to push the metrics into the
   * metricsBuilder.
   * 
   * @param metricsBuilder Builder to accept metrics
   * @param all push all or only changed?
   */
  @Override
  public void getMetrics(MetricsBuilder metricsBuilder, boolean all) {

    MetricsRecordBuilder mrb = metricsBuilder.addRecord(metricsName)
        .setContext(metricsContext);

    // fsWrap can be null because this function is called inside of init.
    if (fsWrap != null) {
      mrb.addGauge(ENTITYGROUP_COUNT, ENTITYGROUP_COUNT_DESC,fsWrap.getNumOnlineEntityGroups())
          .addGauge(FS_START_TIME_NAME, FS_START_TIME_DESC, fsWrap.getStartCode())
          .addCounter(READ_REQUEST_COUNT, READ_REQUEST_COUNT_DESC,
              (int) fsWrap.getReadRequestsPerSecond())
          .addCounter(WRITE_REQUEST_COUNT, WRITE_REQUEST_COUNT_DESC,
              (int) fsWrap.getWriteRequestsPerSecond())
          .tag(ZOOKEEPER_QUORUM_NAME, ZOOKEEPER_QUORUM_DESC,
              fsWrap.getZookeeperQuorum())
          .tag(SERVER_NAME_NAME, SERVER_NAME_DESC, fsWrap.getServerName())
          .tag(CLUSTER_ID_NAME, CLUSTER_ID_DESC, fsWrap.getClusterId());
    }

    metricsRegistry.snapshot(mrb, all);
  }
}

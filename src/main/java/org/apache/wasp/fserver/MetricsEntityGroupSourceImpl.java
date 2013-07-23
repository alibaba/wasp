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
package org.apache.wasp.fserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.wasp.fserver.metrics.MetricsEntityGroupAggregateSource;
import org.apache.wasp.fserver.metrics.MetricsEntityGroupSource;
import org.apache.wasp.fserver.metrics.MetricsEntityGroupWrapper;
import org.apache.wasp.fserver.metrics.MetricsFServerSource;
import org.apache.wasp.metrics.BaseSourceImpl;
import org.apache.wasp.metrics.DynamicMetricsRegistry;
import org.apache.wasp.metrics.JmxCacheBuster;
import org.apache.wasp.metrics.lib.MetricHistogram;

public class MetricsEntityGroupSourceImpl extends BaseSourceImpl implements
    MetricsEntityGroupSource {

  private final MetricsEntityGroupWrapper egWrapper;
  private boolean closed = false;
  private MetricsEntityGroupAggregateSourceImpl agg;
  private DynamicMetricsRegistry registry;
  private static final Log LOG = LogFactory
      .getLog(MetricsEntityGroupSourceImpl.class);

  private String egNamePrefix;
  private String egInsertKey;
  private String egDeleteKey;
  private String egUpdateKey;
  private String egSelectKey;
  private MetricMutableCounterLong egInsert;
  private MetricMutableCounterLong egDelete;
  private MetricMutableCounterLong egUpdate;
  private MetricMutableCounterLong egSelect;

  private final MetricHistogram obtainRowLockHisto;
  private final MetricMutableCounterLong slowObtainRowLock;
  private final MetricHistogram prepareInsertEntityHisto;
  private final MetricMutableCounterLong slowPrepareInsertEntity;
  private final MetricHistogram prepareUpdateEntityHisto;
  private final MetricMutableCounterLong slowPrepareUpdateEntity;
  private final MetricHistogram prepareDeleteEntityHisto;
  private final MetricMutableCounterLong slowPrepareDeleteEntity;
  private final MetricHistogram redoLogAppendHisto;
  private final MetricMutableCounterLong slowRedoLogAppend;
  private final MetricHistogram backgroundRedoLogHisto;
  private final MetricMutableCounterLong slowBackgroundRedoLog;

  public MetricsEntityGroupSourceImpl(MetricsEntityGroupWrapper egWrapper,
      MetricsEntityGroupAggregateSourceImpl aggregate) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT,
        METRICS_JMX_CONTEXT, egWrapper, aggregate);
  }

  public MetricsEntityGroupSourceImpl(String metricsName,
      String metricsDescription, String metricsContext,
      String metricsJmxContext, MetricsEntityGroupWrapper egWrapper,
      MetricsEntityGroupAggregateSourceImpl aggregate) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.egWrapper = egWrapper;
    agg = aggregate;
    agg.register(this);

    LOG.debug("Creating new MetricsEntityGroupSourceImpl for table "
        + egWrapper.getTableName() + " " + egWrapper.getEntityGroupName());

    registry = agg.getMetricsRegistry();

    egNamePrefix = "table." + egWrapper.getTableName() + "." + "entitygroup."
        + egWrapper.getEntityGroupName() + ".";

    String suffix = "Count";

    egInsertKey = egNamePrefix + MetricsFServerSource.INSERT_KEY + suffix;
    egInsert = registry.getLongCounter(egInsertKey, 0l);

    egDeleteKey = egNamePrefix + MetricsFServerSource.DELETE_KEY
        + suffix;
    egDelete = registry.getLongCounter(egDeleteKey, 0l);

    egUpdateKey = egNamePrefix + MetricsFServerSource.UPDATE_KEY
        + suffix;
    egUpdate = registry.getLongCounter(egUpdateKey, 0l);

    egSelectKey = egNamePrefix + MetricsFServerSource.SELECT_KEY + suffix;
    egSelect = registry.getLongCounter(egSelectKey, 0l);

    obtainRowLockHisto = getMetricsRegistry().newHistogram(OBTAINROWLOCK_KEY);
    slowObtainRowLock = getMetricsRegistry().newCounter(SLOW_OBTAINROWLOCK_KEY,
        SLOW_OBTAINROWLOCK_DESC, 0l);
    prepareInsertEntityHisto = getMetricsRegistry().newHistogram(
        PREPAREINSERTENTITY_KEY);
    slowPrepareInsertEntity = getMetricsRegistry().newCounter(
        SLOW_PREPAREINSERTENTITY_KEY, SLOW_PREPAREINSERTENTITY_DESC, 0l);
    prepareUpdateEntityHisto = getMetricsRegistry().newHistogram(
        PREPAREUPDATEENTITY_KEY);
    slowPrepareUpdateEntity = getMetricsRegistry().newCounter(
        SLOW_PREPAREUPDATEENTITY_KEY, SLOW_PREPAREUPDATEENTITY_DESC, 0l);
    prepareDeleteEntityHisto = getMetricsRegistry().newHistogram(
        PREPAREDELETEENTITY_KEY);
    slowPrepareDeleteEntity = getMetricsRegistry().newCounter(
        SLOW_PREPAREDELETEENTITY_KEY, SLOW_PREPAREDELETEENTITY_DESC, 0l);
    redoLogAppendHisto = getMetricsRegistry().newHistogram(REDOLOGAPPEND_KEY);
    slowRedoLogAppend = getMetricsRegistry().newCounter(SLOW_REDOLOGAPPEND_KEY,
        SLOW_REDOLOGAPPEND_DESC, 0l);
    backgroundRedoLogHisto = getMetricsRegistry().newHistogram(
        BACKGROUNDREDOLOG_KEY);
    slowBackgroundRedoLog = getMetricsRegistry().newCounter(
        SLOW_BACKGROUNDREDOLOG_KEY, SLOW_BACKGROUNDREDOLOG_DESC, 0l);
  }

  @Override
  public void close() {
    closed = true;
    agg.deregister(this);

    LOG.trace("Removing entity group Metrics: "
        + egWrapper.getEntityGroupName());
    registry.removeMetric(egInsertKey);
    registry.removeMetric(egDeleteKey);
    registry.removeMetric(egUpdateKey);
    registry.removeMetric(egSelectKey);

    JmxCacheBuster.clearJmxCache();
  }

  @Override
  public void updateInsert() {
    egInsert.incr();
  }

  @Override
  public void updateDelete() {
    egDelete.incr();
  }

  @Override
  public void updateUpdate() {
    egUpdate.incr();
  }

  @Override
  public void updateSelect() {
    egSelect.incr();
  }

  @Override
  public void updateObtainRowLock(long t) {
    obtainRowLockHisto.add(t);
  }

  @Override
  public void incrSlowObtainRowLock() {
    slowObtainRowLock.incr();
  }

  @Override
  public void updatePrepareInsertEntity(long t) {
    prepareInsertEntityHisto.add(t);
  }

  @Override
  public void incrSlowPrepareInsertEntity() {
    slowPrepareInsertEntity.incr();
  }

  @Override
  public void updatePrepareUpdateEntity(long t) {
    prepareUpdateEntityHisto.add(t);
  }

  @Override
  public void incrSlowPrepareUpdateEntity() {
    slowPrepareUpdateEntity.incr();
  }

  @Override
  public void updatePrepareDeleteEntity(long t) {
    prepareDeleteEntityHisto.add(t);
  }

  @Override
  public void incrSlowPrepareDeleteEntity() {
    slowPrepareDeleteEntity.incr();
  }

  @Override
  public void updateRedoLogAppend(long t) {
    redoLogAppendHisto.add(t);
  }

  @Override
  public void incrSlowRedoLogAppend() {
    slowRedoLogAppend.incr();
  }

  @Override
  public void updateBackgroundRedoLog(long t) {
    backgroundRedoLogHisto.add(t);
  }

  @Override
  public void incrSlowBackgroundRedoLog() {
    slowBackgroundRedoLog.incr();
  }

  @Override
  public MetricsEntityGroupAggregateSource getAggregateSource() {
    return agg;
  }

  @Override
  public int compareTo(MetricsEntityGroupSource source) {

    if (!(source instanceof MetricsEntityGroupSourceImpl))
      return -1;

    MetricsEntityGroupSourceImpl impl = (MetricsEntityGroupSourceImpl) source;
    return this.egWrapper.getEntityGroupName().compareTo(
        impl.egWrapper.getEntityGroupName());
  }

  @Override
  public void snapshot(MetricsRecordBuilder mrb, boolean ignored) {
    if (closed)
      return;
    mrb.addCounter(egNamePrefix
        + MetricsFServerSource.READ_REQUEST_COUNT,
        MetricsFServerSource.READ_REQUEST_COUNT_DESC,
        this.egWrapper.getReadRequestCount());
    mrb.addCounter(egNamePrefix
        + MetricsFServerSource.WRITE_REQUEST_COUNT,
        MetricsFServerSource.WRITE_REQUEST_COUNT_DESC,
        this.egWrapper.getWriteRequestCount());
    mrb.addCounter(egNamePrefix + MetricsFServerSource.TRANSACTION_LOG_SIZE,
        MetricsFServerSource.TRANSACTION_LOG_SIZE_DESC,
        this.egWrapper.getTransactionLogSize());
  }

}

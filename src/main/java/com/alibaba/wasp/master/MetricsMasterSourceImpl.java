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
package com.alibaba.wasp.master;

import com.alibaba.wasp.master.metrics.MetricsMasterSource;
import com.alibaba.wasp.master.metrics.MetricsMasterWrapper;
import com.alibaba.wasp.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeLong;

/**
 * Implementation of MetricsMasterSource.
 * 
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
public class MetricsMasterSourceImpl extends BaseSourceImpl implements
    MetricsMasterSource {
  private final MetricsMasterWrapper masterWrapper;
  private MetricMutableGaugeLong clusterRequestsGauge;
  private MetricMutableGaugeLong ritGauge;
  private MetricMutableGaugeLong ritCountOverThresholdGauge;
  private MetricMutableGaugeLong ritOldestAgeGauge;

  public MetricsMasterSourceImpl(MetricsMasterWrapper masterWrapper) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT,
        METRICS_JMX_CONTEXT, masterWrapper);
  }

  public MetricsMasterSourceImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext,
      MetricsMasterWrapper masterWrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.masterWrapper = masterWrapper;
  }

  @Override
  public void init() {
    super.init();
    clusterRequestsGauge = metricsRegistry.newGauge(CLUSTER_REQUESTS_NAME,
        "", 0l);
    ritGauge = metricsRegistry.newGauge(RIT_COUNT_NAME, "", 0l);
    ritCountOverThresholdGauge = metricsRegistry.newGauge(
        RIT_COUNT_OVER_THRESHOLD_NAME, "", 0l);
    ritOldestAgeGauge = metricsRegistry.newGauge(RIT_OLDEST_AGE_NAME, "", 0l);
  }

  @Override
  public void incRequests(final int inc) {
    this.clusterRequestsGauge.incr(inc);
  }

  @Override
  public void setEGIT(int egitCount) {
    ritGauge.set(egitCount);
  }

  @Override
  public void setEGITCountOverThreshold(int egitCount) {
    ritCountOverThresholdGauge.set(egitCount);
  }

  @Override
  public void setEGITOldestAge(long egitCount) {
    ritOldestAgeGauge.set(egitCount);
  }

  /**
   * Method to export all the metrics.
   * 
   * @param metricsBuilder Builder to accept metrics
   * @param all push all or only changed?
   */
  @Override
  public void getMetrics(MetricsBuilder metricsBuilder, boolean all) {

    MetricsRecordBuilder metricsRecordBuilder = metricsBuilder.addRecord(
        metricsName).setContext(metricsContext);

    // masterWrapper can be null because this function is called inside of init.
    if (masterWrapper != null) {
      metricsRecordBuilder
          .addGauge(MASTER_ACTIVE_TIME_NAME, MASTER_ACTIVE_TIME_DESC,
              masterWrapper.getActiveTime())
          .addGauge(MASTER_START_TIME_NAME, MASTER_START_TIME_DESC,
              masterWrapper.getStartTime())
          .addGauge(AVERAGE_LOAD_NAME, AVERAGE_LOAD_DESC,
              masterWrapper.getAverageLoad())
          .addGauge(NUM_FSERVERS_NAME, NUMBER_OF_FSERVERS_DESC,
              masterWrapper.getFServers())
          .addGauge(NUM_DEAD_FSERVERS_NAME, NUMBER_OF_DEAD_FSERVERS_DESC,
              masterWrapper.getDeadFServers())
          .tag(ZOOKEEPER_QUORUM_NAME, ZOOKEEPER_QUORUM_DESC,
              masterWrapper.getZookeeperQuorum())
          .tag(SERVER_NAME_NAME, SERVER_NAME_DESC,
              masterWrapper.getServerName())
          .tag(CLUSTER_ID_NAME, CLUSTER_ID_DESC, masterWrapper.getClusterId())
          .tag(IS_ACTIVE_MASTER_NAME, IS_ACTIVE_MASTER_DESC,
              String.valueOf(masterWrapper.getIsActiveMaster()));
    }

    metricsRegistry.snapshot(metricsRecordBuilder, all);
  }

}

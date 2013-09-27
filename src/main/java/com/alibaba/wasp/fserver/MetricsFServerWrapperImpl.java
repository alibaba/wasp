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

import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.fserver.metrics.MetricsFServerWrapper;
import com.alibaba.wasp.metrics.MetricsExecutor;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Impl for exposing FServer Information through Hadoop's metrics 2 system.
 */
public class MetricsFServerWrapperImpl implements MetricsFServerWrapper {
  public static final Log LOG = LogFactory.getLog(MetricsFServerWrapperImpl.class);

  public static final int PERIOD = 15;

  private final FServer fserver;

  private ScheduledExecutorService executor;
  private Runnable runnable;

  private volatile double requestsPerSecond = 0.0;
  private volatile double readRequestsPerSecond = 0.0;
  private volatile double writeRequestsPerSecond = 0.0;

  public MetricsFServerWrapperImpl(final FServer fserver) {
    this.fserver = fserver;
    this.executor = MetricsExecutor.getExecutor();
    this.runnable = new FServerMetricsWrapperRunnable();
    this.executor.scheduleWithFixedDelay(this.runnable, PERIOD, PERIOD,
        TimeUnit.SECONDS);
  }

  @Override
  public String getServerName() {
    ServerName serverName = fserver.getServerName();
    if (serverName == null) {
      return "";
    }
    return serverName.getServerName();
  }

  @Override
  public String getClusterId() {
    return fserver.getClusterId();
  }

  @Override
  public String getZookeeperQuorum() {
    ZooKeeperWatcher zk = fserver.getZooKeeper();
    if (zk == null) {
      return "";
    }
    return zk.getQuorum();
  }

  @Override
  public long getStartCode() {
    return fserver.getStartcode();
  }

  @Override
  public long getNumOnlineEntityGroups() {
    return fserver.getNumberOfOnlineEntityGroups();
  }

  @Override
  public double getRequestsPerSecond() {
    return this.requestsPerSecond;
  }

  @Override
  public double getReadRequestsPerSecond() {
    return this.readRequestsPerSecond;
  }

  @Override
  public double getWriteRequestsPerSecond() {
    return this.writeRequestsPerSecond;
  }

  @Override
  public void forceRecompute() {
    this.runnable.run();
  }

  /**
   * This is the runnable that will be executed on the executor every PERIOD
   * number of seconds It will take metrics/numbers from all of the entity
   * pgropus and use them to compute point in time metrics.
   */
  public class FServerMetricsWrapperRunnable implements Runnable {

    private long lastRan = 0;
    private long lastReadRequestCount = 0;
    private long lastWriteRequestCount = 0;

    @Override
    synchronized public void run() {

      long tempReadRequestsCount = 0;
      long tempWriteRequestsCount = 0;

      for (EntityGroup eg : fserver.getOnlineEntityGroupsLocalContext()) {
        tempReadRequestsCount += eg.readRequestsCount.get();
        tempWriteRequestsCount += eg.writeRequestsCount.get();
      }

      // Compute the number of requests per second
      long currentTime = EnvironmentEdgeManager.currentTimeMillis();

      // assume that it took PERIOD seconds to start the executor.
      // this is a guess but it's a pretty good one.
      if (lastRan == 0) {
        lastRan = currentTime - (PERIOD * 1000);
      }

      // If we've time traveled keep the last requests per second.
      if ((currentTime - lastRan) > 10) {
        readRequestsPerSecond = (tempReadRequestsCount - lastReadRequestCount)
            / ((currentTime - lastRan) / 1000.0);
        writeRequestsPerSecond = (tempWriteRequestsCount - lastWriteRequestCount)
            / ((currentTime - lastRan) / 1000.0);
        requestsPerSecond = readRequestsPerSecond + writeRequestsPerSecond;
        lastReadRequestCount = tempReadRequestsCount;
        lastWriteRequestCount = tempWriteRequestsCount;
      }
      lastRan = currentTime;

      // Copy over computed values so that no thread sees half computed values.
    }
  }

}

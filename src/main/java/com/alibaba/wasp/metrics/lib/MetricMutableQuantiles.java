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
package com.alibaba.wasp.metrics.lib;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MetricMutable;
import com.alibaba.wasp.metrics.MetricsExecutor;

/**
 * A histogram implementation that runs in constant space, and exports to
 * hadoop's metrics2 system.
 */
public class MetricMutableQuantiles extends MetricMutable implements
    MetricHistogram {

  static final MetricQuantile[] quantiles = { new MetricQuantile(0.50, 0.050),
      new MetricQuantile(0.75, 0.025), new MetricQuantile(0.90, 0.010),
      new MetricQuantile(0.95, 0.005), new MetricQuantile(0.99, 0.001) };

  static final String[] quantilesSuffix = { "_Median", "_75th_percentile",
      "_90th_percentile", "_95th_percentile", "_99th_percentile" };

  private final int interval;

  private MetricSampleQuantiles estimator;
  private long previousCount = 0;

  protected Map<MetricQuantile, Long> previousSnapshot = null;

  /**
   * Instantiates a new {@link MetricMutableQuantiles} for a metric that rolls
   * itself over on the specified time interval.
   * 
   * @param name of the metric
   * @param description long-form textual description of the metric
   * @param sampleName type of items in the stream (e.g., "Ops")
   * @param valueName type of the values
   * @param interval rollover interval (in seconds) of the estimator
   */
  public MetricMutableQuantiles(String name, String description,
      String sampleName, String valueName, int interval) {
    super(name, description);

    estimator = new MetricSampleQuantiles(quantiles);

    this.interval = interval;
    MetricsExecutor.getExecutor().scheduleAtFixedRate(new RolloverSample(this),
        interval, interval, TimeUnit.SECONDS);
  }

  public MetricMutableQuantiles(String name, String description) {
    this(name, description, "Ops", "", 60);
  }

  @Override
  public synchronized void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || changed()) {
      builder.addCounter(name + "NumOps", description, previousCount);
      for (int i = 0; i < quantiles.length; i++) {
        long newValue = 0;
        // If snapshot is null, we failed to update since the window was empty
        if (previousSnapshot != null) {
          newValue = previousSnapshot.get(quantiles[i]);
        }
        builder.addGauge(name + quantilesSuffix[i], description, newValue);
      }
      if (changed()) {
        clearChanged();
      }
    }
  }

  public synchronized void add(long value) {
    estimator.insert(value);
  }

  public int getInterval() {
    return interval;
  }

  /**
   * Runnable used to periodically roll over the internal
   * {@link org.com.alibaba.wasp.metrics.lib.MetricSampleQuantiles} every
   * interval.
   */
  private static class RolloverSample implements Runnable {

    MetricMutableQuantiles parent;

    public RolloverSample(MetricMutableQuantiles parent) {
      this.parent = parent;
    }

    @Override
    public void run() {
      synchronized (parent) {
        try {
          parent.previousCount = parent.estimator.getCount();
          parent.previousSnapshot = parent.estimator.snapshot();
        } catch (IOException e) {
          // Couldn't get a new snapshot because the window was empty
          parent.previousCount = 0;
          parent.previousSnapshot = null;
        }
        parent.estimator.clear();
      }
      parent.setChanged();
    }

  }
}

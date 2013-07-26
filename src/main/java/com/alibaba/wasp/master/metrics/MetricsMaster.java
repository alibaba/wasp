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
package com.alibaba.wasp.master.metrics;


/**
 * This class is for maintaining the various master statistics and publishing
 * them through the metrics interfaces.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
public class MetricsMaster {
  private MetricsMasterSource masterSource;

  public MetricsMaster(MetricsMasterWrapper masterWrapper) {
    masterSource = MetricsMasterSourceFactory.create(masterWrapper);
  }

  // for unit-test usage
  public MetricsMasterSource getMetricsSource() {
    return masterSource;
  }

  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final int inc) {
    masterSource.incRequests(inc);

  }

  /**
   * set new value for number of entity group in transition.
   * @param egitCount
   */
  public void updateEGITCount(int egitCount) {
    masterSource.setEGIT(egitCount);
  }

  /**
   * update EGIT count that are in this state for more than the threshold as
   * defined by the property egit.metrics.threshold.time.
   * @param egitCountOverThreshold
   */
  public void updateEGITCountOverThreshold(int egitCountOverThreshold) {
    masterSource.setEGITCountOverThreshold(egitCountOverThreshold);
  }

  /**
   * update the timestamp for oldest entity group in transition metrics.
   * @param timestamp
   */
  public void updateEGITOldestAge(long timestamp) {
    masterSource.setEGITOldestAge(timestamp);
  }
}

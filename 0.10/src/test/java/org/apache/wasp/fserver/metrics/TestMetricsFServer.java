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
package org.apache.wasp.fserver.metrics;

import static org.junit.Assert.*;

import org.apache.wasp.fserver.MetricsFServerSourceImpl;
import org.junit.Test;

public class TestMetricsFServer {

  public MetricsAssertHelper HELPER = new MetricsAssertHelperImpl();

  @Test
  public void testWrapperSource() {
    MetricsFServer mfs = constructNewMetricsFServer();
    MetricsFServerSource serverSource = mfs.getMetricsSource();
    HELPER.assertTag("serverName", "test", serverSource);
    HELPER.assertTag("clusterId", "tClusterId", serverSource);
    HELPER.assertTag("zookeeperQuorum", "zk", serverSource);
    HELPER.assertGauge("fserverStartTime", 100, serverSource);
    HELPER.assertGauge("entityGroupCount", 101, serverSource);
    HELPER.assertCounter("readRequestCount", 201, serverSource);
    HELPER.assertCounter("writeRequestCount", 202, serverSource);
  }

  @Test
  public void testConstuctor() {
    MetricsFServer mfs = constructNewMetricsFServer();
    assertNotNull("There should be a hadoop1/hadoop2 metrics source",
        mfs.getMetricsSource());
    assertNotNull("The FServerMetricsWrapper should be accessable",
        mfs.getFServerWrapper());
  }

  @Test
  public void testSlowCount() {
    MetricsFServer mfs = constructNewMetricsFServer();
    MetricsFServerSource serverSource = mfs.getMetricsSource();
    for (int i = 0; i < 16; i++) {
      mfs.updateInsert(16);
      mfs.updateInsert(1006);
    }
    HELPER.assertCounter("insertNumOps", 32, serverSource);
    HELPER.assertCounter("slowInsertCount", 16, serverSource);
  }
  
  private MetricsFServer constructNewMetricsFServer() {
    MetricsFServerWrapper mfsWrapper = new MetricsFServerWrapperStub();
    MetricsFServer mfs = new MetricsFServer(mfsWrapper,new MetricsFServerSourceImpl(mfsWrapper));
    return mfs;
  }
}

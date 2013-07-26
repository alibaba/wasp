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


/**
 * This class is for maintaining the various fserver statistics and publishing
 * them through the metrics interfaces.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
public class MetricsFServer {
  private MetricsFServerSource serverSource;
  private MetricsFServerWrapper fserverWrapper;

  public MetricsFServer(MetricsFServerWrapper fserverWrapper) {
    this.fserverWrapper = fserverWrapper;
    serverSource = MetricsFServerSourceFactory.createServer(fserverWrapper);
  }

  // for unit-test usage
  MetricsFServer(MetricsFServerWrapper fserverWrapper,
      MetricsFServerSource serverSource) {
    this.fserverWrapper = fserverWrapper;
    this.serverSource = serverSource;
  }

  // for unit-test usage
  MetricsFServerSource getMetricsSource() {
    return serverSource;
  }

  public MetricsFServerWrapper getFServerWrapper() {
    return fserverWrapper;
  }

  public void updateInsert(long t) {
    if (t > 1000) {
      serverSource.incrSlowInsert();
    }
    serverSource.updateInsert(t);
  }

  public void updateGenPlan(long t) {
    if (t > 1000) {
      serverSource.incrSlowGenPlan();
    }
    serverSource.updateGenPlan(t);
  }

  public void updateUpdate(long t) {
    if (t > 1000) {
      serverSource.incrSlowUpdate();
    }
    serverSource.updateUpdate(t);
  }

  public void updateDelete(long t) {
    if (t > 1000) {
      serverSource.incrSlowDelete();
    }
    serverSource.updateDelete(t);
  }

  public void updateGet(long t) {
    if (t > 1000) {
      serverSource.incrSlowGet();
    }
    serverSource.updateGet(t);
  }

  public void updateScan(long t) {
    if (t > 1000) {
      serverSource.incrSlowScan();
    }
    serverSource.updateScan(t);
  }
}

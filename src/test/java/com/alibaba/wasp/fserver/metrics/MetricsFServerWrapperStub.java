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


public class MetricsFServerWrapperStub implements MetricsFServerWrapper {

  @Override
  public String getServerName() {
    return "test";
  }

  @Override
  public String getClusterId() {
    return "tClusterId";
  }

  @Override
  public String getZookeeperQuorum() {
    return "zk";
  }

  @Override
  public long getStartCode() {
    return 100;
  }

  @Override
  public long getNumOnlineEntityGroups() {
    return 101;
  }

  @Override
  public double getRequestsPerSecond() {
    return 403;
  }

  @Override
  public void forceRecompute() {
    // IGNORED.
  }

  @Override
  public double getReadRequestsPerSecond() {
    return 201;
  }

  @Override
  public double getWriteRequestsPerSecond() {
    return 202;
  }

}

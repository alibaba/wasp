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

/**
 * This is the interface that will expose FServer information to implementations
 * of the MetricsFServerSource.
 */
public interface MetricsFServerWrapper {

  /**
   * Get ServerName
   */
  public String getServerName();

  /**
   * Get the Cluster ID
   * 
   * @return Cluster ID
   */
  public String getClusterId();

  /**
   * Get the Zookeeper Quorum Info
   * 
   * @return Zookeeper Quorum Info
   */
  public String getZookeeperQuorum();

  /**
   * Get FServer start time
   * 
   * @return Start time of FServer in milliseconds
   */
  public long getStartCode();

  /**
   * The number of online entity groups
   */
  long getNumOnlineEntityGroups();

  /**
   * Get the number of requests per second.
   */
  double getRequestsPerSecond();

  /**
   * Get the number of read requests per second on this fserver.
   */
  double getReadRequestsPerSecond();

  /**
   * Get the number of write requests per second on this fserver.
   */
  double getWriteRequestsPerSecond();

  /**
   * Force a re-computation of the metrics.
   */
  void forceRecompute();
}

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

import org.apache.wasp.metrics.BaseSource;

/**
 * This interface will be implemented by a MetricsSource that will export
 * metrics from multiple entity groups into the hadoop metrics system.
 */
public interface MetricsEntityGroupAggregateSource extends BaseSource {

  /**
   * The name of the metrics
   */
  static final String METRICS_NAME = "EntityGroups";

  /**
   * The name of the metrics context that metrics will be under.
   */
  static final String METRICS_CONTEXT = "fserver";

  /**
   * Description
   */
  static final String METRICS_DESCRIPTION = "Metrics about Wasp FServer entity groups and tables";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  static final String METRICS_JMX_CONTEXT = "FServer,sub=" + METRICS_NAME;

  /**
   * Register a MetricsEntityGroupSource as being open.
   * 
   * @param source the source for the entity group being opened.
   */
  void register(MetricsEntityGroupSource source);

  /**
   * Remove a entity group's source. This is called when a entity group is
   * closed.
   * 
   * @param source The entity group to remove.
   */
  void deregister(MetricsEntityGroupSource source);
}

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

import org.apache.wasp.fserver.MetricsEntityGroupAggregateSourceImpl;
import org.apache.wasp.fserver.MetricsEntityGroupSourceImpl;
import org.apache.wasp.fserver.MetricsFServerSourceImpl;

/**
 * Factory to create MetricsFServerSource when given a MetricsFServerWrapper
 */
public class MetricsFServerSourceFactory {
  private static enum FactoryStorage {
    INSTANCE;
    private MetricsFServerSource serverSource;
    private MetricsEntityGroupAggregateSourceImpl aggImpl;
  }

  private static synchronized MetricsEntityGroupAggregateSourceImpl getAggregate() {
    if (FactoryStorage.INSTANCE.aggImpl == null) {
      FactoryStorage.INSTANCE.aggImpl = new MetricsEntityGroupAggregateSourceImpl();
    }
    return FactoryStorage.INSTANCE.aggImpl;
  }


  public static  synchronized MetricsFServerSource createServer(MetricsFServerWrapper fserverWrapper) {
    if (FactoryStorage.INSTANCE.serverSource == null) {
      FactoryStorage.INSTANCE.serverSource = new MetricsFServerSourceImpl(fserverWrapper);
    }
    return FactoryStorage.INSTANCE.serverSource;
  }

  public static MetricsEntityGroupSource createEntityGroup(MetricsEntityGroupWrapper wrapper) {
    return new MetricsEntityGroupSourceImpl(wrapper, getAggregate());
  }
}

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
package org.apache.wasp.fserver;

import java.util.TreeSet;

import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.wasp.fserver.metrics.MetricsEntityGroupAggregateSource;
import org.apache.wasp.fserver.metrics.MetricsEntityGroupSource;
import org.apache.wasp.metrics.BaseSourceImpl;

public class MetricsEntityGroupAggregateSourceImpl extends BaseSourceImpl
    implements MetricsEntityGroupAggregateSource {

  private final TreeSet<MetricsEntityGroupSource> entityGroupSources = new TreeSet<MetricsEntityGroupSource>();

  public MetricsEntityGroupAggregateSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT,
        METRICS_JMX_CONTEXT);
  }

  public MetricsEntityGroupAggregateSourceImpl(String metricsName,
      String metricsDescription, String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public void register(MetricsEntityGroupSource source) {
    entityGroupSources.add(source);
  }

  @Override
  public void deregister(MetricsEntityGroupSource source) {
    entityGroupSources.remove(source);
  }

  /**
   * Yes this is a get function that doesn't return anything. Thanks Hadoop for
   * breaking all expectations of java programmers. Instead of returning
   * anything Hadoop metrics expects getMetrics to push the metrics into the
   * metricsBuilder.
   * 
   * @param metricsBuilder Builder to accept metrics
   * @param all push all or only changed?
   */
  @Override
  public void getMetrics(MetricsBuilder metricsBuilder, boolean all) {

    MetricsRecordBuilder mrb = metricsBuilder.addRecord(metricsName)
        .setContext(metricsContext);

    if (entityGroupSources != null) {
      for (MetricsEntityGroupSource egMetricSource : entityGroupSources) {
        egMetricSource.snapshot(mrb, all);
      }
    }

    metricsRegistry.snapshot(mrb, all);
  }
}

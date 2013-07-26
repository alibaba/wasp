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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestMetricsEntityGroupSourceImpl {

  @Test
  public void testCompareTo() throws Exception {

    MetricsEntityGroupSource one = MetricsFServerSourceFactory
        .createEntityGroup(new EntityGroupWrapperStub("TEST"));
    MetricsEntityGroupSource oneClone = MetricsFServerSourceFactory
        .createEntityGroup(new EntityGroupWrapperStub("TEST"));
    MetricsEntityGroupSource two = MetricsFServerSourceFactory
        .createEntityGroup(new EntityGroupWrapperStub("TWO"));

    assertEquals(0, one.compareTo(oneClone));

    assertTrue(one.compareTo(two) < 0);
    assertTrue(two.compareTo(one) > 0);
  }

  class EntityGroupWrapperStub implements MetricsEntityGroupWrapper {

    private String entityGroupName;

    public EntityGroupWrapperStub(String entityGroupName) {

      this.entityGroupName = entityGroupName;
    }

    @Override
    public String getTableName() {
      return null; // To change body of implemented methods use File | Settings
                   // | File Templates.
    }

    @Override
    public String getEntityGroupName() {
      return this.entityGroupName;
    }

    @Override
    public long getReadRequestCount() {
      return 0; // To change body of implemented methods use File | Settings |
                // File Templates.
    }

    @Override
    public long getWriteRequestCount() {
      return 0; // To change body of implemented methods use File | Settings |
                // File Templates.
    }

    @Override
    public long getTransactionLogSize() {
      // TODO Auto-generated method stub
      return 0;
    }
  }
}

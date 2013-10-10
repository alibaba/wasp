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
package com.alibaba.wasp.fserver;

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.fserver.metrics.MetricsEntityGroupWrapper;
import com.alibaba.wasp.meta.FTable;

public class MetricsEntityGroupWrapperImpl implements MetricsEntityGroupWrapper {
  private final EntityGroup entiyGroup;

  public MetricsEntityGroupWrapperImpl(EntityGroup entiyGroup) {
    this.entiyGroup = entiyGroup;
  }

  @Override
  public String getTableName() {
    FTable tableDesc = this.entiyGroup.getTableDesc();
    if (tableDesc == null) {
      return "";
    }
    return tableDesc.getTableName();
  }

  @Override
  public String getEntityGroupName() {
    EntityGroupInfo entiyGroupInfo = this.entiyGroup.getEntityGroupInfo();
    if (entiyGroupInfo == null) {
      return "";
    }
    return entiyGroupInfo.getEncodedName();
  }

  @Override
  public long getReadRequestCount() {
    return this.entiyGroup.getReadRequestsCount();
  }

  @Override
  public long getWriteRequestCount() {
    return this.entiyGroup.getWriteRequestsCount();
  }

  @Override
  public long getTransactionLogSize() {
    return this.entiyGroup.getTransactionLogSize();
  }
}

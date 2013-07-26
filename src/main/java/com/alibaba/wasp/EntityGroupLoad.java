/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.wasp;

import com.alibaba.wasp.protobuf.generated.WaspProtos;import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.protobuf.generated.WaspProtos.EntityGroupLoadProtos;

/**
 * Encapsulates per-entityGroup load metrics.
 */
public class EntityGroupLoad {
  public WaspProtos.EntityGroupLoadProtos entityGroupLoadPB;

  public EntityGroupLoad(WaspProtos.EntityGroupLoadProtos entityGroupLoadPB) {
    this.entityGroupLoadPB = entityGroupLoadPB;
  }

  /**
   * @return the entityGroup name
   */
  public byte[] getName() {
    return entityGroupLoadPB.getEntityGroupSpecifier().getValue().toByteArray();
  }

  /**
   * @return the entityGroup name as a string
   */
  public String getNameAsString() {
    return Bytes.toString(getName());
  }

  /**
   * @return the number of requests made to entityGroup
   */
  public long getRequestsCount() {
    return getReadRequestsCount() + getWriteRequestsCount();
  }

  /**
   * @return the number of read requests made to entityGroup
   */
  public long getReadRequestsCount() {
    return entityGroupLoadPB.getReadRequestsCount();
  }

  /**
   * @return the number of write requests made to entityGroup
   */
  public long getWriteRequestsCount() {
    return entityGroupLoadPB.getWriteRequestsCount();
  }

  /**
   * @return the number of transactionlog made to entityGroup
   */
  public long getTransactionLogSize() {
    return entityGroupLoadPB.getTransactionLogSize();
  }
}
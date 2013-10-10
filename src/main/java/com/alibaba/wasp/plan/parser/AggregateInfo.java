/**
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
package com.alibaba.wasp.plan.parser;

import com.alibaba.wasp.meta.Field;

/**
 *
 */
public class AggregateInfo {

  public enum AggregateType{
    SUM("SUM"),COUNT("COUNT");

    public String methodName;

    public String getMethodName(){
      return this.methodName;
    }

    AggregateType(String methodName) {
      this.methodName = methodName;
    }
  }

  public final AggregateType aggregateType;

  public final Field field;

  public AggregateInfo(AggregateType aggregateType, Field field) throws UnsupportedException {
    this.aggregateType = aggregateType;
    this.field = field;
  }

  public AggregateType getAggregateType() {
    return aggregateType;
  }

  public Field getField() {
    return field;
  }

  public static AggregateType getAggregateTypeByMethod(String method) throws UnsupportedException {
    for (AggregateType aggregateType : AggregateType.values()) {
      if(aggregateType.getMethodName().equalsIgnoreCase(method)) {
        return aggregateType;
      }
    }
    throw new UnsupportedException("un support Aggregate Method : " + method);
  }

}

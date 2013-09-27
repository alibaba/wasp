/**
 * Copyright 2010 The Apache Software Foundation
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
package com.alibaba.wasp.plan.parser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

public class QueryInfo {
  public static enum QueryType {
    GET,
    SCAN,
    AGGREGATE
  }

  /** map field to value **/
  private LinkedHashMap<String, Condition> eqConditions = new LinkedHashMap<String, Condition>();

  private LinkedHashMap<String, Condition> rangeConditions = new LinkedHashMap<String, Condition>();

  private QueryType type;

  private boolean forUpdate = false;

  private AggregateInfo aggregateInfo;

  public QueryInfo(QueryType type, LinkedHashMap<String, Condition> fieldValue,
      LinkedHashMap<String, Condition> rangeCondition) throws UnsupportedException {
    this(type, fieldValue, rangeCondition, false);
  }

  public QueryInfo(QueryType type, LinkedHashMap<String, Condition> fieldValue,
      LinkedHashMap<String, Condition> rangeConditions, boolean forUpdate) throws UnsupportedException {
    this.type = type;
    this.forUpdate = forUpdate;
    this.eqConditions = fieldValue;
    this.rangeConditions = rangeConditions;
  }

  public Condition getField(String fieldName) {
    Condition condition = eqConditions.get(fieldName);
    if (condition != null) {
      return condition;
    }
    condition = rangeConditions.get(fieldName);
    if(condition != null) {
      return condition;
    }
    return null;
  }

  public QueryType getType() {
    return type;
  }

  public void setType(QueryType type) {
    this.type = type;
  }

  public boolean isForUpdate() {
    return forUpdate;
  }

  public void setForUpdate(boolean forUpdate) {
    this.forUpdate = forUpdate;
  }

  /**
   * @return the eqConditions
   */
  public LinkedHashMap<String, Condition> getEqConditions() {
    return eqConditions;
  }


  /**
   *
   * @return the rangeConditions
   */
  public LinkedHashMap<String, Condition> getRangeConditions() {
    return rangeConditions;
  }

  public AggregateInfo getAggregateInfo() {
    return aggregateInfo;
  }

  public void setAggregateInfo(AggregateInfo aggregateInfo) {
    this.aggregateInfo = aggregateInfo;
  }

  /**
   * Return all condition field name.
   * 
   * @return
   */
  public List<String> getAllConditionFieldName() {
    List<String> fields = new ArrayList<String>(eqConditions.size()
        + (rangeConditions == null ? 0 : rangeConditions.size()));
    Set<String> fieldSet = new TreeSet<String>();
    fieldSet.addAll(eqConditions.keySet());
    fieldSet.addAll(rangeConditions.keySet());
    fields.addAll(fieldSet);
    return fields;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TYPE: ");
    sb.append(type);
    sb.append("\n");
    sb.append(eqConditions.size() + rangeConditions.size());
    sb.append(" Filter Field:");
    Iterator<Entry<String, Condition>> iter = eqConditions.entrySet()
        .iterator();
    sb.append("\n");
    while (iter.hasNext()) {
      sb.append("\t");
      Condition entry = iter.next().getValue();
      sb.append(entry.toString());
      sb.append("\n");
    }
    sb.append("\t");
    iter = rangeConditions.entrySet()
        .iterator();
    sb.append("\n");
    while (iter.hasNext()) {
      sb.append("\t");
      Condition entry = iter.next().getValue();
      sb.append(entry.toString());
      sb.append("\n");
    }
    sb.append("\n");
    return sb.toString();
  }
}
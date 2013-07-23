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
package org.apache.wasp.plan.parser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class QueryInfo {
  public static enum QueryType {
    GET,
      SCAN
  }

  /** map field to value **/
  private LinkedHashMap<String, Condition> eqConditions = new LinkedHashMap<String, Condition>();

  private Condition rangeCondition;

  private QueryType type;

  public QueryInfo(QueryType type, LinkedHashMap<String, Condition> fieldValue,
      List<Condition> rangeCondition) throws UnsupportedException {
    this.type = type;
    initializeConditions(fieldValue, rangeCondition);
  }

  public Condition getField(String fieldName) {
    Condition condition = eqConditions.get(fieldName);
    if (condition != null) {
      return condition;
    }
    if (rangeCondition != null
        && rangeCondition.getFieldName().equalsIgnoreCase(fieldName)) {
      return rangeCondition;
    }
    return null;
  }

  private void initializeConditions(
      LinkedHashMap<String, Condition> fieldValue,
      List<Condition> rangeCondition) throws UnsupportedException {
    if (rangeCondition != null) {
      if (rangeCondition.size() > 1) {
        throw new UnsupportedException("More than one range condition.");
      } else if (rangeCondition.size() == 1) {
        this.rangeCondition = rangeCondition.get(0);
      } else {
        this.rangeCondition = null;
      }
    }
    this.eqConditions = fieldValue;
  }

  public QueryType getType() {
    return type;
  }

  public void setType(QueryType type) {
    this.type = type;
  }

  /**
   * @return the eqConditions
   */
  public LinkedHashMap<String, Condition> getEqConditions() {
    return eqConditions;
  }

  /**
   * @return the rangeCondition
   */
  public Condition getRangeCondition() {
    return rangeCondition;
  }

  /**
   * Return all condition field name.
   * 
   * @return
   */
  public List<String> getAllConditionFieldName() {
    List<String> fields = new ArrayList<String>(eqConditions.size()
        + (rangeCondition == null ? 0 : 1));
    Set<String> keySet = eqConditions.keySet();
    if (rangeCondition != null) {
      if (keySet.contains(rangeCondition.getFieldName())) {
        // keySet.add(rangeCondition.getFieldName());
        // java.lang.UnsupportedOperationException
        fields.addAll(keySet);
      } else {
        fields.addAll(keySet);
        fields.add(rangeCondition.getFieldName());
      }
    } else {
      fields.addAll(keySet);
    }
    return fields;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("TYPE: ");
    sb.append(type);
    sb.append("\n");
    sb.append(eqConditions.size() + (rangeCondition == null ? 0 : 1));
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
    if (rangeCondition != null) {
      sb.append(rangeCondition.toString());
    }
    sb.append("\n");
    return sb.toString();
  }
}
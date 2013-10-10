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
package com.alibaba.wasp.plan;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.wasp.plan.parser.QueryInfo;

public class DQLPlan extends Plan {
  
  /** common attribute **/
  private final Map<String, String> queryAttributes = new HashMap<String, String>();

  /** finished **/
  private boolean done = false;

  /** started **/
  private boolean started = false;

  private int fetchRows;
  
  private QueryInfo queryInfo;

  /**
   * Default constructor.
   */
  public DQLPlan() {
  }

  /**
   * @return the done
   */
  public boolean isDone() {
    return done;
  }

  /**
   * @param done the done to set
   */
  public void setDone(boolean done) {
    this.done = done;
  }

  /**
   * @return the started
   */
  public boolean isStarted() {
    return started;
  }

  /**
   * @param started the started to set
   */
  public void setStarted(boolean started) {
    this.started = started;
  }

  /**
   * @return the fetchRows
   */
  public int getFetchRows() {
    return fetchRows;
  }

  /**
   * @param fetchRows the fetchRows to set
   */
  public void setFetchRows(int fetchRows) {
    this.fetchRows = fetchRows;
  }

  /**
   * @return the queryAttributes
   */
  public Map<String, String> getQueryAttributes() {
    return queryAttributes;
  }
  

  /**
   * @return the queryInfo
   */
  public QueryInfo getQueryInfo() {
    return queryInfo;
  }

  /**
   * @param queryInfo
   *          the queryInfo to set
   */
  public void setQueryInfo(QueryInfo queryInfo) {
    this.queryInfo = queryInfo;
  }
}
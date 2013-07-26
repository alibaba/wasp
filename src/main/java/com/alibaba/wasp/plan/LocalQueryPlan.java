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
package com.alibaba.wasp.plan;

import com.alibaba.wasp.plan.action.GetAction;
import com.alibaba.wasp.plan.action.ScanAction;

/**
 * Driver parses sql to plan which includes entityGroup's local Query.Queryplan
 * normal is "select * from t where a=1;".
 * 
 */
public class LocalQueryPlan extends DQLPlan {

  /** info of entityGroup **/
  private ScanAction scanAction;

  /** info of entityGroup **/
  private GetAction getAction;

  public LocalQueryPlan(ScanAction scanAction) {
    this.scanAction = scanAction;
  }

  public LocalQueryPlan(GetAction action) {
    this.getAction = action;
  }

  /**
   * @return the getAction
   */
  public GetAction getGetAction() {
    return getAction;
  }

  /**
   * @param getAction
   *          the getAction to set
   */
  public void setGetAction(GetAction getAction) {
    this.getAction = getAction;
  }

  /**
   * @return the actions
   */
  public ScanAction getScanAction() {
    return scanAction;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "LocalQueryPlan [scanAction=" + scanAction + ", getAction=" + getAction
        + ", fetchRows=" + this.getFetchRows() + "]";
  }
}

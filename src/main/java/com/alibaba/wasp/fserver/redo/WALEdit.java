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
package com.alibaba.wasp.fserver.redo;

import com.alibaba.wasp.plan.action.Primary;

public class WALEdit {

  private Primary action;

  private Transaction t;

  /**
   * @param action
   * @param t
   */
  public WALEdit(Primary action, Transaction t) {
    this.action = action;
    this.t = t;
  }

  /**
   * @return the action
   */
  public Primary getAction() {
    return action;
  }

  /**
   * @param action
   *          the action to set
   */
  public void setAction(Primary action) {
    this.action = action;
  }

  /**
   * @return the t
   */
  public Transaction getT() {
    return t;
  }

  /**
   * @param t
   *          the t to set
   */
  public void setT(Transaction t) {
    this.t = t;
  }
}
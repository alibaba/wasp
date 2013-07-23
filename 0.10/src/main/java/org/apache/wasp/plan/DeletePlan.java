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
package org.apache.wasp.plan;

import java.util.List;

import org.apache.wasp.plan.action.DeleteAction;

/**
 * Driver parses sql to plan which includes Delete normal is
 * "DELETE FROM users WHERE id = 123;".
 * 
 */
public class DeletePlan extends DMLPlan {

  private List<DeleteAction> actions;

  public DeletePlan(List<DeleteAction> actions) {
    this.actions = actions;
  }

  public List<DeleteAction> getActions() {
    return actions;
  }

  public void setActions(List<DeleteAction> actions) {
    this.actions = actions;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "DeletePlan [actions=" + actions + "]";
  }
}
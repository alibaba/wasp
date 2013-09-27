/**
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
package com.alibaba.wasp.master;

import com.alibaba.wasp.EntityGroupInfo;

import java.util.concurrent.Callable;


/**
 * A callable object tEntityGroupInfo the corresponding action that needs to be
 * taken for unassignment of a entityGroup in transition. Implementing as future
 * callable we are able to act on the timeout asynchronously.
 */
public class UnAssignCallable implements Callable<Object> {
  private AssignmentManager assignmentManager;

  private EntityGroupInfo egInfo;

  public UnAssignCallable(AssignmentManager assignmentManager, EntityGroupInfo egInfo) {
    this.assignmentManager = assignmentManager;
    this.egInfo = egInfo;
  }

  @Override
  public Object call() throws Exception {
    assignmentManager.unassign(egInfo, true);
    return null;
  }
}

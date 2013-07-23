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
package org.apache.wasp.fserver;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.plan.action.Action;

public interface TwoPhaseCommitProtocol {

  /**
   * cancel this commit
   * 
   * @throws IOException
   */
  abstract void rollback();

  /**
   * apply
   * 
   * @throws IOException
   */
  abstract void commit();

  /**
   * one phase:pre commit
   * 
   * @throws IOException
   */
  abstract boolean prepare();

  /**
   * 
   * @param transcations
   * @throws IOException
   */
  abstract boolean submit(Map<EntityGroupInfo, List<Action>> transcations);
}

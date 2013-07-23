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
package org.apache.wasp.plan.execute;

import java.util.List;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.wasp.fserver.FServer;
import org.apache.wasp.fserver.TwoPhaseCommitProtocol;
import org.apache.wasp.plan.DQLPlan;
import org.apache.wasp.plan.DeletePlan;
import org.apache.wasp.plan.InsertPlan;
import org.apache.wasp.plan.UpdatePlan;
import org.apache.wasp.protobuf.generated.ClientProtos.StringDataTypePair;
import org.apache.wasp.protobuf.generated.ClientProtos.QueryResultProto;
import org.apache.wasp.protobuf.generated.ClientProtos.WriteResultProto;

import com.google.protobuf.ServiceException;

public interface Execution {

  /**
   * execute QueryPlan
   * 
   * @param plan
   * @param sessionId
   * @return
   * @throws ServiceException
   */
  public Pair<Boolean, Pair<String, Pair<List<QueryResultProto>, List<StringDataTypePair>>>> execQueryPlan(
      DQLPlan plan, String sessionId, boolean closeSession)
      throws ServiceException;

  /**
   * Execute UpdatePlan with some regular 1. when only have one Action in the
   * UpdatePlan. it will update without 2pc 2. If not 2pc. if the Action execute
   * on local EntityGroup. just call
   * {@link FServer#update(byte[], org.apache.wasp.plan.UpdateAction)} ,
   * otherwise it needed process the action by RPC. 3. If 2pc. just call
   * {@link TwoPhaseCommitProtocol#submit(java.util.Map)}
   * 
   * @param plan
   */
  public List<WriteResultProto> execUpdatePlan(UpdatePlan plan)
      throws ServiceException;

  /**
   * Execute InsertPlan with some regular 1. when only have one Action in the
   * InsertPlan. it will insert without 2pc 2. If not 2pc. if the Action execute
   * on local EntityGroup. just call
   * {@link FServer#insert(byte[], org.apache.wasp.plan.InsertAction)} ,
   * otherwise it needed process the action by RPC. 3. If 2pc. just call
   * {@link TwoPhaseCommitProtocol#submit(java.util.Map)}
   * 
   * @param plan
   * @return
   */
  public List<WriteResultProto> execInsertPlan(InsertPlan plan)
      throws ServiceException;

  /**
   * Execute DeletePlan with some regular 1. when only have one Action in the
   * DeletePlan. it will delete without 2pc 2. If not 2pc. if the Action execute
   * on local EntityGroup. just call
   * {@link FServer#delete(byte[], org.apache.wasp.plan.DeleteAction)} ,
   * otherwise it needed process the action by RPC. 3. If 2pc. just call
   * {@link TwoPhaseCommitProtocol#submit(java.util.Map)}
   * 
   * @param plan
   * @return
   */
  public List<WriteResultProto> execDeletePlan(DeletePlan plan)
      throws ServiceException;
}
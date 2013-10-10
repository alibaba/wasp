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
package com.alibaba.wasp.plan.execute;

import com.alibaba.wasp.plan.DMLTransactionPlan;
import com.alibaba.wasp.plan.DQLPlan;
import com.alibaba.wasp.plan.DeletePlan;
import com.alibaba.wasp.plan.InsertPlan;
import com.alibaba.wasp.plan.UpdatePlan;
import com.alibaba.wasp.protobuf.generated.ClientProtos.QueryResultProto;
import com.alibaba.wasp.protobuf.generated.ClientProtos.StringDataTypePair;
import com.alibaba.wasp.protobuf.generated.ClientProtos.WriteResultProto;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;

public interface Execution {

  /**
   * execute QueryPlan
   * 
   * @param plan
   * @param sessionId
   * @return
   * @throws com.google.protobuf.ServiceException
   */
  public Pair<Boolean, Pair<String, Pair<List<QueryResultProto>, List<StringDataTypePair>>>> execQueryPlan(
      DQLPlan plan, String sessionId, boolean closeSession)
      throws ServiceException;

  /**
   * Execute UpdatePlan with some regular 1. when only have one Action in the
   * UpdatePlan. it will update without 2pc 2. If not 2pc. if the Action execute
   * on local EntityGroup. just call
   * {@link com.alibaba.wasp.fserver.FServer#update(byte[], com.alibaba.wasp.plan.action.UpdateAction)} ,
   * otherwise it needed process the action by RPC. 3. If 2pc. just call
   * {@link com.alibaba.wasp.fserver.TwoPhaseCommitProtocol#submit(java.util.Map)}
   *
   * @param plan
   */
  public List<WriteResultProto> execUpdatePlan(UpdatePlan plan)
      throws ServiceException;

  /**
   * Execute InsertPlan with some regular 1. when only have one Action in the
   * InsertPlan. it will insert without 2pc 2. If not 2pc. if the Action execute
   * on local EntityGroup. just call
   * {@link com.alibaba.wasp.fserver.FServer#insert(byte[], com.alibaba.wasp.plan.action.InsertAction)} ,
   * otherwise it needed process the action by RPC. 3. If 2pc. just call
   * {@link com.alibaba.wasp.fserver.TwoPhaseCommitProtocol#submit(java.util.Map)}
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
   * {@link com.alibaba.wasp.fserver.FServer#delete(byte[], com.alibaba.wasp.plan.action.DeleteAction)} ,
   * otherwise it needed process the action by RPC. 3. If 2pc. just call
   * {@link com.alibaba.wasp.fserver.TwoPhaseCommitProtocol#submit(java.util.Map)}
   *
   * @param plan
   * @return
   */
  public List<WriteResultProto> execDeletePlan(DeletePlan plan)
      throws ServiceException;

  /**
   *
   * Execute DMLTransactionPlan with some regular
   * 1. the plans in a same transaction
   * 2. the plans effect tables must be filiation.
   * 3. run with eg transaction. not 2pc
   *
   * @param dmlTransactionPlan
   * @return
   * @throws com.google.protobuf.ServiceException
   */
  public List<WriteResultProto> execDMLTransactionPlans(DMLTransactionPlan dmlTransactionPlan)
      throws ServiceException;

}
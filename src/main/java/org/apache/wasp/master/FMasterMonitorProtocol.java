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
package org.apache.wasp.master;

import org.apache.wasp.protobuf.generated.MasterMonitorProtos.GetClusterStatusRequest;
import org.apache.wasp.protobuf.generated.MasterMonitorProtos.GetClusterStatusResponse;
import org.apache.wasp.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusRequest;
import org.apache.wasp.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusResponse;
import org.apache.wasp.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsRequest;
import org.apache.wasp.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsResponse;
import org.apache.wasp.protobuf.generated.MasterMonitorProtos.MasterMonitorService;
import org.apache.wasp.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.wasp.protobuf.generated.MasterProtos.IsMasterRunningResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Protocol that a client uses to communicate with the FMaster (for monitoring
 * purposes).
 */
public interface FMasterMonitorProtocol extends
    MasterMonitorService.BlockingInterface, MasterProtocol {

  public static final long VERSION = 1L;

  /**
   * Used by the client to get the number of entityGroups that have received the
   * updated schema
   * 
   * @param controller
   *          Unused (set to null).
   * @param req
   *          GetSchemaAlterStatusRequest that contains:<br>
   *          - tableName
   * @return GetSchemaAlterStatusResponse indicating the number of entityGroups
   *         updated. yetToUpdateEntityGroups is the entityGroups that are yet
   *         to be updated totalEntityGroups is the total number of entityGroups
   *         of the table
   * @throws ServiceException
   */
  @Override
  public GetSchemaAlterStatusResponse getSchemaAlterStatus(
      RpcController controller, GetSchemaAlterStatusRequest req)
      throws ServiceException;

  /**
   * Get list of TableDescriptors for requested tables.
   * 
   * @param controller
   *          Unused (set to null).
   * @param req
   *          GetTableDescriptorsRequest that contains:<br>
   *          - tableNames: requested tables, or if empty, all are requested
   * @return GetTableDescriptorsResponse
   * @throws ServiceException
   */
  @Override
  public GetTableDescriptorsResponse getTableDescriptors(
      RpcController controller, GetTableDescriptorsRequest req)
      throws ServiceException;

  /**
   * Return cluster status.
   * 
   * @param controller
   *          Unused (set to null).
   * @param req
   *          GetClusterStatusRequest
   * @return status object
   * @throws ServiceException
   */
  @Override
  public GetClusterStatusResponse getClusterStatus(RpcController controller,
      GetClusterStatusRequest req) throws ServiceException;

  /**
   * @param c
   *          Unused (set to null).
   * @param req
   *          IsMasterRunningRequest
   * @return IsMasterRunningRequest that contains:<br>
   *         isMasterRunning: true if master is available
   * @throws ServiceException
   */
  @Override
  public IsMasterRunningResponse isMasterRunning(RpcController c,
      IsMasterRunningRequest req) throws ServiceException;
}

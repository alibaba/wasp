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
package com.alibaba.wasp.master;

import com.alibaba.wasp.protobuf.generated.MasterAdminProtos;import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.AssignEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.AssignEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.BalanceRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.BalanceResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.CreateTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.CreateTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DeleteTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DeleteTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DisableTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DisableTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.EnableTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.EnableTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.IsTableLockedRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.IsTableLockedResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MasterAdminService;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.ModifyTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.ModifyTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MoveEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MoveEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.OfflineEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.OfflineEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.SetBalancerRunningRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.SetBalancerRunningResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.SetTableStateRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.SetTableStateResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.ShutdownRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.ShutdownResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.StopMasterRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.StopMasterResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.TruncateTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.TruncateTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.UnassignEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.UnassignEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.UnlockTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.UnlockTableResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Protocol that a client uses to communicate with the FMaster (for admin
 * purposes).
 */
public interface FMasterAdminProtocol extends
    MasterAdminProtos.MasterAdminService.BlockingInterface, MasterProtocol {

  public static final long VERSION = 1L;

  /* EntityGroup-level */

  /**
   * Move a entityGroup to a specified destination server.
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.MoveEntityGroupResponse moveEntityGroup(RpcController controller,
      MasterAdminProtos.MoveEntityGroupRequest request) throws ServiceException;

  /**
   * Assign a entityGroup to a server chosen at random.
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.AssignEntityGroupResponse assignEntityGroup(RpcController controller,
      MasterAdminProtos.AssignEntityGroupRequest request) throws ServiceException;

  /**
   * Unassign a entityGroup from current hosting fserver. EntityGroup will then
   * be assigned to a fserver chosen at random. EntityGroup could be reassigned
   * back to the same server. Use {@link #moveEntityGroup} if you want to
   * control the entityGroup movement.
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.UnassignEntityGroupResponse unassignEntityGroup(
      RpcController controller, MasterAdminProtos.UnassignEntityGroupRequest request)
      throws ServiceException;

  /**
   * Offline a entityGroup from the assignment manager's in-memory state. The
   * entity should be in a closed state and there will be no attempt to
   * automatically reassign the entityGroup as in unassign. This is a special
   * method, and should only be used by experts or hbck.
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.OfflineEntityGroupResponse offlineEntityGroup(
      RpcController controller, MasterAdminProtos.OfflineEntityGroupRequest request)
      throws ServiceException;

  /* Table-level */

  /**
   * Creates a new table asynchronously. If splitKeys are specified, then the
   * table will be created with an initial set of multiple entities. If
   * splitKeys is null, the table will be created with a single entityGroup.
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.CreateTableResponse createTable(RpcController controller,
      MasterAdminProtos.CreateTableRequest request) throws ServiceException;

  /**
   * Deletes a table
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.DeleteTableResponse deleteTable(RpcController controller,
      MasterAdminProtos.DeleteTableRequest request) throws ServiceException;

  /**
   * Puts the table on-line (only needed if table has been previously taken
   * offline)
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.EnableTableResponse enableTable(RpcController controller,
      MasterAdminProtos.EnableTableRequest request) throws ServiceException;

  /**
   * Take table offline
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.DisableTableResponse disableTable(RpcController controller,
      MasterAdminProtos.DisableTableRequest request) throws ServiceException;

  /**
   * Modify a table's metadata
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.ModifyTableResponse modifyTable(RpcController controller,
      MasterAdminProtos.ModifyTableRequest request) throws ServiceException;

  /**
   * Truncate a table
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.TruncateTableResponse truncateTable(RpcController controller,
      MasterAdminProtos.TruncateTableRequest request) throws ServiceException;

  /* Cluster-level */

  /**
   * Shutdown an wasp cluster.
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.ShutdownResponse shutdown(RpcController controller,
      MasterAdminProtos.ShutdownRequest request) throws ServiceException;

  /**
   * Stop Wasp Master only. Does not shutdown the cluster.
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.StopMasterResponse stopMaster(RpcController controller,
      MasterAdminProtos.StopMasterRequest request) throws ServiceException;

  /**
   * Run the balancer. Will run the balancer and if entities to move, it will go
   * ahead and do the reassignments. Can NOT run for various reasons. Check
   * logs.
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.BalanceResponse balance(RpcController controller,
      MasterAdminProtos.BalanceRequest request) throws ServiceException;

  /**
   * Turn the load balancer on or off.
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.SetBalancerRunningResponse setBalancerRunning(
      RpcController controller, MasterAdminProtos.SetBalancerRunningRequest request)
      throws ServiceException;

  /**
   * Return if the table locked.
   * 
   * @throws ServiceException
   */
  public MasterAdminProtos.IsTableLockedResponse isTableLocked(RpcController controller,
      MasterAdminProtos.IsTableLockedRequest request) throws ServiceException;

  /**
   * Unlock the table
   * @throws ServiceException
   */
  public MasterAdminProtos.UnlockTableResponse unlockTable(RpcController controller, MasterAdminProtos.UnlockTableRequest request)
      throws ServiceException;

  /**
   * Set table state
   * @throws ServiceException
   */
  public MasterAdminProtos.SetTableStateResponse
      setTableState(RpcController controller, MasterAdminProtos.SetTableStateRequest request) throws ServiceException;
}

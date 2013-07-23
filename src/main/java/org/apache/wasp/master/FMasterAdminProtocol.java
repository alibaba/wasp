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

import org.apache.wasp.protobuf.generated.MasterAdminProtos.AssignEntityGroupRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.AssignEntityGroupResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.BalanceRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.BalanceResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.CreateTableRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.CreateTableResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.DeleteTableRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.DeleteTableResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.DisableTableRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.DisableTableResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.EnableTableRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.EnableTableResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.IsTableLockedRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.IsTableLockedResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.MasterAdminService;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.ModifyTableRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.ModifyTableResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.MoveEntityGroupRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.MoveEntityGroupResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.OfflineEntityGroupRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.OfflineEntityGroupResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.SetBalancerRunningRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.SetBalancerRunningResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.SetTableStateRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.SetTableStateResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.ShutdownRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.ShutdownResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.StopMasterRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.StopMasterResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.TruncateTableRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.TruncateTableResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.UnassignEntityGroupRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.UnassignEntityGroupResponse;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.UnlockTableRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.UnlockTableResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Protocol that a client uses to communicate with the FMaster (for admin
 * purposes).
 */
public interface FMasterAdminProtocol extends
    MasterAdminService.BlockingInterface, MasterProtocol {

  public static final long VERSION = 1L;

  /* EntityGroup-level */

  /**
   * Move a entityGroup to a specified destination server.
   * 
   * @throws ServiceException
   */
  public MoveEntityGroupResponse moveEntityGroup(RpcController controller,
      MoveEntityGroupRequest request) throws ServiceException;

  /**
   * Assign a entityGroup to a server chosen at random.
   * 
   * @throws ServiceException
   */
  public AssignEntityGroupResponse assignEntityGroup(RpcController controller,
      AssignEntityGroupRequest request) throws ServiceException;

  /**
   * Unassign a entityGroup from current hosting fserver. EntityGroup will then
   * be assigned to a fserver chosen at random. EntityGroup could be reassigned
   * back to the same server. Use {@link #moveEntityGroup} if you want to
   * control the entityGroup movement.
   * 
   * @throws ServiceException
   */
  public UnassignEntityGroupResponse unassignEntityGroup(
      RpcController controller, UnassignEntityGroupRequest request)
      throws ServiceException;

  /**
   * Offline a entityGroup from the assignment manager's in-memory state. The
   * entity should be in a closed state and there will be no attempt to
   * automatically reassign the entityGroup as in unassign. This is a special
   * method, and should only be used by experts or hbck.
   * 
   * @throws ServiceException
   */
  public OfflineEntityGroupResponse offlineEntityGroup(
      RpcController controller, OfflineEntityGroupRequest request)
      throws ServiceException;

  /* Table-level */

  /**
   * Creates a new table asynchronously. If splitKeys are specified, then the
   * table will be created with an initial set of multiple entities. If
   * splitKeys is null, the table will be created with a single entityGroup.
   * 
   * @throws ServiceException
   */
  public CreateTableResponse createTable(RpcController controller,
      CreateTableRequest request) throws ServiceException;

  /**
   * Deletes a table
   * 
   * @throws ServiceException
   */
  public DeleteTableResponse deleteTable(RpcController controller,
      DeleteTableRequest request) throws ServiceException;

  /**
   * Puts the table on-line (only needed if table has been previously taken
   * offline)
   * 
   * @throws ServiceException
   */
  public EnableTableResponse enableTable(RpcController controller,
      EnableTableRequest request) throws ServiceException;

  /**
   * Take table offline
   * 
   * @throws ServiceException
   */
  public DisableTableResponse disableTable(RpcController controller,
      DisableTableRequest request) throws ServiceException;

  /**
   * Modify a table's metadata
   * 
   * @throws ServiceException
   */
  public ModifyTableResponse modifyTable(RpcController controller,
      ModifyTableRequest request) throws ServiceException;

  /**
   * Truncate a table
   * 
   * @throws ServiceException
   */
  public TruncateTableResponse truncateTable(RpcController controller,
      TruncateTableRequest request) throws ServiceException;

  /* Cluster-level */

  /**
   * Shutdown an wasp cluster.
   * 
   * @throws ServiceException
   */
  public ShutdownResponse shutdown(RpcController controller,
      ShutdownRequest request) throws ServiceException;

  /**
   * Stop Wasp Master only. Does not shutdown the cluster.
   * 
   * @throws ServiceException
   */
  public StopMasterResponse stopMaster(RpcController controller,
      StopMasterRequest request) throws ServiceException;

  /**
   * Run the balancer. Will run the balancer and if entities to move, it will go
   * ahead and do the reassignments. Can NOT run for various reasons. Check
   * logs.
   * 
   * @throws ServiceException
   */
  public BalanceResponse balance(RpcController controller,
      BalanceRequest request) throws ServiceException;

  /**
   * Turn the load balancer on or off.
   * 
   * @throws ServiceException
   */
  public SetBalancerRunningResponse setBalancerRunning(
      RpcController controller, SetBalancerRunningRequest request)
      throws ServiceException;

  /**
   * Return if the table locked.
   * 
   * @throws ServiceException
   */
  public IsTableLockedResponse isTableLocked(RpcController controller,
      IsTableLockedRequest request) throws ServiceException;

  /**
   * Unlock the table
   * @throws ServiceException
   */
  public UnlockTableResponse unlockTable(RpcController controller, UnlockTableRequest request)
      throws ServiceException;

  /**
   * Set table state
   * @throws ServiceException
   */
  public SetTableStateResponse
      setTableState(RpcController controller, SetTableStateRequest request) throws ServiceException;
}

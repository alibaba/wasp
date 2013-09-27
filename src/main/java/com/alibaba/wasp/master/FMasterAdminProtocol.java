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

import com.alibaba.wasp.protobuf.generated.MasterAdminProtos;
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
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.MoveEntityGroupResponse moveEntityGroup(RpcController controller,
                                                                   MasterAdminProtos.MoveEntityGroupRequest request) throws ServiceException;

  /**
   * Assign a entityGroup to a server chosen at random.
   *
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.AssignEntityGroupResponse assignEntityGroup(RpcController controller,
                                                                       MasterAdminProtos.AssignEntityGroupRequest request) throws ServiceException;

  /**
   * Unassign a entityGroup from current hosting fserver. EntityGroup will then
   * be assigned to a fserver chosen at random. EntityGroup could be reassigned
   * back to the same server. Use {@link #moveEntityGroup} if you want to
   * control the entityGroup movement.
   *
   * @throws com.google.protobuf.ServiceException
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
   * @throws com.google.protobuf.ServiceException
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
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.CreateTableResponse createTable(RpcController controller,
                                                           MasterAdminProtos.CreateTableRequest request) throws ServiceException;

  /**
   * Deletes a table
   *
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.DeleteTableResponse deleteTable(RpcController controller,
                                                           MasterAdminProtos.DeleteTableRequest request) throws ServiceException;

  /**
   * Puts the table on-line (only needed if table has been previously taken
   * offline)
   *
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.EnableTableResponse enableTable(RpcController controller,
                                                           MasterAdminProtos.EnableTableRequest request) throws ServiceException;

  /**
   * Take table offline
   *
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.DisableTableResponse disableTable(RpcController controller,
                                                             MasterAdminProtos.DisableTableRequest request) throws ServiceException;

  /**
   * Modify a table's metadata
   *
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.ModifyTableResponse modifyTable(RpcController controller,
                                                           MasterAdminProtos.ModifyTableRequest request) throws ServiceException;

  /**
   * Truncate a table
   *
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.TruncateTableResponse truncateTable(RpcController controller,
                                                               MasterAdminProtos.TruncateTableRequest request) throws ServiceException;

  /* Cluster-level */

  /**
   * Shutdown an wasp cluster.
   *
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.ShutdownResponse shutdown(RpcController controller,
                                                     MasterAdminProtos.ShutdownRequest request) throws ServiceException;

  /**
   * Stop Wasp Master only. Does not shutdown the cluster.
   *
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.StopMasterResponse stopMaster(RpcController controller,
                                                         MasterAdminProtos.StopMasterRequest request) throws ServiceException;

  /**
   * Run the balancer. Will run the balancer and if entities to move, it will go
   * ahead and do the reassignments. Can NOT run for various reasons. Check
   * logs.
   *
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.BalanceResponse balance(RpcController controller,
                                                   MasterAdminProtos.BalanceRequest request) throws ServiceException;

  /**
   * Turn the load balancer on or off.
   *
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.SetBalancerRunningResponse setBalancerRunning(
      RpcController controller, MasterAdminProtos.SetBalancerRunningRequest request)
      throws ServiceException;

  /**
   * Return if the table locked.
   *
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.IsTableLockedResponse isTableLocked(RpcController controller,
                                                               MasterAdminProtos.IsTableLockedRequest request) throws ServiceException;

  /**
   * Unlock the table
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.UnlockTableResponse unlockTable(RpcController controller, MasterAdminProtos.UnlockTableRequest request)
      throws ServiceException;

  /**
   * Set table state
   * @throws com.google.protobuf.ServiceException
   */
  public MasterAdminProtos.SetTableStateResponse
      setTableState(RpcController controller, MasterAdminProtos.SetTableStateRequest request) throws ServiceException;
}

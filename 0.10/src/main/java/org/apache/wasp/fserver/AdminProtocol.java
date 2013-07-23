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
package org.apache.wasp.fserver;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.wasp.ipc.VersionedProtocol;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.CloseEntityGroupRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.CloseEntityGroupResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.DisableTableRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.DisableTableResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.EnableTableRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.EnableTableResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupsRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupsResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.SplitEntityGroupRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.SplitEntityGroupResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public interface AdminProtocol extends FServerAdminService.BlockingInterface,
    VersionedProtocol, Stoppable, Abortable {
  public static final long VERSION = 1L;

  /**
   * open a entityGroup from current hosting fServer. Use
   * {@link #closeEntityGroup} if you want to close the entityGroup.
   * 
   * @see org.apache.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#openEntityGroup(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupRequest)
   */
  @Override
  public OpenEntityGroupResponse openEntityGroup(
      RpcController controller,
      org.apache.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupRequest request)
      throws ServiceException;

  /**
   * close a entityGroup from current hosting fserver. Use
   * {@link #openEntityGroup} if you want to open a entityGroup.
   * 
   * @throws com.google.protobuf.ServiceException
   */
  @Override
  public CloseEntityGroupResponse closeEntityGroup(RpcController controller,
      CloseEntityGroupRequest request) throws ServiceException;

  /**
   * split a entityGroup from current hosting fserver. Use
   * {@link #openEntityGroup} if you want to open a entityGroup.
   * 
   * @throws com.google.protobuf.ServiceException
   */
  @Override
  public SplitEntityGroupResponse splitEntityGroup(RpcController controller,
      SplitEntityGroupRequest request) throws ServiceException;

  /**
   * @see org.apache.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#disableTable(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.FServerAdminProtos.DisableTableRequest)
   */
  @Override
  public DisableTableResponse disableServerTable(RpcController controller,
      DisableTableRequest request) throws ServiceException;

  /**
   * @see org.apache.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#enableTable(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.FServerAdminProtos.EnableTableRequest)
   */
  @Override
  public EnableTableResponse enableServerTable(RpcController controller,
      EnableTableRequest request) throws ServiceException;

  /**
   * @see org.apache.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#getEntityGroupInfo(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoRequest)
   */
  @Override
  public GetEntityGroupInfoResponse getEntityGroupInfo(
      RpcController controller, GetEntityGroupInfoRequest request)
      throws ServiceException;

  /**
   * @see org.apache.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#getOnlineEntityGroups(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsRequest)
   */
  @Override
  public GetOnlineEntityGroupsResponse getOnlineEntityGroups(
      RpcController controller, GetOnlineEntityGroupsRequest request)
      throws ServiceException;

  /**
   * @see org.apache.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#openEntityGroups(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupsRequest)
   */
  @Override
  public OpenEntityGroupsResponse openEntityGroups(RpcController controller,
      OpenEntityGroupsRequest request) throws ServiceException;
}

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
package com.alibaba.wasp.fserver;

import com.alibaba.wasp.ipc.VersionedProtocol;import com.alibaba.wasp.protobuf.generated.FServerAdminProtos;import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Stoppable;
import com.alibaba.wasp.ipc.VersionedProtocol;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.DisableTableRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.DisableTableResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.EnableTableRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.EnableTableResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupsRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupsResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.SplitEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.SplitEntityGroupResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public interface AdminProtocol extends FServerAdminProtos.FServerAdminService.BlockingInterface,
    VersionedProtocol, Stoppable, Abortable {
  public static final long VERSION = 1L;

  /**
   * open a entityGroup from current hosting fServer. Use
   * {@link #closeEntityGroup} if you want to close the entityGroup.
   * 
   * @see com.alibaba.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#openEntityGroup(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupRequest )
   */
  @Override
  public FServerAdminProtos.OpenEntityGroupResponse openEntityGroup(
      RpcController controller,
      FServerAdminProtos.OpenEntityGroupRequest request)
      throws ServiceException;

  /**
   * close a entityGroup from current hosting fserver. Use
   * {@link #openEntityGroup} if you want to open a entityGroup.
   * 
   * @throws com.google.protobuf.ServiceException
   */
  @Override
  public FServerAdminProtos.CloseEntityGroupResponse closeEntityGroup(RpcController controller,
      FServerAdminProtos.CloseEntityGroupRequest request) throws ServiceException;

  /**
   * split a entityGroup from current hosting fserver. Use
   * {@link #openEntityGroup} if you want to open a entityGroup.
   * 
   * @throws com.google.protobuf.ServiceException
   */
  @Override
  public FServerAdminProtos.SplitEntityGroupResponse splitEntityGroup(RpcController controller,
      FServerAdminProtos.SplitEntityGroupRequest request) throws ServiceException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#disableTable(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.DisableTableRequest )
   */
  @Override
  public FServerAdminProtos.DisableTableResponse disableServerTable(RpcController controller,
      FServerAdminProtos.DisableTableRequest request) throws ServiceException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#enableTable(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.EnableTableRequest )
   */
  @Override
  public FServerAdminProtos.EnableTableResponse enableServerTable(RpcController controller,
      FServerAdminProtos.EnableTableRequest request) throws ServiceException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#getEntityGroupInfo(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoRequest )
   */
  @Override
  public FServerAdminProtos.GetEntityGroupInfoResponse getEntityGroupInfo(
      RpcController controller, FServerAdminProtos.GetEntityGroupInfoRequest request)
      throws ServiceException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#getOnlineEntityGroups(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsRequest )
   */
  @Override
  public FServerAdminProtos.GetOnlineEntityGroupsResponse getOnlineEntityGroups(
      RpcController controller, FServerAdminProtos.GetOnlineEntityGroupsRequest request)
      throws ServiceException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#openEntityGroups(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupsRequest )
   */
  @Override
  public FServerAdminProtos.OpenEntityGroupsResponse openEntityGroups(RpcController controller,
      FServerAdminProtos.OpenEntityGroupsRequest request) throws ServiceException;
}

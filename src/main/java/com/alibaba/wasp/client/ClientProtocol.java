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
package com.alibaba.wasp.client;

import com.alibaba.wasp.ipc.VersionedProtocol;
import com.alibaba.wasp.protobuf.generated.ClientProtos;
import com.alibaba.wasp.ipc.VersionedProtocol;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ClientService;
import com.alibaba.wasp.protobuf.generated.ClientProtos.DeleteRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.DeleteResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.GetRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.GetResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.InsertRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.InsertResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ScanRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ScanResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.UpdateRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.UpdateResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * 
 * Protocol that a wasp client and wasp server use to communicate with a wasp
 * server.
 * 
 */
public interface ClientProtocol extends ClientProtos.ClientService.BlockingInterface,
    VersionedProtocol {
  public static final long VERSION = 1L;

  /**
   * @see com.alibaba.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#execute(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteRequest )
   */
  @Override
  public ClientProtos.ExecuteResponse execute(RpcController controller,
      ClientProtos.ExecuteRequest request) throws ServiceException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#get(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.GetRequest )
   */
  @Override
  public ClientProtos.GetResponse get(RpcController controller, ClientProtos.GetRequest request)
      throws ServiceException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#scan(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.ScanRequest )
   */
  @Override
  public ClientProtos.ScanResponse scan(RpcController controller, ClientProtos.ScanRequest request)
      throws ServiceException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#insert(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.InsertRequest )
   */
  @Override
  public ClientProtos.InsertResponse insert(RpcController controller, ClientProtos.InsertRequest request)
      throws ServiceException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#update(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.UpdateRequest )
   */
  @Override
  public ClientProtos.UpdateResponse update(RpcController controller, ClientProtos.UpdateRequest request)
      throws ServiceException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#delete(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.DeleteRequest )
   */
  @Override
  public ClientProtos.DeleteResponse delete(RpcController controller, ClientProtos.DeleteRequest request)
      throws ServiceException;
}

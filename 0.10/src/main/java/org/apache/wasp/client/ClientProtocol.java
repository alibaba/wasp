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
package org.apache.wasp.client;

import org.apache.wasp.ipc.VersionedProtocol;
import org.apache.wasp.protobuf.generated.ClientProtos.ClientService;
import org.apache.wasp.protobuf.generated.ClientProtos.DeleteRequest;
import org.apache.wasp.protobuf.generated.ClientProtos.DeleteResponse;
import org.apache.wasp.protobuf.generated.ClientProtos.ExecuteRequest;
import org.apache.wasp.protobuf.generated.ClientProtos.ExecuteResponse;
import org.apache.wasp.protobuf.generated.ClientProtos.GetRequest;
import org.apache.wasp.protobuf.generated.ClientProtos.GetResponse;
import org.apache.wasp.protobuf.generated.ClientProtos.InsertRequest;
import org.apache.wasp.protobuf.generated.ClientProtos.InsertResponse;
import org.apache.wasp.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.wasp.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.wasp.protobuf.generated.ClientProtos.UpdateRequest;
import org.apache.wasp.protobuf.generated.ClientProtos.UpdateResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * 
 * Protocol that a wasp client and wasp server use to communicate with a wasp
 * server.
 * 
 */
public interface ClientProtocol extends ClientService.BlockingInterface,
    VersionedProtocol {
  public static final long VERSION = 1L;

  /**
   * @see org.apache.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#execute(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.ClientProtos.ExecuteRequest)
   */
  @Override
  public ExecuteResponse execute(RpcController controller,
      ExecuteRequest request) throws ServiceException;

  /**
   * @see org.apache.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#get(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.ClientProtos.GetRequest)
   */
  @Override
  public GetResponse get(RpcController controller, GetRequest request)
      throws ServiceException;

  /**
   * @see org.apache.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#scan(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.ClientProtos.ScanRequest)
   */
  @Override
  public ScanResponse scan(RpcController controller, ScanRequest request)
      throws ServiceException;

  /**
   * @see org.apache.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#insert(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.ClientProtos.InsertRequest)
   */
  @Override
  public InsertResponse insert(RpcController controller, InsertRequest request)
      throws ServiceException;

  /**
   * @see org.apache.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#update(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.ClientProtos.UpdateRequest)
   */
  @Override
  public UpdateResponse update(RpcController controller, UpdateRequest request)
      throws ServiceException;

  /**
   * @see org.apache.wasp.protobuf.generated.ClientProtos.ClientService.BlockingInterface#delete(com.google.protobuf.RpcController,
   *      org.apache.wasp.protobuf.generated.ClientProtos.DeleteRequest)
   */
  @Override
  public DeleteResponse delete(RpcController controller, DeleteRequest request)
      throws ServiceException;
}

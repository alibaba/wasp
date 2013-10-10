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

import com.alibaba.wasp.ipc.VersionedProtocol;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerReportRequest;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerReportResponse;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerStartupRequest;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerStartupResponse;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerStatusService;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.ReportRSFatalErrorRequest;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.ReportRSFatalErrorResponse;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import java.io.IOException;

/**
 * Protocol that a FServer uses to communicate its status to the FMaster.
 */
public interface FServerStatusProtocol extends
    FServerStatusService.BlockingInterface, VersionedProtocol {

  public static final long VERSION = 1L;

  /**
   * 
   * @see com.alibaba.wasp.ipc.VersionedProtocol#getProtocolVersion(String,
   *      long)
   */
  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerStatusService
   *      .BlockingInterface#fServerStartup(com.google.protobuf.RpcController,
   *      org.apache
   *      .wasp.protobuf.generated.FServerStatusProtos.FServerStartupRequest)
   */
  @Override
  public FServerStartupResponse fServerStartup(RpcController controller,
                                               FServerStartupRequest request) throws ServiceException;

  /**
   * 
   * @see com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerStatusService
   *      .BlockingInterface#fServerReport(com.google.protobuf.RpcController,
   *      org.apache
   *      .wasp.protobuf.generated.FServerStatusProtos.FServerReportRequest)
   */
  @Override
  public FServerReportResponse fServerReport(RpcController controller,
                                             FServerReportRequest request) throws ServiceException;

  /**
   * @see com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerStatusService
   *      .BlockingInterface#reportRSFatalError(com.google.protobuf.RpcController,
   *      org
   *      .apache.wasp.protobuf.generated.FServerStatusProtos.ReportRSFatalErrorRequest
   *      )
   */
  @Override
  public ReportRSFatalErrorResponse reportRSFatalError(
      RpcController controller, ReportRSFatalErrorRequest request)
      throws ServiceException;
}

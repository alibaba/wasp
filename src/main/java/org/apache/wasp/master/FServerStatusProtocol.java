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

import java.io.IOException;

import org.apache.wasp.ipc.VersionedProtocol;
import org.apache.wasp.protobuf.generated.FServerStatusProtos.FServerReportRequest;
import org.apache.wasp.protobuf.generated.FServerStatusProtos.FServerReportResponse;
import org.apache.wasp.protobuf.generated.FServerStatusProtos.FServerStartupRequest;
import org.apache.wasp.protobuf.generated.FServerStatusProtos.FServerStartupResponse;
import org.apache.wasp.protobuf.generated.FServerStatusProtos.FServerStatusService;
import org.apache.wasp.protobuf.generated.FServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.wasp.protobuf.generated.FServerStatusProtos.ReportRSFatalErrorResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Protocol that a FServer uses to communicate its status to the FMaster.
 */
public interface FServerStatusProtocol extends
    FServerStatusService.BlockingInterface, VersionedProtocol {

  public static final long VERSION = 1L;

  /**
   * 
   * @see org.apache.wasp.ipc.VersionedProtocol#getProtocolVersion(java.lang.String,
   *      long)
   */
  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException;

  /**
   * @see org.apache.wasp.protobuf.generated.FServerStatusProtos.FServerStatusService
   *      .BlockingInterface#fServerStartup(com.google.protobuf.RpcController,
   *      org.apache
   *      .wasp.protobuf.generated.FServerStatusProtos.FServerStartupRequest)
   */
  @Override
  public FServerStartupResponse fServerStartup(RpcController controller,
      FServerStartupRequest request) throws ServiceException;

  /**
   * 
   * @see org.apache.wasp.protobuf.generated.FServerStatusProtos.FServerStatusService
   *      .BlockingInterface#fServerReport(com.google.protobuf.RpcController,
   *      org.apache
   *      .wasp.protobuf.generated.FServerStatusProtos.FServerReportRequest)
   */
  @Override
  public FServerReportResponse fServerReport(RpcController controller,
      FServerReportRequest request) throws ServiceException;

  /**
   * @see org.apache.wasp.protobuf.generated.FServerStatusProtos.FServerStatusService
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

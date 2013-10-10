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
import com.alibaba.wasp.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import com.alibaba.wasp.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import com.alibaba.wasp.protobuf.generated.MasterProtos.MasterService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public interface MasterProtocol extends VersionedProtocol,
    MasterService.BlockingInterface {

  /**
   * @param c
   *          Unused (set to null).
   * @param req
   *          IsMasterRunningRequest
   * @return IsMasterRunningRequest that contains:<br>
   *         isMasterRunning: true if master is available
   * @throws com.google.protobuf.ServiceException
   */
  public IsMasterRunningResponse isMasterRunning(RpcController c,
                                                 IsMasterRunningRequest req) throws ServiceException;
}

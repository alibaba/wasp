/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.wasp.master;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

import com.alibaba.wasp.FConstants;import com.alibaba.wasp.ServerName;import com.alibaba.wasp.WaspTestingUtility;import com.alibaba.wasp.ipc.WaspRPC;import com.alibaba.wasp.protobuf.generated.MasterProtos;import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.ipc.RemoteException;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.ipc.WaspRPC;
import com.alibaba.wasp.master.FMaster;
import com.alibaba.wasp.master.FMasterMonitorProtocol;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.junit.Test;

import com.google.protobuf.ServiceException;

public class TestWaspRPCException {

  @Test
  public void testRPCException() throws Exception {
    WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
    TEST_UTIL.getHBaseTestingUtility().startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(FConstants.MASTER_PORT, "0");
    TEST_UTIL.getConfiguration().set(FConstants.ZOOKEEPER_QUORUM,
        TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM));
    TEST_UTIL.getConfiguration().set(FConstants.ZOOKEEPER_CLIENT_PORT,
        TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT));
    FMaster hm = new FMaster(conf);

    ServerName sm = hm.getServerName();
    InetSocketAddress isa = new InetSocketAddress(sm.getHostname(),
        sm.getPort());
    int i = 0;
    // retry the RPC a few times; we have seen SocketTimeoutExceptions if we
    // try to connect too soon. Retry on SocketTimeoutException.
    while (i < 20) {
      try {
        FMasterMonitorProtocol inf = (FMasterMonitorProtocol) WaspRPC.getProxy(
            FMasterMonitorProtocol.class, FMasterMonitorProtocol.VERSION, isa,
            conf, 100 * 10);
        inf.isMasterRunning(null, MasterProtos.IsMasterRunningRequest.getDefaultInstance());
        fail();
      } catch (ServiceException ex) {
        IOException ie = ProtobufUtil.getRemoteException(ex);
        if (ie instanceof RemoteException) {
          ie = ((RemoteException) ie).unwrapRemoteException();
        }
        if (!(ie instanceof SocketTimeoutException)) {
          if (ie.getMessage().indexOf("Server is not running yet") != -1) {
            return;
          }
        } else {
          System.err.println("Got SocketTimeoutException. Will retry. ");
        }
      } catch (Throwable t) {
        fail("Unexpected throwable: " + t);
      }
      Thread.sleep(100);
      i++;
    }
    fail();
  }

}

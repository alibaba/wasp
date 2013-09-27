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
package com.alibaba.wasp.ipc;

import com.alibaba.wasp.ipc.protobuf.generated.TestProtos;
import com.alibaba.wasp.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Test for testing protocol buffer based RPC mechanism. This test depends on
 * test.proto definition of types in src/test/protobuf/test.proto and protobuf
 * service definition from src/test/protobuf/test_rpc_service.proto
 */
public class TestProtoBufRpc {
  public final static String ADDRESS = "0.0.0.0";
  public final static int PORT = 0;
  private static InetSocketAddress addr;
  private static Configuration conf;
  private static RpcServer server;

  public interface TestRpcService extends
      TestProtobufRpcProto.BlockingInterface, VersionedProtocol {
    public long VERSION = 1;
  }

  public static class PBServerImpl implements TestRpcService {

    @Override
    public TestProtos.EmptyResponseProto ping(RpcController unused,
        TestProtos.EmptyRequestProto request) throws ServiceException {
      return TestProtos.EmptyResponseProto.newBuilder().build();
    }

    @Override
    public TestProtos.EchoResponseProto echo(RpcController unused, TestProtos.EchoRequestProto request)
        throws ServiceException {
      return TestProtos.EchoResponseProto.newBuilder().setMessage(request.getMessage())
          .build();
    }

    @Override
    public TestProtos.EmptyResponseProto error(RpcController unused,
        TestProtos.EmptyRequestProto request) throws ServiceException {
      throw new ServiceException("error", new IOException("error"));
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      return 0;
    }
  }

  @Before
  public void setUp() throws IOException { // Setup server for both protocols
    conf = new Configuration();

    // Create server side implementation
    PBServerImpl serverImpl = new PBServerImpl();
    // Get RPC server for server side implementation
    server = WaspRPC.getServer(TestRpcService.class, serverImpl,
        new Class[]{TestRpcService.class}, ADDRESS, PORT, conf);
    addr = server.getListenerAddress();
    server.start();
    server.openServer();
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  private static TestRpcService getClient() throws IOException {
    return (TestRpcService) WaspRPC.getProxy(TestRpcService.class, 0, addr,
        conf, 10000);
  }

  @Test
  public void testProtoBufRpc() throws Exception {
    TestRpcService client = getClient();
    testProtoBufRpc(client);
  }

  // separated test out so that other tests can call it.
  public static void testProtoBufRpc(TestRpcService client) throws Exception {
    // Test ping method
    TestProtos.EmptyRequestProto emptyRequest = TestProtos.EmptyRequestProto.newBuilder().build();
    client.ping(null, emptyRequest);

    // Test echo method
    TestProtos.EchoRequestProto echoRequest = TestProtos.EchoRequestProto.newBuilder()
        .setMessage("hello").build();
    TestProtos.EchoResponseProto echoResponse = client.echo(null, echoRequest);
    Assert.assertEquals(echoResponse.getMessage(), "hello");

    // Test error method - error should be thrown as RemoteException
    try {
      client.error(null, emptyRequest);
      Assert.fail("Expected exception is not thrown");
    } catch (ServiceException e) {
//      e.printStackTrace();
    }
  }
}
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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.RequestConverter;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteResponse;

import com.google.protobuf.ServiceException;

public class FClientCallable extends ServerCallable<ExecuteResponse> implements
    Closeable {

  /** FServer tracker **/
  private FClientFServerTracker fserverTracker;

  /** rand for client request. **/
  private Random rand = new Random();

  private final String sql;

  private int fetchSize;

  private final ReadModel model;

  private String sessionId;

  /**
   * @param connection
   * @param fserverTracker
   * @param sql
   * @param model
   * @throws IOException
   */
  public FClientCallable(FConnection connection,
      FClientFServerTracker fserverTracker, String sql, ReadModel model,
      int fetchSize) throws IOException {
    super(connection, null, null);
    this.fserverTracker = fserverTracker;
    this.sql = sql;
    this.model = model;
    this.fetchSize = fetchSize;
  }

  /**
   * Connect to the server hosting entityGroup with row from tablename.
   * 
   * @param reload
   *          Set this to true if connection should re-find the entityGroup
   * @throws java.io.IOException
   *           e
   */
  public void connect(final boolean reload) throws IOException {
    if (server == null || reload) {
      List<ServerName> sns = fserverTracker.getOnlineServers();
      ServerName sn = sns.get(rand.nextInt(sns.size()));
      this.server = connection.getClient(sn.getHostname(), sn.getPort());
    }
  }

  /**
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public ExecuteResponse call() throws IOException {
    ExecuteRequest request = RequestConverter.buildExecuteRequest(sql, model, fetchSize, sessionId);
    try {
      ExecuteResponse response = server.execute(null, request);
      sessionId = response.getSessionId();
      return response;
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  /**
   * Close
   */
  @Override
  public void close() throws IOException {
    if (sessionId != null) {
      ExecuteRequest request = RequestConverter.buildExecuteRequest(sessionId,
          sql, true);
      try {
        server.execute(null, request);
      } catch (ServiceException e) {
        throw ProtobufUtil.getRemoteException(e);
      }
    }
  }
}
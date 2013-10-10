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
package com.alibaba.wasp.session;

import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.client.FClient;
import com.alibaba.wasp.jdbc.command.CommandInterface;
import com.alibaba.wasp.protobuf.generated.ClientProtos.QueryResultProto;
import com.alibaba.wasp.protobuf.generated.ClientProtos.StringDataTypePair;
import com.alibaba.wasp.security.User;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.util.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A session represents an embedded database connection. When using the server
 * mode, this object resides on the server side and communicates with a
 * SessionRemote object on the client side.
 */
public class ExecutionEngineSession implements SessionInterface {

  private final User user;

  private final String sessionId;

  private boolean closed = false;

  private Executor executor;

  public ExecutionEngineSession(User user, String sessionId) {
    this.user = user;
    this.sessionId = sessionId;
  }

  /**
   * return Session owner.
   * 
   * @return
   */
  public User getUser() {
    return user;
  }

  /**
   * @return the sessionId
   */
  public String getSessionId() {
    return sessionId;
  }

  @Override
  public void setSessionId(String sessionId) {

  }

  /**
   * 
   * @see Object#hashCode()
   */
  public int hashCode() {
    return sessionId.hashCode();
  }

  /**
   *
   * @see Object#toString()
   */
  public String toString() {
    return "Session#" + sessionId + " (user: " + user.getName() + ")";
  }

  @Override
  public void close() throws IOException {
    if (executor != null) {
      executor.close();
      executor = null;
    }
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  /**
   * @return the executor
   */
  public Executor getExecutor() {
    return executor;
  }

  /**
   * @param executor
   *          the executor to set
   */
  public void setExecutor(Executor executor) {
    this.executor = executor;
  }

  public interface Executor<T> extends Closeable {

    /**
     *
     * @return T
     * @throws com.google.protobuf.ServiceException
     */
    public T execute() throws ServiceException;

    /**
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException;
  }

  public interface QueryExecutor extends
      Executor<Pair<List<QueryResultProto>, List<StringDataTypePair>>> {

    /**
     * the last scan.
     *
     * @return
     */
    public boolean isLastScan();
  }

  /**
   * @see com.alibaba.wasp.session.SessionInterface#prepareCommand(com.alibaba.wasp.client.FClient,
   *      String, int, com.alibaba.wasp.ReadModel, boolean)
   */
  @Override
  public CommandInterface prepareCommand(FClient fClient, String sql,
      int fetchSize, ReadModel readModel, boolean autoCommit, ExecuteSession statementSession) {
    // NO-implements
    return null;
  }

  @Override
  public CommandInterface prepareCommand(FClient fClient, List<String> sqls, boolean autoCommit, ExecuteSession statementSession) {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public void afterWriting() {
  }
}
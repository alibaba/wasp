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
package org.apache.wasp.session;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.wasp.ReadModel;
import org.apache.wasp.client.FClient;
import org.apache.wasp.jdbc.command.CommandInterface;
import org.apache.wasp.protobuf.generated.ClientProtos.QueryResultProto;
import org.apache.wasp.protobuf.generated.ClientProtos.StringDataTypePair;
import org.apache.wasp.security.User;

import com.google.protobuf.ServiceException;

/**
 * A session represents an embedded database connection. When using the server
 * mode, this object resides on the server side and communicates with a
 * SessionRemote object on the client side.
 */
public class Session implements SessionInterface {

  private final User user;

  private final String sessionId;

  private boolean closed = false;

  private Executor executor;

  public Session(User user, String sessionId) {
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

  /**
   * 
   * @see java.lang.Object#hashCode()
   */
  public int hashCode() {
    return sessionId.hashCode();
  }

  /**
   * 
   * @see java.lang.Object#toString()
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
     * @throws ServiceException
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
   * @see org.apache.wasp.session.SessionInterface#prepareCommand(FClient,
   *      java.lang.String, int)
   */
  @Override
  public CommandInterface prepareCommand(FClient fClient, String sql,
      int fetchSize, ReadModel readModel) {
    // NO-implements
    return null;
  }

  public void afterWriting() {
  }
}
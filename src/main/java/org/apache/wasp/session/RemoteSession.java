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
package org.apache.wasp.session;

import org.apache.wasp.ReadModel;
import org.apache.wasp.client.FClient;
import org.apache.wasp.jdbc.ConnectionInfo;
import org.apache.wasp.jdbc.JdbcException;
import org.apache.wasp.jdbc.command.CommandInterface;
import org.apache.wasp.jdbc.command.CommandRemote;

import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The client side part of a session when using the server mode. This object
 * communicates with a Session on the server side.
 */
public class RemoteSession implements SessionInterface {

  public static final int SESSION_PREPARE = 0;
  public static final int SESSION_CLOSE = 1;
  public static final int COMMAND_EXECUTE_QUERY = 2;
  public static final int COMMAND_EXECUTE_UPDATE = 3;
  public static final int COMMAND_CLOSE = 4;
  public static final int RESULT_FETCH_ROWS = 5;
  public static final int RESULT_RESET = 6;
  public static final int RESULT_CLOSE = 7;
  public static final int COMMAND_COMMIT = 8;
  public static final int CHANGE_ID = 9;
  public static final int COMMAND_GET_META_DATA = 10;
  public static final int SESSION_PREPARE_READ_PARAMS = 11;
  public static final int SESSION_SET_ID = 12;
  public static final int SESSION_CANCEL_STATEMENT = 13;
  public static final int SESSION_CHECK_KEY = 14;
  public static final int SESSION_SET_AUTOCOMMIT = 15;

  public static final int STATUS_ERROR = 0;
  public static final int STATUS_OK = 1;
  public static final int STATUS_CLOSED = 2;
  public static final int STATUS_OK_STATE_CHANGED = 3;

  private AtomicInteger nextId = new AtomicInteger();
  private ConnectionInfo connectionInfo;
  private final Object lobSyncObject = new Object();
  private String sessionId;
  private int lastReconnect;

  public RemoteSession(ConnectionInfo ci) {
    this.connectionInfo = ci;
  }

  public void cancel() {
    // this method is called when closing the connection
    // the statement that is currently running is not canceled in this case
    // however Statement.cancel is supported
  }

  public int getPowerOffCount() {
    return 0;
  }

  public void setPowerOffCount(int count) {
    throw JdbcException.getUnsupportedException("remote");
  }

  public int getNextId() {
    return nextId.incrementAndGet();
  }

  public int getCurrentId() {
    return nextId.get();
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public String getSessionId() {
    return this.sessionId;
  }

  @Override
  public void close() {
    // To change body of implemented methods use File | Settings | File
    // Templates.
  }

  public boolean isClosed() {
    return false;
  }

  @Override
  public CommandInterface prepareCommand(FClient fClient,String sql, int fetchSize, ReadModel readModel) {
    checkClosed();
    return new CommandRemote(fClient, this, sql, fetchSize, readModel);
  }

  public void checkPowerOff() {
    // OK
  }

  public void checkWritingAllowed() {
    // OK
  }

  public String getDatabasePath() {
    return "";
  }

  public String getLobCompressionAlgorithm(int type) {
    return null;
  }

  public int getMaxLengthInplaceLob() {
    return -1;
  }

  public Object getLobSyncObject() {
    return lobSyncObject;
  }

  public int getLastReconnect() {
    return lastReconnect;
  }

  public boolean isReconnectNeeded(boolean write) {
    return false;
  }

  public SessionInterface reconnect(boolean write) {
    return this;
  }

  public Connection getLobConnection() {
    return null;
  }

  public void checkClosed() {
    // OK
  }

  public void removeServer(IOException e) {
    // To change body of created methods use File | Settings | File Templates.
  }

  public void cancelStatement(int id) {
    // To change body of created methods use File | Settings | File Templates.
  }

  public void afterWriting() {
  }
}
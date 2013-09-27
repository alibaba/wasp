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
package com.alibaba.wasp.session;

import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.client.FClient;
import com.alibaba.wasp.jdbc.command.CommandInterface;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A local or remote session. A session represents a database connection.
 */
public interface SessionInterface extends Closeable {

  /**
   * Roll back pending transactions and close the session.
   */
  public void close() throws IOException;

  /**
   * Check if close was called.
   * 
   * @return if the session has been closed
   */
  public boolean isClosed();

  CommandInterface prepareCommand(FClient fClient, String sql, int fetchSize,
                                  ReadModel readModel, boolean autoCommit, ExecuteSession statementSession);

  CommandInterface prepareCommand(FClient fClient, List<String> sqls, boolean autoCommit,
                                  ExecuteSession statementSession);

  public void afterWriting();

  public String getSessionId();

  public void setSessionId(String sessionId);

}

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

import com.alibaba.wasp.jdbc.ConnectionInfo;
import com.alibaba.wasp.security.User;

import java.sql.SQLException;

/**
 * A class that implements this interface can create new ExecutionEngine
 * sessions.
 */
public class SessionFactory {

  /**
   * Return session used by fserver.
   * 
   * @param sessionId
   * @param user
   * @return
   */
  public static ExecutionEngineSession createExecutionEngineSession(String sessionId, User user) {
    return new ExecutionEngineSession(user, sessionId);
  }

  public static ExecuteSession createExecuteSession(){
    return new ExecuteSession();
  }

  /**
   * Return session used by jdbc connection.
   * 
   * @param ci
   * @return
   * @throws java.sql.SQLException
   */
  public static ConnectionSession createConnectionSession(ConnectionInfo ci)
      throws SQLException {
    return new ConnectionSession(ci);
  }
}
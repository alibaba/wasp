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

import org.apache.wasp.jdbc.ConnectionInfo;
import org.apache.wasp.security.User;

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
  public static Session createSession(String sessionId, User user) {
    return new Session(user, sessionId);
  }

  public static QuerySession createQuerySession(){
    return new QuerySession();
  }

  /**
   * Return session used by jdbc.
   * 
   * @param ci
   * @return
   * @throws SQLException
   */
  public static RemoteSession createSession(ConnectionInfo ci)
      throws SQLException {
    return new RemoteSession(ci);
  }
}
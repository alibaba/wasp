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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.util.Pair;
import com.alibaba.wasp.ReadModel;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Used to communicate with wasp cluster.
 * 
 */
public interface FClientInterface extends Closeable, Abortable {

  /**
   * Execute sql.
   * 
   * @param sql
   * 
   * @return List<ExecuteResult>
   */
  public Pair<String, Pair<Boolean, List<ExecuteResult>>> execute(String sql, ReadModel mode, int fetchSize)
      throws IOException;

  /**
   * Execute sql.
   *
   * @param sql
   *
   * @return List<ExecuteResult>
   */
  public Pair<String, Pair<Boolean, List<ExecuteResult>>> execute(String sql)
      throws IOException;

  /**
   * Next result.
   * 
   * @param sessionId
   * @return
   * @throws IOException
   */
  public Pair<String, Pair<Boolean, List<ExecuteResult>>> next(String sessionId)
      throws IOException;

  /**
   * Returns the {@link Configuration} object used by this instance.
   * <p>
   * The reference returned is not a copy, so any change made to it will affect
   * this instance.
   */
  public Configuration getConfiguration();

  /**
   * Clear local session and close remote session.
   * 
   * @param sessionId
   */
  public void closeSession(String sessionId) throws IOException;
}

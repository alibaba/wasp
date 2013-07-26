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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.UnknownSessionException;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteResultProto;
import com.alibaba.wasp.protobuf.generated.ClientProtos.StringDataTypePair;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Wasp Client. Used to communicate with wasp cluster.
 * 
 */
public class FClient implements FClientInterface {

  /** Connection instance. **/
  private FConnection connection = null;

  /** conf **/
  private Configuration configuration;

  /** sessionid2callables **/
  private HashMap<String, FClientCallable> callables = new HashMap<String, FClientCallable>();

  /** Our zk client. **/
  private ZooKeeperWatcher zooKeeper;

  /** FServer tracker **/
  private FClientFServerTracker fserverTracker;

  private boolean closed = false;

  public static final String FCLIENT = "fclient";

  /**
   * Default constructor.
   * 
   * @param conf
   *          configuration instance
   * @throws IOException
   */
  public FClient(Configuration conf) throws IOException {
    if (conf == null) {
      this.connection = null;
      return;
    }
    this.connection = FConnectionManager.getConnection(conf);
    this.configuration = conf;
    this.zooKeeper = new ZooKeeperWatcher(configuration, FCLIENT + ":", this);
    this.fserverTracker = new FClientFServerTracker(zooKeeper);
    try {
      this.fserverTracker.start();
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * @throws RuntimeException
   * @throws IOException
   * @see com.alibaba.wasp.client.FClientInterface#execute(java.lang.String,
   *      com.alibaba.wasp.ReadModel, int)
   */
  @Override
  public Pair<String, Pair<Boolean, List<ExecuteResult>>> execute(
      final String sql, final ReadModel model, final int fetchSize)
      throws IOException {
    FClientCallable callable = new FClientCallable(connection, fserverTracker,
        sql, model, fetchSize);
    ExecuteResponse response = null;

    try {
      response = callable.withRetries();
      if (response.hasSessionId()) {
        callables.put(response.getSessionId(), callable);
      }
      List<ExecuteResultProto> results = response.getResultList();
      List<StringDataTypePair> metaDatas = response.getMetaList();
      return new Pair<String, Pair<Boolean, List<ExecuteResult>>>(
          response.getSessionId(), new Pair<Boolean, List<ExecuteResult>>(
              response.getLastScan(), ProtobufUtil.toExecuteResult(results,
                  metaDatas)));
    } catch (Exception e) {
      if (response != null) {
        callables.remove(response.getSessionId());
      }
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException(e);
    }
  }

  @Override
  public Pair<String, Pair<Boolean, List<ExecuteResult>>> execute(String sql)
      throws IOException {
    return execute(sql, ReadModel.SNAPSHOT, configuration.getInt(
        FConstants.WASP_JDBC_FETCHSIZE, FConstants.DEFAULT_WASP_JDBC_FETCHSIZE));
  }

  @Override
  public Pair<String, Pair<Boolean, List<ExecuteResult>>> next(String sessionId)
      throws IOException {
    FClientCallable callable = callables.get(sessionId);
    if (callable == null) {
      throw new UnknownSessionException(sessionId);
    }
    ExecuteResponse response = null;
    try {
      response = callable.withRetries();
      List<ExecuteResultProto> results = response.getResultList();
      List<StringDataTypePair> metaDatas = response.getMetaList();
      return new Pair<String, Pair<Boolean, List<ExecuteResult>>>(
          response.getSessionId(), new Pair<Boolean, List<ExecuteResult>>(
              response.getLastScan(), ProtobufUtil.toExecuteResult(results,
                  metaDatas)));
    } catch (Exception e) {
      if (response != null) {
        callables.remove(response.getSessionId());
      }
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      throw new IOException(e);
    }
  }

  @Override
  public void closeSession(String sessionId) throws IOException {
    FClientCallable callable = callables.get(sessionId);
    if (callable == null) {
      throw new UnknownSessionException(sessionId);
    }
    callable.close();
  }

  /**
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    if (this.closed) {
      return;
    }
    for (FClientCallable callable : callables.values()) {
      callable.close();
    }
    fserverTracker = null;
    zooKeeper.close();
    if (this.connection != null) {
      this.connection.close();
    }
    this.closed = true;
  }

  @Override
  public void abort(String s, Throwable throwable) {
    // To change body of implemented methods use File | Settings | File
    // Templates.
  }

  @Override
  public boolean isAborted() {
    return false; // To change body of implemented methods use File | Settings |
                  // File Templates.
  }
}
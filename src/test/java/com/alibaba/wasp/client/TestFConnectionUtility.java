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

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupLocation;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.ZooKeeperConnectionException;
import org.apache.hadoop.conf.Configuration;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * {@link com.alibaba.wasp.client.FConnection} testing utility.
 */
public class TestFConnectionUtility {
  /*
   * Not part of {@link WaspTestingUtility} because this class is not in same
   * package as {@link FConnection}. Would have to reveal ugly {@link
   * FConnectionManager} innards to WaspTestingUtility to give it access.
   */
  /**
   * Get a Mocked {@link com.alibaba.wasp.client.FConnection} that goes with the
   * passed <code>conf</code> configuration instance. Minimally the mock will
   * return
   * <code>conf</conf> when {@link com.alibaba.wasp.client.FConnection#getConfiguration()} is invoked.
   * Be sure to shutdown the connection when done by calling
   * {@link FConnectionManager#deleteConnection(org.apache.hadoop.conf.Configuration, boolean)} else it
   * will stick around; this is probably not what you want.
   *
   * @param conf
   *          configuration
   * @return FConnection object for <code>conf</code>
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   */
  public static FConnection getMockedConnection(final Configuration conf)
      throws ZooKeeperConnectionException {
    FConnectionManager.FConnectionKey connectionKey = new FConnectionManager.FConnectionKey(
        conf);
    synchronized (FConnectionManager.WASP_INSTANCES) {
      FConnectionManager.FConnectionImplementation connection = FConnectionManager.WASP_INSTANCES
          .get(connectionKey);
      if (connection == null) {
        connection = Mockito
            .mock(FConnectionManager.FConnectionImplementation.class);
        Mockito.when(connection.getConfiguration()).thenReturn(conf);
        FConnectionManager.WASP_INSTANCES.put(connectionKey, connection);
      }
      return connection;
    }
  }

  /**
   * Calls {@link #getMockedConnection(org.apache.hadoop.conf.Configuration)}
   * and then mocks a few more of the popular
   * {@link com.alibaba.wasp.client.FConnection} methods so they do 'normal'
   * operation (see return doc below for list). Be sure to shutdown the
   * connection when done by calling
   * {@link FConnectionManager#deleteConnection(org.apache.hadoop.conf.Configuration, boolean)}
   * else it will stick around; this is probably not what you want.
   *
   * @param conf
   *          Configuration to use itself a mock.
   * @param sn
   *          ServerName to include in the entityGroup location returned by this
   *          <code>implementation</code>
   * @param egi
   *          EntityGroupInfo to include in the location returned when
   *          getRegionLocation is called on the mocked connection
   * @return Mock up a connection that returns a
   *         {@link org.apache.hadoop.conf.Configuration} when
   *         {@link com.alibaba.wasp.client.FConnection#getConfiguration()} is
   *         called, a 'location' when
   *         {@link com.alibaba.wasp.client.FConnection#getEntityGroupLocation(byte[], byte[], boolean)}
   *         is called, is called (Be sure call
   *         {@link FConnectionManager#deleteConnection(org.apache.hadoop.conf.Configuration, boolean)}
   *         when done with this mocked Connection.
   * @throws java.io.IOException
   */
  public static FConnection getMockedConnectionAndDecorate(
      final Configuration conf, final ServerName sn, final EntityGroupInfo egi)
      throws IOException {
    FConnection c = TestFConnectionUtility.getMockedConnection(conf);
    Mockito.doNothing().when(c).close();
    // Make it so we return a particular location when asked.
    final EntityGroupLocation loc = new EntityGroupLocation(egi,
        sn.getHostname(), sn.getPort());
    Mockito.when(
        c.getEntityGroupLocation((byte[]) Mockito.any(),
            (byte[]) Mockito.any(), Mockito.anyBoolean())).thenReturn(loc);
    Mockito.when(
        c.locateEntityGroup((byte[]) Mockito.any(), (byte[]) Mockito.any()))
        .thenReturn(loc);
    return c;
  }

  /**
   * Get a Mockito spied-upon {@link com.alibaba.wasp.client.FConnection} that
   * goes with the passed <code>conf</code> configuration instance. Be sure to
   * shutdown the connection when done by calling
   * {@link FConnectionManager#deleteConnection(org.apache.hadoop.conf.Configuration, boolean)}
   * else it will stick around; this is probably not what you want.
   * 
   * @param conf
   *          configuration
   * @return FConnection object for <code>conf</code>
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   * @see http
   *      ://mockito.googlecode.com/svn/branches/1.6/javadoc/org/mockito/Mockito
   *      .html#spy(T)
   */
  public static FConnection getSpiedConnection(final Configuration conf)
      throws ZooKeeperConnectionException {
    FConnectionManager.FConnectionKey connectionKey = new FConnectionManager.FConnectionKey(
        conf);
    synchronized (FConnectionManager.WASP_INSTANCES) {
      FConnectionManager.FConnectionImplementation connection = FConnectionManager.WASP_INSTANCES
          .get(connectionKey);
      if (connection == null) {
        connection = Mockito
            .spy(new FConnectionManager.FConnectionImplementation(conf, true));
        FConnectionManager.WASP_INSTANCES.put(connectionKey, connection);
      }
      return connection;
    }
  }

  /**
   * @return Count of extant connection instances
   */
  public static int getConnectionCount() {
    synchronized (FConnectionManager.WASP_INSTANCES) {
      return FConnectionManager.WASP_INSTANCES.size();
    }
  }
}

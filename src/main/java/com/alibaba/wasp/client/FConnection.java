/**
 *
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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupLocation;
import com.alibaba.wasp.MasterNotRunningException;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.fserver.AdminProtocol;
import com.alibaba.wasp.master.FMasterAdminProtocol;
import com.alibaba.wasp.master.FMasterMonitorProtocol;
import com.alibaba.wasp.master.FMetaServerProtocol;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;

/**
 * Cluster connection. Hosts a connection to the ZooKeeper ensemble and
 * thereafter into the Wasp cluster.Keeps a cache of entityGroup locations and
 * then knows how to recalibrate after they move.
 * {@link com.alibaba.wasp.client.FConnectionManager} manages instances of this
 * class.
 * 
 * <p>
 * FConnection instances can be shared. Sharing is usually what you want because
 * rather than each FConnection instance having to do its own cache of
 * entityGroup locations. Sharing makes cleanup of FConnections awkward. See
 * {@link com.alibaba.wasp.client.FConnectionManager} for cleanup discussion.
 * 
 * @see com.alibaba.wasp.client.FConnectionManager
 */
public interface FConnection extends Abortable, Closeable {

  /**
   * @return Configuration instance being used by this HConnection instance.
   */
  public Configuration getConfiguration();

  /**
   * Retrieve ZooKeeperWatcher used by this connection.
   * 
   * @return ZooKeeperWatcher handle being used by the connection.
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public ZooKeeperWatcher getZooKeeperWatcher() throws IOException;

  /** @return - true if the master server is running */
  public boolean isMasterRunning() throws MasterNotRunningException,
      ZooKeeperConnectionException;

  /**
   * A table that isTableEnabled == false and isTableDisabled == false is
   * possible. This happens when a table has a lot of entityGroups that must be
   * processed.
   * 
   * @param tableName
   *          table name
   * @return true if the table is enabled, false otherwise
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableEnabled(byte[] tableName) throws IOException;

  /**
   * @param tableName
   *          table name
   * @return true if the table is disabled, false otherwise
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableDisabled(byte[] tableName) throws IOException;

  /**
   * @param tableName
   *          table name
   * @return true if all entityGroups of the table are available, false
   *         otherwise
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException;

  /**
   * @param tableName
   *          table name
   * @return true if the table is locked, false otherwise
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableLocked(byte[] tableName) throws IOException;

  /**
   * List all the userspace tables. In other words, scan the META table.
   * 
   * If we wanted this to be really fast, we could implement a special catalog
   * table that just contains table names and their descriptors. Right now, it
   * only exists as part of the META table's entityGroup info.
   * 
   * @return - returns an array of FTableDescriptors
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public FTable[] listTables() throws IOException;

  /**
   * @param tableName
   *          table name
   * @return table metadata
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public FTable getFTableDescriptor(byte[] tableName) throws IOException;

  /**
   * Find the location of the entityGroup of <i>tableName</i> that <i>row</i>
   * lives in.
   * 
   * @param tableName
   *          name of the table <i>row</i> is in
   * @param row
   *          row key you're trying to find the entityGroup of
   * @return EntityGroupLocation that describes where to find the entityGroup in
   *         question
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public EntityGroupLocation locateEntityGroup(final byte[] tableName,
      final byte[] row) throws IOException;

  /**
   * Allows flushing the entityGroup cache.
   */
  public void clearEntityGroupCache();

  /**
   * Allows flushing the entityGroup cache of all locations that pertain to
   * <code>tableName</code>
   * 
   * @param tableName
   *          Name of the table whose entityGroups we are to remove from cache.
   */
  public void clearEntityGroupCache(final byte[] tableName);

  /**
   * Find the location of the entityGroup of <i>tableName</i> that <i>row</i>
   * lives in, ignoring any value that might be in the cache.
   * 
   * @param tableName
   *          name of the table <i>row</i> is in
   * @param row
   *          row key you're trying to find the entityGroup of
   * @return EntityGroupLocation that describes where to find the entityGroup in
   *         question
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public EntityGroupLocation relocateEntityGroup(final byte[] tableName,
      final byte[] row) throws IOException;

  /**
   * Find entityGroup location hosting passed row
   * 
   * @param tableName
   *          table name
   * @param row
   *          Row to find.
   * @param reload
   *          If true do not use cache, otherwise bypass.
   * @return Location of row.
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  EntityGroupLocation getEntityGroupLocation(byte[] tableName, byte[] row,
      boolean reload) throws IOException;

  /**
   * Pass in a ServerCallable with your particular bit of logic defined and this
   * method will manage the process of doing retries with timed waits and
   * refinds of missing entityGroups.
   * 
   * @param <T>
   *          the type of the return value
   * @param callable
   *          callable to run
   * @return an object of type T
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   * @throws RuntimeException
   *           other unspecified error
   */
  public <T> T getFServerWithRetries(ServerCallable<T> callable)
      throws IOException, RuntimeException;

  /**
   * Pass in a ServerCallable with your particular bit of logic defined and this
   * method will pass it to the defined FServer.
   * 
   * @param <T>
   *          the type of the return value
   * @param callable
   *          callable to run
   * @return an object of type T
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   * @throws RuntimeException
   *           other unspecified error
   */
  public <T> T getFServerWithoutRetries(ServerCallable<T> callable)
      throws IOException, RuntimeException;

  /**
   * Enable or disable entityGroup cache prefetch for the table. It will be
   * applied for the given table's all Table instances within this connection.
   * By default, the cache prefetch is enabled.
   * 
   * @param tableName
   *          name of table to configure.
   * @param enable
   *          Set to true to enable entityGroup cache prefetch.
   */
  public void setEntityGroupCachePrefetch(final byte[] tableName,
      final boolean enable);

  /**
   * Check whether entityGroup cache prefetch is enabled or not.
   * 
   * @param tableName
   *          name of table to check
   * @return true if table's entityGroup cache prefetch is enabled. Otherwise it
   *         is disabled.
   */
  public boolean getEntityGroupCachePrefetch(final byte[] tableName);

  /**
   * Load the entityGroup map and warm up the global entityGroup cache for the
   * table.
   * 
   * @param tableName
   *          name of the table to perform entityGroup cache prewarm.
   * @param entityGroups
   *          a entityGroup map.
   */
  public void prewarmEntityGroupCache(final byte[] tableName,
      final Map<EntityGroupInfo, ServerName> entityGroups);

  /**
   * @param tableNames
   *          List of table names
   * @return FTD[] table metadata
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public FTable[] getFTableDescriptors(List<String> tableNames)
      throws IOException;

  /**
   * @return true if this connection is closed
   */
  public boolean isClosed();

  /**
   * Clear any caches that pertain to server name <code>sn</code>
   * 
   * @param sn
   *          A server name as hostname:port
   */
  public void clearCaches(final String sn);

  /**
   * Gets the locations of all entityGroups in the specified table,
   * <i>tableName</i>.
   * 
   * @param tableName
   *          table to get entityGroups of
   * @return list of entityGroup locations for all entityGroups of table
   * @throws java.io.IOException
   */
  public List<EntityGroupLocation> locateEntityGroups(byte[] tableName)
      throws IOException;

  /**
   * Find the location of the entityGroup of <i>tableName</i> that <i>row</i>
   * lives in, ignoring any value that might be in the cache.
   * 
   * @param tableName
   *          name of the table <i>row</i> is in
   * @param row
   *          row key you're trying to find the entityGroup of
   * @return EntityGroupLocation that describes where to find the entityGroup in
   *         question
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public int relocateEntityGroupsInCache(byte[] tableName) throws IOException;

  /**
   * Returns a {@link FMasterAdminProtocol} to the active master
   */
  public FMasterAdminProtocol getMasterAdmin() throws IOException;

  /**
   * Returns an {@link FMasterMonitorProtocol} to the active master
   */
  public FMasterMonitorProtocol getMasterMonitor() throws IOException;

  /**
   * Returns an {@link FMetaServerProtocol} to the active master
   */
  public FMetaServerProtocol getMetaServer() throws IOException;

  /**
   * Establishes a connection to the entityGroup server at the specified
   * address.
   * 
   * @param hostname
   *          EntityGroupServer hostname
   * @param port
   *          EntityGroupServer port
   * @param getMaster
   *          - do we check if master is alive
   * @return proxy for EntityGroupServer
   * @throws IOException
   *           if a remote or network exception occurs
   */
  public AdminProtocol getAdmin(String hostname, int port) throws IOException;

  /**
   * Establishes a connection to the entityGroup server at the specified
   * address, and return a entityGroup client protocol.
   * 
   * @param hostname
   *          EntityGroupServer hostname
   * @param port
   *          EntityGroupServer port
   * @return ClientProtocol proxy for EntityGroupServer
   * @throws IOException
   *           if a remote or network exception occurs
   * 
   */
  public ClientProtocol getClient(final String hostname, final int port)
      throws IOException;

  /**
   * This function allows WaspAdmin and potentially others to get a shared
   * MasterAdminProtocol connection.
   * 
   * @return The shared instance. Never returns null.
   * @throws MasterNotRunningException
   */
  public MasterAdminKeepAliveConnection getKeepAliveMasterAdmin()
      throws MasterNotRunningException;

  /**
   * This function allows WaspAdminProtocol and potentially others to get a
   * shared MasterMonitor connection.
   * 
   * @return The shared instance. Never returns null.
   * @throws MasterNotRunningException
   */
  public MasterMonitorKeepAliveConnection getKeepAliveMasterMonitor()
      throws MasterNotRunningException;

  /**
   * This funcation allows FMetaServerProtocol and and potentially others to get
   * a shared MetaServer connection.
   * 
   * @return
   * @throws MasterNotRunningException
   */
  public MetaServerKeepAliveConnection getKeepAliveMasterMetaServer()
      throws MasterNotRunningException;
}

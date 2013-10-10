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

import com.alibaba.wasp.*;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.fserver.AdminProtocol;
import com.alibaba.wasp.ipc.VersionedProtocol;
import com.alibaba.wasp.ipc.WaspRPC;
import com.alibaba.wasp.master.FMasterAdminProtocol;
import com.alibaba.wasp.master.FMasterMonitorProtocol;
import com.alibaba.wasp.master.FMetaServerProtocol;
import com.alibaba.wasp.master.MasterProtocol;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.RequestConverter;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsRequest;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsResponse;
import com.alibaba.wasp.zookeeper.MasterAddressTracker;
import com.alibaba.wasp.zookeeper.ZKTableReadOnly;
import com.alibaba.wasp.zookeeper.ZKUtil;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SoftValueSortedMap;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.KeeperException;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class FConnectionManager {

  private static final Log LOG = LogFactory.getLog(FConnectionManager.class);

  // An LRU Map of FConnectionKey -> FConnection (TableServer). All
  // access must be synchronized. This map is not private because tests
  // need to be able to tinker with it.
  static final Map<FConnectionKey, FConnectionImplementation> WASP_INSTANCES;

  public static final int MAX_CACHED_WASP_INSTANCES;

  static {
    // We set instances to one more than the value specified for {@link
    // FConstants#ZOOKEEPER_MAX_CLIENT_CNXNS}. By default, the zk default max
    // connections to the ensemble from the one client is 30, so in that case we
    // should run into zk issues before the LRU hit this value of 31.
    MAX_CACHED_WASP_INSTANCES = WaspConfiguration.create().getInt(
        FConstants.ZOOKEEPER_MAX_CLIENT_CNXNS,
        FConstants.DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS) + 1;
    WASP_INSTANCES = new LinkedHashMap<FConnectionKey, FConnectionImplementation>(
        (int) (MAX_CACHED_WASP_INSTANCES / 0.75F) + 1, 0.75F, true) {

      private static final long serialVersionUID = -17756812924832264L;

      @Override
      protected boolean removeEldestEntry(
          Entry<FConnectionKey, FConnectionImplementation> eldest) {
        return size() > MAX_CACHED_WASP_INSTANCES;
      }
    };
  }

  /**
   * Get the connection that goes with the passed <code>conf</code>
   * configuration instance. If no current connection exists, method creates a
   * new connection for the passed <code>conf</code> instance.
   *
   * @param conf
   *          configuration
   * @return HConnection object for <code>conf</code>
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   */
  public static FConnection getConnection(Configuration conf)
      throws ZooKeeperConnectionException {
    FConnectionKey connectionKey = new FConnectionKey(conf);
    synchronized (WASP_INSTANCES) {
      FConnectionImplementation connection = WASP_INSTANCES.get(connectionKey);
      if (connection == null) {
        connection = new FConnectionImplementation(conf, true);
        WASP_INSTANCES.put(connectionKey, connection);
      }
      connection.incCount();
      return connection;
    }
  }

  static class FConnectionImplementation implements FConnection, Closeable {

    static final Log LOG = LogFactory.getLog(FConnectionImplementation.class);

    private final Class<? extends AdminProtocol> adminClass;

    private final Class<? extends ClientProtocol> clientClass;

    /** Parameter name for what client protocol to use. */
    public static final String CLIENT_PROTOCOL_CLASS = "wasp.clientprotocol.class";

    /** Default client protocol class name. */
    public static final String DEFAULT_CLIENT_PROTOCOL_CLASS = ClientProtocol.class
        .getName();

    /** Parameter name for what admin protocol to use. */
    public static final String ADMIN_PROTOCOL_CLASS = "wasp.adminprotocol.class";

    /** Default admin protocol class name. */
    public static final String DEFAULT_ADMIN_PROTOCOL_CLASS = AdminProtocol.class
        .getName();

    private final int numRetries;
    private final int rpcTimeout;
    private final int maxRPCAttempts;
    private final long pause;

    private final Configuration conf;

    private volatile boolean closed;
    private volatile boolean aborted;

    private volatile ZooKeeperWatcher zooKeeper;

    private volatile ClusterId clusterId;

    private final Object entityGroupLock = new Object();

    // indicates whether this connection's life cycle is managed
    private final boolean managed;

    // ZooKeeper-based master address tracker
    private volatile MasterAddressTracker masterAddressTracker;

    // Known entityGroup ServerName.toString() -> EntityGroup Client/Admin
    private final ConcurrentHashMap<String, Map<String, VersionedProtocol>> servers = new ConcurrentHashMap<String, Map<String, VersionedProtocol>>();

    private final ConcurrentHashMap<String, String> connectionLock = new ConcurrentHashMap<String, String>();

    private boolean stopProxy = true;

    private int refCount;

    private final Object masterAndZKLock = new Object();

    // keepAlive time, in ms. No reason to make it configurable.
    private static final long keepAlive = 5 * 60 * 1000;

    private final DelayedClosing delayedClosing = DelayedClosing
        .createAndStart(this);

    /**
     * constructor
     *
     * @param conf
     *          Configuration object
     */
    @SuppressWarnings("unchecked")
    public FConnectionImplementation(Configuration conf, boolean managed)
        throws ZooKeeperConnectionException {
      this.conf = conf;
      this.managed = managed;
      this.closed = false;
      try {
        String adminClassName = conf.get(ADMIN_PROTOCOL_CLASS,
            DEFAULT_ADMIN_PROTOCOL_CLASS);
        this.adminClass = (Class<? extends AdminProtocol>) Class
            .forName(adminClassName);
        String clientClassName = conf.get(CLIENT_PROTOCOL_CLASS,
            DEFAULT_CLIENT_PROTOCOL_CLASS);

        try {
          this.clientClass = (Class<? extends ClientProtocol>) Class
              .forName(clientClassName);
        } catch (ClassNotFoundException e) {
          throw new UnsupportedOperationException(
              "Unable to find client protocol " + clientClassName, e);
        }
      } catch (ClassNotFoundException e) {
        throw new UnsupportedOperationException(e);
      }

      this.numRetries = conf.getInt(FConstants.WASP_CLIENT_RETRIES_NUMBER,
          FConstants.DEFAULT_WASP_CLIENT_RETRIES_NUMBER);
      this.rpcTimeout = conf.getInt(FConstants.WASP_RPC_TIMEOUT_KEY,
          FConstants.DEFAULT_WASP_RPC_TIMEOUT);
      this.maxRPCAttempts = conf.getInt(FConstants.WASP_CLIENT_RPC_MAXATTEMPTS,
          FConstants.DEFAULT_WASP_CLIENT_RPC_MAXATTEMPTS);
      this.pause = conf.getLong(FConstants.WASP_CLIENT_PAUSE,
          FConstants.DEFAULT_WASP_CLIENT_PAUSE);
    }

    /**
     * Map of table to table {@link com.alibaba.wasp.EntityGroupLocation}s. The
     * table key is made by doing a
     * {@link org.apache.hadoop.hbase.util.Bytes#mapKey(byte[])} of the table's
     * name.
     */
    private final Map<Integer, SoftValueSortedMap<byte[], EntityGroupLocation>> cachedEntityGroupLocations = new HashMap<Integer, SoftValueSortedMap<byte[], EntityGroupLocation>>();

    // The presence of a server in the map implies it's likely that there is an
    // entry in cachedEntityGroupLocations that map to this server; but the
    // absence
    // of a server in this map guarentees that there is no entry in cache that
    // maps to the absent server.
    private final Set<String> cachedServers = new HashSet<String>();

    // entityGroup cache prefetch is enabled by default. this set contains all
    // tables whose entityGroup cache prefetch are disabled.
    private final Set<Integer> entityGroupCachePrefetchDisabledTables = new CopyOnWriteArraySet<Integer>();

    @Override
    public Configuration getConfiguration() {
      return this.conf;
    }

    @Override
    public ZooKeeperWatcher getZooKeeperWatcher()
        throws ZooKeeperConnectionException {
      if (zooKeeper == null) {
        try {
          this.zooKeeper = new ZooKeeperWatcher(conf, "fconnection", this);
        } catch (ZooKeeperConnectionException zce) {
          throw zce;
        } catch (IOException e) {
          throw new ZooKeeperConnectionException("An error is preventing"
              + " Wasp from connecting to ZooKeeper", e);
        }
      }
      return zooKeeper;
    }

    private synchronized void ensureZookeeperTrackers()
        throws ZooKeeperConnectionException {
      // initialize zookeeper and master address manager
      if (zooKeeper == null) {
        zooKeeper = getZooKeeperWatcher();
      }
      if (clusterId == null) {
        clusterId = new ClusterId();
      }
      if (masterAddressTracker == null) {
        masterAddressTracker = new MasterAddressTracker(zooKeeper, this);
        masterAddressTracker.start();
      }
    }

    private synchronized void resetZooKeeperTrackers() {
      if (masterAddressTracker != null) {
        masterAddressTracker.stop();
        masterAddressTracker = null;
      }
      clusterId = null;
      if (zooKeeper != null) {
        zooKeeper.close();
        zooKeeper = null;
      }
    }

    @Override
    public boolean isMasterRunning() throws MasterNotRunningException,
        ZooKeeperConnectionException {
      getKeepAliveMasterMonitor().close();
      return true;
    }

    @Override
    public boolean isTableEnabled(byte[] tableName) throws IOException {
      FTable.isLegalTableName(Bytes.toString(tableName));
      return testTableOnlineState(tableName, true);
    }

    /*
     * @param True if table is online
     */
    private boolean testTableOnlineState(byte[] tableName, boolean online)
        throws IOException {
      getZooKeeperWatcher();
      String tableNameStr = Bytes.toString(tableName);
      try {
        if (online) {
          return ZKTableReadOnly.isEnabledTable(this.zooKeeper, tableNameStr);
        }
        return ZKTableReadOnly.isDisabledTable(this.zooKeeper, tableNameStr);
      } catch (KeeperException e) {
        throw new IOException("Enable/Disable failed", e);
      }
    }

    /**
     * Return if this client has no reference
     *
     * @return true if this client has no reference; false otherwise
     */
    boolean isZeroReference() {
      return refCount == 0;
    }

    /**
     * Increment this client's reference count.
     */
    void incCount() {
      ++refCount;
    }

    /**
     * Decrement this client's reference count.
     */
    void decCount() {
      if (refCount > 0) {
        --refCount;
      }
    }

    @Override
    public boolean isTableDisabled(byte[] tableName) throws IOException {
      FTable.isLegalTableName(Bytes.toString(tableName));
      return testTableOnlineState(tableName, false);
    }

    @Override
    public boolean isTableAvailable(final byte[] tableName) throws IOException {
      if (!isTableEnabled(tableName)) {
        return false;
      }
      try {
        FTable fTable = this.getFTableDescriptor(tableName);
        byte[] rootTableName = null;
        if (fTable != null) {
          rootTableName = fTable.isRootTable() ? tableName : Bytes
              .toBytes(fTable.getParentName());
        }
        if (rootTableName == null || !isTableEnabled(rootTableName)) {
          return false;
        }
        return this
            .getKeepAliveMasterAdmin()
            .isTableAvailable(null,
                RequestConverter.buildIsTableAvailableRequest(rootTableName))
            .getAvailable();
      } catch (ServiceException e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean isTableLocked(final byte[] tableName) throws IOException {
      try {
        return this
            .getKeepAliveMasterAdmin()
            .isTableLocked(null,
                RequestConverter.buildIsTableLockedRequest(tableName))
            .getLocked();
      } catch (ServiceException e) {
        throw new IOException(e);
      }
    }

    @Override
    public FTable[] listTables() throws IOException {
      MasterMonitorKeepAliveConnection master = getKeepAliveMasterMonitor();
      try {
        GetTableDescriptorsRequest req = RequestConverter
            .buildGetTableDescriptorsRequest(null);
        return ProtobufUtil.getHTableDescriptorArray(master
            .getTableDescriptors(null, req));
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      } finally {
        master.close();
      }
    }

    @Override
    public FTable getFTableDescriptor(byte[] tableName) throws IOException {
      if (tableName == null || tableName.length == 0)
        return null;

      MasterMonitorKeepAliveConnection master = getKeepAliveMasterMonitor();
      GetTableDescriptorsResponse htds;
      try {
        List<String> tables = new ArrayList<String>();
        tables.add(Bytes.toString(tableName));
        GetTableDescriptorsRequest req = RequestConverter
            .buildGetTableDescriptorsRequest(tables);
        htds = master.getTableDescriptors(null, req);
        if (htds.getTableSchemaList().size() > 0) {
          return ProtobufUtil.convertITableSchema(htds.getTableSchemaList()
              .get(0));
        }
        return null;
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      } finally {
        master.close();
      }
    }

    @Override
    public EntityGroupLocation locateEntityGroup(byte[] tableName, byte[] row)
        throws IOException {
      return locateEntityGroup(tableName, row, true);
    }

    private EntityGroupLocation locateEntityGroup(final byte[] tableName,
        final byte[] row, boolean useCache) throws IOException {
      if (this.closed)
        throw new IOException(toString() + " closed");
      if (tableName == null || tableName.length == 0) {
        throw new IllegalArgumentException(
            "table name cannot be null or zero length");
      }
      ensureZookeeperTrackers();
      FTable table = TableSchemaCacheReader.getInstance(this.conf).getSchema(
          Bytes.toString(tableName));
      if (table.isRootTable()) {
        // entityGroup not in the cache - have to go to read the meta table
        return locateEntityGroupInMeta(tableName, row, useCache,
            entityGroupLock);
      } else {
        return locateEntityGroupInMeta(Bytes.toBytes(table.getParentName()),
            row, useCache, entityGroupLock);
      }
    }

    /**
     * Search the meta table for the EntityGroupLocation info that contains the
     * table and row we're seeking.
     */
    private EntityGroupLocation locateEntityGroupInMeta(byte[] tableName,
        byte[] row, boolean useCache, Object entityGroupLock)
        throws IOException {
      EntityGroupLocation location;
      // If we are supposed to be using the cache, look in the cache to see if
      // we already have the entityGroup.
      if (useCache) {
        location = getCachedLocation(tableName, row);
        if (location != null) {
          return location;
        }
      }

      for (int tries = 0; true; tries++) {
        if (tries >= numRetries) {
          throw new NoServerForEntityGroupException(
              "Unable to find entityGroup for " + Bytes.toStringBinary(row)
                  + " after " + numRetries + " tries.");
        }

        try {
          synchronized (entityGroupLock) {
            // we may want to pre-fetch some entityGroup info
            // into the global entityGroup cache for this table.
            if (getEntityGroupCachePrefetch(tableName)) {
              prefetchEntityGroupCache(tableName, row);
            }

            // Check the cache again for a hit in case some other thread made
            // the same query while we were waiting on the lock. If not supposed
            // to
            // be using the cache, delete any existing cached location so it
            // won't interfere.
            if (useCache) {
              location = getCachedLocation(tableName, row);
              if (location != null) {
                return location;
              }
            } else {
              deleteCachedLocation(tableName, row);
            }
          }

          // convert the row result into the EntityGroupLocation we need!
          location = FMetaReader.scanEntityGroupLocation(conf, tableName, row);
          if (location == null) {
            throw new NoServerForEntityGroupException(Bytes.toString(tableName));
          }

          EntityGroupInfo entityGroupInfo = location.getEntityGroupInfo();
          // possible we got a entityGroup of a different table...
          if (!Bytes.equals(entityGroupInfo.getTableName(), tableName)) {
            throw new TableNotFoundException("Table '"
                + Bytes.toString(tableName) + "' was not found, got: "
                + Bytes.toString(entityGroupInfo.getTableName()) + ".");
          }
          if (entityGroupInfo.isSplit()) {
            throw new EntityGroupOfflineException(
                "the only available entityGroup for"
                    + " the required row is a split parent,"
                    + " the daughters should be online soon: "
                    + entityGroupInfo.getEntityGroupNameAsString());
          }
          if (entityGroupInfo.isOffline()) {
            throw new EntityGroupOfflineException(
                "the entityGroup is offline, could"
                    + " be caused by a disable table call: "
                    + entityGroupInfo.getEntityGroupNameAsString());
          }

          String hostAndPort = location.getHostnamePort();
          if (hostAndPort.equals("")) {
            throw new NoServerForEntityGroupException(
                "No server address listed " + " for entityGroup "
                    + entityGroupInfo.getEntityGroupNameAsString()
                    + " containing row " + Bytes.toStringBinary(row));
          }

          // Instantiate the location
          cacheLocation(tableName, location);
          return location;
        } catch (TableNotFoundException e) {
          // if we got this error, probably means the table just plain doesn't
          // exist. rethrow the error immediately. this should always be coming
          // from the HTable constructor.
          throw e;
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = ((RemoteException) e).unwrapRemoteException();
          }
          if (tries < numRetries - 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("locateEntityGroupInMeta, attempt=" + tries + " of "
                  + this.numRetries + " failed; retrying after sleep of "
                  + ConnectionUtils.getPauseTime(this.pause, tries)
                  + " because: " + e.getMessage());
            }
          } else {
            throw e;
          }
        }

        try {
          Thread.sleep(ConnectionUtils.getPauseTime(this.pause, tries));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Giving up trying to location entityGroup in "
              + "fmeta: thread is interrupted.");
        }
      }
    }

    /*
     * Search .FMETA. for the EntityGroupLocation info that contains the table
     * and row we're seeking. It will prefetch certain number of entityGroups
     * info and save them to the global entityGroup cache.
     */
    private void prefetchEntityGroupCache(final byte[] tableName,
        final byte[] row) {
      // Implement a new visitor for MetaScanner, and use it to walk through
      // the .FMETA.
      try {
        EntityGroupLocation locatoin = FMetaReader.scanEntityGroupLocation(
            conf, tableName, row);
        cacheLocation(tableName, locatoin);
      } catch (IOException e) {
        LOG.warn("Encountered problems when prefetch META table: ", e);
      }
    }

    @Override
    public void clearEntityGroupCache() {
      synchronized (this.cachedEntityGroupLocations) {
        this.cachedEntityGroupLocations.clear();
        this.cachedServers.clear();
      }
    }

    @Override
    public void clearEntityGroupCache(byte[] tableName) {
      synchronized (this.cachedEntityGroupLocations) {
        this.cachedEntityGroupLocations.remove(Bytes.mapKey(tableName));
      }
    }

    @Override
    public EntityGroupLocation relocateEntityGroup(byte[] tableName, byte[] row)
        throws IOException {
      return locateEntityGroup(tableName, row, false);
    }

    /**
     * Either the passed <code>isa</code> is null or <code>hostname</code> can
     * be but not both.
     *
     * @param hostname
     * @param port
     * @param protocolClass
     * @param version
     * @return Proxy.
     * @throws java.io.IOException
     */
    VersionedProtocol getProtocol(final String hostname, final int port,
        final Class<? extends VersionedProtocol> protocolClass,
        final long version) throws IOException {
      String rsName = Addressing.createHostAndPortStr(hostname, port);
      // See if we already have a connection (common case)
      Map<String, VersionedProtocol> protocols = this.servers.get(rsName);
      if (protocols == null) {
        protocols = new HashMap<String, VersionedProtocol>();
        Map<String, VersionedProtocol> existingProtocols = this.servers
            .putIfAbsent(rsName, protocols);
        if (existingProtocols != null) {
          protocols = existingProtocols;
        }
      }
      String protocol = protocolClass.getName();
      VersionedProtocol server = protocols.get(protocol);
      if (server == null) {
        // create a unique lock for this RS + protocol (if necessary)
        String lockKey = protocol + "@" + rsName;
        this.connectionLock.putIfAbsent(lockKey, lockKey);
        // get the RS lock
        synchronized (this.connectionLock.get(lockKey)) {
          // do one more lookup in case we were stalled above
          server = protocols.get(protocol);
          if (server == null) {
            try {
              // Only create isa when we need to.
              InetSocketAddress address = new InetSocketAddress(hostname, port);
              // definitely a cache miss. establish an RPC for this RS
              server = WaspRPC.waitForProxy(protocolClass, version, address,
                  this.conf, this.maxRPCAttempts, this.rpcTimeout,
                  this.rpcTimeout);
              protocols.put(protocol, server);
            } catch (RemoteException e) {
              LOG.warn("RemoteException connecting to RS", e);
              // Throw what the RemoteException was carrying.
              throw e.unwrapRemoteException();
            }
          }
        }
      }
      return server;
    }

    @Override
    public EntityGroupLocation getEntityGroupLocation(byte[] tableName,
        byte[] row, boolean reload) throws IOException {
      return reload ? relocateEntityGroup(tableName, row) : locateEntityGroup(
          tableName, row);
    }

    @Override
    public <T> T getFServerWithRetries(ServerCallable<T> callable)
        throws IOException, RuntimeException {
      return callable.withRetries();
    }

    @Override
    public <T> T getFServerWithoutRetries(ServerCallable<T> callable)
        throws IOException, RuntimeException {
      return callable.withoutRetries();
    }

    @Override
    public void setEntityGroupCachePrefetch(byte[] tableName, boolean enable) {
      if (!enable) {
        entityGroupCachePrefetchDisabledTables.add(Bytes.mapKey(tableName));
      } else {
        entityGroupCachePrefetchDisabledTables.remove(Bytes.mapKey(tableName));
      }
    }

    @Override
    public boolean getEntityGroupCachePrefetch(byte[] tableName) {
      return !entityGroupCachePrefetchDisabledTables.contains(Bytes
          .mapKey(tableName));
    }

    @Override
    public void prewarmEntityGroupCache(byte[] tableName,
        Map<EntityGroupInfo, ServerName> entityGroups) {
      for (Entry<EntityGroupInfo, ServerName> e : entityGroups.entrySet()) {
        ServerName sn = e.getValue();
        if (sn == null || sn.getHostAndPort() == null)
          continue;
        cacheLocation(tableName,
            new EntityGroupLocation(e.getKey(), sn.getHostname(), sn.getPort()));
      }
    }

    /*
     * @param tableName
     *
     * @return Map of cached locations for passed <code>tableName</code>
     */
    private SoftValueSortedMap<byte[], EntityGroupLocation> getTableLocations(
        final byte[] tableName) {
      // find the map of cached locations for this table
      Integer key = Bytes.mapKey(tableName);
      SoftValueSortedMap<byte[], EntityGroupLocation> result;
      synchronized (this.cachedEntityGroupLocations) {
        result = this.cachedEntityGroupLocations.get(key);
        // if tableLocations for this table isn't built yet, make one
        if (result == null) {
          result = new SoftValueSortedMap<byte[], EntityGroupLocation>(
              Bytes.BYTES_COMPARATOR);
          this.cachedEntityGroupLocations.put(key, result);
        }
      }
      return result;
    }

    @Override
    public FTable[] getFTableDescriptors(List<String> tableNames)
        throws IOException {
      if (tableNames == null || tableNames.isEmpty())
        return new FTable[0];
      MasterMonitorKeepAliveConnection master = getKeepAliveMasterMonitor();
      try {
        GetTableDescriptorsRequest req = RequestConverter
            .buildGetTableDescriptorsRequest(tableNames);
        return ProtobufUtil.getHTableDescriptorArray(master
            .getTableDescriptors(null, req));
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      } finally {
        master.close();
      }
    }

    @Override
    public boolean isClosed() {
      return this.closed;
    }

    @Override
    public void clearCaches(String sn) {
      clearCachedLocationForServer(sn);
    }

    @Override
    public List<EntityGroupLocation> locateEntityGroups(byte[] tableName)
        throws IOException {
      return locateEntityGroupsInMeta(tableName, true, entityGroupLock);
    }

    @Override
    public int relocateEntityGroupsInCache(byte[] tableName) throws IOException {
      locateEntityGroupsInMeta(tableName, true, entityGroupLock);
      return getNumberOfCachedEntityGroupLocations(tableName);
    }

    private List<EntityGroupLocation> locateEntityGroupsInMeta(
        byte[] tableName, boolean useCache, Object entityGroupLock)
        throws IOException {
      List<EntityGroupLocation> locations;
      List<EntityGroupLocation> realLocations = new ArrayList<EntityGroupLocation>();
      final byte[] rootTableName = FMetaReader.getRootTable(conf, tableName);
      // If we are supposed to be using the cache, look in the cache to see if
      // we already have the entityGroup.

      // build the key of the meta entityGroup we should be looking for.
      // the extra 9's on the end are necessary to allow "exact" matches
      // without knowing the precise entityGroup names.
      for (int tries = 0; true; tries++) {
        if (tries >= numRetries) {
          throw new NoServerForEntityGroupException(
              "Unable to find entityGroups for table "
                  + Bytes.toStringBinary(tableName) + " after " + numRetries
                  + " tries.");
        }

        try {

          // convert the row result into the EntityGroupLocation we need!
          locations = FMetaReader.getEntityGroupLocations(conf, tableName);
          if (locations == null || locations.size() == 0) {
            throw new NoServerForEntityGroupException(Bytes.toString(tableName));
          }

          for (EntityGroupLocation location : locations) {

            EntityGroupInfo entityGroupInfo = location.getEntityGroupInfo();
            // possible we got a entityGroup of a different table...
            if (!Bytes.equals(entityGroupInfo.getTableName(), rootTableName)) {
              throw new TableNotFoundException("Table '"
                  + Bytes.toString(tableName) + "' was not found, got: "
                  + Bytes.toString(entityGroupInfo.getTableName()) + ".");
            }
            if (entityGroupInfo.isSplit()) {
              throw new EntityGroupOfflineException(
                  "the only available entityGroup for"
                      + " the required row is a split parent,"
                      + " the daughters should be online soon: "
                      + entityGroupInfo.getEntityGroupNameAsString());
            }
            if (entityGroupInfo.isOffline()) {
              throw new EntityGroupOfflineException(
                  "the entityGroup is offline, could"
                      + " be caused by a disable table call: "
                      + entityGroupInfo.getEntityGroupNameAsString());
            }

            String hostAndPort = location.getHostnamePort();
            if (hostAndPort.equals("")) {
              throw new NoServerForEntityGroupException(
                  "No server address listed " + " for entityGroup "
                      + entityGroupInfo.getEntityGroupNameAsString()
                      + " containing row "
                      + Bytes.toStringBinary(entityGroupInfo.getStartKey()));
            }

            // Instantiate the location
            cacheLocation(tableName, location);
            realLocations.add(location);
          }
          return realLocations;
        } catch (TableNotFoundException e) {
          // if we got this error, probably means the table just plain doesn't
          // exist. rethrow the error immediately. this should always be coming
          // from the HTable constructor.
          throw e;
        } catch (IOException e) {
          if (e instanceof RemoteException) {
            e = ((RemoteException) e).unwrapRemoteException();
          }
          if (tries < numRetries - 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("locateEntityGroupInMeta, attempt=" + tries + " of "
                  + this.numRetries + " failed; retrying after sleep of "
                  + ConnectionUtils.getPauseTime(this.pause, tries)
                  + " because: " + e.getMessage());
            }
          } else {
            throw e;
          }
        }

        try {
          Thread.sleep(ConnectionUtils.getPauseTime(this.pause, tries));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Giving up trying to location entityGroup in "
              + "fmeta: thread is interrupted.");
        }
      }
    }

    /**
     * Put a newly discovered EntityGroupLocation into the cache.
     */
    private void cacheLocation(final byte[] tableName,
        final EntityGroupLocation location) {
      byte[] startKey = location.getEntityGroupInfo().getStartKey();
      Map<byte[], EntityGroupLocation> tableLocations = getTableLocations(tableName);
      boolean hasNewCache = false;
      synchronized (this.cachedEntityGroupLocations) {
        cachedServers.add(location.getHostnamePort());
        hasNewCache = (tableLocations.put(startKey, location) == null);
      }
      if (hasNewCache) {
        LOG.debug("Cached location for "
            + location.getEntityGroupInfo().getEntityGroupNameAsString()
            + " is " + location.getHostnamePort());
      }
    }

    /*
     * Return the number of cached entityGroup for a table. It will only be
     * called from a unit test.
     */
    int getNumberOfCachedEntityGroupLocations(final byte[] tableName) {
      Integer key = Bytes.mapKey(tableName);
      synchronized (this.cachedEntityGroupLocations) {
        Map<byte[], EntityGroupLocation> tableLocs = this.cachedEntityGroupLocations
            .get(key);

        if (tableLocs == null) {
          return 0;
        }
        return tableLocs.values().size();
      }
    }

    /**
     * Check the entityGroup cache to see whether a entityGroup is cached yet or
     * not. Called by unit tests.
     *
     * @param tableName
     *          tableName
     * @param row
     *          row
     * @return entityGroup cached or not.
     */
    boolean isEntityGroupCached(final byte[] tableName, final byte[] row) {
      EntityGroupLocation location = getCachedLocation(tableName, row);
      return location != null;
    }

    /*
     * Search the cache for a location that fits our table and row key. Return
     * null if no suitable entityGroup is located.
     *
     * @param tableName
     *
     * @param row
     *
     * @return Null or entityGroup location found in cache.
     */
    EntityGroupLocation getCachedLocation(final byte[] tableName,
        final byte[] row) {
      SoftValueSortedMap<byte[], EntityGroupLocation> tableLocations = getTableLocations(tableName);

      // start to examine the cache. we can only do cache actions
      // if there's something in the cache for this table.
      if (tableLocations.isEmpty()) {
        return null;
      }

      EntityGroupLocation possibleEntityGroup = tableLocations.get(row);
      if (possibleEntityGroup != null) {
        return possibleEntityGroup;
      }

      possibleEntityGroup = tableLocations.lowerValueByKey(row);
      if (possibleEntityGroup == null) {
        return null;
      }

      // make sure that the end key is greater than the row we're looking
      // for, otherwise the row actually belongs in the next entityGroup, not
      // this one. the exception case is when the end key is
      // FConstants.EMPTY_END_ROW, signifying that the entityGroup we're
      // checking is actually the last entityGroup in the table.
      byte[] endKey = possibleEntityGroup.getEntityGroupInfo().getEndKey();
      if (Bytes.equals(endKey, FConstants.EMPTY_END_ROW)||
          KeyValue.getRowComparator(tableName).compareRows(
              endKey, 0, endKey.length, row, 0, row.length) > 0) {
        return possibleEntityGroup;
      }

      // Passed all the way through, so we got nothing - complete cache miss
      return null;
    }

    /**
     * Delete a cached location
     *
     * @param tableName
     *          tableName
     * @param row
     */
    void deleteCachedLocation(final byte[] tableName, final byte[] row) {
      synchronized (this.cachedEntityGroupLocations) {
        Map<byte[], EntityGroupLocation> tableLocations = getTableLocations(tableName);
        // start to examine the cache. we can only do cache actions
        // if there's something in the cache for this table.
        if (!tableLocations.isEmpty()) {
          EntityGroupLocation egl = getCachedLocation(tableName, row);
          if (egl != null) {
            tableLocations.remove(egl.getEntityGroupInfo().getStartKey());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Removed "
                  + egl.getEntityGroupInfo().getEntityGroupNameAsString()
                  + " for tableName=" + Bytes.toString(tableName)
                  + " from cache " + "because of " + Bytes.toStringBinary(row));
            }
          }
        }
      }
    }

    /*
     * Delete all cached entries of a table that maps to a specific location.
     *
     * @param tablename
     *
     * @param server
     */
    private void clearCachedLocationForServer(final String server) {
      boolean deletedSomething = false;
      synchronized (this.cachedEntityGroupLocations) {
        if (!cachedServers.contains(server)) {
          return;
        }
        for (Map<byte[], EntityGroupLocation> tableLocations : cachedEntityGroupLocations
            .values()) {
          for (Entry<byte[], EntityGroupLocation> e : tableLocations
              .entrySet()) {
            if (e.getValue().getHostnamePort().equals(server)) {
              tableLocations.remove(e.getKey());
              deletedSomething = true;
            }
          }
        }
        cachedServers.remove(server);
      }
      if (deletedSomething && LOG.isDebugEnabled()) {
        LOG.debug("Removed all cached entityGroup locations that map to "
            + server);
      }
    }

    @Override
    public void abort(String msg, Throwable t) {
      if (t instanceof KeeperException) {
        LOG.info("This client just lost it's session with ZooKeeper, will"
            + " automatically reconnect when needed.");
        if (t instanceof KeeperException.SessionExpiredException) {
          LOG.info("ZK session expired. This disconnect could have been"
              + " caused by a network partition or a long-running GC pause,"
              + " either way it's recommended that you verify your environment.");
          resetZooKeeperTrackers();
        }
        return;
      }
      if (t != null)
        LOG.fatal(msg, t);
      else
        LOG.fatal(msg);
      this.aborted = true;
      close();

    }

    @Override
    public boolean isAborted() {
      return this.aborted;
    }

    /**
     * @return the refCount
     */
    public int getRefCount() {
      return refCount;
    }

    public void stopProxyOnClose(boolean stopProxy) {
      this.stopProxy = stopProxy;
    }

    @Override
    public AdminProtocol getAdmin(final String hostname, final int port)
        throws IOException {
      return (AdminProtocol) getProtocol(hostname, port, adminClass,
          AdminProtocol.VERSION);
    }

    @Override
    public ClientProtocol getClient(final String hostname, final int port)
        throws IOException {
      return (ClientProtocol) getProtocol(hostname, port, clientClass,
          ClientProtocol.VERSION);
    }

    private static class MasterProtocolState {
      public MasterProtocol protocol;
      public int userCount;
      public long keepAliveUntil = Long.MAX_VALUE;
      public final Class<? extends MasterProtocol> protocolClass;
      public long version;

      public MasterProtocolState(
          final Class<? extends MasterProtocol> protocolClass, long version) {
        this.protocolClass = protocolClass;
        this.version = version;
      }
    }

    private boolean isKeepAliveMasterConnectedAndRunning(
        MasterProtocolState protocolState) {
      if (protocolState.protocol == null) {
        return false;
      }
      try {
        return protocolState.protocol.isMasterRunning(null,
            RequestConverter.buildIsMasterRunningRequest())
            .getIsMasterRunning();
      } catch (UndeclaredThrowableException e) {
        // It's somehow messy, but we can receive exceptions such as
        // java.net.ConnectException but they're not declared. So we catch
        // it...
        LOG.info("Master connection is not running anymore",
            e.getUndeclaredThrowable());
        return false;
      } catch (ServiceException se) {
        LOG.warn("Checking master connection", se);
        return false;
      }
    }

    private ZooKeeperKeepAliveConnection keepAliveZookeeper;
    private int keepAliveZookeeperUserCount;
    private boolean canCloseZKW = true;
    private long keepZooKeeperWatcherAliveUntil = Long.MAX_VALUE;

    /**
     * Retrieve a shared ZooKeeperWatcher. You must close it it once you've have
     * finished with it.
     *
     * @return The shared instance. Never returns null.
     */
    public ZooKeeperKeepAliveConnection getKeepAliveZooKeeperWatcher()
        throws IOException {
      synchronized (masterAndZKLock) {

        if (keepAliveZookeeper == null) {
          // We don't check that our link to ZooKeeper is still valid
          // But there is a retry mechanism in the ZooKeeperWatcher itself
          keepAliveZookeeper = new ZooKeeperKeepAliveConnection(conf,
              this.toString(), this);
        }
        keepAliveZookeeperUserCount++;
        keepZooKeeperWatcherAliveUntil = Long.MAX_VALUE;

        return keepAliveZookeeper;
      }
    }

    /**
     * Create a new Master proxy. Try once only.
     */
    private MasterProtocol createMasterInterface(
        MasterProtocolState masterProtocolState) throws IOException,
        KeeperException, ServiceException {

      ZooKeeperKeepAliveConnection zkw;
      try {
        zkw = getKeepAliveZooKeeperWatcher();
      } catch (IOException e) {
        throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
      }

      try {
        checkIfBaseNodeAvailable(zkw);
        ServerName sn = MasterAddressTracker.getMasterAddress(zkw);
        if (sn == null) {
          String msg = "ZooKeeper available but no active master location found";
          LOG.info(msg);
          throw new MasterNotRunningException(msg);
        }

        InetSocketAddress isa = new InetSocketAddress(sn.getHostname(),
            sn.getPort());
        MasterProtocol tryMaster = (MasterProtocol) WaspRPC.getProxy(
            masterProtocolState.protocolClass, masterProtocolState.version,
            isa, this.conf, this.rpcTimeout);

        if (tryMaster.isMasterRunning(null,
            RequestConverter.buildIsMasterRunningRequest())
            .getIsMasterRunning()) {
          return tryMaster;
        } else {
          WaspRPC.stopProxy(tryMaster);
          String msg = "Can create a proxy to master, but it is not running";
          LOG.info(msg);
          throw new MasterNotRunningException(msg);
        }
      } finally {
        zkw.close();
      }
    }

    /**
     * Create a master, retries if necessary.
     */
    private MasterProtocol createMasterWithRetries(
        MasterProtocolState masterProtocolState)
        throws MasterNotRunningException {

      // The lock must be at the beginning to prevent multiple master creation
      // (and leaks) in a multithread context
      synchronized (this.masterAndZKLock) {
        Exception exceptionCaught = null;
        MasterProtocol master = null;
        int tries = 0;
        while (!this.closed && master == null) {
          tries++;
          try {
            master = createMasterInterface(masterProtocolState);
          } catch (IOException e) {
            exceptionCaught = e;
          } catch (KeeperException e) {
            exceptionCaught = e;
          } catch (ServiceException e) {
            exceptionCaught = e;
          }

          if (exceptionCaught != null)
            // It failed. If it's not the last try, we're going to wait a little
            if (tries < numRetries) {
              long pauseTime = ConnectionUtils.getPauseTime(this.pause, tries);
              LOG.info("getMaster attempt " + tries + " of " + numRetries
                  + " failed; retrying after sleep of " + pauseTime,
                  exceptionCaught);

              try {
                Thread.sleep(pauseTime);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                    "Thread was interrupted while trying to connect to master.",
                    e);
              }

            } else {
              // Enough tries, we stop now
              LOG.info("getMaster attempt " + tries + " of " + numRetries
                  + " failed; no more retrying.", exceptionCaught);
              throw new MasterNotRunningException(exceptionCaught);
            }
        }

        if (master == null) {
          // implies this.closed true
          throw new MasterNotRunningException(
              "Connection was closed while trying to get master");
        }

        return master;
      }
    }

    private void checkIfBaseNodeAvailable(ZooKeeperWatcher zkw)
        throws MasterNotRunningException {
      String errorMsg;
      try {
        if (ZKUtil.checkExists(zkw, zkw.baseZNode) == -1) {
          errorMsg = "The node "
              + zkw.baseZNode
              + " is not in ZooKeeper. "
              + "It should have been written by the master. "
              + "Check the value configured in 'zookeeper.znode.parent'. "
              + "There could be a mismatch with the one configured in the master.";
          LOG.error(errorMsg);
          throw new MasterNotRunningException(errorMsg);
        }
      } catch (KeeperException e) {
        errorMsg = "Can't get connection to ZooKeeper: " + e.getMessage();
        LOG.error(errorMsg);
        throw new MasterNotRunningException(errorMsg, e);
      }
    }

    private static class MasterProtocolHandler implements InvocationHandler {
      private FConnectionImplementation connection;
      private MasterProtocolState protocolStateTracker;

      protected MasterProtocolHandler(FConnectionImplementation connection,
          MasterProtocolState protocolStateTracker) {
        this.connection = connection;
        this.protocolStateTracker = protocolStateTracker;
      }

      @Override
      public Object invoke(Object proxy, Method method, Object[] args)
          throws Throwable {
        if (method.getName().equals("close")
            && method.getParameterTypes().length == 0) {
          release(connection, protocolStateTracker);
          return null;
        } else {
          try {
            return method.invoke(protocolStateTracker.protocol, args);
          } catch (InvocationTargetException e) {
            // We will have this for all the exception, checked on not, sent
            // by any layer, including the functional exception
            Throwable cause = e.getCause();
            if (cause == null) {
              throw new RuntimeException(
                  "Proxy invocation failed and getCause is null", e);
            }
            if (cause instanceof UndeclaredThrowableException) {
              cause = cause.getCause();
            }
            throw cause;
          }
        }
      }

      private void release(FConnectionImplementation connection,
          MasterProtocolState target) {
        connection.releaseMaster(target);
      }
    }

    private void releaseMaster(MasterProtocolState protocolState) {
      if (protocolState.protocol == null) {
        return;
      }
      synchronized (masterAndZKLock) {
        --protocolState.userCount;
        if (protocolState.userCount <= 0) {
          protocolState.keepAliveUntil = System.currentTimeMillis() + keepAlive;
        }
      }
    }

    void releaseZooKeeperWatcher(ZooKeeperWatcher zkw) {
      if (zkw == null) {
        return;
      }
      synchronized (masterAndZKLock) {
        --keepAliveZookeeperUserCount;
        if (keepAliveZookeeperUserCount <= 0) {
          keepZooKeeperWatcherAliveUntil = System.currentTimeMillis()
              + keepAlive;
        }
      }
    }

    /**
     * This function allows HBaseAdmin and potentially others to get a shared
     * master connection.
     *
     * @return The shared instance. Never returns null.
     * @throws com.alibaba.wasp.MasterNotRunningException
     */
    private Object getKeepAliveMasterProtocol(
        MasterProtocolState protocolState, Class connectionClass)
        throws MasterNotRunningException {
      synchronized (masterAndZKLock) {
        if (!isKeepAliveMasterConnectedAndRunning(protocolState)) {
          if (protocolState.protocol != null) {
            WaspRPC.stopProxy(protocolState.protocol);
          }
          protocolState.protocol = null;
          protocolState.protocol = createMasterWithRetries(protocolState);
        }
        protocolState.userCount++;
        protocolState.keepAliveUntil = Long.MAX_VALUE;

        return Proxy.newProxyInstance(connectionClass.getClassLoader(),
            new Class[] { connectionClass }, new MasterProtocolHandler(this,
                protocolState));
      }
    }

    MasterProtocolState masterAdminProtocol = new MasterProtocolState(
        FMasterAdminProtocol.class, FMasterAdminProtocol.VERSION);
    MasterProtocolState masterMonitorProtocol = new MasterProtocolState(
        FMasterMonitorProtocol.class, FMasterMonitorProtocol.VERSION);

    /**
     *
     * @throws com.alibaba.wasp.MasterNotRunningException
     * @see com.alibaba.wasp.client.FConnection#getKeepAliveMasterAdmin()
     */
    @Override
    public MasterAdminKeepAliveConnection getKeepAliveMasterAdmin()
        throws MasterNotRunningException {
      return (MasterAdminKeepAliveConnection) getKeepAliveMasterProtocol(
          masterAdminProtocol, MasterAdminKeepAliveConnection.class);
    }

    /**
     *
     * @throws com.alibaba.wasp.MasterNotRunningException
     * @see com.alibaba.wasp.client.FConnection#getKeepAliveMasterMonitor()
     */
    @Override
    public MasterMonitorKeepAliveConnection getKeepAliveMasterMonitor()
        throws MasterNotRunningException {
      return (MasterMonitorKeepAliveConnection) getKeepAliveMasterProtocol(
          masterMonitorProtocol, MasterMonitorKeepAliveConnection.class);
    }

    /**
     *
     * @throws com.alibaba.wasp.MasterNotRunningException
     * @see com.alibaba.wasp.client.FConnection#getKeepAliveMasterMetaServer()
     */
    @Override
    public MetaServerKeepAliveConnection getKeepAliveMasterMetaServer()
        throws MasterNotRunningException {
      return (MetaServerKeepAliveConnection) getKeepAliveMasterProtocol(
          masterMonitorProtocol, MetaServerKeepAliveConnection.class);
    }

    /**
     * @see com.alibaba.wasp.client.FConnection#getMasterAdmin()
     */
    @Override
    public FMasterAdminProtocol getMasterAdmin() throws IOException {
      return getKeepAliveMasterAdmin();
    }

    /**
     * @see com.alibaba.wasp.client.FConnection#getMasterMonitor()
     */
    @Override
    public FMasterMonitorProtocol getMasterMonitor() throws IOException {
      return getKeepAliveMasterMonitor();
    }

    /**
     * @see com.alibaba.wasp.client.FConnection#getMetaServer()
     */
    @Override
    public FMetaServerProtocol getMetaServer() throws IOException {
      return getKeepAliveMasterMetaServer();
    }

    private void closeZooKeeperWatcher() {
      synchronized (masterAndZKLock) {
        if (keepAliveZookeeper != null) {
          LOG.info("Closing zookeeper sessionid=0x"
              + Long.toHexString(keepAliveZookeeper.getRecoverableZooKeeper()
                  .getSessionId()));
          keepAliveZookeeper.internalClose();
          keepAliveZookeeper = null;
        }
        keepAliveZookeeperUserCount = 0;
      }
    }

    private void closeMasterProtocol(MasterProtocolState protocolState) {
      if (protocolState.protocol != null) {
        LOG.info("Closing master protocol: "
            + protocolState.protocolClass.getName());
        WaspRPC.stopProxy(protocolState.protocol);
        protocolState.protocol = null;
      }
      protocolState.userCount = 0;
    }

    void close(boolean stopProxy) {
      if (this.closed) {
        return;
      }
      delayedClosing.stop("Closing connection");
      if (stopProxy) {
        closeMaster();
        for (Map<String, VersionedProtocol> i : servers.values()) {
          for (VersionedProtocol server : i.values()) {
            WaspRPC.stopProxy(server);
          }
        }
      }
      closeZooKeeperWatcher();
      this.servers.clear();
      this.closed = true;
    }

    @Override
    public void close() {
      if (managed) {
        FConnectionManager.deleteConnection(this, stopProxy, false);
      } else {
        close(true);
      }
      if (LOG.isTraceEnabled()) {
        LOG.debug("" + this.zooKeeper + " closed.");
      }
    }

    private void closeMaster() {
      synchronized (masterAndZKLock) {
        closeMasterProtocol(masterAdminProtocol);
        closeMasterProtocol(masterMonitorProtocol);
      }
    }

    /**
     * Creates a Chore thread to check the connections to master & zookeeper and
     * close them when they reach their closing time (
     * {@link MasterProtocolState} and {@link #keepZooKeeperWatcherAliveUntil}
     * ). Keep alive time is managed by the release functions and the variable
     * {@link #keepAlive}
     */
    private static class DelayedClosing extends Chore implements Stoppable {
      private FConnectionImplementation hci;
      Stoppable stoppable;

      private DelayedClosing(FConnectionImplementation hci, Stoppable stoppable) {
        super("ZooKeeperWatcher and Master delayed closing for connection "
            + hci, 60 * 1000, // We check every minutes
            stoppable);
        this.hci = hci;
        this.stoppable = stoppable;
      }

      static DelayedClosing createAndStart(FConnectionImplementation hci) {
        Stoppable stoppable = new Stoppable() {
          private volatile boolean isStopped = false;

          @Override
          public void stop(String why) {
            isStopped = true;
          }

          @Override
          public boolean isStopped() {
            return isStopped;
          }
        };

        return new DelayedClosing(hci, stoppable);
      }

      protected void closeMasterProtocol(MasterProtocolState protocolState) {
        if (System.currentTimeMillis() > protocolState.keepAliveUntil) {
          hci.closeMasterProtocol(protocolState);
          protocolState.keepAliveUntil = Long.MAX_VALUE;
        }
      }

      @Override
      protected void chore() {
        synchronized (hci.masterAndZKLock) {
          if (hci.canCloseZKW) {
            if (System.currentTimeMillis() > hci.keepZooKeeperWatcherAliveUntil) {

              hci.closeZooKeeperWatcher();
              hci.keepZooKeeperWatcherAliveUntil = Long.MAX_VALUE;
            }
          }
          closeMasterProtocol(hci.masterAdminProtocol);
          closeMasterProtocol(hci.masterMonitorProtocol);
        }
      }

      @Override
      public void stop(String why) {
        stoppable.stop(why);
      }

      @Override
      public boolean isStopped() {
        return stoppable.isStopped();
      }
    }
  }

  /**
   * Denotes a unique key to a {@link FConnection} instance.
   *
   * In essence, this class captures the properties in
   * {@link org.apache.hadoop.conf.Configuration} that may be used in the
   * process of establishing a connection. In light of that, if any new such
   * properties are introduced into the mix, they must be added to the
   * {@link FConnectionKey#properties} list.
   *
   */
  static class FConnectionKey {
    public static String[] CONNECTION_PROPERTIES = new String[] {
        FConstants.ZOOKEEPER_QUORUM, FConstants.ZOOKEEPER_ZNODE_PARENT,
        FConstants.ZOOKEEPER_CLIENT_PORT,
        FConstants.ZOOKEEPER_RECOVERABLE_WAITTIME,
        FConstants.WASP_CLIENT_RETRIES_NUMBER,
        FConstants.WASP_CLIENT_RPC_MAXATTEMPTS,
        FConstants.WASP_RPC_TIMEOUT_KEY, };

    private Map<String, String> properties;
    private String username;

    public FConnectionKey(Configuration conf) {
      Map<String, String> m = new HashMap<String, String>();
      if (conf != null) {
        for (String property : CONNECTION_PROPERTIES) {
          String value = conf.get(property);
          if (value != null) {
            m.put(property, value);
          }
        }
      }
      this.properties = Collections.unmodifiableMap(m);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      if (username != null) {
        result = username.hashCode();
      }
      for (String property : CONNECTION_PROPERTIES) {
        String value = properties.get(property);
        if (value != null) {
          result = prime * result + value.hashCode();
        }
      }

      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      FConnectionKey that = (FConnectionKey) obj;
      if (this.username != null && !this.username.equals(that.username)) {
        return false;
      } else if (this.username == null && that.username != null) {
        return false;
      }
      if (this.properties == null) {
        if (that.properties != null) {
          return false;
        }
      } else {
        if (that.properties == null) {
          return false;
        }
        for (String property : CONNECTION_PROPERTIES) {
          String thisValue = this.properties.get(property);
          String thatValue = that.properties.get(property);
          if (thisValue == thatValue) {
            continue;
          }
          if (thisValue == null || !thisValue.equals(thatValue)) {
            return false;
          }
        }
      }
      return true;
    }

    @Override
    public String toString() {
      return "FConnectionKey{" + "properties=" + properties + ", username='"
          + username + '\'' + '}';
    }
  }

  private static void deleteConnection(FConnection connection,
      boolean stopProxy, boolean staleConnection) {
    synchronized (WASP_INSTANCES) {
      for (Entry<FConnectionKey, FConnectionImplementation> connectionEntry : WASP_INSTANCES
          .entrySet()) {
        if (connectionEntry.getValue() == connection) {
          deleteConnection(connectionEntry.getKey(), stopProxy, staleConnection);
          break;
        }
      }
    }
  }

  private static void deleteConnection(FConnectionKey connectionKey,
      boolean stopProxy, boolean staleConnection) {
    synchronized (WASP_INSTANCES) {
      FConnectionImplementation connection = WASP_INSTANCES.get(connectionKey);
      if (connection != null) {
        connection.decCount();
        if (connection.isZeroReference() || staleConnection) {
          WASP_INSTANCES.remove(connectionKey);
          connection.close(stopProxy);
        } else if (stopProxy) {
          connection.stopProxyOnClose(stopProxy);
        }
      } else {
        LOG.error("Connection not found in the list, can't delete it "
            + "(connection key=" + connectionKey
            + "). May be the key was modified?");
      }
    }
  }

  /**
   * Delete connection information for the instance specified by configuration.
   * If there are no more references to it, this will then close connection to
   * the zookeeper ensemble and let go of all resources.
   *
   * @param conf
   *          configuration whose identity is used to find {@link FConnection}
   *          instance.
   * @param stopProxy
   *          Shuts down all the proxy's put up to cluster members including to
   *          cluster FMaster. Calls
   *          {@link com.alibaba.wasp.ipc.WaspRPC#stopProxy(com.alibaba.wasp.ipc.VersionedProtocol)} .
   */
  public static void deleteConnection(Configuration conf, boolean stopProxy) {
    deleteConnection(new FConnectionKey(conf), stopProxy, false);
  }

  /**
   * Delete information for all connections.
   *
   * @param stopProxy
   *          stop the proxy as well
   * @throws java.io.IOException
   */
  public static void deleteAllConnections(boolean stopProxy) {
    synchronized (WASP_INSTANCES) {
      Set<FConnectionKey> connectionKeys = new HashSet<FConnectionKey>();
      connectionKeys.addAll(WASP_INSTANCES.keySet());
      for (FConnectionKey connectionKey : connectionKeys) {
        deleteConnection(connectionKey, stopProxy, false);
      }
      WASP_INSTANCES.clear();
    }
  }

  /**
   * Set the number of retries to use serverside when trying to communicate with
   * another server over {@link com.alibaba.wasp.client.FConnection}. Used
   * updating catalog tables, etc. Call this method before we create any
   * Connections.
   * 
   * @param c
   *          The Configuration instance to set the retries into.
   * @param log
   *          Used to log what we set in here.
   */
  public static void setServerSideFConnectionRetries(final Configuration c,
      final Log log) {
    int fcRetries = c.getInt(FConstants.WASP_CLIENT_RETRIES_NUMBER,
        FConstants.DEFAULT_WASP_CLIENT_RETRIES_NUMBER);
    // Go big. Multiply by 10. If we can't get to meta after this many retries
    // then something seriously wrong.
    int serversideMultiplier = c.getInt(
        "wasp.client.serverside.retries.multiplier", 10);
    int retries = fcRetries * serversideMultiplier;
    c.setInt(FConstants.WASP_CLIENT_RETRIES_NUMBER, retries);
    log.debug("Set serverside FConnection retries=" + retries);
  }
}

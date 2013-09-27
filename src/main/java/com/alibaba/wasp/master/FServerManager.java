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
package com.alibaba.wasp.master;

import com.alibaba.wasp.ClockOutOfSyncException;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.PleaseHoldException;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.ServerLoad;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.YouAreDeadException;
import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.client.FConnection;
import com.alibaba.wasp.client.FConnectionManager;
import com.alibaba.wasp.fserver.AdminProtocol;
import com.alibaba.wasp.fserver.EntityGroupOpeningState;
import com.alibaba.wasp.master.handler.ServerShutdownHandler;
import com.alibaba.wasp.monitoring.MonitoredTask;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.RequestConverter;
import com.alibaba.wasp.protobuf.ResponseConverter;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupResponse;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The FServerManager class manages info about fservers.
 * <p>
 * Maintains lists of online and dead servers. Processes the startups,
 * shutdowns, and deaths of fservers.
 * <p>
 * Servers are distinguished in two different ways. A given server has a
 * location, specified by hostname and port, and of which there can only be one
 * online at any given time. A server instance is specified by the location
 * (hostname and port) as well as the startcode (timestamp from when the server
 * was started). This is used to differentiate a restarted instance of a given
 * server from the original instance.
 */
public class FServerManager {
  private static final Log LOG = LogFactory.getLog(FServerManager.class);

  // Set if we are to shutdown the cluster.
  private volatile boolean clusterShutdown = false;

  /** Map of registered servers to their current load */
  private final Map<ServerName, ServerLoad> onlineServers = new ConcurrentHashMap<ServerName, ServerLoad>();

  // TODO: This is strange to have two maps but HSI above is used on both sides
  /**
   * Map from full server-instance name to the RPC connection for this server.
   */
  private final Map<ServerName, AdminProtocol> serverConnections = new HashMap<ServerName, AdminProtocol>();

  /**
   * List of fservers <ServerName> that should not get any more new
   * entityGroups.
   */
  private final ArrayList<ServerName> drainingServers = new ArrayList<ServerName>();

  private final Server master;
  private final FMasterServices services;
  private final FConnection connection;

  private final DeadServer deadservers;

  private final long maxSkew;
  private final long warningSkew;

  /**
   * Set of fservers which are dead but not processed immediately. If one server
   * died before master enables ServerShutdownHandler, the server will be added
   * to this set and will be processed through calling
   * {@link org.apache.hadoop.hbase.master.ServerManager#processQueuedDeadServers()} by master.
   * <p>
   * A dead server is a server instance known to be dead, not listed in the
   * /wasp/rs znode any more. It may have not been submitted to
   * ServerShutdownHandler yet because the handler is not enabled.
   * <p>
   * A dead server, which has been submitted to ServerShutdownHandler while the
   * handler is not enabled, is queued up.
   * <p>
   * So this is a set of fservers known to be dead but not submitted to
   * ServerShutdownHander for processing yet.
   */
  private Set<ServerName> queuedDeadServers = new HashSet<ServerName>();

  /**
   * Set of fservers which are dead and submitted to ServerShutdownHandler to
   * process but not fully processed immediately.
   * <p>
   * If one server died before assignment manager finished the failover cleanup,
   * the server will be added to this set and will be processed through calling
   * {@link org.apache.hadoop.hbase.master.ServerManager#processQueuedDeadServers()} by assignment manager.
   * <p>
   * For all the fservers in this set, HLog split is already completed.
   * <p>
   * ServerShutdownHandler processes a dead server submitted to the handler
   * after the handler is enabled. It may not be able to complete the processing
   * because root/meta is not yet online or master is currently in startup mode.
   * In this case, the dead server will be parked in this set temporarily.
   */
  private Set<ServerName> requeuedDeadServers = new HashSet<ServerName>();

  /**
   * Constructor.
   *
   * @param master
   * @param services
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   */
  public FServerManager(final Server master, final FMasterServices services)
      throws ZooKeeperConnectionException {
    this(master, services, true);
  }

  FServerManager(final Server master, final FMasterServices services,
      final boolean connect) throws ZooKeeperConnectionException {
    this.master = master;
    this.services = services;
    Configuration c = master.getConfiguration();
    maxSkew = c.getLong("wasp.master.maxclockskew", 30000);
    warningSkew = c.getLong("wasp.master.warningclockskew", 10000);
    this.deadservers = new DeadServer();
    this.connection = connect ? FConnectionManager.getConnection(c) : null;
  }

  /**
   * Let the server manager know a new fserver has come online
   *
   * @param ia
   *          The remote address
   * @param port
   *          The remote port
   * @param serverStartcode
   * @param serverCurrentTime
   *          The current time of the fserver in ms
   * @return The ServerName we know this server as.
   * @throws java.io.IOException
   */
  ServerName fserverStartup(final InetAddress ia, final int port,
      final long serverStartcode, long serverCurrentTime) throws IOException {
    // Test for case where we get a entityGroup startup message from a fserver
    // that has been quickly restarted but whose znode expiration handler has
    // not yet run, or from a server whose fail we are currently processing.
    // Test its host+port combo is present in serverAddresstoServerInfo. If it
    // is, reject the server and trigger its expiration. The next time it comes
    // in, it should have been removed from serverAddressToServerInfo and queued
    // for processing by ProcessServerShutdown.
    ServerName sn = new ServerName(ia.getHostName(), port, serverStartcode);
    checkClockSkew(sn, serverCurrentTime);
    checkIsDead(sn, "STARTUP");
    checkAlreadySameHostPort(sn);
    recordNewServer(sn, ServerLoad.EMPTY_SERVERLOAD);
    return sn;
  }

  void fserverReport(ServerName sn, ServerLoad sl) throws YouAreDeadException,
      PleaseHoldException {
    checkIsDead(sn, "REPORT");
    if (!this.onlineServers.containsKey(sn)) {
      // Already have this host+port combo and its just different start code?
      checkAlreadySameHostPort(sn);
      // Just let the server in. Presume master joining a running cluster.
      // recordNewServer is what happens at the end of reportServerStartup.
      // The only thing we are skipping is passing back to the fserver
      // the ServerName to use. Here we presume a master has already done
      // that so we'll press on with whatever it gave us for ServerName.
      recordNewServer(sn, sl);
    } else {
      this.onlineServers.put(sn, sl);
    }
  }

  /**
   * Test to see if we have a server of same host and port already.
   *
   * @param serverName
   * @throws com.alibaba.wasp.PleaseHoldException
   */
  void checkAlreadySameHostPort(final ServerName serverName)
      throws PleaseHoldException {
    ServerName existingServer = ServerName.findServerWithSameHostnamePort(
        getOnlineServersList(), serverName);
    if (existingServer != null) {
      String message = "Server serverName=" + serverName
          + " rejected; we already have " + existingServer.toString()
          + " registered with same hostname and port";
      LOG.info(message);
      if (existingServer.getStartcode() < serverName.getStartcode()) {
        LOG.info("Triggering server recovery; existingServer " + existingServer
            + " looks stale, new server:" + serverName);
        expireServer(existingServer);
      }
      // master has completed the initialization
      throw new PleaseHoldException(message);
    }
  }

  /**
   * Checks if the clock skew between the server and the master. If the clock
   * skew exceeds the configured max, it will throw an exception; if it exceeds
   * the configured warning threshold, it will log a warning but start normally.
   *
   * @param serverName
   *          Incoming servers's name
   * @param serverCurrentTime
   * @throws com.alibaba.wasp.ClockOutOfSyncException
   *           if the skew exceeds the configured max value
   */
  private void checkClockSkew(final ServerName serverName,
      final long serverCurrentTime) throws ClockOutOfSyncException {
    long skew = System.currentTimeMillis() - serverCurrentTime;
    if (skew > maxSkew) {
      String message = "Server " + serverName + " has been "
          + "rejected; Reported time is too far out of sync with master.  "
          + "Time difference of " + skew + "ms > max allowed of " + maxSkew
          + "ms";
      LOG.warn(message);
      throw new ClockOutOfSyncException(message);
    } else if (skew > warningSkew) {
      String message = "Reported time for server " + serverName
          + " is out of sync with master " + "by " + skew
          + "ms. (Warning threshold is " + warningSkew + "ms; "
          + "error threshold is " + maxSkew + "ms)";
      LOG.warn(message);
    }
  }

  /**
   * If this server is on the dead list, reject it with a YouAreDeadException.
   * If it was dead but came back with a new start code, remove the old entry
   * from the dead list.
   *
   * @param serverName
   * @param what
   *          START or REPORT
   * @throws com.alibaba.wasp.YouAreDeadException
   */
  private void checkIsDead(final ServerName serverName, final String what)
      throws YouAreDeadException {
    if (this.deadservers.isDeadServer(serverName)) {
      // host name, port and start code all match with existing one of the
      // dead servers. So, this server must be dead.
      String message = "Server " + what + " rejected; currently processing "
          + serverName + " as dead server";
      LOG.debug(message);
      throw new YouAreDeadException(message);
    }
    // remove dead server with same hostname and port of newly checking in rs
    // after master
    // initialization.
    if ((this.services == null || ((FMaster) this.services).isInitialized())
        && this.deadservers.cleanPreviousInstance(serverName)) {
      // This server has now become alive after we marked it as dead.
      // We removed it's previous entry from the dead list to reflect it.
      LOG.debug(what + ":" + " Server " + serverName + " came back up,"
          + " removed it from the dead servers list");
    }
  }

  /**
   * Adds the onlineServers list.
   *
   * @param hsl
   * @param serverName
   *          The remote servers name.
   */
  void recordNewServer(final ServerName serverName, final ServerLoad sl) {
    LOG.info("Registering server=" + serverName);
    this.onlineServers.put(serverName, sl);
    this.serverConnections.remove(serverName);
  }

  /**
   * @param serverName
   * @return ServerLoad if serverName is known else null
   */
  public ServerLoad getLoad(final ServerName serverName) {
    return this.onlineServers.get(serverName);
  }

  /**
   * Compute the average load across all fservers. Currently, this uses a very
   * naive computation - just uses the number of entityGroups being served,
   * ignoring stats about number of requests.
   *
   * @return the average load
   */
  public double getAverageLoad() {
    int totalLoad = 0;
    int numServers = 0;
    double averageLoad = 0.0;
    for (ServerLoad sl : this.onlineServers.values()) {
      numServers++;
      totalLoad += sl.getNumberOfEntityGroups();
    }
    averageLoad = (double) totalLoad / (double) numServers;
    return averageLoad;
  }

  /** @return the count of active fservers */
  int countOfFServers() {
    // Presumes onlineServers is a concurrent map
    return this.onlineServers.size();
  }

  /**
   * @return Read-only map of servers to serverinfo
   */
  public Map<ServerName, ServerLoad> getOnlineServers() {
    // Presumption is that iterating the returned Map is OK.
    synchronized (this.onlineServers) {
      return Collections.unmodifiableMap(this.onlineServers);
    }
  }

  public Set<ServerName> getDeadServers() {
    return this.deadservers.clone();
  }

  /**
   * Checks if any dead servers are currently in progress.
   *
   * @return true if any FS are being processed as dead, false if not
   */
  public boolean areDeadServersInProgress() {
    return this.deadservers.areDeadServersInProgress();
  }

  void letFServersShutdown() {
    long previousLogTime = 0;
    while (!onlineServers.isEmpty()) {

      if (System.currentTimeMillis() > (previousLogTime + 1000)) {
        StringBuilder sb = new StringBuilder();
        for (ServerName key : this.onlineServers.keySet()) {
          if (sb.length() > 0) {
            sb.append(", ");
          }
          sb.append(key);
        }
        LOG.info("Waiting on fserver(s) to go down " + sb.toString());
        previousLogTime = System.currentTimeMillis();
      }

      synchronized (onlineServers) {
        try {
          onlineServers.wait(100);
        } catch (InterruptedException ignored) {
          // continue
        }
      }
    }
  }

  /*
   * Expire the passed server. Add it to list of dead servers and queue a
   * shutdown processing.
   */
  public synchronized void expireServer(final ServerName serverName) {
    if (!this.onlineServers.containsKey(serverName)) {
      LOG.warn("Received expiration of " + serverName
          + " but server is not currently online");
    }
    if (this.deadservers.contains(serverName)) {
      // TODO: Can this happen? It shouldn't be online in this case?
      LOG.warn("Received expiration of " + serverName
          + " but server shutdown is already in progress");
      return;
    }
    // Remove the server from the known servers lists and update load info BUT
    // add to deadservers first; do this so it'll show in dead servers list if
    // not in online servers list.
    this.deadservers.add(serverName);
    this.onlineServers.remove(serverName);
    synchronized (onlineServers) {
      onlineServers.notifyAll();
    }
    this.serverConnections.remove(serverName);
    // If cluster is going down, yes, servers are going to be expiring; don't
    // process as a dead server
    if (this.clusterShutdown) {
      LOG.info("Cluster shutdown set; " + serverName
          + " expired; onlineServers=" + this.onlineServers.size());
      if (this.onlineServers.isEmpty()) {
        master.stop("Cluster shutdown set; onlineServer=0");
      }
      return;
    }
    this.services.getExecutorService().submit(
        new ServerShutdownHandler(this.master, this.services, this.deadservers,
            serverName));
    LOG.debug("Added=" + serverName
        + " to dead servers, submitted shutdown handler to be executed");
  }

  public synchronized void processDeadServer(final ServerName serverName) {
    // When assignment manager is cleaning up the zookeeper nodes and rebuilding
    // the
    // in-memory entityGroup states, fservers could be down. Root/meta table can
    // and
    // should be re-assigned, log splitting can be done too. However, it is
    // better to
    // wait till the cleanup is done before re-assigning user entityGroups.
    //
    // We should not wait in the server shutdown handler thread since it can
    // clog
    // the handler threads and root/meta table could not be re-assigned in case
    // the corresponding server is down. So we queue them up here instead.
    if (!services.getAssignmentManager().isFailoverCleanupDone()) {
      requeuedDeadServers.add(serverName);
      return;
    }

    this.deadservers.add(serverName);
    this.services.getExecutorService().submit(
        new ServerShutdownHandler(this.master, this.services, this.deadservers,
            serverName));
  }

  /**
   * Process the servers which died during master's initialization. It will be
   * called after HMaster#assignRootAndMeta and AssignmentManager#joinCluster.
   * */
  synchronized void processQueuedDeadServers() {
    Iterator<ServerName> serverIterator = queuedDeadServers.iterator();
    while (serverIterator.hasNext()) {
      expireServer(serverIterator.next());
      serverIterator.remove();
    }

    if (!services.getAssignmentManager().isFailoverCleanupDone()) {
      LOG.info("AssignmentManager hasn't finished failover cleanup");
    }
    serverIterator = requeuedDeadServers.iterator();
    while (serverIterator.hasNext()) {
      processDeadServer(serverIterator.next());
      serverIterator.remove();
    }
  }

  /*
   * Remove the server from the drain list.
   */
  public boolean removeServerFromDrainList(final ServerName sn) {
    // Warn if the server (sn) is not online. ServerName is of the form:
    // <hostname> , <port> , <startcode>

    if (!this.isServerOnline(sn)) {
      LOG.warn("Server " + sn + " is not currently online. "
          + "Removing from draining list anyway, as requested.");
    }
    // Remove the server from the draining servers lists.
    return this.drainingServers.remove(sn);
  }

  /*
   * Add the server to the drain list.
   */
  public boolean addServerToDrainList(final ServerName sn) {
    // Warn if the server (sn) is not online. ServerName is of the form:
    // <hostname> , <port> , <startcode>

    if (!this.isServerOnline(sn)) {
      LOG.warn("Server " + sn + " is not currently online. "
          + "Ignoring request to add it to draining list.");
      return false;
    }
    // Add the server to the draining servers lists, if it's not already in
    // it.
    if (this.drainingServers.contains(sn)) {
      LOG.warn("Server " + sn + " is already in the draining server list."
          + "Ignoring request to add it again.");
      return false;
    }
    return this.drainingServers.add(sn);
  }

  // RPC methods to fservers

  /**
   * Sends an OPEN RPC to the specified server to open the specified
   * entityGroup.
   * <p>
   * Open should not fail but can if server just crashed.
   * <p>
   *
   * @param server
   *          server to open a entityGroup
   * @param entityGroup
   *          entityGroup to open
   * @param versionOfOfflineNode
   *          that needs to be present in the offline node when FS tries to
   *          change the state from OFFLINE to other states.
   */
  public EntityGroupOpeningState sendEntityGroupOpen(final ServerName server,
      EntityGroupInfo entityGroup, int versionOfOfflineNode) throws IOException {
    AdminProtocol admin = getServerConnection(server);
    if (admin == null) {
      LOG.warn("Attempting to send OPEN RPC to server " + server.toString()
          + " failed because no RPC connection found to this server");
      return EntityGroupOpeningState.FAILED_OPENING;
    }
    OpenEntityGroupRequest request = RequestConverter
        .buildOpenEntityGroupRequest(entityGroup, versionOfOfflineNode);
    try {
      OpenEntityGroupResponse response = admin.openEntityGroup(null, request);
      return ResponseConverter.getEntityGroupOpeningState(response);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Sends an OPEN RPC to the specified server to open the specified
   * entityGroup.
   * <p>
   * Open should not fail but can if server just crashed.
   * <p>
   *
   * @param server
   *          server to open a entityGroup
   * @param entityGroupOpenInfos
   *          info of a list of entityGroups to open
   * @return a list of entityGroup opening states
   */
  public List<EntityGroupOpeningState> sendEntityGroupsOpen(ServerName server,
      List<EntityGroupInfo> entityGroupOpenInfos) throws IOException {
    AdminProtocol admin = getServerConnection(server);
    if (admin == null) {
      LOG.warn("Attempting to send OPEN RPC to server " + server.toString()
          + " failed because no RPC connection found to this server");
      return null;
    }

    OpenEntityGroupRequest request = RequestConverter
        .buildOpenEntityGroupRequest(entityGroupOpenInfos);
    try {
      OpenEntityGroupResponse response = admin.openEntityGroup(null, request);
      return ResponseConverter.getEntityGroupOpeningStateList(response);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Sends an CLOSE RPC to the specified server to close the specified
   * entityGroup.
   * <p>
   * A entityGroup server could reject the close request because it either does
   * not have the specified entityGroup or the entityGroup is being split.
   *
   * @param server
   *          server to open a entityGroup
   * @param entityGroup
   *          entityGroup to open
   * @param versionOfClosingNode
   *          the version of znode to compare when FS transitions the znode from
   *          CLOSING state.
   * @param dest
   *          - if the entityGroup is moved to another server, the destination
   *          server. null otherwise.
   * @return true if server acknowledged close, false if not
   * @throws java.io.IOException
   */
  public boolean sendEntityGroupClose(ServerName server,
      EntityGroupInfo entityGroup, int versionOfClosingNode, ServerName dest,
      boolean transitionInZK) throws IOException {
    if (server == null) {
      throw new NullPointerException("Passed server is null");
    }
    AdminProtocol admin = getServerConnection(server);
    if (admin == null) {
      throw new IOException("Attempting to send CLOSE RPC to server "
          + server.toString() + " for entityGroup "
          + entityGroup.getEntityGroupNameAsString()
          + " failed because no RPC connection found to this server");
    }
    return ProtobufUtil.closeEntityGroup(admin, entityGroup,
        versionOfClosingNode, transitionInZK);
  }

  public boolean sendEntityGroupClose(ServerName server,
      EntityGroupInfo entityGroup, int versionOfClosingNode) throws IOException {
    return sendEntityGroupClose(server, entityGroup, versionOfClosingNode,
        null, true);
  }

  /**
   * Sends an ENABLE TABLE RPC to the specified server to close the specified
   * entityGroup.
   *
   * @param server
   *          server to disable a table.
   * @return true if server acknowledged close, false if not
   * @throws java.io.IOException
   */
  public boolean sendEnableTable(ServerName server, String tableName)
      throws IOException {
    if (server == null) {
      throw new NullPointerException("Passed server is null");
    }
    AdminProtocol admin = getServerConnection(server);
    if (admin == null) {
      throw new IOException("Attempting to send enableTable RPC to server "
          + server.toString()
          + " failed because no RPC connection found to this server");
    }

    com.alibaba.wasp.protobuf.generated.FServerAdminProtos.EnableTableRequest request = RequestConverter
        .buildServerEnableTableRequest(tableName);
    try {
      return admin.enableServerTable(null, request).getSuccess();
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  /**
   * Sends an DISABLE TABLE RPC to the specified server to close the specified
   * entityGroup.
   *
   * @param server
   *          server to disable a table.
   * @return true if server acknowledged close, false if not
   * @throws java.io.IOException
   */
  public boolean sendDisableTable(ServerName server, String tableName)
      throws IOException {
    if (server == null) {
      throw new NullPointerException("Passed server is null");
    }
    AdminProtocol admin = getServerConnection(server);
    if (admin == null) {
      throw new IOException("Attempting to send disableTable RPC to server "
          + server.toString()
          + " failed because no RPC connection found to this server");
    }

    com.alibaba.wasp.protobuf.generated.FServerAdminProtos.DisableTableRequest request = RequestConverter
        .buildServerDisableTableRequest(tableName);
    try {
      return admin.disableServerTable(null, request).getSuccess();
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  /**
   * @param sn
   * @return
   * @throws java.io.IOException
   * @throws com.alibaba.wasp.client.RetriesExhaustedException
   *           wrapping a ConnectException if failed putting up proxy.
   */
  private AdminProtocol getServerConnection(final ServerName sn)
      throws IOException {
    AdminProtocol admin = this.serverConnections.get(sn);
    if (admin == null) {
      LOG.debug("New connection to " + sn.toString());
      admin = this.connection.getAdmin(sn.getHostname(), sn.getPort());
      this.serverConnections.put(sn, admin);
    }
    return admin;
  }

  /**
   * Wait for the fservers to report in. We will wait until one of this
   * condition is met: - the master is stopped - the
   * 'wasp.master.wait.on.fservers.timeout' is reached - the
   * 'wasp.master.wait.on.fservers.maxtostart' number of fservers is reached -
   * the 'wasp.master.wait.on.fservers.mintostart' is reached AND there have
   * been no new fserver in for 'wasp.master.wait.on.fservers.interval' time
   * 
   * @throws InterruptedException
   */
  public void waitForFServers(MonitoredTask status) throws InterruptedException {
    final long interval = this.master.getConfiguration().getLong(
        "wasp.master.wait.on.fservers.interval", 1500);
    final long timeout = this.master.getConfiguration().getLong(
        "wasp.master.wait.on.fservers.timeout", 4500);
    final int minToStart = this.master.getConfiguration().getInt(
        "wasp.master.wait.on.fservers.mintostart", 1);
    final int maxToStart = this.master.getConfiguration().getInt(
        "wasp.master.wait.on.fservers.maxtostart", Integer.MAX_VALUE);

    long now = System.currentTimeMillis();
    final long startTime = now;
    long slept = 0;
    long lastLogTime = 0;
    long lastCountChange = startTime;
    int count = countOfFServers();
    int oldCount = 0;
    while (!this.master.isStopped() && slept < timeout && count < maxToStart
        && (lastCountChange + interval > now || count < minToStart)) {

      // Log some info at every interval time or if there is a change
      if (oldCount != count || lastLogTime + interval < now) {
        lastLogTime = now;
        String msg = "Waiting for fservers count to settle; currently"
            + " checked in " + count + ", slept for " + slept + " ms,"
            + " expecting minimum of " + minToStart + ", maximum of "
            + maxToStart + ", timeout of " + timeout + " ms, interval of "
            + interval + " ms.";
        LOG.info(msg);
        status.setStatus(msg);
      }

      // We sleep for some time
      final long sleepTime = 50;
      Thread.sleep(sleepTime);
      now = System.currentTimeMillis();
      slept = now - startTime;

      oldCount = count;
      count = countOfFServers();
      if (count != oldCount) {
        lastCountChange = now;
      }
    }

    LOG.info("Finished waiting for fservers count to settle;" + " checked in "
        + count + ", slept for " + slept + " ms," + " expecting minimum of "
        + minToStart + ", maximum of " + maxToStart + "," + " master is "
        + (this.master.isStopped() ? "stopped." : "running."));
  }

  /**
   * @return A copy of the internal list of online servers.
   */
  public List<ServerName> getOnlineServersList() {
    // TODO: optimize the load balancer call so we don't need to make a new list
    // TODO: FIX. THIS IS POPULAR CALL.
    return new ArrayList<ServerName>(this.onlineServers.keySet());
  }

  /**
   * @return A copy of the internal list of draining servers.
   */
  public List<ServerName> getDrainingServersList() {
    return new ArrayList<ServerName>(this.drainingServers);
  }

  /**
   * @return A copy of the internal set of deadNotExpired servers.
   */
  Set<ServerName> getDeadNotExpiredServers() {
    return new HashSet<ServerName>(this.queuedDeadServers);
  }

  public boolean isServerOnline(ServerName serverName) {
    return serverName != null && onlineServers.containsKey(serverName);
  }

  /**
   * Check if a server is known to be dead. A server can be online, or known to
   * be dead, or unknown to this manager (i.e, not online, not known to be dead
   * either. it is simply not tracked by the master any more, for example, a
   * very old previous instance).
   */
  public synchronized boolean isServerDead(ServerName serverName) {
    return serverName == null || deadservers.isDeadServer(serverName)
        || queuedDeadServers.contains(serverName)
        || requeuedDeadServers.contains(serverName);
  }

  public void shutdownCluster() {
    this.clusterShutdown = true;
    this.master.stop("Cluster shutdown requested");
  }

  public boolean isClusterShutdown() {
    return this.clusterShutdown;
  }

  /**
   * Stop the ServerManager. Currently closes the connection to the master.
   */
  public void stop() {
    if (connection != null) {
      try {
        connection.close();
      } catch (IOException e) {
        LOG.error("Attempt to close connection to master failed", e);
      }
    }
  }

  /**
   * Creates a list of possible destinations for a entityGroup. It contains the
   * online servers, but not the draining or dying servers.
   * 
   * @param serverToExclude
   *          can be null if there is no server to exclude
   */
  public List<ServerName> createDestinationServersList(
      final ServerName serverToExclude) {
    final List<ServerName> destServers = getOnlineServersList();

    if (serverToExclude != null) {
      destServers.remove(serverToExclude);
    }

    // Loop through the draining server list and remove them from the server
    // list
    final List<ServerName> drainingServersCopy = getDrainingServersList();
    if (!drainingServersCopy.isEmpty()) {
      for (final ServerName server : drainingServersCopy) {
        destServers.remove(server);
      }
    }

    // Remove the deadNotExpired servers from the server list.
    removeDeadNotExpiredServers(destServers);

    return destServers;
  }

  /**
   * Calls {@link #createDestinationServersList} without server to exclude.
   */
  public List<ServerName> createDestinationServersList() {
    return createDestinationServersList(null);
  }

  /**
   * Loop through the deadNotExpired server list and remove them from the
   * servers. This function should be used carefully outside of this class. You
   * should use a high level method such as
   * {@link #createDestinationServersList()} instead of managing you own list.
   */
  void removeDeadNotExpiredServers(List<ServerName> servers) {
    Set<ServerName> deadNotExpiredServersCopy = this.getDeadNotExpiredServers();
    if (!deadNotExpiredServersCopy.isEmpty()) {
      for (ServerName server : deadNotExpiredServersCopy) {
        LOG.debug("Removing dead but not expired server: " + server
            + " from eligible server pool.");
        servers.remove(server);
      }
    }
  }

  /**
   * To clear any dead server with same host name and port of any online server
   */
  void clearDeadServersWithSameHostNameAndPortOfOnlineServer() {
    ServerName sn = null;
    for (ServerName serverName : getOnlineServersList()) {
      while ((sn = ServerName.findServerWithSameHostnamePort(this.deadservers,
          serverName)) != null) {
        this.deadservers.remove(sn);
      }
    }
  }

}

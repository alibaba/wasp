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
package com.alibaba.wasp.fserver;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.ObjectName;

import com.alibaba.wasp.DaemonThreadFactory;import com.alibaba.wasp.RemoteExceptionHandler;import com.alibaba.wasp.ServerName;import com.alibaba.wasp.UnknownScannerException;import com.alibaba.wasp.YouAreDeadException;import com.alibaba.wasp.ZNodeClearer;import com.alibaba.wasp.client.FConnectionManager;import com.alibaba.wasp.conf.WaspConfiguration;import com.alibaba.wasp.executor.ExecutorService;import com.alibaba.wasp.fserver.handler.CloseEntityGroupHandler;import com.alibaba.wasp.fserver.metrics.MetricsFServer;import com.alibaba.wasp.fserver.metrics.MetricsFServerWrapper;import com.alibaba.wasp.ipc.RpcServer;import com.alibaba.wasp.ipc.ServerNotRunningYetException;import com.alibaba.wasp.ipc.WaspRPC;import com.alibaba.wasp.ipc.WaspRPCErrorHandler;import com.alibaba.wasp.master.FServerStatusProtocol;import com.alibaba.wasp.meta.FMetaEditor;import com.alibaba.wasp.meta.FMetaScanner;import com.alibaba.wasp.meta.FTable;import com.alibaba.wasp.plan.BaseDriver;
import com.alibaba.wasp.plan.action.ColumnStruct;
import com.alibaba.wasp.plan.action.DeleteAction;import com.alibaba.wasp.plan.action.GetAction;import com.alibaba.wasp.plan.action.ScanAction;import com.alibaba.wasp.plan.action.UpdateAction;import com.alibaba.wasp.protobuf.ResponseConverter;import com.alibaba.wasp.protobuf.generated.ClientProtos;import com.alibaba.wasp.protobuf.generated.FServerAdminProtos;import com.alibaba.wasp.protobuf.generated.MetaProtos;import com.alibaba.wasp.protobuf.generated.WaspProtos;import com.alibaba.wasp.storage.StorageActionManager;import com.alibaba.wasp.util.InfoServer;import com.alibaba.wasp.zookeeper.ClusterStatusTracker;import com.alibaba.wasp.zookeeper.MasterAddressTracker;import com.alibaba.wasp.zookeeper.ZKClusterId;import com.alibaba.wasp.zookeeper.ZooKeeperNodeTracker;import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;
import com.alibaba.wasp.ClockOutOfSyncException;
import com.alibaba.wasp.DaemonThreadFactory;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupLoad;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.NotServingEntityGroupException;
import com.alibaba.wasp.RemoteExceptionHandler;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.UnknownScannerException;
import com.alibaba.wasp.YouAreDeadException;
import com.alibaba.wasp.ZNodeClearer;
import com.alibaba.wasp.client.ClientProtocol;
import com.alibaba.wasp.client.FConnection;
import com.alibaba.wasp.client.FConnectionManager;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.executor.ExecutorService;
import com.alibaba.wasp.executor.ExecutorService.ExecutorType;
import com.alibaba.wasp.fserver.handler.CloseEntityGroupHandler;
import com.alibaba.wasp.fserver.handler.OpenEntityGroupHandler;
import com.alibaba.wasp.fserver.metrics.MetricsFServer;
import com.alibaba.wasp.fserver.metrics.MetricsFServerWrapper;
import com.alibaba.wasp.ipc.NettyServer;
import com.alibaba.wasp.ipc.RpcServer;
import com.alibaba.wasp.ipc.ServerNotRunningYetException;
import com.alibaba.wasp.ipc.WaspRPC;
import com.alibaba.wasp.ipc.WaspRPCErrorHandler;
import com.alibaba.wasp.master.FServerStatusProtocol;
import com.alibaba.wasp.meta.FMetaEditor;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FMetaScanner;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.BaseDriver;
import com.alibaba.wasp.plan.action.DeleteAction;
import com.alibaba.wasp.plan.action.GetAction;
import com.alibaba.wasp.plan.action.InsertAction;
import com.alibaba.wasp.plan.action.ScanAction;
import com.alibaba.wasp.plan.action.UpdateAction;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.RequestConverter;
import com.alibaba.wasp.protobuf.ResponseConverter;
import com.alibaba.wasp.protobuf.generated.ClientProtos.DeleteRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.DeleteResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.GetRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.GetResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.InsertRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.InsertResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.QueryResultProto;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ScanRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ScanResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.UpdateRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.UpdateResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEncodedEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEncodedEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.DisableTableRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.DisableTableResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.EnableTableRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.EnableTableResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetServerInfoRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetServerInfoResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupsRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupsResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.SplitEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.SplitEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.StopServerRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.StopServerResponse;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerStartupRequest;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerStartupResponse;
import com.alibaba.wasp.protobuf.generated.MetaProtos.ReadModelProto;
import com.alibaba.wasp.protobuf.generated.WaspProtos;
import com.alibaba.wasp.protobuf.generated.WaspProtos.EntityGroupInfoProtos;
import com.alibaba.wasp.protobuf.generated.WaspProtos.EntityGroupLoadProtos;
import com.alibaba.wasp.protobuf.generated.WaspProtos.EntityGroupOpeningState;
import com.alibaba.wasp.protobuf.generated.WaspProtos.StringStringPair;
import com.alibaba.wasp.storage.StorageActionManager;
import com.alibaba.wasp.util.InfoServer;
import com.alibaba.wasp.zookeeper.ClusterStatusTracker;
import com.alibaba.wasp.zookeeper.MasterAddressTracker;
import com.alibaba.wasp.zookeeper.ZKClusterId;
import com.alibaba.wasp.zookeeper.ZKUtil;
import com.alibaba.wasp.zookeeper.ZooKeeperNodeTracker;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * FServer makes a set of EntityGroups available to clients. It checks in with
 * the FMaster. There are many EntityGroups in a single wasp deployment.
 */
public class FServer implements ClientProtocol, AdminProtocol, Runnable,
    FServerServices {
  public static final Log LOG = LogFactory.getLog(FServer.class);

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running action tests
  // of FServer in isolation.
  protected volatile boolean stopped = false;

  // A state before we go into stopped state. At this stage we're closing user
  // space entityGroups.
  private boolean stopping = false;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and action tests.
  protected volatile boolean abortRequested;

  // A sleeper that sleeps for msgInterval.
  private final Sleeper sleeper;

  private MetricsFServer metricsFServer;

  // Server to handle client requests. Default access so can be accessed by
  // action tests.
  RpcServer rpcServer;

  // Instance of the wasp executor service.
  private ExecutorService service;

  private java.util.concurrent.ExecutorService pool;

  private BaseDriver driver;

  // Cluster Status Tracker
  private ClusterStatusTracker clusterStatusTracker;

  private TableSchemaCacheReader tableSchemaReader;

  // Info server. Default access so can be used by unit tests. FSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  InfoServer infoServer;

  public static final String FSERVER = "fserver";

  /** fserver configuration name */
  public static final String FSERVER_CONF = "fserver_conf";

  private FServerStatusProtocol waspMaster;

  // Port we put up the webui on.
  protected int webuiport = -1;

  /**
   * Space is reserved in FS constructor and then released when aborting to
   * recover from an OOME.
   */
  private final LinkedList<byte[]> reservedSpace = new LinkedList<byte[]>();

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  protected final Configuration conf;

  private final int rpcTimeout;

  private final int msgInterval;

  /**
   * The lease timeout period for client scanners (milliseconds).
   */
  private final int scannerLeaseTimeoutPeriod;

  private final Random rand = new Random();

  /**
   * MX Bean for FServerInfo
   */
  private ObjectName mxBean = null;

  /**
   * This servers start code.
   */
  private final long startcode;

  // flag set after we're done setting up server threads
  protected volatile boolean isOnline;

  // zookeeper connection and watcher
  private ZooKeeperWatcher zooKeeper;

  /**
   * The server name the Master sees us as. Its made from the hostname the
   * master passes us, port, and server startcode. Gets set after registration
   * against Master. The hostname can differ from the hostname in {@link #isa}
   * but usually doesn't if both servers resolve .
   */
  private ServerName serverNameFromMasterPOV;

  private final InetSocketAddress isa;

  protected final int numEntityGroupsToReport;

  private final StorageActionManager actionManager;

  /**
   * EntityGroupName vs current action in progress true - if open entityGroup
   * action in progress false - if close entityGroup action in progress
   */
  private final ConcurrentSkipListMap<byte[], Boolean> entityGroupsInTransitionInFS = new ConcurrentSkipListMap<byte[], Boolean>(
      Bytes.BYTES_COMPARATOR);

  // master address manager and watcher
  private MasterAddressTracker masterAddressManager;

  protected final ConcurrentHashMap<String, EntityGroupScanner> scanners = new ConcurrentHashMap<String, EntityGroupScanner>();

  /*
   * Strings to be used in forming the exception message for
   * EntityGroupsAlreadyInTransitionException.
   */
  private static final String OPEN = "OPEN";
  private static final String CLOSE = "CLOSE";

  private AtomicInteger requestCount = new AtomicInteger();

  // Split
  public SplitThread splitThread;

  private Leases leases;

  /**
   * Map of entityGroups currently being served by this FServer. Key is the
   * encoded entityGroup name. All access should be synchronized.
   */
  protected final Map<String, EntityGroup> onlineEntityGroups = new ConcurrentHashMap<String, EntityGroup>();

  private GlobalEntityGroup globalEntityGroup;

  private volatile boolean killed = false;

  /**
   * Starts a FServer at the default location
   * 
   * @param conf
   * @throws IOException
   * @throws InterruptedException
   */
  public FServer(Configuration conf) throws IOException, InterruptedException {
    this.conf = conf;
    this.isOnline = false;
    // Set how many times to retry talking to another server over FConnection.
    FConnectionManager.setServerSideFConnectionRetries(this.conf, LOG);

    // Config'ed params
    this.msgInterval = conf.getInt("wasp.fserver.msginterval", 3 * 1000);

    this.sleeper = new Sleeper(this.msgInterval, this);

    this.numEntityGroupsToReport = conf.getInt(
        "wasp.fserver.numentitygroupstoreport", 10);

    this.rpcTimeout = conf.getInt(FConstants.WASP_RPC_TIMEOUT_KEY,
        FConstants.DEFAULT_WASP_RPC_TIMEOUT);

    this.abortRequested = false;
    this.stopped = false;
    this.actionManager = new StorageActionManager(conf);

    // Server to handle client requests.
    String hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
        conf.get("wasp.fserver.dns.interface", "default"),
        conf.get("wasp.fserver.dns.nameserver", "default")));
    int port = conf.getInt(FConstants.FSERVER_PORT,
        FConstants.DEFAULT_FSERVER_PORT);
    // Creation of a HSA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }

    this.rpcServer = WaspRPC.getServer(FServer.class, this, new Class<?>[] {
        ClientProtocol.class, AdminProtocol.class, WaspRPCErrorHandler.class,
        OnlineEntityGroups.class }, initialIsa.getHostName(), // BindAddress is
                                                              // IP we got for
                                                              // this server.
        initialIsa.getPort(), conf);
    // Set our address.
    this.isa = this.rpcServer.getListenerAddress();

    this.leases = new Leases(conf.getInt(FConstants.THREAD_WAKE_FREQUENCY,
        10 * 1000));

    this.startcode = System.currentTimeMillis();

    int maxThreads = conf.getInt("wasp.transaction.threads.max", 150);

    this.pool = new ThreadPoolExecutor(1, maxThreads, 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), new DaemonThreadFactory(
            "thread factory"));
    ((ThreadPoolExecutor) this.pool).allowCoreThreadTimeOut(true);

    this.scannerLeaseTimeoutPeriod = conf.getInt(
        FConstants.WASP_CLIENT_SCANNER_TIMEOUT_PERIOD,
        FConstants.DEFAULT_WASP_CLIENT_SCANNER_TIMEOUT_PERIOD);

    this.driver = new BaseDriver(this);
    this.splitThread = new SplitThread(this);
    this.globalEntityGroup = new GlobalEntityGroup(this);
  }

  String getClusterId() {
    return this.conf.get(FConstants.CLUSTER_ID);
  }

  /**
   * Bring up connection to zk ensemble and then wait until a master for this
   * cluster and then after that, wait until cluster 'up' flag has been set.
   * This is the order in which master does things. Finally put up a catalog
   * tracker.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  private void initializeZooKeeper() throws IOException, InterruptedException {
    // Open connection to zookeeper and set primary watcher
    this.zooKeeper = new ZooKeeperWatcher(conf, FSERVER + ":"
        + this.isa.getPort(), this);

    // Create the master address manager, register with zk, and start it. Then
    // block until a master is available. No point in starting up if no master
    // running.
    this.masterAddressManager = new MasterAddressTracker(this.zooKeeper, this);
    this.masterAddressManager.start();
    blockAndCheckIfStopped(this.masterAddressManager);

    // Wait on cluster being up. Master will set this flag up in zookeeper
    // when ready.
    this.clusterStatusTracker = new ClusterStatusTracker(this.zooKeeper, this);
    this.clusterStatusTracker.start();
    blockAndCheckIfStopped(this.clusterStatusTracker);

    // Retrieve clusterId
    // Since cluster status is now up
    // ID should have already been set by HMaster
    try {
      String clusterId = ZKClusterId.readClusterIdZNode(this.zooKeeper)
          .toString();
      if (clusterId == null) {
        this.abort("Cluster ID has not been set");
      }
      this.conf.set(FConstants.CLUSTER_ID, clusterId);
      LOG.info("ClusterId : " + clusterId);
    } catch (KeeperException e) {
      this.abort("Failed to retrieve Cluster ID", e);
    }
  }

  private void startServiceThreads() throws IOException {
    String n = Thread.currentThread().getName();

    // Start executor services
    this.service = new ExecutorService(getServerName().toString());
    this.service.startExecutorService(ExecutorService.ExecutorType.FSERVER_OPEN_ENTITYGROUP,
        conf.getInt("wasp.fserver.executor.openentitygroup.threads", 3));
    this.service.startExecutorService(ExecutorService.ExecutorType.FSERVER_CLOSE_ENTITYGROUP,
        conf.getInt("wasp.fserver.executor.closeentitygroup.threads", 3));

    this.leases.setName(n + ".leaseChecker");
    this.leases.start();

    // Put up the webui. Webui may come up on port other than configured if
    // that port is occupied. Adjust serverInfo if this is the case.
    this.webuiport = putUpWebUI();

    this.rpcServer.start();
    this.rpcServer.openServer();
  }

  /**
   * Puts up the webui.
   * 
   * @return Returns final port -- maybe different from what we started with.
   * @throws IOException
   */
  private int putUpWebUI() throws IOException {

    int port = this.conf.getInt(FConstants.FSERVER_INFO_PORT,
        FConstants.DEFAULT_FSERVER_INFOPORT);

    // -1 is for disabling info server
    if (port < 0)
      return port;
    String addr = this.conf.get("wasp.fserver.info.bindAddress", "0.0.0.0");
    // check if auto port bind enabled
    boolean auto = this.conf.getBoolean(FConstants.FSERVER_INFO_PORT_AUTO,
        false);
    while (true) {
      try {
        this.infoServer = new InfoServer("fserver", addr, port, false,
            this.conf);
        this.infoServer.addServlet("status", "/fs-status",
            FSStatusServlet.class);
        this.infoServer.addServlet("dump", "/dump", FSDumpServlet.class);
        this.infoServer.setAttribute(FSERVER, this);
        this.infoServer.setAttribute(FSERVER_CONF, conf);
        this.infoServer.start();
        break;
      } catch (BindException e) {
        if (!auto) {
          // auto bind disabled throw BindException
          throw e;
        }
        // auto bind enabled, try to use another port
        LOG.info("Failed binding http info server to port: " + port);
        port++;
      }
    }
    return port;
  }

  @Override
  public boolean isStopping() {
    return this.stopping;
  }

  @Override
  public Map<byte[], Boolean> getEntityGroupsInTransitionInFS() {
    return this.entityGroupsInTransitionInFS;
  }

  @Override
  public void addToOnlineEntityGroups(EntityGroup eg) {
    this.onlineEntityGroups.put(eg.getEntityGroupInfo().getEncodedName(), eg);
  }

  @Override
  public boolean removeFromOnlineEntityGroups(String encodedEntityGroupName) {
    EntityGroup toReturn = null;
    toReturn = this.onlineEntityGroups.remove(encodedEntityGroupName);
    return toReturn != null;
  }

  @Override
  public EntityGroup getFromOnlineEntityGroups(String encodedEntityGroupName) {
    return this.onlineEntityGroups.get(encodedEntityGroupName);
  }

  @Override
  public List<EntityGroup> getOnlineEntityGroups(byte[] tableName)
      throws IOException {
    List<EntityGroup> tableEntityGroups = new ArrayList<EntityGroup>();
    synchronized (this.onlineEntityGroups) {
      for (EntityGroup entityGroup : this.onlineEntityGroups.values()) {
        EntityGroupInfo entityGroupInfo = entityGroup.getEntityGroupInfo();
        if (Bytes.equals(entityGroupInfo.getTableName(), tableName)) {
          tableEntityGroups.add(entityGroup);
        }
      }
    }
    return tableEntityGroups;
  }

  @Override
  public void run() {
    try {
      initializeZooKeeper();
    } catch (Exception t) {
      this.rpcServer.stop();
      abort("Initialization of RS failed.  Hence aborting RS.", t);
    }
    try {
      while (keepLooping()) {
        FServerStartupResponse w = reportForDuty();
        if (w == null) {
          LOG.warn("reportForDuty failed; sleeping and then retrying.");
          this.sleeper.sleep();
        } else {
          handleReportForDutyResponse(w);
          break;
        }
      }

      // We registered with the Master. Go into run mode.
      long lastMsg = 0;
      long oldRequestCount = -1;
      // The main run loop.
      while (!this.stopped && isHealthy()) {
        if (!isClusterUp()) {
          if (isOnlineEntityGroupsEmpty()) {
            stop("Exiting; cluster shutdown set and not carrying any entityGroups");
          } else if (!this.stopping) {
            this.stopping = true;
            LOG.info("Closing user entityGroups");
            closeUserEntityGroups(this.abortRequested);
          } else if (this.stopping) {
            boolean allUserEntityGroupsOffline = areAllUserEntityGroupsOffline();
            if (allUserEntityGroupsOffline) {
              // Set stopped if no requests since last time we went around the
              // loop.
              if (oldRequestCount == this.requestCount.get()) {
                stop("Stopped;");
                break;
              }
              oldRequestCount = this.requestCount.get();
            } else {
              // Make sure all entityGroups have been closed -- some
              // entityGroups may
              // have not got it because we were splitting at the time of
              // the call to closeUserEntityGroups.
              closeUserEntityGroups(this.abortRequested);
            }
            LOG.debug("Waiting on " + getOnlineEntityGroupsAsPrintableString());
          }
          driver.close();
        }
        long now = System.currentTimeMillis();
        if ((now - lastMsg) >= msgInterval) {
          doMetrics();
          tryFServerReport(lastMsg, now);
          lastMsg = System.currentTimeMillis();
        }
        if (!this.stopped)
          this.sleeper.sleep();
      }
    } catch (Throwable t) {
      abort("Unhandled exception: " + t.getMessage(), t);
    }
    // Run shutdown.
    if (mxBean != null) {
      MBeans.unregister(mxBean);
      mxBean = null;
    }
    this.leases.closeAfterLeasesExpire();
    this.rpcServer.stop();
    if (this.splitThread != null) {
      this.splitThread.interruptIfNecessary();
    }

    if (this.killed) {
      // Just skip out w/o closing entityGroups. Used when testing.
    } else if (abortRequested) {
      closeUserEntityGroups(abortRequested); // Don't leave any open file
                                             // handles
      LOG.info("aborting server " + this.serverNameFromMasterPOV);
    } else {
      closeUserEntityGroups(abortRequested);
      closeAllScanners();
      LOG.info("stopping server " + this.serverNameFromMasterPOV);
    }
    globalEntityGroup.close();

    if (!this.killed) {
      waitOnAllEntityGroupsToClose(abortRequested);
      LOG.info("stopping server " + this.serverNameFromMasterPOV
          + "; all entityGroups closed.");
    }

    // Make sure the proxy is down.
    if (this.waspMaster != null) {
      WaspRPC.stopProxy(this.waspMaster);
      this.waspMaster = null;
    }
    this.leases.close();
    if (infoServer != null) {
      try {
        infoServer.stop();
      } catch (Exception e) {
        LOG.warn("Failed stop infoServer", e);
      }
    }

    if (!killed) {
      join();
    }

    try {
      deleteMyEphemeralNode();
    } catch (KeeperException e) {
      LOG.warn("Failed deleting my ephemeral node", e);
    }
    // We may have failed to delete the znode at the previous step, but
    // we delete the file anyway: a second attempt to delete the znode is likely
    // to fail again.
    ZNodeClearer.deleteMyEphemeralNodeOnDisk();
    this.zooKeeper.close();
    LOG.info("stopping server " + this.serverNameFromMasterPOV
        + "; zookeeper connection closed.");

    LOG.info(Thread.currentThread().getName() + " exiting");
  }

  private boolean areAllUserEntityGroupsOffline() {
    if (getNumberOfOnlineEntityGroups() > 2) {
      return false;
    }
    boolean allUserEntityGroupsOffline = true;
    for (Map.Entry<String, EntityGroup> e : this.onlineEntityGroups.entrySet()) {
      if (e != null) {
        allUserEntityGroupsOffline = false;
        break;
      }
    }
    return allUserEntityGroupsOffline;
  }

  /**
   * Verify that server is healthy
   */
  private boolean isHealthy() {
    // Verify that all threads are alive
    if (!leases.isAlive()) {
      stop("One or more threads are no longer alive -- stop");
      return false;
    }
    return true;
  }

  void tryFServerReport(long reportStartTime, long reportEndTime)
      throws IOException {
    WaspProtos.ServerLoadProtos sl = buildServerLoad(reportStartTime,
        reportEndTime);

    this.requestCount.set(0);
    this.driver.resetMetricsCount();
    try {
      this.waspMaster.fServerReport(
          null,
          RequestConverter.buildFServerReportRequest(sl,
              ProtobufUtil.toServerName(this.serverNameFromMasterPOV)));
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof YouAreDeadException) {
        // This will be caught and handled as a fatal error in run()
        throw ioe;
      }
      // Couldn't connect to the master, get location from zk and reconnect
      // Method blocks until new master is found or we are stopped
      getMaster();
    }
  }

  private WaspProtos.ServerLoadProtos buildServerLoad(long reportStartTime,
      long reportEndTime) {
    MetricsFServerWrapper fserverWrapper = this.metricsFServer
        .getFServerWrapper();
    Collection<EntityGroup> entityGroups = getOnlineEntityGroupsLocalContext();

    WaspProtos.ServerLoadProtos.Builder serverLoad = WaspProtos.ServerLoadProtos
        .newBuilder();
    serverLoad.setNumberOfRequests((int) fserverWrapper.getRequestsPerSecond());
    serverLoad.setTotalNumberOfRequests(requestCount.get());
    serverLoad.setNumberOfConnections(rpcServer.getNumberOfConnections());
    for (EntityGroup entityGroup : entityGroups) {
      serverLoad
          .addEntityGroupsLoads(createEntityGroupLoad(entityGroup).entityGroupLoadPB);
    }
    serverLoad.setReportStartTime(reportStartTime);
    serverLoad.setReportEndTime(reportEndTime);
    serverLoad.setAvgWriteTime(driver.getAvgWriteTime());
    serverLoad.setAvgReadTime(driver.getAvgReadTime());

    return serverLoad.build();
  }

  String getOnlineEntityGroupsAsPrintableString() {
    StringBuilder sb = new StringBuilder();
    for (EntityGroup r : this.onlineEntityGroups.values()) {
      if (sb.length() > 0)
        sb.append(", ");
      sb.append(r.getEntityGroupInfo().getEncodedName());
    }
    return sb.toString();
  }

  /**
   * For tests and web ui. This method will only work if FServer is in the same
   * JVM as client; EntityGroup cannot be serialized to cross an rpc.
   * 
   * @see #getOnlineEntityGroups()
   */
  public Collection<EntityGroup> getOnlineEntityGroupsLocalContext() {
    Collection<EntityGroup> entityGroups = this.onlineEntityGroups.values();
    return Collections.unmodifiableCollection(entityGroups);
  }

  /**
   * Register bean with platform management server
   */
  void registerMBean() {
    MXBeanImpl mxBeanInfo = MXBeanImpl.init(this);
    mxBean = MBeans.register("FServer", "FServer", mxBeanInfo);
    LOG.info("Registered FServer MXBean");
  }

  /**
   * Let the master know we're here Run initialization using parameters passed
   * us by the master.
   * 
   * @return A Map of key/value configurations we got from the Master else null
   *         if we failed to register.
   * 
   * @throws IOException
   */
  private FServerStartupResponse reportForDuty() throws IOException {
    FServerStartupResponse result = null;
    ServerName masterServerName = getMaster();
    if (masterServerName == null)
      return result;
    try {
      this.requestCount.set(0);
      LOG.info("Telling master at " + masterServerName + " that we are up "
          + "with port=" + this.isa.getPort() + ", startcode=" + this.startcode);
      long now = EnvironmentEdgeManager.currentTimeMillis();
      int port = this.isa.getPort();
      FServerStartupRequest.Builder request = FServerStartupRequest
          .newBuilder();
      request.setPort(port);
      request.setServerStartCode(this.startcode);
      request.setServerCurrentTime(now);
      result = this.waspMaster.fServerStartup(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof ClockOutOfSyncException) {
        LOG.fatal("Master rejected startup because clock is out of sync", ioe);
        // Re-throw IOE will cause ES to abort
        throw ioe;
      } else {
        LOG.warn("error telling master we are up", se);
      }
    }
    return result;
  }

  /*
   * Run init. Sets up hlog and starts up all server threads.
   * 
   * @param c Extra configuration.
   */
  protected void handleReportForDutyResponse(final FServerStartupResponse c)
      throws IOException {
    try {
      for (WaspProtos.StringStringPair e : c.getMapEntriesList()) {
        String key = e.getName();
        // The hostname the master sees us as.
        if (key.equals(FConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)) {
          String hostnameFromMasterPOV = e.getValue();
          this.serverNameFromMasterPOV = new ServerName(hostnameFromMasterPOV,
              this.isa.getPort(), this.startcode);
          LOG.info("Master passed us hostname to use. Was="
              + this.isa.getHostName() + ", Now="
              + this.serverNameFromMasterPOV.getHostname());
          continue;
        }
        String value = e.getValue().toString();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Config from master: " + key + "=" + value);
        }
        this.conf.set(key, value);
      }

      // Set our ephemeral znode up in zookeeper now we have a name.
      createMyEphemeralNode();

      // Save it in a file, this will allow to see if we crash
      ZNodeClearer.writeMyEphemeralNodeOnDisk(getMyEphemeralNodePath());

      this.metricsFServer = new MetricsFServer(new MetricsFServerWrapperImpl(
          this));
      this.tableSchemaReader = TableSchemaCacheReader.getInstance(this.conf);

      startServiceThreads();
      LOG.info("Serving as "
          + this.serverNameFromMasterPOV
          + ", RPC listening on "
          + this.isa
          + ", sessionid=0x"
          + Long.toHexString(this.zooKeeper.getRecoverableZooKeeper()
              .getSessionId()));
      isOnline = true;
    } catch (Throwable e) {
      this.isOnline = false;
      stop("Failed initialization");
      throw convertThrowableToIOE(cleanup(e, "Failed init"),
          "FServer startup failed");
    } finally {
      sleeper.skipSleepCycle();
    }
  }

  private void createMyEphemeralNode() throws KeeperException {
    ZKUtil.createEphemeralNodeAndWatch(this.zooKeeper,
        getMyEphemeralNodePath(), FConstants.EMPTY_BYTE_ARRAY);
  }

  private void deleteMyEphemeralNode() throws KeeperException {
    ZKUtil.deleteNode(this.zooKeeper, getMyEphemeralNodePath());
  }

  private String getMyEphemeralNodePath() {
    return ZKUtil.joinZNode(this.zooKeeper.fsZNode, getServerName().toString());
  }

  /**
   * @see FServer#abort(String, Throwable)
   */
  public void abort(String reason) {
    abort(reason, null);
  }

  /**
   * Cause the server to exit without closing the entityGroups it is serving,
   * the log it is using and without notifying the master. OOME.
   * 
   * @param reason
   *          the reason we are aborting
   * @param cause
   *          the exception that caused the abort, or null
   */
  @Override
  public void abort(String reason, Throwable cause) {
    String msg = "ABORTING FServer " + this + ": " + reason;
    if (cause != null) {
      LOG.fatal(msg, cause);
    } else {
      LOG.fatal(msg);
    }
    this.abortRequested = true;
    this.reservedSpace.clear();

    if (this.metricsFServer != null) {
      LOG.info("Dump of metrics: " + this.metricsFServer);
    }
    // Do our best to report our abort to the master, but this may not work
    try {
      if (cause != null) {
        msg += "\nCause:\n" + StringUtils.stringifyException(cause);
      }
    } catch (Throwable t) {
      LOG.warn("Unable to report fatal error to master", t);
    }
    stop(reason);

  }

  @Override
  public boolean isAborted() {
    return this.abortRequested;
  }

  @Override
  public void stop(String reason) {
    this.stopped = true;
    LOG.info("STOPPED: " + reason);
    // Wakes run() if it is sleeping
    sleeper.skipSleepCycle();
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * Checks to see if the storage system is still accessible. If not, sets
   * abortRequested and stopRequested
   * 
   * @return false if storage system is not available
   */
  public boolean checkStorageSystem() {
    try {
      HTableInterface htable = this.getActionManager().getTable(
          conf.get(FConstants.METASTORE_TABLE,
              FConstants.DEFAULT_METASTORE_TABLE));
      htable.close();
    } catch (Throwable e) {
      return false;
    }
    return true;
  }

  /**
   * @return False if cluster shutdown in progress
   */
  private boolean isClusterUp() {
    return this.clusterStatusTracker.isClusterUp();
  }

  /**
   * Wait on entityGroups close.
   */
  private void waitOnAllEntityGroupsToClose(final boolean abort) {
    // Wait till all entityGroups are closed before going out.
    int lastCount = -1;
    long previousLogTime = 0;
    Set<String> closedEntityGroups = new HashSet<String>();
    while (!isOnlineEntityGroupsEmpty()) {
      int count = getNumberOfOnlineEntityGroups();
      // Only print a message if the count of entityGroups has changed.
      if (count != lastCount) {
        // Log every second at most
        if (System.currentTimeMillis() > (previousLogTime + 1000)) {
          previousLogTime = System.currentTimeMillis();
          lastCount = count;
          LOG.info("Waiting on " + count + " entityGroups to close");
          // Only print out entityGroups still closing if a small number else
          // will
          // swamp the log.
          if (count < 10 && LOG.isDebugEnabled()) {
            LOG.debug(this.onlineEntityGroups);
          }
        }
      }
      // Ensure all user entityGroups have been sent a close. Use this to
      // protect against the case where an open comes in after we start the
      // iterator of onlineEntityGroups to close all user entityGroups.
      for (Map.Entry<String, EntityGroup> e : this.onlineEntityGroups
          .entrySet()) {
        EntityGroupInfo egi = e.getValue().getEntityGroupInfo();
        if (!this.entityGroupsInTransitionInFS.containsKey(egi
            .getEncodedNameAsBytes())
            && !closedEntityGroups.contains(egi.getEncodedName())) {
          closedEntityGroups.add(egi.getEncodedName());
          // Don't update zk with this close transition; pass false.
          closeEntityGroup(egi, abort, false);
        }
      }
      // No entityGroups in RIT, we could stop waiting now.
      if (this.entityGroupsInTransitionInFS.isEmpty()) {
        if (!isOnlineEntityGroupsEmpty()) {
          LOG.info("We were exiting though online entityGroups are not empty, because some entityGroups failed closing");
        }
        break;
      }
      Threads.sleep(200);
    }
  }

  boolean isOnlineEntityGroupsEmpty() {
    return this.onlineEntityGroups.isEmpty();
  }

  public int getNumberOfOnlineEntityGroups() {
    return this.onlineEntityGroups.size();
  }

  /**
   * reset the metrics count
   */
  protected void doMetrics() {
    // TODO
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it.
   * <p/>
   * Method will block until a master is available. You can break from this
   * block by requesting the server stop.
   * 
   * @return master + port, or null if server has been stopped
   */
  public ServerName getMaster() {
    ServerName masterServerName = null;
    long previousLogTime = 0;
    FServerStatusProtocol master = null;
    while (keepLooping() && master == null) {
      masterServerName = this.masterAddressManager.getMasterAddress();
      if (masterServerName == null) {
        if (!keepLooping()) {
          // give up with no connection.
          LOG.debug("No master found and cluster is stopped; bailing out");
          return null;
        }
        LOG.debug("No master found; retry");
        previousLogTime = System.currentTimeMillis();

        sleeper.sleep();
        continue;
      }

      InetSocketAddress isa = new InetSocketAddress(
          masterServerName.getHostname(), masterServerName.getPort());

      LOG.info("Attempting connect to Master server at "
          + this.masterAddressManager.getMasterAddress());
      try {
        // Do initial RPC setup. The final argument indicates that the RPC
        // should retry indefinitely.
        master = (FServerStatusProtocol) WaspRPC.waitForProxy(
            FServerStatusProtocol.class, FServerStatusProtocol.VERSION, isa,
            this.conf, -1, this.rpcTimeout, this.rpcTimeout);
        LOG.info("Connected to master at " + isa);
      } catch (IOException e) {
        e = e instanceof RemoteException ? ((RemoteException) e)
            .unwrapRemoteException() : e;
        if (e instanceof ServerNotRunningYetException) {
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            LOG.info("Master isn't available yet, retrying");
            previousLogTime = System.currentTimeMillis();
          }
        } else {
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            LOG.warn("Unable to connect to master. Retrying. Error was:", e);
            previousLogTime = System.currentTimeMillis();
          }
        }
        try {
          Thread.sleep(200);
        } catch (InterruptedException ignored) {
        }
      }
    }
    this.waspMaster = master;
    return masterServerName;
  }

  /**
   * Closes all entityGroups. Called on our way out. Assumes that its not
   * possible for new entityGroups to be added to onlineEntityGroups while this
   * method runs.
   */
  protected void closeAllEntityGroups(final boolean abort) {
    closeUserEntityGroups(abort);
  }

  /**
   * Schedule closes on all user entityGroups. Should be safe calling multiple
   * times because it wont' close entityGroups that are already closed or that
   * are closing.
   * 
   * @param abort
   *          Whether we're running an abort.
   */
  void closeUserEntityGroups(final boolean abort) {
    this.lock.writeLock().lock();
    try {
      for (Map.Entry<String, EntityGroup> e : this.onlineEntityGroups
          .entrySet()) {
        EntityGroup eg = e.getValue();
        if (eg.isAvailable()) {
          // Don't update zk with this close transition; pass false.
          closeEntityGroup(eg.getEntityGroupInfo(), abort, false);
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /**
   * @return the request count
   */
  public AtomicInteger getRequestCount() {
    return this.requestCount;
  }

  /**
   * @return True if we should break loop because cluster is going down or this
   *         server has been stopped or hdfs has gone bad.
   */
  private boolean keepLooping() {
    return !this.stopped && isClusterUp();
  }

  /**
   * @return time stamp in millis of when this entityGroup server was started
   */
  public long getStartcode() {
    return this.startcode;
  }

  /**
   * Protected utility method for safely obtaining an EntityGroup handle.
   * 
   * @param entityGroupName
   *          Name of online {@link EntityGroup} to return
   * @return {@link EntityGroup} for <code>entityGroupName</code>
   */
  protected EntityGroup getEntityGroup(final byte[] entityGroupName)
      throws NotServingEntityGroupException {
    EntityGroup entityGroup = null;
    entityGroup = getOnlineEntityGroup(entityGroupName);
    if (entityGroup == null) {
      throw new NotServingEntityGroupException("EntityGroup is not online: "
          + Bytes.toStringBinary(entityGroupName));
    }
    return entityGroup;
  }

  /**
   * Get the top N most loaded entityGroups this server is serving so we can
   * tell the master which entityGroups it can reallocate if we're overloaded.
   * Actually calculate which entityGroups are most loaded. (Right now, we're
   * just grabbing the first N entityGroups being served regardless of load.)
   */
  protected EntityGroupInfo[] getMostLoadedEntityGroups() {
    ArrayList<EntityGroupInfo> entityGroups = new ArrayList<EntityGroupInfo>();
    for (EntityGroup eg : onlineEntityGroups.values()) {
      if (!eg.isAvailable()) {
        continue;
      }
      if (entityGroups.size() < numEntityGroupsToReport) {
        entityGroups.add(eg.getEntityGroupInfo());
      } else {
        break;
      }
    }
    return entityGroups.toArray(new EntityGroupInfo[entityGroups.size()]);
  }

  /**
   * Called to verify that this server is up and running.
   * 
   * @throws IOException
   */
  protected void checkOpen() throws IOException {
    if (this.stopped || this.abortRequested) {
      throw new FServerStoppedException("Server " + getServerName()
          + " not running" + (this.abortRequested ? ", aborting" : ""));
    }
  }

  /**
   * @see com.alibaba.wasp.client.ClientProtocol#execute(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteRequest )
   */
  @Override
  public ClientProtos.ExecuteResponse execute(RpcController controller,
      ClientProtos.ExecuteRequest request) throws ServiceException {
    MetaProtos.ReadModelProto readModel = request.getReadModel();
    return driver.execute(request.getSql(), request.getSessionId(),
        ProtobufUtil.toReadModel(readModel), request.getCloseSession(),
        request.getFetchSize());
  }

  /**
   * @see com.alibaba.wasp.client.ClientProtocol#get(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.GetRequest )
   */
  @Override
  public ClientProtos.GetResponse get(RpcController controller, ClientProtos.GetRequest request)
      throws ServiceException {
    return get(request.getEntityGroup().getValue().toByteArray(),
        ProtobufUtil.toGetAction(request.getGet()));
  }

  /**
   * 
   * @param entityGroupName
   *          eg name
   * @param getAction
   *          action true or false
   * @return
   * @throws ServiceException
   */
  public ClientProtos.GetResponse get(byte[] entityGroupName, GetAction getAction)
      throws ServiceException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    try {
      try {
        checkOpen();
      } catch (IOException e) {
        throw e;
      }
      requestCount.incrementAndGet();
      EntityGroup entityGroup = this.getEntityGroup(entityGroupName);
      getAction.setConf(conf);
      Result result = entityGroup.get(getAction);
      this.metricsFServer.updateGet(EnvironmentEdgeManager.currentTimeMillis()
          - before);
      return ResponseConverter.buildGetResponse(result, getAction.getColumns());
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * @see com.alibaba.wasp.client.ClientProtocol#scan(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.ScanRequest )
   */
  @Override
  public ClientProtos.ScanResponse scan(RpcController controller, ClientProtos.ScanRequest request)
      throws ServiceException {
    return scan(request.getEntityGroup().getValue().toByteArray(),
        ProtobufUtil.convertScanAction(request.getScanAction()),
        request.getScannerId() == -1 ? false : true, request.getScannerId(),
        request.hasCloseScanner() ? request.getCloseScanner() : false);
  }

  /**
   * 
   * @param entityGroupName
   *          eg name
   * @param scanAction
   *          action
   * @param hasScannerId
   *          true or false
   * @param scannerId
   *          id
   * @param closeScanner
   *          close this scanner
   * @return
   * @throws ServiceException
   */
  public ClientProtos.ScanResponse scan(byte[] entityGroupName, ScanAction scanAction,
      boolean hasScannerId, long scannerId, boolean closeScanner)
      throws ServiceException {
    return scan(entityGroupName, scanAction, hasScannerId, scannerId,
        closeScanner, false, null);
  }

  /**
   * 
   * @param entityGroupName
   *          eg name
   * @param scanAction
   *          action
   * @param hasScannerId
   *          true or false
   * @param scannerId
   *          id
   * @param closeScanner
   *          close this scanner
   * @param global
   *          true or false
   * @param tableDesc
   * @return
   * @throws ServiceException
   */
  public ClientProtos.ScanResponse scan(byte[] entityGroupName, ScanAction scanAction,
      boolean hasScannerId, long scannerId, boolean closeScanner,
      boolean global, FTable tableDesc) throws ServiceException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    try {
      String scannerName = null;
      if (hasScannerId) {
        scannerName = String.valueOf(scannerId);
      }

      Leases.Lease lease = null;

      try {
        checkOpen();
      } catch (IOException e) {
        if (scannerName != null) {
          try {
            leases.cancelLease(scannerName);
          } catch (LeaseException le) {
            LOG.info("Server shutting down and client tried to access missing scanner "
                + scannerName);
          }
        }
        throw e;
      }
      requestCount.incrementAndGet();
      try {
        EntityGroupScanner scanner = null;
        ClientProtos.ScanResponse.Builder builder = ClientProtos.ScanResponse.newBuilder();
        if (hasScannerId) {
          scanner = scanners.get(scannerName);
          if (scanner == null) {
            throw new UnknownScannerException("Name: " + scannerName
                + ", already closed?");
          }
        } else {
          EntityGroup entityGroup = null;
          if (tableDesc == null) {
            entityGroup = getEntityGroup(entityGroupName);
            tableDesc = entityGroup.getTableDesc();
          }
          if (!global) {
            if (entityGroup == null)
              entityGroup = getEntityGroup(entityGroupName);
            scanAction.setConf(conf);
            scanner = entityGroup.getScanner(scanAction);
            scanner.setTableDesc(entityGroup.getTableDesc());
          } else {
            scanner = this.globalEntityGroup.getScanner(scanAction);
            scanner.setTableDesc(tableDesc);
          }
          scannerId = addScanner(scanner);
          scanner.setScannerID(scannerId);
          scannerName = String.valueOf(scannerId);
        }

        if (!closeScanner) {
          try {
            // Remove lease while its being processed in server; protects
            // against
            // case
            // where processing of request takes > lease expiration time.
            lease = leases.removeLease(scannerName);
            List<ClientProtos.QueryResultProto> results = new ArrayList<ClientProtos.QueryResultProto>(
                scanner.getCaching());

            // Collect values to be returned here
            scanner.next(results);

            // convert result to query result.
            if (results.isEmpty()) {
              results = null;
            } else {
              builder.addAllResult(results);
            }
          } finally {
            // We're done. On way out re-add the above removed lease.
            // Adding resets expiration time on lease.
            if (scanners.containsKey(scannerName)) {
              if (lease != null)
                leases.addLease(lease);
            }
          }
        }

        if (closeScanner) {
          scanner = scanners.remove(scannerName);
          if (scanner != null) {
            scanner.close();
            leases.cancelLease(scannerName);
          }
        }

        builder.setScannerId(scannerId);
        this.metricsFServer.updateScan(EnvironmentEdgeManager
            .currentTimeMillis() - before);
        List<ColumnStruct> buildColumns = scanAction.getColumns().size() == 0 ?
            scanAction.getStoringColumns() : scanAction.getColumns();
        return ResponseConverter.buildScanResponse(builder, buildColumns);
      } catch (Exception t) {
        if (scannerName != null && t instanceof NotServingEntityGroupException) {
          scanners.remove(scannerName);
        }
        LOG.error("Unexpected exception.", t);
        throw convertThrowableToIOE(cleanup(t));
      }
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
   * IOE if it isn't already.
   * 
   * @param t
   *          Throwable
   * 
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  protected Throwable cleanup(final Throwable t) {
    return cleanup(t, null);
  }

  /**
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
   * IOE if it isn't already.
   * 
   * @param t
   *          Throwable
   * 
   * @param msg
   *          Message to log in error. Can be null.
   * 
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  protected Throwable cleanup(final Throwable t, final String msg) {
    // Don't log as error if NSRE; NSRE is 'normal' operation.
    if (t instanceof NotServingEntityGroupException) {
      LOG.debug("NotServingEntityGroupException; " + t.getMessage());
      return t;
    }
    if (msg == null) {
      LOG.error("", RemoteExceptionHandler.checkThrowable(t));
    } else {
      LOG.error(msg, RemoteExceptionHandler.checkThrowable(t));
    }
    if (!checkOOME(t)) {
      checkStorageSystem();
    }
    return t;
  }

  /**
   * @param t
   * 
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  protected IOException convertThrowableToIOE(final Throwable t) {
    return convertThrowableToIOE(t, null);
  }

  /**
   * @param t
   * 
   * @param msg
   *          Message to put in new IOE if passed <code>t</code> is not an IOE
   * 
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  protected IOException convertThrowableToIOE(final Throwable t,
      final String msg) {
    return (t instanceof IOException ? (IOException) t : msg == null
        || msg.length() == 0 ? new IOException(t) : new IOException(msg, t));
  }

  /**
   * Check if an OOME and, if so, abort immediately to avoid creating more
   * objects.
   * 
   * @param e
   * 
   * @return True if we OOME'd and are aborting.
   */
  public boolean checkOOME(final Throwable e) {
    boolean stop = false;
    try {
      if (e instanceof OutOfMemoryError
          || (e.getCause() != null && e.getCause() instanceof OutOfMemoryError)
          || (e.getMessage() != null && e.getMessage().contains(
              "java.lang.OutOfMemoryError"))) {
        stop = true;
        LOG.fatal("Run out of memory; FServer will abort itself immediately", e);
      }
    } finally {
      if (stop) {
        Runtime.getRuntime().halt(1);
      }
    }
    return stop;
  }

  /**
   * Add scanner to lease manager,so scanner will be reused by scanID.
   * 
   * @param scanner
   * @throws Leases.LeaseStillHeldException
   */
  protected long addScanner(EntityGroupScanner scanner)
      throws Leases.LeaseStillHeldException {
    long scannerId = -1;
    while (true) {
      scannerId = rand.nextLong();
      if (scannerId == -1) {
        continue;
      }
      String scannerName = String.valueOf(scannerId);
      EntityGroupScanner existing = scanners.putIfAbsent(scannerName, scanner);
      if (existing == null) {
        this.leases.createLease(scannerName, this.scannerLeaseTimeoutPeriod,
            new ScannerListener(scannerName));
        break;
      }
    }
    return scannerId;
  }

  /**
   * @see com.alibaba.wasp.client.ClientProtocol#insert(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.InsertRequest )
   */
  @Override
  public ClientProtos.InsertResponse insert(RpcController controller, ClientProtos.InsertRequest request)
      throws ServiceException {
    try {
      requestCount.incrementAndGet();
      InsertAction insertAction = ProtobufUtil.toInsertAction(request
          .getInsertAction());
      return insert(request.getEntityGroup().getValue().toByteArray(),
          insertAction);
    } catch (IOException ie) {
      this.checkStorageSystem();
      throw new ServiceException(ie);
    }
  }

  /**
   * @see com.alibaba.wasp.client.ClientProtocol#update(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.UpdateRequest )
   */
  @Override
  public ClientProtos.UpdateResponse update(RpcController controller, ClientProtos.UpdateRequest request)
      throws ServiceException {
    try {
      requestCount.incrementAndGet();
      UpdateAction updateAction = ProtobufUtil.toUpdateAction(request
          .getUpdateAction());
      return this.update(request.getEntityGroup().getValue().toByteArray(),
          updateAction);
    } catch (IOException ie) {
      this.checkStorageSystem();
      throw new ServiceException(ie);
    }
  }

  /**
   * @see com.alibaba.wasp.client.ClientProtocol#delete(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.ClientProtos.DeleteRequest )
   */
  @Override
  public ClientProtos.DeleteResponse delete(RpcController controller, ClientProtos.DeleteRequest request)
      throws ServiceException {
    try {
      requestCount.incrementAndGet();
      DeleteAction deleteAction = ProtobufUtil.toDeleteAction(request
          .getDeleteAction());
      return this.delete(request.getEntityGroup().getValue().toByteArray(),
          deleteAction);
    } catch (IOException ie) {
      this.checkStorageSystem();
      throw new ServiceException(ie);
    }
  }

  /**
   * @see com.alibaba.wasp.fserver.AdminProtocol#getEntityGroupInfo(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoRequest )
   */
  @Override
  public FServerAdminProtos.GetEntityGroupInfoResponse getEntityGroupInfo(
      RpcController controller, FServerAdminProtos.GetEntityGroupInfoRequest request)
      throws ServiceException {
    ByteString entityGroupName = request.getEntityGroup().getValue();
    FServerAdminProtos.GetEntityGroupInfoResponse.Builder builder = FServerAdminProtos.GetEntityGroupInfoResponse
        .newBuilder();
    try {
      builder.setEntityGroupInfo(getEntityGroupInfo(
          entityGroupName.toByteArray()).convert());
    } catch (IOException e) {
      LOG.error("Getting EntityGroupInfo.", e);
      throw new ServiceException(e);
    }
    return builder.build();
  }

  private EntityGroupInfo getEntityGroupInfo(byte[] entityGroupName)
      throws NotServingEntityGroupException, IOException {
    checkOpen();
    requestCount.incrementAndGet();
    return getEntityGroup(entityGroupName).getEntityGroupInfo();
  }

  /**
   * @see com.alibaba.wasp.fserver.AdminProtocol#getOnlineEntityGroups(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsRequest )
   */
  @Override
  public FServerAdminProtos.GetOnlineEntityGroupsResponse getOnlineEntityGroups(
      RpcController controller, FServerAdminProtos.GetOnlineEntityGroupsRequest request)
      throws ServiceException {
    try {
      checkOpen();
      FServerAdminProtos.GetOnlineEntityGroupsResponse.Builder builder = FServerAdminProtos.GetOnlineEntityGroupsResponse
          .newBuilder();
      for (Map.Entry<String, EntityGroup> e : this.onlineEntityGroups
          .entrySet()) {
        builder
            .addEntityGroupInfos(e.getValue().getEntityGroupInfo().convert());
      }
      return builder.build();
    } catch (IOException e) {
      LOG.error("Getting OnlineEntityGroups.", e);
      throw new ServiceException(e);
    }
  }

  /**
   * @see com.alibaba.wasp.fserver.AdminProtocol#openEntityGroups(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupsRequest )
   */
  @Override
  public FServerAdminProtos.OpenEntityGroupsResponse openEntityGroups(RpcController controller,
      FServerAdminProtos.OpenEntityGroupsRequest request) throws ServiceException {
    List<EntityGroupInfo> entityGroups = new ArrayList<EntityGroupInfo>();
    for (WaspProtos.EntityGroupInfoProtos entityGroupInfo : request
        .getEntityGroupInfosList()) {
      entityGroups.add(EntityGroupInfo.convert(entityGroupInfo));
    }
    try {
      openEntityGroups(entityGroups);
    } catch (IOException e) {
      LOG.error("Opening EntityGroups.", e);
      throw new ServiceException(e);
    }
    FServerAdminProtos.OpenEntityGroupsResponse.Builder builder = FServerAdminProtos.OpenEntityGroupsResponse
        .newBuilder();
    return builder.build();
  }

  private void openEntityGroups(List<EntityGroupInfo> entityGroups)
      throws IOException {
    checkOpen();
    LOG.info("Received request to open " + entityGroups.size()
        + " entityGroup(s)");
    Map<String, FTable> tables = new HashMap<String, FTable>();
    for (EntityGroupInfo entityGroup : entityGroups) {
      openEntityGroupWithTableBuffer(entityGroup, -1, tables);
    }
  }

  /**
   * @see com.alibaba.wasp.fserver.AdminProtocol#openEntityGroup(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupRequest )
   */
  @Override
  public FServerAdminProtos.OpenEntityGroupResponse openEntityGroup(RpcController controller,
      FServerAdminProtos.OpenEntityGroupRequest request) throws ServiceException {
    try {
      checkOpen();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
    // requestCount.increment();
    FServerAdminProtos.OpenEntityGroupResponse.Builder builder = FServerAdminProtos.OpenEntityGroupResponse
        .newBuilder();
    int entityGroupCount = request.getEntityGroupCount();
    boolean isBulkAssign = entityGroupCount > 1;
    int versionOfOfflineNode = request.getVersionOfOfflineNode();
    if (isBulkAssign)
      versionOfOfflineNode = -1;
    for (WaspProtos.EntityGroupInfoProtos entityGroupOpenInfo : request
        .getEntityGroupList()) {
      EntityGroupInfo entityGroup = EntityGroupInfo
          .convert(entityGroupOpenInfo);
      try {
        openEntityGroup(entityGroup, versionOfOfflineNode);
        builder.addOpeningState(WaspProtos.EntityGroupOpeningState.OPENED);
      } catch (EntityGroupAlreadyInTransitionException rie) {
        LOG.warn("EntityGroup is already in transition", rie);
        if (isBulkAssign) {
          builder.addOpeningState(WaspProtos.EntityGroupOpeningState.OPENED);
        } else {
          throw new ServiceException(rie);
        }
      } catch (IOException ie) {
        LOG.warn(
            "Failed opening entityGroup "
                + entityGroup.getEntityGroupNameAsString(), ie);
        if (isBulkAssign) {
          builder.addOpeningState(WaspProtos.EntityGroupOpeningState.FAILED_OPENING);
        } else {
          throw new ServiceException(ie);
        }
      }
    }
    return builder.build();
  }

  private WaspProtos.EntityGroupOpeningState openEntityGroup(EntityGroupInfo entityGroup,
      int versionOfOfflineNode) throws IOException {
    return openEntityGroupWithTableBuffer(entityGroup, versionOfOfflineNode,
        null);
  }

  private WaspProtos.EntityGroupOpeningState openEntityGroupWithTableBuffer(
      EntityGroupInfo entityGroup, int versionOfOfflineNode,
      final Map<String, FTable> tables) throws IOException {
    checkOpen();
    checkIfEntityGroupInTransition(entityGroup, OPEN);
    EntityGroup onlineEntityGroup = this.getFromOnlineEntityGroups(entityGroup
        .getEncodedName());
    if (null != onlineEntityGroup) {
      // Cross check with META if still this FS is owning the entityGroup.
      Pair<EntityGroupInfo, ServerName> p = FMetaScanner.getEntityGroup(conf,
          entityGroup.getEntityGroupName());
      if (this.getServerName().equals(p.getSecond())) {
        LOG.warn("Attempted open of " + entityGroup.getEncodedName()
            + " but already online on this server");
        return WaspProtos.EntityGroupOpeningState.ALREADY_OPENED;
      } else {
        LOG.warn("The entityGroup " + entityGroup.getEncodedName()
            + " is online on this server but FMETA does not have this server.");
        this.removeFromOnlineEntityGroups(entityGroup.getEncodedName());
      }
    }
    LOG.info("Received request to open entityGroup: "
        + entityGroup.getEntityGroupNameAsString() + "from "
        + NettyServer.getRemoteAddress());
    FTable table;
    if (tables == null) {
      table = TableSchemaCacheReader.getService(tableSchemaReader.getConf())
          .getTable(Bytes.toString(entityGroup.getTableName()));
    } else {
      table = tables.get(entityGroup.getTableNameAsString());
      if (table == null) {
        table = TableSchemaCacheReader.getService(tableSchemaReader.getConf())
            .getTable(Bytes.toString(entityGroup.getTableName()));
        tables.put(entityGroup.getTableNameAsString(), table);
      }
    }
    if (table == null) {
      LOG.warn("Table schema:" + Bytes.toString(entityGroup.getTableName())
          + " is null!");
    }
    this.entityGroupsInTransitionInFS.putIfAbsent(
        entityGroup.getEncodedNameAsBytes(), true);
    // Need to pass the expected version in the constructor.
    this.service.submit(new OpenEntityGroupHandler(this, this, entityGroup,
        table, EventHandler.EventType.M_FSERVER_OPEN_ENTITYGROUP,
        versionOfOfflineNode));
    return WaspProtos.EntityGroupOpeningState.OPENED;
  }

  private void checkIfEntityGroupInTransition(EntityGroupInfo entityGroup,
      String currentAction) throws EntityGroupAlreadyInTransitionException {
    byte[] encodedName = entityGroup.getEncodedNameAsBytes();
    if (this.entityGroupsInTransitionInFS.containsKey(encodedName)) {
      boolean openAction = this.entityGroupsInTransitionInFS.get(encodedName);
      // The below exception message will be used in master.
      throw new EntityGroupAlreadyInTransitionException("Received:"
          + currentAction + " for the entityGroup:"
          + entityGroup.getEntityGroupNameAsString()
          + " ,which we are already trying to " + (openAction ? OPEN : CLOSE)
          + ".");
    }
  }

  /**
   * @see com.alibaba.wasp.fserver.AdminProtocol#closeEntityGroup(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEntityGroupRequest )
   */
  @Override
  public FServerAdminProtos.CloseEntityGroupResponse closeEntityGroup(RpcController controller,
      FServerAdminProtos.CloseEntityGroupRequest request) throws ServiceException {
    WaspProtos.EntityGroupInfoProtos entityGroupInfoProto = request.getEntityGroup();
    EntityGroupInfo entityGroupInfo = EntityGroupInfo
        .convert(entityGroupInfoProto);
    LOG.info("Received close entityGroup: "
        + Bytes.toStringBinary(entityGroupInfo.getEncodedNameAsBytes())
        + ". Use ZK:" + request.getZk() + " from "
        + NettyServer.getRemoteAddress());
    FServerAdminProtos.CloseEntityGroupResponse.Builder builder = FServerAdminProtos.CloseEntityGroupResponse
        .newBuilder();
    try {
      builder.setSuccess(closeEntityGroup(entityGroupInfo, request.getZk(),
          request.getVersionOfClosingNode()));
    } catch (IOException e) {
      LOG.error("Closing EntityGroup.", e);
      throw new ServiceException(e);
    }
    return builder.build();
  }

  private boolean closeEntityGroup(EntityGroupInfo entityGroup)
      throws IOException {
    return closeEntityGroup(entityGroup, true, -1);
  }

  private boolean closeEntityGroup(EntityGroupInfo entityGroup,
      final boolean zk, final int versionOfClosingNode) throws IOException {
    checkOpen();
    LOG.info("Received close entityGroup: "
        + entityGroup.getEntityGroupNameAsString()
        + ". Version of ZK closing node:" + versionOfClosingNode + " from "
        + NettyServer.getRemoteAddress());
    boolean hasit = this.onlineEntityGroups.containsKey(entityGroup
        .getEncodedName());
    if (!hasit) {
      LOG.warn("Received close for entityGroup we are not serving; "
          + entityGroup.getEncodedName());
      throw new NotServingEntityGroupException("Received close for "
          + entityGroup.getEntityGroupNameAsString()
          + " but we are not serving it");
    }
    checkIfEntityGroupInTransition(entityGroup, CLOSE);
    return closeEntityGroup(entityGroup, false, zk, versionOfClosingNode);
  }

  /**
   * @param eg
   *          entityGroup to close
   * @param abort
   *          True if we are aborting
   * @param zk
   *          True if we are to update zk about the entityGroup close; if the
   *          close was orchestrated by master, then update zk. If the close is
   *          being run by the FServer because its going down, don't update zk.
   * @return True if closed a entityGroup.
   */
  private boolean closeEntityGroup(EntityGroupInfo eg, final boolean abort,
      final boolean zk) {
    return closeEntityGroup(eg, abort, zk, -1);
  }

  /**
   * @param eg
   *          EntityGroup to close
   * @param abort
   *          True if we are aborting
   * @param zk
   *          True if we are to update zk about the entityGroup close; if the
   *          close was orchestrated by master, then update zk. If the close is
   *          being run by the FServer because its going down, don't update zk.
   * @param versionOfClosingNode
   *          the version of znode to compare when FS transitions the znode from
   *          CLOSING state.
   * @return True if closed a entityGroup.
   */
  private boolean closeEntityGroup(EntityGroupInfo eg, final boolean abort,
      final boolean zk, final int versionOfClosingNode) {
    if (this.entityGroupsInTransitionInFS.containsKey(eg
        .getEncodedNameAsBytes())) {
      LOG.warn("Received close for entityGroup we are already opening or closing; "
          + eg.getEncodedName());
      return false;
    }
    this.entityGroupsInTransitionInFS.putIfAbsent(eg.getEncodedNameAsBytes(),
        false);
    CloseEntityGroupHandler crh = new CloseEntityGroupHandler(this, this, eg,
        abort, zk, versionOfClosingNode,
        EventHandler.EventType.M_FSERVER_CLOSE_ENTITYGROUP);
    this.service.submit(crh);
    return true;
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#closeEncodedEntityGroup(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEncodedEntityGroupRequest )
   */
  @Override
  public FServerAdminProtos.CloseEncodedEntityGroupResponse closeEncodedEntityGroup(
      RpcController controller, FServerAdminProtos.CloseEncodedEntityGroupRequest request)
      throws ServiceException {
    FServerAdminProtos.CloseEncodedEntityGroupResponse.Builder builder = FServerAdminProtos.CloseEncodedEntityGroupResponse
        .newBuilder();
    try {
      String encodedName = Bytes.toString(request.getEntityGroup().getValue()
          .toByteArray());
      EntityGroup entityGroup = this.onlineEntityGroups.get(encodedName);
      if (entityGroup == null) {
        LOG.warn("Received close for entityGroup with encodedEntityGroupName, we are not serving; "
            + encodedName);
        throw new NotServingEntityGroupException("Received close for "
            + encodedName + " but we are not serving it");
      }
      EntityGroupInfo entityGroupInfo = entityGroup.getEntityGroupInfo();
      LOG.info("Received close entityGroup: "
          + Bytes.toStringBinary(entityGroupInfo.getEncodedNameAsBytes())
          + ". Use ZK:" + request.getZk() + " from "
          + NettyServer.getRemoteAddress());

      builder.setSuccess(closeEntityGroup(entityGroupInfo, request.getZk(),
          request.getVersionOfClosingNode()));
    } catch (IOException e) {
      LOG.error("Closing EntityGroup.", e);
      throw new ServiceException(e);
    }
    return builder.build();
  }

  /**
   * @see com.alibaba.wasp.fserver.AdminProtocol#splitEntityGroup(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.SplitEntityGroupRequest )
   */
  @Override
  public FServerAdminProtos.SplitEntityGroupResponse splitEntityGroup(RpcController controller,
      FServerAdminProtos.SplitEntityGroupRequest request) throws ServiceException {
    ByteString splitPoint = null;
    if (request.hasSplitPoint()) {
      splitPoint = request.getSplitPoint();
    }
    byte[] entityGroupName = request.getEntityGroup().getValue().toByteArray();
    try {
      if (splitPoint == null) {
        splitEntityGroup(entityGroupName, null);
      } else {
        splitEntityGroup(entityGroupName, splitPoint.toByteArray());
      }
    } catch (IOException e) {
      LOG.error("Spliting EntityGroup.", e);
      throw new ServiceException(e);
    }

    return ResponseConverter.buildSplitEntityGroupResponse();
  }

  private void splitEntityGroup(byte[] entityGroupName, byte[] splitPoint)
      throws NotServingEntityGroupException, IOException {
    checkOpen();
    EntityGroup entityGroup = getEntityGroup(entityGroupName);
    LOG.info("Spliting " + entityGroup.getEntityGroupNameAsString() + " from "
        + NettyServer.getRemoteAddress());
    entityGroup.forceSplit(splitPoint);
    splitThread.requestSplit(entityGroup, entityGroup.checkSplit());
  }

  /**
   * @param entityGroupName
   * @return EntityGroup for the passed binary <code>entityGroupName</code> or
   *         null if named entityGroup is not member of the online entityGroups.
   */
  public EntityGroup getOnlineEntityGroup(final byte[] entityGroupName) {
    return getFromOnlineEntityGroups(EntityGroupInfo
        .encodeEntityGroupName(entityGroupName));
  }

  /**
   * @see com.alibaba.wasp.fserver.AdminProtocol#disableServerTable(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.DisableTableRequest )
   *      (com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.
   *      FServerAdminProtos.DisableTableRequest)
   */
  @Override
  public FServerAdminProtos.DisableTableResponse disableServerTable(RpcController controller,
      FServerAdminProtos.DisableTableRequest request) throws ServiceException {
    boolean isCloseSuccess = true;
    try {
      List<EntityGroupInfo> entityGroupInfos = new ArrayList<EntityGroupInfo>();
      for (EntityGroup entityGroup : onlineEntityGroups.values()) {
        String tableName = entityGroup.getEntityGroupInfo()
            .getTableNameAsString();
        if (tableName.equals(request.getTableName())) {
          entityGroupInfos.add(entityGroup.getEntityGroupInfo());
        }
      }

      for (EntityGroupInfo entityGroupInfo : entityGroupInfos) {
        if (!closeEntityGroup(entityGroupInfo)) {
          isCloseSuccess = false;
        }
      }

      FConnection connection = FConnectionManager.getConnection(conf);
      connection.clearEntityGroupCache(Bytes.toBytes(request.getTableName()));
      tableSchemaReader.removeSchema(request.getTableName());

      FServerAdminProtos.DisableTableResponse.Builder builder = FServerAdminProtos.DisableTableResponse.newBuilder();
      builder.setSuccess(isCloseSuccess);
      return builder.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  /**
   * @see com.alibaba.wasp.fserver.AdminProtocol#enableServerTable(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.EnableTableRequest )
   *      (com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.
   *      FServerAdminProtos.EnableTableRequest)
   */
  @Override
  public FServerAdminProtos.EnableTableResponse enableServerTable(RpcController controller,
      FServerAdminProtos.EnableTableRequest request) throws ServiceException {
    FConnection connection;
    try {
      connection = FConnectionManager.getConnection(conf);
      connection.relocateEntityGroupsInCache(FMetaReader.getRootTable(conf,
          request.getTableName()));
      tableSchemaReader.refreshSchema(request.getTableName());
      FServerAdminProtos.EnableTableResponse.Builder buider = FServerAdminProtos.EnableTableResponse.newBuilder();
      buider.setSuccess(true);
      return buider.build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  @Override
  public java.util.concurrent.ExecutorService getThreadPool() {
    return this.pool;
  }

  @Override
  public void postOpenDeployTasks(EntityGroup entityGroup, boolean daughter)
      throws IOException {
    checkOpen();

    LOG.info("Post open deploy tasks for entityGroup="
        + entityGroup.getEntityGroupNameAsString() + ", daughter=" + daughter);
    if (daughter) {
      // If daughter of a split, update whole row, not just location.
      FMetaEditor.addDaughter(conf, entityGroup.getEntityGroupInfo(),
          this.serverNameFromMasterPOV);
    } else {
      FMetaEditor.updateLocation(conf, entityGroup.getEntityGroupInfo(),
          this.serverNameFromMasterPOV);
    }
    LOG.info("Done with post open deploy task for entityGroup="
        + entityGroup.getEntityGroupNameAsString() + ", daughter=" + daughter);
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zooKeeper;
  }

  @Override
  public ServerName getServerName() {
    // Our server name could change after we talk to the master.
    return this.serverNameFromMasterPOV == null ? new ServerName(
        this.isa.getHostName(), this.isa.getPort(), this.startcode)
        : this.serverNameFromMasterPOV;
  }

  /**
   * Utility for constructing an instance of the passed FServer class.
   * 
   * @param fserverClass
   * @param conf
   * @return FServer instance.
   */
  public static FServer constructFServer(Class<? extends FServer> fserverClass,
      final Configuration conf) {
    try {
      Constructor<? extends FServer> c = fserverClass
          .getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "FServer: "
          + fserverClass.toString(), e);
    }
  }

  /**
   * @param fs
   * @return Thread the FServer is running in correctly named.
   * @throws IOException
   */
  public static Thread startFServer(final FServer fs) throws IOException {
    return startFServer(fs, "fserver" + fs.isa.getPort());
  }

  /**
   * @param fs
   * @param name
   * @return Thread the FServer is running in correctly named.
   * @throws IOException
   */
  public static Thread startFServer(final FServer fs, final String name)
      throws IOException {
    Thread t = new Thread(fs);
    t.setName(name);
    t.start();
    return t;
  }

  /**
   * 
   * @param entityGroup
   * @return
   */
  private EntityGroupLoad createEntityGroupLoad(EntityGroup entityGroup) {
    WaspProtos.EntityGroupLoadProtos.Builder builder = WaspProtos.EntityGroupLoadProtos.newBuilder();
    builder.setEntityGroupSpecifier(RequestConverter
        .getEntityGroupSpecifier(entityGroup.getEntityGroupInfo()));
    builder.setReadRequestsCount(entityGroup.getReadRequestsCount());
    builder.setWriteRequestsCount(entityGroup.getWriteRequestsCount());
    builder.setTransactionLogSize(entityGroup.getTransactionLogSize());
    return new EntityGroupLoad(builder.build());
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    Configuration conf = WaspConfiguration.create();
    @SuppressWarnings("unchecked")
    Class<? extends FServer> fserverClass = (Class<? extends FServer>) conf
        .getClass(FConstants.FSERVER_IMPL, FServer.class);
    new FServerCommandLine(fserverClass).doMain(args);
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    return 0;
  }

  public MetricsFServer getMetrics() {
    return metricsFServer;
  }

  /**
   * @return true if online, false if not.
   */
  public boolean isOnline() {
    return isOnline;
  }

  public void waitForServerOnline() {
    while (!isOnline() && !isStopped()) {
      sleeper.sleep();
    }
  }

  /**
   * @see com.alibaba.wasp.fserver.FServerServices#getLeases()
   */
  @Override
  public Leases getLeases() {
    return this.leases;
  }

  /**
   * @see OnlineEntityGroups#getOnlineEntityGroups()
   */
  @Override
  public Collection<EntityGroup> getOnlineEntityGroups() throws IOException {
    return this.onlineEntityGroups.values();
  }

  /**
   * Public method for update.
   * 
   * @param entityGroupName
   * @param updateAction
   * @return
   * @throws IOException
   */
  public ClientProtos.UpdateResponse update(byte[] entityGroupName, UpdateAction updateAction)
      throws IOException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    EntityGroup entityGroup = getEntityGroup(entityGroupName);
    updateAction.setConf(conf);
    OperationStatus status = entityGroup.update(updateAction);
    this.metricsFServer.updateUpdate(EnvironmentEdgeManager.currentTimeMillis()
        - before);
    return ResponseConverter.buildUpdateResponse(status);
  }

  /**
   * Public method for delete.
   * 
   * @param entityGroupName
   * @param deleteAction
   * @return
   * @throws IOException
   */
  public ClientProtos.DeleteResponse delete(byte[] entityGroupName, DeleteAction deleteAction)
      throws IOException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    EntityGroup entityGroup = getEntityGroup(entityGroupName);
    deleteAction.setConf(conf);
    OperationStatus status = entityGroup.delete(deleteAction);
    this.metricsFServer.updateDelete(EnvironmentEdgeManager.currentTimeMillis()
        - before);
    return ResponseConverter.buildDeleteResponse(status);
  }

  /**
   * Public method for insert.
   * 
   * @param entityGroupName
   * @param insertAction
   * @return
   * @throws IOException
   */
  public ClientProtos.InsertResponse insert(byte[] entityGroupName, InsertAction insertAction)
      throws IOException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    EntityGroup entityGroup = getEntityGroup(entityGroupName);
    insertAction.setConf(conf);
    OperationStatus status = entityGroup.insert(insertAction);
    this.metricsFServer.updateInsert(EnvironmentEdgeManager.currentTimeMillis()
        - before);
    return ResponseConverter.buildInsertResponse(status);
  }

  /**
   * Instantiated as a scanner lease. If the lease times out, the scanner is
   * closed
   */
  public class ScannerListener implements LeaseListener {
    private final String scannerName;

    ScannerListener(final String n) {
      this.scannerName = n;
    }

    public void leaseExpired() {
      EntityGroupScanner s = scanners.remove(this.scannerName);
      if (s != null) {
        LOG.info("Scanner " + this.scannerName
            + " lease expired on entityGroup "
            + s.getEntityGroupInfo().getEntityGroupNameAsString());
        try {
          s.close();
        } catch (IOException e) {
          LOG.error("Closing scanner for "
              + s.getEntityGroupInfo().getEntityGroupNameAsString(), e);
        }
      } else {
        LOG.info("Scanner " + this.scannerName + " lease expired");
      }
    }
  }

  public ExecutorService getExecutorService() {
    return service;
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#getServerInfo(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetServerInfoRequest )
   */
  @Override
  public FServerAdminProtos.GetServerInfoResponse getServerInfo(RpcController controller,
      FServerAdminProtos.GetServerInfoRequest request) throws ServiceException {
    return ResponseConverter.buildGetServerInfoResponse(getServerName(),
        this.webuiport);
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.FServerAdminProtos.FServerAdminService.BlockingInterface#stopServer(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.FServerAdminProtos.StopServerRequest )
   */
  @Override
  public FServerAdminProtos.StopServerResponse stopServer(RpcController controller,
      FServerAdminProtos.StopServerRequest request) throws ServiceException {
    String message = "StopServer from " + NettyServer.getRemoteAddress();
    LOG.info(message);
    this.stop(message);
    FServerAdminProtos.StopServerResponse.Builder builder = FServerAdminProtos.StopServerResponse.newBuilder();
    return builder.build();
  }

  /**
   * @return the actionManager
   */
  @Override
  public StorageActionManager getActionManager() {
    return actionManager;
  }

  /**
   * @see com.alibaba.wasp.fserver.FServerServices#getGlobalEntityGroup()
   */
  @Override
  public EntityGroupServices getGlobalEntityGroup() {
    return globalEntityGroup;
  }

  /**
   * Utilty method to wait indefinitely on a znode availability while checking
   * if the fserver is shut down
   * 
   * @param tracker
   *          znode tracker to use
   * @throws IOException
   *           any IO exception, plus if the RS is stopped
   * @throws InterruptedException
   */
  private void blockAndCheckIfStopped(ZooKeeperNodeTracker tracker)
      throws IOException, InterruptedException {
    while (tracker.blockUntilAvailable(this.msgInterval, false) == null) {
      if (this.stopped) {
        throw new IOException("Received the shutdown message while waiting.");
      }
    }
  }

  /**
   * Wait on all threads to finish. Presumption is that all closes and stops
   * have already been called.
   */
  protected void join() {
    if (this.splitThread != null) {
      this.splitThread.join();
    }
    if (this.service != null)
      this.service.shutdown();
  }

  private void closeAllScanners() {
    // Close any outstanding scanners. Means they'll get an UnknownScanner
    // exception next time they come in.
    for (Map.Entry<String, EntityGroupScanner> e : this.scanners.entrySet()) {
      try {
        e.getValue().close();
      } catch (IOException ioe) {
        LOG.warn("Closing scanner " + e.getKey(), ioe);
      }
    }
  }

  public EntityGroupLoad createEntityGroupLoad(EntityGroupInfo egi)
      throws IOException {
    FTable table = FMetaReader.getTable(conf,
        Bytes.toString(egi.getTableName()));
    EntityGroup entityGroup = new EntityGroup(conf, egi, table, this);
    return createEntityGroupLoad(entityGroup);
  }

  /*
   * Simulate a kill -9 of this server. Exits w/o closing entityGroups or
   * cleaninup logs but it does close socket in case want to bring up server on
   * old hostname+port immediately.
   */
  protected void kill() {
    this.killed = true;
    abort("Simulated kill");
  }
}

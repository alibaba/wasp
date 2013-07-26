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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.net.DNS;
import com.alibaba.wasp.ClusterId;
import com.alibaba.wasp.ClusterStatus;
import com.alibaba.wasp.DeserializationException;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.MasterNotRunningException;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.PleaseHoldException;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.ServerLoad;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.TableNotDisabledException;
import com.alibaba.wasp.TableNotFoundException;
import com.alibaba.wasp.UnknownEntityGroupException;
import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.client.FConnectionManager;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.executor.ExecutorService;
import com.alibaba.wasp.executor.ExecutorService.ExecutorType;
import com.alibaba.wasp.ipc.NettyServer;
import com.alibaba.wasp.ipc.RpcServer;
import com.alibaba.wasp.ipc.WaspRPC;
import com.alibaba.wasp.master.balancer.BalancerChore;
import com.alibaba.wasp.master.balancer.ClusterStatusChore;
import com.alibaba.wasp.master.balancer.LoadBalancerFactory;
import com.alibaba.wasp.master.handler.CreateIndexHandler;
import com.alibaba.wasp.master.handler.CreateTableHandler;
import com.alibaba.wasp.master.handler.DeleteIndexHandler;
import com.alibaba.wasp.master.handler.DeleteTableHandler;
import com.alibaba.wasp.master.handler.DisableTableHandler;
import com.alibaba.wasp.master.handler.EnableTableHandler;
import com.alibaba.wasp.master.handler.ModifyTableHandler;
import com.alibaba.wasp.master.handler.ServerShutdownHandler;
import com.alibaba.wasp.master.handler.TableEventHandler;
import com.alibaba.wasp.master.handler.TruncateTableHandler;
import com.alibaba.wasp.master.metrics.MetricsMaster;
import com.alibaba.wasp.meta.AbstractMetaService;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FMetaScanner;
import com.alibaba.wasp.meta.FMetaScanner.MetaScannerVisitor;
import com.alibaba.wasp.meta.FMetaScanner.MetaScannerVisitorBase;
import com.alibaba.wasp.meta.FMetaUtil;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.FTable.TableType;
import com.alibaba.wasp.meta.Index;
import com.alibaba.wasp.meta.StorageCleanChore;
import com.alibaba.wasp.monitoring.MonitoredTask;
import com.alibaba.wasp.monitoring.TaskMonitor;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerReportRequest;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerReportResponse;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerStartupRequest;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerStartupResponse;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.ReportRSFatalErrorRequest;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.ReportRSFatalErrorResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.AssignEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.AssignEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.BalanceRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.BalanceResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.CreateIndexRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.CreateIndexResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.CreateTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.CreateTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DeleteTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DeleteTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DisableTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DisableTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DropIndexRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DropIndexResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.EnableTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.EnableTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.FetchEntityGroupSizeRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.FetchEntityGroupSizeResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupWithScanRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupWithScanResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupsRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupsResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetTableEntityGroupsRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetTableEntityGroupsResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.IsTableAvailableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.IsTableAvailableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.IsTableLockedRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.IsTableLockedResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.ModifyTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.ModifyTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MoveEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MoveEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.OfflineEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.OfflineEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.SetBalancerRunningRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.SetBalancerRunningResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.SetTableStateRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.SetTableStateResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.ShutdownRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.ShutdownResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.StopMasterRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.StopMasterResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.TableExistsRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.TableExistsResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.TruncateTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.TruncateTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.UnassignEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.UnassignEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.UnlockTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.UnlockTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetClusterStatusRequest;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetClusterStatusResponse;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusRequest;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusResponse;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsRequest;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsResponse;
import com.alibaba.wasp.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import com.alibaba.wasp.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import com.alibaba.wasp.protobuf.generated.WaspProtos.EntityGroupSpecifier.EntityGroupSpecifierType;
import com.alibaba.wasp.protobuf.generated.WaspProtos.ServerLoadProtos;
import com.alibaba.wasp.protobuf.generated.WaspProtos.StringStringPair;
import com.alibaba.wasp.util.InfoServer;
import com.alibaba.wasp.zookeeper.ClusterStatusTracker;
import com.alibaba.wasp.zookeeper.DrainingServerTracker;
import com.alibaba.wasp.zookeeper.FServerTracker;
import com.alibaba.wasp.zookeeper.LoadBalancerTracker;
import com.alibaba.wasp.zookeeper.ZKClusterId;
import com.alibaba.wasp.zookeeper.ZKUtil;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class FMaster extends HasThread implements FMasterAdminProtocol,
    FServerStatusProtocol, FMasterMonitorProtocol, FMasterServices, Server {
  private static final Log LOG = LogFactory.getLog(FMaster.class.getName());

  /**
   * MASTER is name of the webapp and the attribute name used stuffing this
   * instance into web context.
   **/
  public static final String MASTER = "master";

  private ClusterId clusterId;

  /** The configuration for the Master **/
  private final Configuration conf;

  /** server for the web ui **/
  private InfoServer infoServer;

  /** Our zk client. **/
  private ZooKeeperWatcher zooKeeper;

  /** Manager and zk listener for master election. **/
  private ActiveMasterManager activeMasterManager;

  /** FServer tracker **/
  private FServerTracker fserverTracker;

  /** Draining fserver tracker **/
  private DrainingServerTracker drainingServerTracker;

  /** Tracker for load balancer state **/
  private LoadBalancerTracker loadBalancerTracker;

  /** RPC server for the FMaster **/
  private final RpcServer rpcServer;

  /** Table Lock Manager **/
  private TableLockManager tableLockManager;

  /**
   * Set after we've called WaspServer#openServer and ready to receive RPCs. Set
   * back to false after we stop rpcServer. Used by tests.
   **/
  private volatile boolean rpcServerOpen = false;

  /**
   * This servers address.
   */
  private final InetSocketAddress isa;

  // Metrics for the FMaster
  private final MetricsMaster metricsMaster;

  // TODO Metrics for the FMaster
  // private final MasterMetrics metrics;

  // server manager to deal with fserver info
  private FServerManager serverManager;

  // manager of assignment nodes in zookeeper
  AssignmentManager assignmentManager;
  // Cluster status zk tracker and local setter
  private ClusterStatusTracker clusterStatusTracker;

  // This flag is for stopping this Master instance. Its set when we are
  // stopping or aborting
  private volatile boolean stopped = false;
  // Set on abort -- usually failure of our zk session.
  private volatile boolean abort = false;
  // flag set after we become the active master (used for testing)
  private volatile boolean isActiveMaster = false;
  // flag set after we complete initialization once active (used for testing)
  private volatile boolean initialized = false;

  // Instance of the wasp executor service.
  ExecutorService executorService;

  private LoadBalancer balancer;
  private Thread balancerChore;
  private Thread clusterStatusChore;
  private StorageCleanChore storageCleaner;

  private CatalogJanitor catalogJanitorChore;

  private final ServerName serverName;

  // Time stamps for when a hmaster was started and when it became active
  private long masterStartTime;
  private long masterActiveTime;

  /** time interval for emitting metrics values */
  private final int msgInterval;

  /**
   * MX Bean for MasterInfo
   */
  private ObjectName mxBean = null;

  /**
   * Initializes the FMaster. The steps are as follows:
   * <p>
   * <ol>
   * <li>Initialize FMaster RPC and address
   * <li>Connect to ZooKeeper.
   * </ol>
   * <p>
   * Remaining steps of initialization occur in {@link #run()} so that they run
   * in their own thread rather than within the context of the constructor.
   * 
   * @throws InterruptedException
   */
  public FMaster(final Configuration conf) throws IOException, KeeperException,
      InterruptedException {
    this.conf = new Configuration(conf);
    // Set how many times to retry talking to another server over HConnection.
    FConnectionManager.setServerSideFConnectionRetries(this.conf, LOG);
    // Server to handle client requests.
    String hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
        conf.get("wasp.master.dns.interface", "default"),
        conf.get("wasp.master.dns.nameserver", "default")));
    int port = conf.getInt(FConstants.MASTER_PORT,
        FConstants.DEFAULT_MASTER_PORT);
    // Creation of a ISA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }
    this.rpcServer = WaspRPC.getServer(FMaster.class, this, new Class<?>[] {
        FMasterMonitorProtocol.class, FMasterAdminProtocol.class,
        FServerStatusProtocol.class, FMetaServerProtocol.class },
        initialIsa.getHostName(), // BindAddress is IP we got for this server.
        initialIsa.getPort(), conf);
    // Set our address.
    this.isa = this.rpcServer.getListenerAddress();
    this.serverName = new ServerName(this.isa.getHostName(),
        this.isa.getPort(), System.currentTimeMillis());

    // set the thread name now we have an address
    setName(MASTER + "-" + this.serverName.toString());

    this.zooKeeper = new ZooKeeperWatcher(conf, MASTER + ":" + isa.getPort(),
        this, true);

    // metrics interval: using the same property as fserver.
    this.msgInterval = conf.getInt("wasp.fserver.msginterval", 3 * 1000);

    this.metricsMaster = new MetricsMaster(new MetricsMasterWrapperImpl(this));
  }

  /**
   * Stall startup if we are designated a backup master; i.e. we want someone
   * else to become the master before proceeding.
   * 
   * @param c
   * @param amm
   * @throws InterruptedException
   */
  private static void stallIfBackupMaster(final Configuration c,
      final ActiveMasterManager amm) throws InterruptedException {
    // If we're a backup master, stall until a primary to writes his address
    if (!c.getBoolean(FConstants.MASTER_TYPE_BACKUP,
        FConstants.DEFAULT_MASTER_TYPE_BACKUP)) {
      return;
    }
    LOG.debug("FMaster started in backup mode.  "
        + "Stalling until master znode is written.");
    // This will only be a minute or so while the cluster starts up,
    // so don't worry about setting watches on the parent znode
    while (!amm.isActiveMaster()) {
      LOG.debug("Waiting for master address ZNode to be written "
          + "(Also watching cluster state node)");
      Thread.sleep(c.getInt("zookeeper.session.timeout", 180 * 1000));
    }

  }

  MetricsMaster getMetrics() {
    return metricsMaster;
  }

  /**
   * Main processing loop for the FMaster.
   * <ol>
   * <li>Block until becoming active master
   * <li>Finish initialization via finishInitialization(MonitoredTask)
   * <li>Enter loop until we are stopped
   * <li>Stop services and perform cleanup once stopped
   * </ol>
   */
  @Override
  public void run() {
    MonitoredTask startupStatus = TaskMonitor.get().createStatus(
        "Master startup");
    startupStatus.setDescription("Master startup");
    masterStartTime = System.currentTimeMillis();
    try {
      /*
       * Block on becoming the active master.
       * 
       * We race with other masters to write our address into ZooKeeper. If we
       * succeed, we are the primary/active master and finish initialization.
       * 
       * If we do not succeed, there is another active master and we should now
       * wait until it dies to try and become the next active master. If we do
       * not succeed on our first attempt, this is no longer a cluster startup.
       */
      becomeActiveMaster(startupStatus);

      // We are either the active master or we were asked to shutdown
      if (!this.stopped) {
        finishInitialization(startupStatus, false);
        loop();
      }
    } catch (Throwable t) {
      abort("Unhandled exception. Starting shutdown.", t);
    } finally {
      startupStatus.cleanup();

      stopChores();
      // Wait for all the remaining fservers to report in IFF we were
      // running a cluster shutdown AND we were NOT aborting.
      if (!this.abort && this.serverManager != null
          && this.serverManager.isClusterShutdown()) {
        this.serverManager.letFServersShutdown();
      }
      stopServiceThreads();
      // Stop services started for both backup and active masters
      if (this.activeMasterManager != null)
        this.activeMasterManager.stop();
      if (this.serverManager != null)
        this.serverManager.stop();
      if (this.assignmentManager != null)
        this.assignmentManager.stop();
      this.zooKeeper.close();
    }
    LOG.info("FMaster main thread exiting");
  }

  /**
   * Try becoming active master.
   * 
   * @param startupStatus
   * @return True if we could successfully become the active master.
   * @throws InterruptedException
   */
  private boolean becomeActiveMaster(MonitoredTask startupStatus)
      throws InterruptedException {
    this.activeMasterManager = new ActiveMasterManager(zooKeeper,
        this.serverName, this);
    this.zooKeeper.registerListener(activeMasterManager);
    stallIfBackupMaster(this.conf, this.activeMasterManager);

    // The ClusterStatusTracker is setup before the other
    // ZKBasedSystemTrackers because it's needed by the activeMasterManager
    // to check if the cluster should be shutdown.
    this.clusterStatusTracker = new ClusterStatusTracker(getZooKeeper(), this);
    this.clusterStatusTracker.start();
    return this.activeMasterManager.blockUntilBecomingActiveMaster(
        startupStatus, this.clusterStatusTracker);
  }

  /**
   * Initialize all ZK based system trackers.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  private void initializeZKBasedSystemTrackers() throws IOException,
      InterruptedException, KeeperException {
    this.balancer = LoadBalancerFactory.getLoadBalancer(conf);
    this.loadBalancerTracker = new LoadBalancerTracker(zooKeeper, this);
    this.loadBalancerTracker.start();
    this.assignmentManager = new AssignmentManager(this, serverManager,
        this.balancer, this.executorService, this.metricsMaster);
    zooKeeper.registerListenerFirst(assignmentManager);

    this.fserverTracker = new FServerTracker(zooKeeper, this,
        this.serverManager);
    this.fserverTracker.start();

    this.drainingServerTracker = new DrainingServerTracker(zooKeeper, this,
        this.serverManager);
    this.drainingServerTracker.start();

    this.tableLockManager = new TableLockManager();

    // Set the cluster as up. If new FSs, they'll be waiting on this before
    // going ahead with their startup.
    boolean wasUp = this.clusterStatusTracker.isClusterUp();
    if (!wasUp)
      this.clusterStatusTracker.setClusterUp();

    LOG.info("Server active/primary master; "
        + this.serverName
        + ", sessionid=0x"
        + Long.toHexString(this.zooKeeper.getRecoverableZooKeeper()
            .getSessionId()) + ", cluster-up flag was=" + wasUp);
  }

  // Check if we should stop every 100ms
  private Sleeper stopSleeper = new Sleeper(100, this);

  private void loop() {
    long lastMsgTs = 0l;
    long now = 0l;
    while (!this.stopped) {
      now = System.currentTimeMillis();
      if ((now - lastMsgTs) >= this.msgInterval) {
        doMetrics();
        lastMsgTs = System.currentTimeMillis();
      }
      stopSleeper.sleep();
    }
  }

  /**
   * Emit the FMaster metrics, such as entityGroup in transition metrics.
   * Surrounding in a try block just to be sure metrics doesn't abort FMaster.
   */
  private void doMetrics() {
    try {
      this.assignmentManager.updateEntityGroupsInTransitionMetrics();
    } catch (Throwable e) {
      LOG.error("Couldn't update metrics: " + e.getMessage());
    }
  }

  /**
   * Finish initialization of FMaster after becoming the primary master.
   * 
   * <ol>
   * <li>Initialize master components - server manager, assignment manager,
   * fserver tracker, etc</li>
   * <li>Start necessary service threads - rpc server, info server, executor
   * services, etc</li>
   * <li>Set cluster as UP in ZooKeeper</li>
   * <li>Wait for FServers to check-in</li>
   * <li>
   * <li>Handle either fresh cluster start or master failover</li>
   * </ol>
   * 
   * @param masterRecovery
   * 
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  private void finishInitialization(MonitoredTask status, boolean masterRecovery)
      throws IOException, InterruptedException, KeeperException {

    isActiveMaster = true;

    // init cluster ID from zk
    status.setStatus("Publishing Cluster ID in ZooKeeper");
    this.clusterId = ZKClusterId.readClusterIdZNode(this.zooKeeper);
    if (this.clusterId == null) {
      this.clusterId = new ClusterId();
      ZKClusterId.setClusterId(this.zooKeeper, this.clusterId);
    }

    /*
     * We are active master now... go initialize components we need to run.
     * Note, there may be dross in zk from previous runs; it'll get addressed
     * below after we determine if cluster startup or failover.
     */
    this.masterActiveTime = System.currentTimeMillis();

    if (!masterRecovery) {
      this.executorService = new ExecutorService(getServerName().toString());
      this.serverManager = createServerManager(this, this);
    }

    // check the FMeta if we should create and init it
    FMetaUtil.checkAndInit(conf);

    status.setStatus("Initializing ZK system trackers");
    initializeZKBasedSystemTrackers();

    if (!masterRecovery) {
      // start up all service threads.
      status.setStatus("Initializing master service threads");
      startServiceThreads();
    }

    // Wait for fservers to report in.
    this.serverManager.waitForFServers(status);
    // Check zk for fservers that are up but didn't register
    for (ServerName sn : this.fserverTracker.getOnlineServers()) {
      if (!this.serverManager.isServerOnline(sn)) {
        // Not registered; add it.
        LOG.info("Registering server found up in zk but who has not yet "
            + "reported in: " + sn);
        this.serverManager.recordNewServer(sn, ServerLoad.EMPTY_SERVERLOAD);
      }
    }

    if (!masterRecovery) {
      this.assignmentManager.startTimeOutMonitor();
    }

    this.balancer.setMasterServices(this);
    // Fix up assignment manager status
    status.setStatus("Starting assignment manager");
    this.assignmentManager.joinCluster();

    this.balancer.setClusterStatus(getClusterStatus());

    // Fixing up missing daughters if any
    status.setStatus("Fixing up missing daughters");
    fixupDaughters(status);

    if (!masterRecovery) {
      // Start balancer and meta catalog janitor after meta and entityGroups
      // have
      // been assigned.
      status.setStatus("Starting balancer and catalog janitor");
      this.clusterStatusChore = getAndStartClusterStatusChore(this);
      this.balancerChore = getAndStartBalancerChore(this);
      this.catalogJanitorChore = new CatalogJanitor(this, this);
      startCatalogJanitorChore();
    }

    status.markComplete("Initialization successful");
    LOG.info("Master has completed initialization");
    initialized = true;
    // clear the dead servers with same host name and port of online server
    // because we are not
    // removing dead server with same hostname and port of rs which is trying to
    // check in before
    // master initialization.
    this.serverManager.clearDeadServersWithSameHostNameAndPortOfOnlineServer();
  }

  /**
   * Useful for testing purpose also where we have master restart scenarios.
   */
  protected void startCatalogJanitorChore() {
    Threads.setDaemonThreadRunning(catalogJanitorChore.getThread());
  }

  /**
   * Create a {@link FServerManager} instance.
   * 
   * @param master
   * @param services
   * @return An instance of {@link FServerManager}
   * @throws ZooKeeperConnectionException
   * @throws IOException
   */
  FServerManager createServerManager(final Server master,
      final FMasterServices services) throws IOException {
    // We put this out here in a method so can do a Mockito.spy and stub it out
    // w/ a mocked up ServerManager.
    return new FServerManager(master, services);
  }

  void fixupDaughters(final MonitoredTask status) throws IOException {
    Map<EntityGroupInfo, Result> offlineSplitParents = FMetaReader
        .getOfflineSplitParents(this.conf);
    // Now work on our list of found parents. See if any we can clean up.
    int fixups = 0;
    for (Map.Entry<EntityGroupInfo, Result> e : offlineSplitParents.entrySet()) {
      ServerName sn = EntityGroupInfo.getServerName(e.getValue());
      if (!serverManager.isServerDead(sn)) { // Otherwise, let SSH take care of
                                             // it
        fixups += ServerShutdownHandler.fixupDaughters(e.getValue(),
            assignmentManager, this.conf);
      }
    }
    if (fixups != 0) {
      LOG.info("Scanned the catalog and fixed up " + fixups
          + " missing daughter entityGroup(s)");
    }
  }

  public long getProtocolVersion(String protocol, long clientVersion) {
    if (FMasterMonitorProtocol.class.getName().equals(protocol)) {
      return FMasterMonitorProtocol.VERSION;
    } else if (FMasterAdminProtocol.class.getName().equals(protocol)) {
      return FMasterAdminProtocol.VERSION;
    } else if (FServerStatusProtocol.class.getName().equals(protocol)) {
      return FServerStatusProtocol.VERSION;
    } else if (FMetaServerProtocol.class.getName().equals(protocol)) {
      return FMetaServerProtocol.VERSION;
    }
    // unknown protocol
    LOG.warn("Version requested for unimplemented protocol: " + protocol);
    return -1;
  }

  /** @return InfoServer object. Maybe null. */
  public InfoServer getInfoServer() {
    return this.infoServer;
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  @Override
  public FServerManager getFServerManager() {
    return this.serverManager;
  }

  @Override
  public ExecutorService getExecutorService() {
    return this.executorService;
  }

  @Override
  public TableLockManager getTableLockManager() {
    return this.tableLockManager;
  }

  /**
   * Get the ZK wrapper object - needed by master_jsp.java
   * 
   * @return the zookeeper wrapper
   */
  public ZooKeeperWatcher getZooKeeperWatcher() {
    return this.zooKeeper;
  }

  /*
   * Start up all services. If any of these threads gets an unhandled exception
   * then they just die with a logged message. This should be fine because in
   * general, we do not expect the master to get such unhandled exceptions as
   * OOMEs; it should be lightly loaded. See what FServer does if need to
   * install an unexpected exception handler.
   */
  void startServiceThreads() throws IOException {

    // Start the executor service pools
    this.executorService.startExecutorService(
        ExecutorType.MASTER_OPEN_ENTITYGROUP,
        conf.getInt("wasp.master.executor.openentitygroup.threads", 5));
    this.executorService.startExecutorService(
        ExecutorType.MASTER_CLOSE_ENTITYGROUP,
        conf.getInt("wasp.master.executor.closeentitygroup.threads", 5));
    this.executorService.startExecutorService(
        ExecutorType.MASTER_SERVER_OPERATIONS,
        conf.getInt("wasp.master.executor.serverops.threads", 3));

    // We depend on there being only one instance of this executor running
    // at a time. To do concurrency, would need fencing of enable/disable of
    // tables.
    this.executorService.startExecutorService(
        ExecutorType.MASTER_TABLE_OPERATIONS, 1);

    // Start storage table cleaner thread
   String n = Thread.currentThread().getName();
   int cleanerInterval = conf.getInt("wasp.master.cleaner.interval", 60 * 60 * 1000);
   this.storageCleaner =
      new StorageCleanChore(cleanerInterval,
         this, conf, AbstractMetaService.getService(conf));
         Threads.setDaemonThreadRunning(storageCleaner.getThread(), n + ".storageCleaner");


    // Put up info server.
    int port = this.conf.getInt(FConstants.MASTER_INFO_PORT, 60080);
    if (port >= 0) {
      String a = this.conf.get("wasp.master.info.bindAddress", "0.0.0.0");
      this.infoServer = new InfoServer(MASTER, a, port, false, this.conf);
      this.infoServer.addServlet("status", "/master-status",
          FMasterStatusServlet.class);
      this.infoServer.addServlet("dump", "/dump", FMasterDumpServlet.class);
      this.infoServer.setAttribute(MASTER, this);
      this.infoServer.start();
    }

    // Start allowing requests to happen.
    this.rpcServer.openServer();
    this.rpcServerOpen = true;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Started service threads");
    }
  }

  /**
   * Use this when trying to figure when its ok to send in rpcs. Used by tests.
   * 
   * @return True if we have successfully run {@link WaspServer#openServer()}
   */
  boolean isRpcServerOpen() {
    return this.rpcServerOpen;
  }

  private void stopServiceThreads() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping service threads");
    }
    if (this.rpcServer != null)
      this.rpcServer.stop();
    this.rpcServerOpen = false;

    // Clean up and close up shop
    if (this.storageCleaner!= null) this.storageCleaner.interrupt();

    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    if (this.executorService != null)
      this.executorService.shutdown();
  }

  private static Thread getAndStartClusterStatusChore(FMaster master) {
    if (master == null || master.balancer == null) {
      return null;
    }
    Chore chore = new ClusterStatusChore(master, master.balancer);
    return Threads.setDaemonThreadRunning(chore.getThread());
  }

  private static Thread getAndStartBalancerChore(final FMaster master) {
    // Start up the load balancer chore
    Chore chore = new BalancerChore(master);
    return Threads.setDaemonThreadRunning(chore.getThread());
  }

  private void stopChores() {
    if (this.balancerChore != null) {
      this.balancerChore.interrupt();
    }
    if (this.clusterStatusChore != null) {
      this.clusterStatusChore.interrupt();
    }
    if (this.catalogJanitorChore != null) {
      this.catalogJanitorChore.interrupt();
    }
  }

  @Override
  public FServerStartupResponse fServerStartup(RpcController controller,
      FServerStartupRequest request) throws ServiceException {
    // Register with server manager
    try {
      InetAddress ia = getRemoteInetAddress(request.getPort(),
          request.getServerStartCode());
      ServerName sn = this.serverManager.fserverStartup(ia, request.getPort(),
          request.getServerStartCode(), request.getServerCurrentTime());

      // Send back some config info
      FServerStartupResponse.Builder resp = createConfigurationSubset();
      StringStringPair.Builder entry = StringStringPair.newBuilder()
          .setName(FConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)
          .setValue(sn.getHostname());
      resp.addMapEntries(entry.build());
      return resp.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /**
   * @return Get remote side's InetAddress
   * @throws UnknownHostException
   */
  InetAddress getRemoteInetAddress(final int port, final long serverStartCode)
      throws UnknownHostException {
    // Do it out here in its own little method so can fake an address when
    // mocking up in tests.
    return NettyServer.getRemoteAddress().getAddress();
  }

  /**
   * @return Subset of configuration to pass initializing FServers. e.g. hbase
   *         address
   */
  protected FServerStartupResponse.Builder createConfigurationSubset() {
    FServerStartupResponse.Builder resp = addConfig(
        FServerStartupResponse.newBuilder(), FConstants.ZOOKEEPER_QUORUM);
    return addConfig(resp, FConstants.ZOOKEEPER_ZNODE_PARENT);
  }

  private FServerStartupResponse.Builder addConfig(
      final FServerStartupResponse.Builder resp, final String key) {
    StringStringPair.Builder entry = StringStringPair.newBuilder().setName(key)
        .setValue(this.conf.get(key));
    resp.addMapEntries(entry.build());
    return resp;
  }

  @Override
  public FServerReportResponse fServerReport(RpcController controller,
      FServerReportRequest request) throws ServiceException {
    try {
      ServerLoadProtos sl = request.getLoad();
      this.serverManager.fserverReport(
          ProtobufUtil.toServerName(request.getServer()), new ServerLoad(sl));
      // Up our metrics.
      if (sl != null && this.metricsMaster != null) {
        this.metricsMaster.incrementRequests(sl.getTotalNumberOfRequests());
      }
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }

    return FServerReportResponse.newBuilder().build();
  }

  @Override
  public ReportRSFatalErrorResponse reportRSFatalError(
      RpcController controller, ReportRSFatalErrorRequest request)
      throws ServiceException {
    String errorText = request.getErrorMessage();
    ServerName sn = ProtobufUtil.toServerName(request.getServer());
    String msg = "FServer " + sn.toString() + " reported a fatal error:\n"
        + errorText;
    LOG.error(msg);
    return ReportRSFatalErrorResponse.newBuilder().build();
  }

  /**
   * @return Maximum time we should run balancer for
   */
  private int getBalancerCutoffTime() {
    int balancerCutoffTime = getConfiguration().getInt(
        "wasp.balancer.max.balancing", -1);
    if (balancerCutoffTime == -1) {
      // No time period set so create one -- do half of balancer period.
      int balancerPeriod = getConfiguration().getInt("wasp.balancer.period",
          300000);
      balancerCutoffTime = balancerPeriod / 2;
      // If nonsense period, set it to balancerPeriod
      if (balancerCutoffTime <= 0)
        balancerCutoffTime = balancerPeriod;
    }
    return balancerCutoffTime;
  }

  public boolean balance() throws IOException {
    // if master not initialized, don't run balancer.
    if (!this.initialized) {
      LOG.debug("Master has not been initialized, don't run balancer.");
      return false;
    }
    // If balance not true, don't run balancer.
    if (!this.loadBalancerTracker.isBalancerOn())
      return false;
    // Do this call outside of synchronized block.
    int maximumBalanceTime = getBalancerCutoffTime();
    long cutoffTime = System.currentTimeMillis() + maximumBalanceTime;
    boolean balancerRan;
    synchronized (this.balancer) {
      // Only allow one balance run at at time.
      if (this.assignmentManager.getEntityGroupStates()
          .isEntityGroupsInTransition()) {
        Map<String, EntityGroupState> entityGroupsInTransition = this.assignmentManager
            .getEntityGroupStates().getEntityGroupsInTransition();
        LOG.debug("Not running balancer because "
            + entityGroupsInTransition.size()
            + " entityGroup(s) in transition: "
            + org.apache.commons.lang.StringUtils.abbreviate(
                entityGroupsInTransition.toString(), 256));
        return false;
      }
      if (this.serverManager.areDeadServersInProgress()) {
        LOG.debug("Not running balancer because processing dead fserver(s): "
            + this.serverManager.getDeadServers());
        return false;
      }

      Map<String, Map<ServerName, List<EntityGroupInfo>>> assignmentsByTable = this.assignmentManager
          .getEntityGroupStates().getAssignmentsByTable();

      List<EntityGroupPlan> plans = new ArrayList<EntityGroupPlan>();
      // Give the balancer the current cluster state.
      this.balancer.setClusterStatus(getClusterStatus());
      for (Map<ServerName, List<EntityGroupInfo>> assignments : assignmentsByTable
          .values()) {
        List<EntityGroupPlan> partialPlans = this.balancer
            .balanceCluster(assignments);
        if (partialPlans != null)
          plans.addAll(partialPlans);
      }
      int rpCount = 0; // number of EntityGroupPlans balanced so far
      long totalRegPlanExecTime = 0;
      balancerRan = plans != null;
      if (plans != null && !plans.isEmpty()) {
        for (EntityGroupPlan plan : plans) {
          LOG.info("balance " + plan);
          long balStartTime = System.currentTimeMillis();
          this.assignmentManager.balance(plan);
          totalRegPlanExecTime += System.currentTimeMillis() - balStartTime;
          rpCount++;
          if (rpCount < plans.size() &&
          // if performing next balance exceeds cutoff time, exit the loop
              (System.currentTimeMillis() + (totalRegPlanExecTime / rpCount)) > cutoffTime) {
            LOG.debug("No more balancing till next balance run; maximumBalanceTime="
                + maximumBalanceTime);
            break;
          }
        }
      }
    }
    return balancerRan;
  }

  @Override
  public BalanceResponse balance(RpcController c, BalanceRequest request)
      throws ServiceException {
    try {
      return BalanceResponse.newBuilder().setBalancerRan(balance()).build();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }

  enum BalanceSwitchMode {
    SYNC,
      ASYNC
  }

  /**
   * Assigns balancer switch according to BalanceSwitchMode
   * 
   * @param b
   *          new balancer switch
   * @param mode
   *          BalanceSwitchMode
   * @return old balancer switch
   */
  public boolean switchBalancer(final boolean b, BalanceSwitchMode mode)
      throws IOException {
    boolean oldValue = this.loadBalancerTracker.isBalancerOn();
    boolean newValue = b;
    try {
      try {
        if (mode == BalanceSwitchMode.SYNC) {
          synchronized (this.balancer) {
            this.loadBalancerTracker.setBalancerOn(newValue);
          }
        } else {
          this.loadBalancerTracker.setBalancerOn(newValue);
        }
      } catch (KeeperException ke) {
        throw new IOException(ke);
      }
      LOG.info("BalanceSwitch=" + newValue);
    } catch (IOException ioe) {
      LOG.warn("Error flipping balance switch", ioe);
    }
    return oldValue;
  }

  public boolean synchronousBalanceSwitch(final boolean b) throws IOException {
    return switchBalancer(b, BalanceSwitchMode.SYNC);
  }

  public boolean balanceSwitch(final boolean b) throws IOException {
    return switchBalancer(b, BalanceSwitchMode.ASYNC);
  }

  @Override
  public SetBalancerRunningResponse setBalancerRunning(
      RpcController controller, SetBalancerRunningRequest req)
      throws ServiceException {
    try {
      boolean prevValue = (req.getSynchronous()) ? synchronousBalanceSwitch(req
          .getOn()) : balanceSwitch(req.getOn());
      return SetBalancerRunningResponse.newBuilder()
          .setPrevBalanceValue(prevValue).build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public MoveEntityGroupResponse moveEntityGroup(RpcController controller,
      MoveEntityGroupRequest req) throws ServiceException {
    final byte[] encodedEntityGroupName = req.getEntityGroup().getValue()
        .toByteArray();
    EntityGroupSpecifierType type = req.getEntityGroup().getType();
    final byte[] destServerName = (req.hasDestServerName()) ? Bytes
        .toBytes(ProtobufUtil.toServerName(req.getDestServerName())
            .getServerName()) : null;
    MoveEntityGroupResponse megr = MoveEntityGroupResponse.newBuilder().build();

    if (type != EntityGroupSpecifierType.ENCODED_ENTITYGROUP_NAME) {
      LOG.warn("moveEntityGroup specifier type: expected: "
          + EntityGroupSpecifierType.ENCODED_ENTITYGROUP_NAME + " actual: "
          + type);
    }
    EntityGroupState entityGroupState = assignmentManager
        .getEntityGroupStates().getEntityGroupState(
            Bytes.toString(encodedEntityGroupName));
    if (entityGroupState == null) {
      throw new ServiceException(new UnknownEntityGroupException(
          Bytes.toStringBinary(encodedEntityGroupName)));
    }

    EntityGroupInfo egInfo = entityGroupState.getEntityGroup();
    ServerName dest;
    if (destServerName == null || destServerName.length == 0) {
      LOG.info("Passed destination servername is null/empty so "
          + "choosing a server at random");
      final List<ServerName> destServers = this.serverManager
          .createDestinationServersList(entityGroupState.getServerName());
      dest = balancer.randomAssignment(egInfo, destServers);
    } else {
      dest = new ServerName(Bytes.toString(destServerName));
      if (dest.equals(entityGroupState.getServerName())) {
        LOG.debug("Skipping move of entityGroup "
            + egInfo.getEntityGroupNameAsString()
            + " because entityGroup already assigned to the same server "
            + dest + ".");
        return megr;
      }
    }

    // Now we can do the move
    EntityGroupPlan egPlan = new EntityGroupPlan(egInfo,
        entityGroupState.getServerName(), dest);

    LOG.info("Added move plan " + egPlan + ", running balancer");
    this.assignmentManager.balance(egPlan);
    return megr;
  }

  @Override
  public CreateTableResponse createTable(RpcController controller,
      CreateTableRequest req) throws ServiceException {
    FTable tableDescriptor = FTable.convert(req.getTableSchema());
    byte[][] splitKeys = ProtobufUtil.getSplitKeysArray(req);
    try {
      createTable(tableDescriptor, splitKeys);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return CreateTableResponse.newBuilder().build();
  }

  @Override
  public DeleteTableResponse deleteTable(RpcController controller,
      DeleteTableRequest request) throws ServiceException {
    byte[] tableName = request.getTableName().toByteArray();
    try {
      checkInitialized();
      this.executorService.submit(new DeleteTableHandler(tableName, this,
          this.assignmentManager));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return DeleteTableResponse.newBuilder().build();
  }

  @Override
  public EnableTableResponse enableTable(RpcController controller,
      EnableTableRequest request) throws ServiceException {
    byte[] tableName = request.getTableName().toByteArray();
    try {
      checkInitialized();
      this.executorService.submit(new EnableTableHandler(this,this,
          this.assignmentManager, tableName, false));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return EnableTableResponse.newBuilder().build();
  }

  @Override
  public DisableTableResponse disableTable(RpcController controller,
      DisableTableRequest request) throws ServiceException {
    byte[] tableName = request.getTableName().toByteArray();
    try {
      checkInitialized();
      this.executorService.submit(new DisableTableHandler(this,
          this.assignmentManager, tableName, this, false));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return DisableTableResponse.newBuilder().build();
  }

  @Override
  public ModifyTableResponse modifyTable(RpcController controller,
      ModifyTableRequest req) throws ServiceException {
    final byte[] tableName = req.getTableName().toByteArray();
    FTable table = FTable.convert(req.getTableSchema());
    try {
      checkInitialized();
      TableEventHandler tblHandle = new ModifyTableHandler(tableName, table,
          this, this);
      this.executorService.submit(tblHandle);
      tblHandle.waitForPersist();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return ModifyTableResponse.newBuilder().build();
  }

  @Override
  public TruncateTableResponse truncateTable(RpcController controller,
      TruncateTableRequest request) throws ServiceException {
    final byte[] tableName = request.getTableName().toByteArray();
    try {
      checkInitialized();
      this.executorService.submit(new TruncateTableHandler(tableName, this,
          this.assignmentManager));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return TruncateTableResponse.newBuilder().build();
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MasterAdminService.BlockingInterface#createIndex(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.MasterAdminProtos.CreateIndexRequest)
   */
  @Override
  public CreateIndexResponse createIndex(RpcController controller,
      CreateIndexRequest request) throws ServiceException {
    Index index = Index.convert(request.getIndexSchema());
    try {
      checkInitialized();
      CreateIndexHandler ciHandle = new CreateIndexHandler(Bytes.toBytes(index
          .getDependentTableName()), index, this, this,
          EventHandler.EventType.C_M_CREATE_INDEX);
      this.executorService.submit(ciHandle);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return CreateIndexResponse.newBuilder().build();
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MasterAdminService.BlockingInterface#deleteIndex(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DropIndexRequest)
   */
  @Override
  public DropIndexResponse deleteIndex(RpcController controller,
      DropIndexRequest request) throws ServiceException {
    try {
      checkInitialized();
      DeleteIndexHandler ciHandle = new DeleteIndexHandler(
          Bytes.toBytes(request.getTableName()), request.getIndexName(), this,
          this, EventHandler.EventType.C_M_DELETE_INDEX);
      this.executorService.submit(ciHandle);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return DropIndexResponse.newBuilder().build();
  }

  public ClusterId getClusterId() {
    return this.clusterId;
  }

  /**
   * @return timestamp in millis when FMaster was started.
   */
  public long getMasterStartTime() {
    return masterStartTime;
  }

  /**
   * @return timestamp in millis when FMaster became the active master.
   */
  public long getMasterActiveTime() {
    return masterActiveTime;
  }

  @Override
  public void abort(final String msg, final Throwable t) {
    if (abortNow(msg, t)) {
      if (t != null)
        LOG.fatal(msg, t);
      else
        LOG.fatal(msg);
      this.abort = true;
      stop("Aborting");
    }
  }

  /**
   * We do the following in a different thread. If it is not completed in time,
   * we will time it out and assume it is not easy to recover.
   * 
   * 1. Create a new ZK session. (since our current one is expired) 2. Try to
   * become a primary master again 3. Initialize all ZK based system trackers.
   * 4. Assign root and meta. (they are already assigned, but we need to update
   * our internal memory state to reflect it) 5. Process any RIT if any during
   * the process of our recovery.
   * 
   * @return True if we could successfully recover from ZK session expiry.
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   * @throws ExecutionException
   */
  private boolean tryRecoveringExpiredZKSession() throws InterruptedException,
      IOException, KeeperException, ExecutionException {

    this.zooKeeper.reconnectAfterExpiration();

    Callable<Boolean> callable = new Callable<Boolean>() {
      public Boolean call() throws InterruptedException, IOException,
          KeeperException {
        MonitoredTask status = TaskMonitor.get().createStatus(
            "Recovering expired ZK session");
        try {
          if (!becomeActiveMaster(status)) {
            return Boolean.FALSE;
          }
          initialized = false;
          finishInitialization(status, true);
          return Boolean.TRUE;
        } finally {
          status.cleanup();
        }
      }
    };

    long timeout = conf
        .getLong("wasp.master.zksession.recover.timeout", 300000);
    java.util.concurrent.ExecutorService executor = Executors
        .newSingleThreadExecutor();
    Future<Boolean> result = executor.submit(callable);
    executor.shutdown();
    if (executor.awaitTermination(timeout, TimeUnit.MILLISECONDS)
        && result.isDone()) {
      Boolean recovered = result.get();
      if (recovered != null) {
        return recovered.booleanValue();
      }
    }
    executor.shutdownNow();
    return false;
  }

  /**
   * Check to see if the current trigger for abort is due to ZooKeeper session
   * expiry, and If yes, whether we can recover from ZK session expiry.
   * 
   * @param msg
   *          Original abort message
   * @param t
   *          The cause for current abort request
   * @return true if we should proceed with abort operation, false other wise.
   */
  private boolean abortNow(final String msg, final Throwable t) {
    if (!this.isActiveMaster) {
      return true;
    }
    if (t != null && t instanceof KeeperException.SessionExpiredException) {
      try {
        LOG.info("Primary Master trying to recover from ZooKeeper session "
            + "expiry.");
        return !tryRecoveringExpiredZKSession();
      } catch (Throwable newT) {
        LOG.error("Primary master encountered unexpected exception while "
            + "trying to recover from ZooKeeper session"
            + " expiry. Proceeding with server abort.", newT);
      }
    }
    return true;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zooKeeper;
  }

  @Override
  public ServerName getServerName() {
    return this.serverName;
  }

  public void shutdown() throws IOException {
    if (mxBean != null) {
      MBeans.unregister(mxBean);
      mxBean = null;
    }
    if (this.assignmentManager != null)
      this.assignmentManager.shutdown();
    if (this.serverManager != null)
      this.serverManager.shutdownCluster();
    try {
      if (this.clusterStatusTracker != null) {
        this.clusterStatusTracker.setClusterDown();
      }
    } catch (KeeperException e) {
      LOG.error("ZooKeeper exception trying to set cluster as down in ZK", e);
    }
  }

  @Override
  public ShutdownResponse shutdown(RpcController controller,
      ShutdownRequest request) throws ServiceException {
    try {
      shutdown();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return ShutdownResponse.newBuilder().build();
  }

  public void stopMaster() throws IOException {
    stop("Stopped by " + Thread.currentThread().getName());
  }

  @Override
  public StopMasterResponse stopMaster(RpcController controller,
      StopMasterRequest request) throws ServiceException {
    try {
      stopMaster();
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return StopMasterResponse.newBuilder().build();
  }

  @Override
  public void stop(final String why) {
    LOG.info(why);
    this.stopped = true;
    // We wake up the stopSleeper to stop immediately
    stopSleeper.skipSleepCycle();
    // If we are a backup master, we need to interrupt wait
    if (this.activeMasterManager != null) {
      synchronized (this.activeMasterManager.clusterHasActiveMaster) {
        this.activeMasterManager.clusterHasActiveMaster.notifyAll();
      }
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  public boolean isAborted() {
    return this.abort;
  }

  void checkInitialized() throws PleaseHoldException {
    if (!this.initialized) {
      throw new PleaseHoldException("Master is initializing");
    }
  }

  /**
   * Report whether this master is currently the active master or not. If not
   * active master, we are parked on ZK waiting to become active.
   * 
   * This method is used for testing.
   * 
   * @return true if active master, false if not.
   */
  public boolean isActiveMaster() {
    return isActiveMaster;
  }

  /**
   * Report whether this master has completed with its initialization and is
   * ready. If ready, the master is also the active master. A standby master is
   * never ready.
   * 
   * This method is used for testing.
   * 
   * @return true if master is ready to go, false if not.
   */
  public boolean isInitialized() {
    return initialized;
  }

  @Override
  public AssignEntityGroupResponse assignEntityGroup(RpcController controller,
      AssignEntityGroupRequest req) throws ServiceException {
    try {
      final byte[] entityGroupName = req.getEntityGroup().getValue()
          .toByteArray();
      EntityGroupSpecifierType type = req.getEntityGroup().getType();
      AssignEntityGroupResponse aegr = AssignEntityGroupResponse.newBuilder()
          .build();

      checkInitialized();
      if (type != EntityGroupSpecifierType.ENTITYGROUP_NAME) {
        LOG.warn("assignEntityGroup specifier type: expected: "
            + EntityGroupSpecifierType.ENTITYGROUP_NAME + " actual: " + type);
      }
      EntityGroupInfo entityGroupInfo = assignmentManager
          .getEntityGroupStates().getEntityGroupInfo(entityGroupName);
      if (entityGroupInfo == null)
        throw new UnknownEntityGroupException(Bytes.toString(entityGroupName));
      assignmentManager.assign(entityGroupInfo, true, true);
      return aegr;
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  public void assignEntityGroup(EntityGroupInfo egInfo) {
    assignmentManager.assign(egInfo, true);
  }

  @Override
  public UnassignEntityGroupResponse unassignEntityGroup(
      RpcController controller, UnassignEntityGroupRequest req)
      throws ServiceException {
    try {
      final byte[] entityGroupName = req.getEntityGroup().getValue()
          .toByteArray();
      EntityGroupSpecifierType type = req.getEntityGroup().getType();
      final boolean force = req.getForce();
      UnassignEntityGroupResponse uegr = UnassignEntityGroupResponse
          .newBuilder().build();

      checkInitialized();
      if (type != EntityGroupSpecifierType.ENTITYGROUP_NAME) {
        LOG.warn("unassignEntityGroup specifier type: expected: "
            + EntityGroupSpecifierType.ENTITYGROUP_NAME + " actual: " + type);
      }
      Pair<EntityGroupInfo, ServerName> pair = FMetaReader
          .getEntityGroupAndLocation(this.conf, entityGroupName);
      if (pair == null)
        throw new UnknownEntityGroupException(Bytes.toString(entityGroupName));
      EntityGroupInfo egInfo = pair.getFirst();
      if (force) {
        this.assignmentManager.entityGroupOffline(egInfo);
        assignEntityGroup(egInfo);
      } else {
        this.assignmentManager.unassign(egInfo, force);
      }
      return uegr;
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }

  }

  /**
   * Get list of TableDescriptors for requested tables.
   * 
   * @param controller
   *          Unused (set to null).
   * @param req
   *          GetTableDescriptorsRequest that contains: - tableNames: requested
   *          tables, or if empty, all are requested
   * @return GetTableDescriptorsResponse
   * @throws ServiceException
   */
  public GetTableDescriptorsResponse getTableDescriptors(
      RpcController controller, GetTableDescriptorsRequest req)
      throws ServiceException {
    GetTableDescriptorsResponse.Builder builder = GetTableDescriptorsResponse
        .newBuilder();
    if (req.getTableNamesCount() == 0) {
      // request for all Tables
      List<FTable> tables = null;
      try {
        tables = FMetaReader.getAllTables(this.conf);
      } catch (IOException e) {
        LOG.warn("Failed getting all tables", e);
      }
      if (tables != null) {
        for (FTable table : tables) {
          try {
            builder.addTableSchema(ProtobufUtil.convertITableSchema(table));
          } catch (MetaException e) {
            throw new ServiceException(e);
          }
        }
      }
    } else {
      for (String s : req.getTableNamesList()) {
        FTable table = null;
        try {
          table = FMetaReader.getTable(this.conf, s);
        } catch (IOException e) {
          LOG.warn("Failed getting descriptor for " + s, e);
        }
        if (table == null)
          continue;
        try {
          builder.addTableSchema(ProtobufUtil.convertITableSchema(table));
        } catch (MetaException e) {
          throw new ServiceException(e);
        }
      }
    }
    return builder.build();
  }

  /**
   * Compute the average load across all fservers. Currently, this uses a very
   * naive computation - just uses the number of entityGroups being served,
   * ignoring stats about number of requests.
   * 
   * @return the average load
   */
  public double getAverageLoad() {
    if (this.assignmentManager == null) {
      return 0;
    }

    EntityGroupStates entityGroupStates = this.assignmentManager
        .getEntityGroupStates();
    if (entityGroupStates == null) {
      return 0;
    }
    return entityGroupStates.getAverageLoad();
  }

  /**
   * Special method, only used by hbck.
   */
  @Override
  public OfflineEntityGroupResponse offlineEntityGroup(
      RpcController controller, OfflineEntityGroupRequest request)
      throws ServiceException {
    final byte[] entityGroupName = request.getEntityGroup().getValue()
        .toByteArray();
    EntityGroupSpecifierType type = request.getEntityGroup().getType();
    if (type != EntityGroupSpecifierType.ENTITYGROUP_NAME) {
      LOG.warn("moveEntityGroup specifier type: expected: "
          + EntityGroupSpecifierType.ENTITYGROUP_NAME + " actual: " + type);
    }

    try {
      Pair<EntityGroupInfo, ServerName> pair = FMetaReader
          .getEntityGroupAndLocation(this.conf, entityGroupName);
      if (pair == null)
        throw new UnknownEntityGroupException(
            Bytes.toStringBinary(entityGroupName));
      EntityGroupInfo egInfo = pair.getFirst();
      this.assignmentManager.entityGroupOffline(egInfo);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
    return OfflineEntityGroupResponse.newBuilder().build();
  }

  /**
   * Utility for constructing an instance of the passed FMaster class.
   * 
   * @param masterClass
   * @param conf
   * @return FMaster instance.
   */
  public static FMaster constructMaster(Class<? extends FMaster> masterClass,
      final Configuration conf) {
    try {
      Constructor<? extends FMaster> c = masterClass
          .getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (InvocationTargetException ite) {
      Throwable target = ite.getTargetException() != null ? ite
          .getTargetException() : ite;
      if (target.getCause() != null)
        target = target.getCause();
      throw new RuntimeException("Failed construction of Master: "
          + masterClass.toString(), target);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of Master: "
          + masterClass.toString()
          + ((e.getCause() != null) ? e.getCause().getMessage() : ""), e);
    }
  }

  /**
   * @see com.alibaba.wasp.master.FMasterCommandLine
   */
  public static void main(String[] args) {
    new FMasterCommandLine(FMaster.class).doMain(args);
  }

  public ClusterStatus getClusterStatus() {
    // Build Set of backup masters from ZK nodes
    List<String> backupMasterStrings;
    try {
      backupMasterStrings = ZKUtil.listChildrenNoWatch(this.zooKeeper,
          this.zooKeeper.backupMasterAddressesZNode);
    } catch (KeeperException e) {
      LOG.warn(this.zooKeeper.prefix("Unable to list backup servers"), e);
      backupMasterStrings = new ArrayList<String>(0);
    }
    List<ServerName> backupMasters = new ArrayList<ServerName>(
        backupMasterStrings.size());
    for (String s : backupMasterStrings) {
      try {
        byte[] bytes = ZKUtil.getData(this.zooKeeper,
            ZKUtil.joinZNode(this.zooKeeper.backupMasterAddressesZNode, s));
        if (bytes != null) {
          ServerName sn;
          try {
            sn = ServerName.parseFrom(bytes);
          } catch (DeserializationException e) {
            LOG.warn("Failed parse, skipping registering backup server", e);
            continue;
          }
          backupMasters.add(sn);
        }
      } catch (KeeperException e) {
        LOG.warn(this.zooKeeper.prefix("Unable to get information about "
            + "backup servers"), e);
      }
    }
    Collections.sort(backupMasters, new Comparator<ServerName>() {
      public int compare(ServerName s1, ServerName s2) {
        return s1.getServerName().compareTo(s2.getServerName());
      }
    });

    return new ClusterStatus(getClusterId().toString(),
        this.serverManager.getOnlineServers(),
        this.serverManager.getDeadServers(), this.serverName, backupMasters,
        this.assignmentManager.getEntityGroupStates()
            .getEntityGroupsInTransition(),
        this.loadBalancerTracker.isBalancerOn());

  }

  @Override
  public AssignmentManager getAssignmentManager() {
    return this.assignmentManager;
  }

  @Override
  public void checkTableModifiable(byte[] tableName) throws IOException {
    String tableNameStr = Bytes.toString(tableName);
    if (!FMetaReader.tableExists(this.conf, tableNameStr)) {
      throw new TableNotFoundException(tableNameStr);
    }
    if (!getAssignmentManager().getZKTable().isDisabledTable(tableNameStr)) {
      throw new TableNotDisabledException(tableName);
    }
  }

  @Override
  public void createTable(FTable desc, byte[][] splitKeys) throws IOException {
    if (!isMasterRunning()) {
      throw new MasterNotRunningException();
    }

    EntityGroupInfo[] newEntityGroups = getEntityGroupInfos(desc, splitKeys);
    checkInitialized();
    this.executorService.submit(new CreateTableHandler(this, this,
        this.assignmentManager, desc, newEntityGroups));
  }

  private EntityGroupInfo[] getEntityGroupInfos(FTable desc, byte[][] splitKeys) {
    if (desc.getTableType() == TableType.ROOT) {
      EntityGroupInfo[] egInfos = null;
      byte[] tableName = Bytes.toBytes(desc.getTableName());
      if (splitKeys == null || splitKeys.length == 0) {
        egInfos = new EntityGroupInfo[] { new EntityGroupInfo(tableName, null,
            null) };
      } else {
        int numEntityGroups = splitKeys.length + 1;
        egInfos = new EntityGroupInfo[numEntityGroups];
        byte[] startKey = null;
        byte[] endKey = null;
        for (int i = 0; i < numEntityGroups; i++) {
          endKey = (i == splitKeys.length) ? null : splitKeys[i];
          egInfos[i] = new EntityGroupInfo(tableName, startKey, endKey);
          startKey = endKey;
        }
      }
      return egInfos;
    } else if (desc.getTableType() == TableType.CHILD) {
      // Child
      return new EntityGroupInfo[0];
    } else {
      throw new RuntimeException("Table is invalid.");
    }
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MasterAdminService.BlockingInterface#fetchEntityGroupSize(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.MasterAdminProtos.FetchEntityGroupSizeRequest)
   */
  @Override
  public FetchEntityGroupSizeResponse fetchEntityGroupSize(
      RpcController controller, FetchEntityGroupSizeRequest request)
      throws ServiceException {
    final byte[] tableNameBytes = request.getTableName().toByteArray();
    final AtomicInteger actualEgCount = new AtomicInteger(0);
    MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
      @Override
      public boolean processRow(org.apache.hadoop.hbase.client.Result rowResult)
          throws IOException {
        EntityGroupInfo info = EntityGroupInfo.getEntityGroupInfo(rowResult);
        if (info == null) {
          LOG.warn("No serialized EntityGroupInfo in " + rowResult);
          return true;
        }
        if (!(Bytes.equals(info.getTableName(), tableNameBytes))) {
          return false;
        }
        ServerName serverName = EntityGroupInfo.getServerName(rowResult);
        // Make sure that entityGroups are assigned to server
        if (!(info.isOffline() || info.isSplit()) && serverName != null
            && serverName.getHostAndPort() != null) {
          actualEgCount.incrementAndGet();
        }
        return true;
      }
    };
    try {
      FMetaScanner.metaScan(conf, visitor, tableNameBytes);
    } catch (IOException e) {
      LOG.error("Failed fetchEntityGroupSize.", e);
      throw new ServiceException(e);
    }
    FetchEntityGroupSizeResponse.Builder res = FetchEntityGroupSizeResponse
        .newBuilder();
    res.setEgSize(actualEgCount.get());
    return res.build();
  }

  /**
   * Get the number of entityGroups of the table that have been updated by the
   * alter.
   * 
   * @return Pair indicating the number of entityGroups updated Pair.getFirst is
   *         the entityGroups that are yet to be updated Pair.getSecond is the
   *         total number of entityGroups of the table
   * @throws IOException
   */
  @Override
  public GetSchemaAlterStatusResponse getSchemaAlterStatus(
      RpcController controller, GetSchemaAlterStatusRequest req)
      throws ServiceException {
    // currently, we query using the table name on the client side. this
    // may overlap with other table operations or the table operation may
    // have completed before querying this API. We need to refactor to a
    // transaction system in the future to avoid these ambiguities.
    byte[] tableName = req.getTableName().toByteArray();
    try {
      Pair<Integer, Integer> pair = this.assignmentManager
          .getReopenStatus(tableName);
      GetSchemaAlterStatusResponse.Builder ret = GetSchemaAlterStatusResponse
          .newBuilder();
      ret.setYetToUpdateEntityGroups(pair.getFirst());
      ret.setTotalEntityGroups(pair.getSecond());
      return ret.build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public GetClusterStatusResponse getClusterStatus(RpcController controller,
      GetClusterStatusRequest req) throws ServiceException {
    GetClusterStatusResponse.Builder response = GetClusterStatusResponse
        .newBuilder();
    response.setClusterStatus(getClusterStatus().convert());
    return response.build();
  }

  public boolean isMasterRunning() {
    return !isStopped();
  }

  @Override
  public IsMasterRunningResponse isMasterRunning(RpcController c,
      IsMasterRunningRequest req) throws ServiceException {
    return IsMasterRunningResponse.newBuilder()
        .setIsMasterRunning(isMasterRunning()).build();
  }

  /**
   * 
   * @param tableName
   * @param rowKey
   * @return
   * @throws IOException
   */
  public Pair<EntityGroupInfo, ServerName> getTableEntityGroupForRow(
      final byte[] tableName, final byte[] rowKey) throws IOException {
    final AtomicReference<Pair<EntityGroupInfo, ServerName>> result = new AtomicReference<Pair<EntityGroupInfo, ServerName>>(
        null);

    MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
      @Override
      public boolean processRow(Result data) throws IOException {
        if (data == null || data.size() <= 0) {
          return true;
        }
        Pair<EntityGroupInfo, ServerName> pair = EntityGroupInfo
            .getEntityGroupInfoAndServerName(data);
        if (pair == null) {
          return false;
        }
        if (!Bytes.equals(pair.getFirst().getTableName(), tableName)) {
          return false;
        }
        result.set(pair);
        return true;
      }
    };

    FMetaScanner.metaScan(conf, visitor, tableName, rowKey, 1);
    return result.get();
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MasterAdminService.BlockingInterface#getEntityGroup(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupRequest)
   */
  @Override
  public GetEntityGroupResponse getEntityGroup(RpcController controller,
      GetEntityGroupRequest request) throws ServiceException {
    try {
      Pair<EntityGroupInfo, ServerName> pair = FMetaReader.getEntityGroup(conf,
          request.getEntityGroupName().toByteArray());
      GetEntityGroupResponse.Builder builder = GetEntityGroupResponse
          .newBuilder();
      if (pair != null) {
        if (pair.getFirst() != null) {
          builder.setEgInfo(pair.getFirst().convert());
        }
        if (pair.getSecond() != null) {
          builder.setServerName(pair.getSecond().convert());
        }
      }
      return builder.build();
    } catch (MetaException e) {
      LOG.error("getEntityGroup for master", e);
      throw new ServiceException(e);
    }
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MasterAdminService.BlockingInterface#getEntityGroupWithScan(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupWithScanRequest)
   */
  @Override
  public GetEntityGroupWithScanResponse getEntityGroupWithScan(
      RpcController controller, GetEntityGroupWithScanRequest request)
      throws ServiceException {
    byte[] tableNameOrEntityGroupName = request.getTableNameOrEntityGroupName()
        .toByteArray();
    Pair<EntityGroupInfo, ServerName> pair;
    try {
      pair = FMetaReader.getEntityGroup(conf, tableNameOrEntityGroupName);
      if (pair == null) {
        final AtomicReference<Pair<EntityGroupInfo, ServerName>> result = new AtomicReference<Pair<EntityGroupInfo, ServerName>>(
            null);
        final String encodedName = Bytes.toString(tableNameOrEntityGroupName);
        MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
          @Override
          public boolean processRow(org.apache.hadoop.hbase.client.Result data)
              throws IOException {
            EntityGroupInfo info = EntityGroupInfo.getEntityGroupInfo(data);
            if (info == null) {
              LOG.warn("No serialized EntityGroupInfo in " + data);
              return true;
            }
            if (!encodedName.equals(info.getEncodedName())) {
              return true;
            }
            ServerName sn = EntityGroupInfo.getServerName(data);
            result.set(new Pair<EntityGroupInfo, ServerName>(info, sn));
            return false; // found the entityGroup, stop
          }
        };

        FMetaScanner.metaScan(conf, visitor);
        pair = result.get();
      }
      GetEntityGroupWithScanResponse.Builder builder = GetEntityGroupWithScanResponse
          .newBuilder();
      if (pair != null) {
        if (pair.getFirst() != null) {
          builder.setEgInfo(pair.getFirst().convert());
        }
        if (pair.getSecond() != null) {
          builder.setServerName(pair.getSecond().convert());
        }
      }
      return builder.build();
    } catch (Exception e) {
      LOG.error("Failed getEntityGroupWithScan.", e);
      throw new ServiceException(e);
    }
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MasterAdminService.BlockingInterface#getEntityGroups(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupsRequest)
   */
  @Override
  public GetEntityGroupsResponse getEntityGroups(RpcController controller,
      GetEntityGroupsRequest request) throws ServiceException {
    GetEntityGroupsResponse.Builder getEgsBuilder = GetEntityGroupsResponse
        .newBuilder();
    try {
      Map<EntityGroupInfo, ServerName> egs = FMetaScanner.allTableEntityGroups(
          getConfiguration(), request.getTableName().toByteArray(), false);
      if (egs != null) {
        for (Map.Entry<EntityGroupInfo, ServerName> entry : egs.entrySet()) {
          GetEntityGroupResponse.Builder getEgBuilder = GetEntityGroupResponse
              .newBuilder();
          getEgBuilder.setEgInfo(entry.getKey().convert());
          getEgBuilder.setServerName(entry.getValue().convert());
          getEgsBuilder.addEntityGroup(getEgBuilder.build());
        }
      }
      return getEgsBuilder.build();
    } catch (IOException e) {
      LOG.error("Failed getEntityGroups.", e);
      throw new ServiceException(e);
    }
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MasterAdminService.BlockingInterface#getTableEntityGroups(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetTableEntityGroupsRequest)
   */
  @Override
  public GetTableEntityGroupsResponse getTableEntityGroups(
      RpcController controller, GetTableEntityGroupsRequest request)
      throws ServiceException {
    GetTableEntityGroupsResponse.Builder getEgsBuilder = GetTableEntityGroupsResponse
        .newBuilder();
    try {
      List<Pair<EntityGroupInfo, ServerName>> egs = FMetaReader
          .getTableEntityGroupsAndLocations(conf, request.getTableName()
              .toByteArray());
      if (egs != null) {
        for (Pair<EntityGroupInfo, ServerName> entry : egs) {
          GetEntityGroupResponse.Builder getEgBuilder = GetEntityGroupResponse
              .newBuilder();
          getEgBuilder.setEgInfo(entry.getFirst().convert());
          getEgBuilder.setServerName(entry.getSecond().convert());
          getEgsBuilder.addEntityGroup(getEgBuilder.build());
        }
      }
      return getEgsBuilder.build();
    } catch (IOException e) {
      LOG.error("Failed getEntityGroups.", e);
      throw new ServiceException(e);
    }
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MasterAdminService.BlockingInterface#tableExists(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.MasterAdminProtos.TableExistsRequest)
   */
  @Override
  public TableExistsResponse tableExists(RpcController controller,
      TableExistsRequest request) throws ServiceException {
    String tableNameString = Bytes.toString(request.getTableName()
        .toByteArray());
    TableExistsResponse.Builder builder = TableExistsResponse.newBuilder();
    try {
      builder.setExist(FMetaReader.tableExists(conf, tableNameString));
    } catch (MetaException e) {
      LOG.error("Failed tableExists.", e);
      throw new ServiceException(e);
    }
    return builder.build();
  }

  /**
   * @see com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MasterAdminService.BlockingInterface#isTableAvailable(com.google.protobuf.RpcController,
   *      com.alibaba.wasp.protobuf.generated.MasterAdminProtos.IsTableAvailableRequest)
   */
  @Override
  public IsTableAvailableResponse isTableAvailable(RpcController controller,
      IsTableAvailableRequest request) throws ServiceException {
    IsTableAvailableResponse.Builder builder = IsTableAvailableResponse
        .newBuilder();
    try {
      builder.setAvailable(isAvailable(request.getTableName().toByteArray()));
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  private boolean isAvailable(final byte[] rootTableName) throws IOException {
    final AtomicBoolean available = new AtomicBoolean(true);
    final AtomicInteger entityGroupCount = new AtomicInteger(0);
    MetaScannerVisitorBase visitor = new MetaScannerVisitorBase() {
      @Override
      public boolean processRow(Result rowResult) throws IOException {
        byte[] value = rowResult.getValue(FConstants.CATALOG_FAMILY,
            FConstants.EGINFO);
        EntityGroupInfo eginfo = EntityGroupInfo.parseFromOrNull(value);
        if (eginfo != null) {
          if (Bytes.equals(eginfo.getTableName(), rootTableName)) {
            value = rowResult.getValue(FConstants.CATALOG_FAMILY,
                FConstants.EGLOCATION);
            if (value == null) {
              available.set(false);
              return false;
            }
            entityGroupCount.incrementAndGet();
          }
        }
        // Returning true means "keep scanning"
        return true;
      }
    };
    FMetaScanner.metaScan(conf, visitor, rootTableName);
    return available.get() && (entityGroupCount.get() > 0);
  }

  /**
   * Return if the table locked.
   * 
   * @throws ServiceException
   */
  @Override
  public IsTableLockedResponse isTableLocked(RpcController controller,
      IsTableLockedRequest request) throws ServiceException {
    IsTableLockedResponse.Builder builder = IsTableLockedResponse.newBuilder();
    String tableName = Bytes.toString(request.getTableName().toByteArray());
    builder.setLocked(tableLockManager.isTableLocked(tableName));
    return builder.build();
  }

  /**
   * Unlock the table
   * @throws ServiceException
   */
  @Override
  public UnlockTableResponse unlockTable(RpcController controller, UnlockTableRequest request)
      throws ServiceException {
    UnlockTableResponse.Builder builder = UnlockTableResponse.newBuilder();
    String tableName = Bytes.toString(request.getTableName().toByteArray());
    if (tableLockManager.isTableLocked(tableName)) {
      tableLockManager.unlockTable(tableName);
      LOG.info("Unlock table '" + tableName + "'");
    }
    return builder.build();
  }

  /**
   * Set table state
   * @throws ServiceException
   */
  public SetTableStateResponse
      setTableState(RpcController controller, SetTableStateRequest request) throws ServiceException {
    SetTableStateResponse.Builder builder = SetTableStateResponse.newBuilder();
    String tableName = Bytes.toString(request.getTableName().toByteArray());
    String state = Bytes.toString(request.getState().toByteArray());
    try {
      if (state.equalsIgnoreCase("DISABLED")) {
        this.assignmentManager.getZKTable().setDisabledTable(tableName);
      } else if (state.equalsIgnoreCase("DISABLING")) {
        this.assignmentManager.getZKTable().setDisablingTable(tableName);
      } else if (state.equalsIgnoreCase("ENABLING")) {
        this.assignmentManager.getZKTable().setEnablingTable(tableName);
      } else if (state.equalsIgnoreCase("ENABLED")) {
        this.assignmentManager.getZKTable().setEnabledTable(tableName);
      } else {
        throw new ServiceException("Wrong state.");
      }

      // Check state
      boolean success = true;
      if (state.equalsIgnoreCase("DISABLED")) {
        success = this.assignmentManager.getZKTable().isDisabledTable(tableName);
      } else if (state.equalsIgnoreCase("DISABLING")) {
        success = this.assignmentManager.getZKTable().isDisablingTable(tableName);
      } else if (state.equalsIgnoreCase("ENABLING")) {
        success = this.assignmentManager.getZKTable().isEnablingTable(tableName);
      } else if (state.equalsIgnoreCase("ENABLED")) {
        success = this.assignmentManager.getZKTable().isEnabledTable(tableName);
      }
      if (!success) {
        throw new ServiceException(tableName + " connot set to state '" + state
            + "', current state is " + this.assignmentManager.getZKTable().getTableState(tableName));
      }
    } catch (KeeperException e) {
      throw new ServiceException(e);
    }

    return builder.build();
  }
}
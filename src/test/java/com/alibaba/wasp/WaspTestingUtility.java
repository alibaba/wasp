/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.wasp;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.wasp.client.WaspAdmin;import com.alibaba.wasp.conf.WaspConfiguration;import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.alibaba.wasp.client.FConnection;
import com.alibaba.wasp.client.WaspAdmin;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.fserver.EntityGroup;
import com.alibaba.wasp.fserver.FServer;
import com.alibaba.wasp.master.FMaster;
import com.alibaba.wasp.meta.AbstractMetaService;
import com.alibaba.wasp.meta.FMetaScanner;
import com.alibaba.wasp.meta.FMetaScanner.BlockingMetaScannerVisitor;
import com.alibaba.wasp.meta.FMetaScanner.MetaScannerVisitor;
import com.alibaba.wasp.meta.FMetaTestUtil;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.zookeeper.ZKAssign;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;

public class WaspTestingUtility {
  static final Log LOG = LogFactory.getLog(WaspTestingUtility.class);

  private HBaseTestingUtility hbaseTestingUtility;
  private Configuration conf;

  private MiniWaspCluster waspCluster = null;

  public WaspTestingUtility() {
    this(WaspConfiguration.create());
  }

  public WaspTestingUtility(Configuration conf) {
    this.conf = conf;
    hbaseTestingUtility = new HBaseTestingUtility(conf);
  }

  /**
   * Returns this classes's instance of {@link Configuration}. Be careful how
   * you use the returned Configuration since {@link FConnection} instances can
   * be shared. The Map of FConnections is keyed by the Configuration. If say, a
   * Connection was being used against a cluster that had been shutdown, see
   * {@link #shutdownMiniCluster()}, then the Connection will no longer be
   * wholesome. Rather than use the return direct, its usually best to make a
   * copy and use that. Do
   * <code>Configuration c = new Configuration(INSTANCE.getConfiguration());</code>
   * 
   * @return Instance of Configuration.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  public static void adjustLogLevel() {
    Logger.getLogger("com.alibaba.wasp").setLevel(Level.DEBUG);
    Logger.getLogger("org.apache.hadoop").setLevel(Level.ERROR);
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.ERROR);
    Logger.getLogger("org.mortbay.log").setLevel(Level.ERROR);
    Logger.getLogger("com.alibaba.wasp.ipc.ProtobufRpcEngine").setLevel(
        Level.ERROR);
    Logger.getLogger("com.alibaba.wasp.ipc.NettyServer.trace").setLevel(
        Level.ERROR);
    Logger.getLogger(MetricsSystemImpl.class).setLevel(Level.ERROR);
    Logger.getLogger(MBeans.class).setLevel(Level.ERROR);
    Logger.getLogger(ZooKeeperServer.class).setLevel(Level.ERROR);
    Logger.getLogger(ZooKeeper.class).setLevel(Level.ERROR);
    Logger.getLogger(PrepRequestProcessor.class).setLevel(Level.ERROR);
    Logger.getLogger(NIOServerCnxn.class).setLevel(Level.ERROR);
    Logger.getLogger(ClientCnxn.class).setLevel(Level.ERROR);
  }

  public void setWaspCluster(MiniWaspCluster waspCluster) {
    this.waspCluster = waspCluster;
  }

  /**
   * Get HBaseTestingUtility
   * 
   * @return HBaseTestingUtility
   */
  public HBaseTestingUtility getHBaseTestingUtility() {
    return this.hbaseTestingUtility;
  }

  /**
   * Start up a minicluster of wasp, optionally hbase, dfs, and zookeeper.
   * Modifies Configuration. Homes the cluster data directory under a random
   * subdirectory in a directory under System property test.build.data.
   * Directory is cleaned up on exit.
   * 
   * @param numSlaves
   *          Number of slaves to start up. We'll start this many fservers,
   *          datanodes and regionservers. If numSlaves is > 1, then make sure
   *          wasp.fserver.info.port is -1 (i.e. no ui per fserver) otherwise
   *          bind errors.
   * @throws Exception
   * @see {@link #shutdownMiniCluster()}
   * @return Mini wasp cluster instance created.
   */
  public MiniWaspCluster startMiniCluster(final int numSlaves) throws Exception {
    return startMiniCluster(1, numSlaves);
  }

  /**
   * Start up a minicluster of wasp, optionally hbase, dfs, and zookeeper.
   * Modifies Configuration. Homes the cluster data directory under a random
   * subdirectory in a directory under System property test.build.data.
   * Directory is cleaned up on exit.
   * 
   * @param numMasters
   *          Number of masters to start up.
   * @param numSlaves
   *          Number of slaves to start up. We'll start this many fservers,
   *          datanodes and regionservers. If numSlaves is > 1, then make sure
   *          wasp.fserver.info.port is -1 (i.e. no ui per fserver) otherwise
   *          bind errors.
   * @throws Exception
   * @see {@link #shutdownMiniCluster()}
   * @return Mini wasp cluster instance created.
   */
  public MiniWaspCluster startMiniCluster(final int numMasters,
      final int numSlaves) throws Exception {
    LOG.info("Starting up minicluster with " + numMasters + " master(s) and "
        + numSlaves + " fserver(s) ");
    this.getConfiguration().setInt(FConstants.MASTER_INFO_PORT, -1);
    this.getConfiguration().setInt(FConstants.FSERVER_INFO_PORT, -1);
    this.getConfiguration().setInt("hbase.regionserver.info.port", -1);
    this.getConfiguration().setInt("hbase.master.info.port", -1);
    hbaseTestingUtility.startMiniCluster(numMasters, numSlaves);
    this.getConfiguration().set(FConstants.ZOOKEEPER_QUORUM,
        this.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM));
    this.getConfiguration().set(FConstants.ZOOKEEPER_CLIENT_PORT,
        this.getConfiguration().get(HConstants.ZOOKEEPER_CLIENT_PORT));
    LOG.info("Connectting Zookeeper by address<"
        + this.getConfiguration().get(FConstants.ZOOKEEPER_QUORUM) + ">:port<"
        + this.getConfiguration().get(FConstants.ZOOKEEPER_CLIENT_PORT) + ">");
    this.getConfiguration().set(
        FConstants.ZOOKEEPER_ZNODE_PARENT,
        this.getConfiguration().get(FConstants.ZOOKEEPER_ZNODE_PARENT,
            FConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT));
    return startMiniWaspCluster(numMasters, numSlaves, null, null);
  }

  /**
   * Starts up mini wasp cluster. Usually used after call to
   * {@link #startMiniCluster(int, int)} when doing stepped startup of clusters.
   * Usually you won't want this. You'll usually want
   * {@link #startMiniCluster(int)}.
   * 
   * @param numMasters
   * @param numSlaves
   * @return Reference to the wasp mini wasp cluster.
   * @throws IOException
   * @throws InterruptedException
   * @see {@link #startMiniCluster(int)}
   */
  public MiniWaspCluster startMiniWaspCluster(final int numMasters,
      final int numSlaves, Class<? extends FMaster> masterClass,
      Class<? extends MiniWaspCluster.MiniWaspClusterFServer> fserverClass)
      throws IOException, InterruptedException {

    // These settings will make the server waits until this exact number of
    // regions servers are connected.
    if (conf.getInt("wasp.master.wait.on.fservers.mintostart", -1) == -1) {
      conf.setInt("wasp.master.wait.on.fservers.mintostart", numSlaves);
    }
    if (conf.getInt("wasp.master.wait.on.fservers.maxtostart", -1) == -1) {
      conf.setInt("wasp.master.wait.on.fservers.maxtostart", numSlaves);
    }
    Configuration c = new Configuration(this.conf);
    this.waspCluster = new MiniWaspCluster(c, numMasters, numSlaves,
        masterClass, fserverClass);

    // Don't leave here till we've done a successful scan of the FMETA
    HTable t = new HTable(c, FConstants.DEFAULT_METASTORE_TABLE);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) {
      continue;
    }
    s.close();
    t.close();

    getWaspAdmin(); // create immediately the waspAdmin
    LOG.info("Wasp Minicluster is up");
    return (MiniWaspCluster) this.waspCluster;
  }

  /**
   * Returns a WaspAdmin instance. This instance is shared between
   * WaspTestingUtility instance users. Don't close it, it will be closed
   * automatically when the cluster shutdowns
   * 
   * @return The WaspAdmin instance.
   * @throws IOException
   */
  public synchronized WaspAdmin getWaspAdmin() throws IOException {
    if (waspAdmin == null) {
      waspAdmin = new WaspAdmin(getConfiguration());
    }
    return waspAdmin;
  }

  private WaspAdmin waspAdmin = null;

  /**
   * Starts the wasp cluster up again after shutting it down previously in a
   * test. Use this if you want to keep hbase/dfs/zk up and just stop/start
   * wasp.
   * 
   * @param servers
   *          number of fservers
   * @throws IOException
   */
  public void restartWaspCluster(int servers) throws IOException,
      InterruptedException {
    conf.setInt("wasp.master.wait.on.fservers.mintostart", servers);
    conf.setInt("wasp.master.wait.on.fservers.maxtostart", servers);
    this.waspCluster = new MiniWaspCluster(this.conf, servers);
    // Don't leave here till we've done a successful scan of the FMETA
    HTable t = new HTable(new Configuration(this.conf),
        FConstants.DEFAULT_METASTORE_TABLE);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) {
      // do nothing
    }
    LOG.info("Wasp has been restarted");
    s.close();
    t.close();
  }

  /**
   * @return Current mini wasp cluster. Only has something in it after a call to
   *         {@link #startMiniCluster(int)}.
   * @see #startMiniCluster(int)
   */
  public MiniWaspCluster getMiniWaspCluster() {
    if (this.waspCluster instanceof MiniWaspCluster) {
      return (MiniWaspCluster) this.waspCluster;
    }
    throw new RuntimeException(waspCluster + " not an instance of "
        + MiniWaspCluster.class.getName());
  }

  /**
   * Stops mini wasp, hbase, zk, and hdfs clusters.
   * 
   * @throws IOException
   * @see {@link #startMiniCluster(int)}
   */
  public void shutdownMiniCluster() throws Exception {
    LOG.info("Shutting down mini wasp cluster");
    shutdownMiniWaspCluster();
    this.hbaseTestingUtility.shutdownMiniCluster();
    LOG.info("Minicluster is down");
  }

  /**
   * Shutdown Wasp mini cluster. Does not shutdown hbase, zk or dfs if running.
   * 
   * @throws IOException
   */
  public void shutdownMiniWaspCluster() throws IOException {
    if (waspAdmin != null) {
      waspAdmin.close();
      waspAdmin = null;
    }
    // unset the configuration for MIN and MAX FS to start
    conf.setInt("wasp.master.wait.on.fservers.mintostart", -1);
    conf.setInt("wasp.master.wait.on.fservers.maxtostart", -1);
    if (this.waspCluster != null) {
      this.waspCluster.shutdown();
      // Wait till wasp is down before going on to shutdown zk.
      this.waspCluster.waitUntilShutDown();
      this.waspCluster = null;
    }
    if (AbstractMetaService.globalFMetaservice != null) {
      AbstractMetaService.globalFMetaservice.close();
      AbstractMetaService.globalFMetaservice = null;
    }
  }

  /**
   * Returns all rows from the .FMETA. table for a given user table
   * 
   * @throws IOException
   *           When reading the rows fails.
   */
  public List<byte[]> getMetaTableRows(byte[] tableName) throws IOException {
    final List<byte[]> rows = new ArrayList<byte[]>();
    MetaScannerVisitor visitor = new BlockingMetaScannerVisitor(conf) {
      @Override
      public boolean processRowInternal(Result result) throws IOException {
        if (result == null || result.isEmpty()) {
          return true;
        }

        EntityGroupInfo entityGroupInfo = FMetaScanner
            .getEntityGroupInfo(result);
        if (entityGroupInfo == null) {
          LOG.warn("Null ENTITYGROUPINFO_QUALIFIER: " + result);
          return true;
        }

        if (entityGroupInfo.isOffline()) {
          return true;
        }
        rows.add(result.getRow());
        return true;
      }
    };
    FMetaScanner.metaScan(conf, visitor);
    return rows;
  }

  /**
   * Tool to get the reference to the region server object that holds the region
   * of the specified user table. It first searches for the meta rows that
   * contain the region of the specified table, then gets the index of that RS,
   * and finally retrieves the RS's reference.
   * 
   * @param tableName
   *          user table to lookup in .META.
   * @return region server that holds it, null if the row doesn't exist
   * @throws IOException
   */
  public FServer getFSForFirstEntityGroupInTable(byte[] tableName)
      throws IOException {
    List<EntityGroup> entityGroups = waspCluster.getEntityGroups(tableName);
    if (entityGroups == null || entityGroups.isEmpty()) {
      return null;
    }
    LOG.debug("Found " + entityGroups.size() + " entityGroups for table "
        + Bytes.toString(tableName));
    byte[] firstEntityGroup = entityGroups.get(0).getEntityGroupName();
    LOG.debug("FirstRow=" + Bytes.toString(firstEntityGroup));
    int index = waspCluster.getServerWith(firstEntityGroup);
    return waspCluster.getFServerThreads().get(index).getFServer();
  }

  /**
   * Get the Mini Wasp cluster.
   * 
   * @return wasp cluster
   */
  public MiniWaspCluster getWaspCluster() {
    return getMiniWaspCluster();
  }

  /**
   * Returns the WaspCluster instance.
   * <p>
   * Returned object can be any of the subclasses of WaspCluster, and the tests
   * referring this should not assume that the cluster is a mini cluster or a
   * distributed one. If the test only works on a mini cluster, then specific
   * method {@link #getMiniWaspCluster()} can be used instead w/o the need to
   * type-cast.
   */
  public WaspCluster getClusterInterface() {
    return waspCluster;
  }

  /**
   * @param tablename
   * @return
   */
  public FTable createTable(byte[] tablename) throws IOException {
    return createTable(Bytes.toString(tablename));
  }

  /**
   * @param tablename
   * @return
   */
  public FTable createTable(String tablename) throws IOException {
    FTable desc = FMetaTestUtil.makeTable(tablename);
    getWaspAdmin().createTable(desc);
    return desc;
  }

  /**
   * @param ft
   */
  public void loadTable(FTable ft) {
    // put some data into the FTable.
  }

  /**
   * 
   * @param table
   * @param timeoutMillis
   * @throws InterruptedException
   * @throws IOException
   */
  public void waitTableAvailable(byte[] table, long timeoutMillis)
      throws IOException, InterruptedException {
    long startWait = System.currentTimeMillis();
    while (!getWaspAdmin().isTableAvailable(table)) {
      assertTrue(
          "Timed out waiting for table to become available "
              + Bytes.toStringBinary(table), System.currentTimeMillis()
              - startWait < timeoutMillis);
      Thread.sleep(200);
    }
  }

  public void waitTableAvailable(byte[] table) throws IOException,
      InterruptedException {
    long startWait = System.currentTimeMillis();
    while (!getWaspAdmin().isTableAvailable(table)) {
      LOG.info("Wait " + (System.currentTimeMillis() - startWait) / 1000
          + " s, for " + Bytes.toString(table) + " to be Available.");
      Thread.sleep(1000);
    }
  }

  public void waitTableNotAvailable(byte[] table) throws IOException,
      InterruptedException {
    long startWait = System.currentTimeMillis();
    while (getWaspAdmin().tableExists(table)) {
      LOG.info("Wait " + (System.currentTimeMillis() - startWait) / 1000
          + " s, for " + Bytes.toString(table) + " to be Available.");
      Thread.sleep(1000);
    }
  }

  /**
   * Creates a znode with OPENED state.
   * 
   * @param TEST_UTIL
   * @param entityGroup
   * @param serverName
   * @return
   * @throws IOException
   * @throws ZooKeeperConnectionException
   * @throws KeeperException
   * @throws NodeExistsException
   */
  public static ZooKeeperWatcher createAndForceNodeToOpenedState(
      WaspTestingUtility TEST_UTIL, EntityGroup entityGroup,
      ServerName serverName) throws ZooKeeperConnectionException, IOException,
      KeeperException, NodeExistsException {
    ZooKeeperWatcher zkw = getZooKeeperWatcher(TEST_UTIL);
    ZKAssign.createNodeOffline(zkw, entityGroup.getEntityGroupInfo(),
        serverName);
    int version = ZKAssign.transitionNodeOpening(zkw,
        entityGroup.getEntityGroupInfo(), serverName);
    ZKAssign.transitionNodeOpened(zkw, entityGroup.getEntityGroupInfo(),
        serverName, version);
    return zkw;
  }

  /**
   * Make sure that at least the specified number of entityGroup servers are
   * running
   * 
   * @param num
   *          minimum number of region servers that should be running
   * @return True if we started some servers
   * @throws IOException
   */
  public boolean ensureSomeFServersAvailable(final int num) throws IOException {
    boolean startedServer = false;

    for (int i = waspCluster.getLiveFServerThreads().size(); i < num; ++i) {
      LOG.info("Started new server=" + waspCluster.startFServer());
      startedServer = true;
    }

    return startedServer;
  }

  /**
   * Gets a ZooKeeperWatcher.
   * 
   * @param TEST_UTIL
   */
  public static ZooKeeperWatcher getZooKeeperWatcher(
      WaspTestingUtility TEST_UTIL) throws ZooKeeperConnectionException,
      IOException {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "unittest", new Abortable() {
          boolean aborted = false;

          @Override
          public void abort(String why, Throwable e) {
            aborted = true;
            throw new RuntimeException("Fatal ZK error, why=" + why, e);
          }

          @Override
          public boolean isAborted() {
            return aborted;
          }
        });
    return zkw;
  }

  public void waitTableEnabled(byte[] table, long timeoutMillis)
      throws InterruptedException, IOException {
    long startWait = System.currentTimeMillis();
    while (!getWaspAdmin().isTableEnabled(table)) {
      assertTrue("Timed out waiting for table " + Bytes.toStringBinary(table),
          System.currentTimeMillis() - startWait < timeoutMillis);
      Thread.sleep(200);
    }
  }

  /**
   * Due to async racing issue, a entityGroup may not be in the online
   * entityGroup list of a entityGroup server yet, after the assignment znode is
   * deleted and the new assignment is recorded in master.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public void assertEntityGroupOnServer(EntityGroupInfo egInfo,
      ServerName serverName, final long timeout) throws IOException,
      InterruptedException {
    long timeoutTime = System.currentTimeMillis() + timeout;
    while (true) {
      List<EntityGroupInfo> entityGroups = getWaspAdmin()
          .getOnlineEntityGroups(serverName);
      if (entityGroups.contains(egInfo))
        return;
      long now = System.currentTimeMillis();
      if (now > timeoutTime)
        break;
      Thread.sleep(10);
    }
    fail("Could not find entityGroup " + egInfo.getEntityGroupNameAsString()
        + " on server " + serverName);
  }

  /**
   * @param tableName
   */
  public void deleteTable(byte[] tableName) throws IOException {
    getWaspAdmin().disableTable(tableName);
    getWaspAdmin().deleteTable(tableName);
  }
}
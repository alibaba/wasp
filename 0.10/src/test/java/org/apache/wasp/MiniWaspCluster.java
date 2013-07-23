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
package org.apache.wasp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.wasp.client.ClientProtocol;
import org.apache.wasp.client.FConnectionManager;
import org.apache.wasp.conf.WaspConfiguration;
import org.apache.wasp.fserver.AdminProtocol;
import org.apache.wasp.fserver.EntityGroup;
import org.apache.wasp.fserver.FServer;
import org.apache.wasp.master.FMaster;
import org.apache.wasp.master.FMasterAdminProtocol;
import org.apache.wasp.master.FMasterMonitorProtocol;
import org.apache.wasp.protobuf.generated.FServerStatusProtos.FServerStartupResponse;
import org.apache.wasp.util.JVMClusterUtil;

/**
 * MiniWaspCluster for test unit. This class creates a single process Wasp
 * cluster. each server. The master uses the 'default' HBase. The FServers, if
 * we are running on HBase, create a HBase instance each and will close down
 * their instance on the way out.
 */
public class MiniWaspCluster extends WaspCluster {
  static final Log LOG = LogFactory.getLog(MiniWaspCluster.class.getName());
  public LocalWaspCluster waspCluster;

  /**
   * Start a MiniWaspCluster.
   * 
   * @param conf
   *          Configuration to be used for cluster
   * @param numFServers
   *          initial number of fservers to start.
   * @throws IOException
   */
  public MiniWaspCluster(Configuration conf, int numFServers)
      throws IOException, InterruptedException {
    this(conf, 1, numFServers);
  }

  /**
   * Start a MiniWaspCluster.
   * 
   * @param conf
   *          Configuration to be used for cluster
   * @param numMasters
   *          initial number of masters to start.
   * @param numFServers
   *          initial number of entityGroup servers to start.
   * @throws IOException
   */
  public MiniWaspCluster(Configuration conf, int numMasters, int numFServers)
      throws IOException, InterruptedException {
    this(conf, numMasters, numFServers, null, null);
  }

  public MiniWaspCluster(Configuration conf, int numMasters, int numFServers,
      Class<? extends FMaster> masterClass,
      Class<? extends MiniWaspCluster.MiniWaspClusterFServer> fserverClass)
      throws IOException, InterruptedException {
    super(conf);
    conf.set(FConstants.MASTER_PORT, "0");
    init(numMasters, numFServers, masterClass, fserverClass);
    this.initialClusterStatus = getClusterStatus();
  }

  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Subclass so can get at protected methods (none at moment).
   */
  public static class MiniWaspClusterFServer extends FServer {
    public static boolean TEST_SKIP_CLOSE = false;

    public MiniWaspClusterFServer(Configuration conf) throws IOException,
        InterruptedException {
      super(conf);
    }

    /*
     * @param c
     * 
     * @param currentfs We return this if we did not make a new one.
     * 
     * @param uniqueName Same name used to help identify the created fs.
     * 
     * @return A new fs instance if we are up on DistributeFileSystem.
     * 
     * @throws IOException
     */

    @Override
    protected void handleReportForDutyResponse(final FServerStartupResponse c)
        throws IOException {
      super.handleReportForDutyResponse(c);
    }

    @Override
    public void run() {
      try {
        runFServer();
      } catch (Throwable t) {
        LOG.error("Exception in run", t);
      }
    }

    private void runFServer() {
      super.run();
    }

    @Override
    public void kill() {
      super.kill();
    }

    public void abort(final String reason, final Throwable cause) {
      abortFServer(reason, cause);
    }

    private void abortFServer(String reason, Throwable cause) {
      super.abort(reason, cause);
    }
  }

  private void init(final int nMasterNodes, final int nEntityGroupNodes,
      Class<? extends FMaster> masterClass,
      Class<? extends MiniWaspCluster.MiniWaspClusterFServer> fserverClass)
      throws IOException, InterruptedException {
    try {
      if (masterClass == null) {
        masterClass = FMaster.class;
      }
      if (fserverClass == null) {
        fserverClass = MiniWaspCluster.MiniWaspClusterFServer.class;
      }

      // start up a LocalWaspCluster
      waspCluster = new LocalWaspCluster(conf, nMasterNodes, 0, masterClass,
          fserverClass);

      // manually add the fservers as other users
      for (int i = 0; i < nEntityGroupNodes; i++) {
        Configuration rsConf = WaspConfiguration.create(conf);
        waspCluster.addFServer(rsConf, i);
      }

      waspCluster.startup();
    } catch (IOException e) {
      shutdown();
      throw e;
    } catch (Throwable t) {
      LOG.error("Error starting cluster", t);
      shutdown();
      throw new IOException("Shutting down", t);
    }
  }

  @Override
  public void startFServer(String hostname) throws IOException {
    this.startFServer();
  }

  @Override
  public void killFServer(ServerName serverName) throws IOException {
    FServer server = getFServer(getFServerIndex(serverName));
    if (server instanceof MiniWaspClusterFServer) {
      LOG.info("Killing " + server.toString());
      ((MiniWaspClusterFServer) server).kill();
    } else {
      abortFServer(getFServerIndex(serverName));
    }
  }

  @Override
  public void stopFServer(ServerName serverName) throws IOException {
    stopFServer(getFServerIndex(serverName));
  }

  @Override
  public void waitForFServerToStop(ServerName serverName, long timeout)
      throws IOException {
    // ignore timeout for now
    waitOnEntityGroupServer(getFServerIndex(serverName));
  }

  @Override
  public void startMaster(String hostname) throws IOException {
    this.startMaster();
  }

  @Override
  public void killMaster(ServerName serverName) throws IOException {
    abortMaster(getMasterIndex(serverName));
  }

  @Override
  public void stopMaster(ServerName serverName) throws IOException {
    stopMaster(getMasterIndex(serverName));
  }

  @Override
  public void waitForMasterToStop(ServerName serverName, long timeout)
      throws IOException {
    // ignore timeout for now
    waitOnMaster(getMasterIndex(serverName));
  }

  /**
   * Starts a entityGroup server thread running
   * 
   * @throws IOException
   * @return New FServerThread
   */
  public JVMClusterUtil.FServerThread startFServer() throws IOException {
    final Configuration newConf = WaspConfiguration.create(conf);
    JVMClusterUtil.FServerThread t = null;

    t = waspCluster.addFServer(newConf, waspCluster.getFServers().size());
    t.start();
    t.waitForServerOnline();
    return t;
  }

  /**
   * Cause a entityGroup server to exit doing basic clean up only on its way
   * out.
   * 
   * @param serverNumber
   *          Used as index into a list.
   */
  public String abortFServer(int serverNumber) {
    FServer server = getFServer(serverNumber);
    LOG.info("Aborting " + server.toString());
    server.abort("Aborting for tests", new Exception("Trace info"));
    return server.toString();
  }

  /**
   * Shut down the specified entityGroup server cleanly
   * 
   * @param serverNumber
   *          Used as index into a list.
   * @return the entityGroups server that was stopped
   */
  public JVMClusterUtil.FServerThread stopFServer(int serverNumber) {
    JVMClusterUtil.FServerThread server = waspCluster.getFServers().get(
        serverNumber);
    LOG.info("Stopping " + server.toString());
    server.getFServer().stop("Stopping rs " + serverNumber);
    return server;
  }

  /**
   * Wait for the specified entityGroup server to stop. Removes this thread from
   * list of running threads.
   * 
   * @param serverNumber
   * @return Name of entityGroup server that just went down.
   */
  public String waitOnEntityGroupServer(final int serverNumber) {
    return this.waspCluster.waitOnFServer(serverNumber);
  }

  /**
   * Starts a master thread running
   * 
   * @throws IOException
   * @return New FServerThread
   */
  public JVMClusterUtil.MasterThread startMaster() throws IOException {
    Configuration c = WaspConfiguration.create(conf);

    JVMClusterUtil.MasterThread t = null;
    t = waspCluster.addMaster(c, waspCluster.getMasters().size());
    t.start();
    return t;
  }

  @Override
  public FMasterAdminProtocol getMasterAdmin() {
    return this.waspCluster.getActiveMaster();
  }

  @Override
  public FMasterMonitorProtocol getMasterMonitor() {
    return this.waspCluster.getActiveMaster();
  }

  /**
   * Returns the current active master, if available.
   * 
   * @return the active HMaster, null if none is active.
   */
  public FMaster getMaster() {
    return this.waspCluster.getActiveMaster();
  }

  /**
   * Returns the master at the specified index, if available.
   * 
   * @return the active HMaster, null if none is active.
   */
  public FMaster getMaster(final int serverNumber) {
    return this.waspCluster.getMaster(serverNumber);
  }

  /**
   * Cause a master to exit without shutting down entire cluster.
   * 
   * @param serverNumber
   *          Used as index into a list.
   */
  public String abortMaster(int serverNumber) {
    FMaster server = getMaster(serverNumber);
    LOG.info("Aborting " + server.toString());
    server.abort("Aborting for tests", new Exception("Trace info"));
    return server.toString();
  }

  /**
   * Shut down the specified master cleanly
   * 
   * @param serverNumber
   *          Used as index into a list.
   * @return the entityGroup server that was stopped
   */
  public JVMClusterUtil.MasterThread stopMaster(int serverNumber) {
    return stopMaster(serverNumber, true);
  }

  /**
   * Shut down the specified master cleanly
   * 
   * @param serverNumber
   *          Used as index into a list.
   * @param shutdownFS
   *          True is we are to shutdown the filesystem as part of this master's
   *          shutdown. Usually we do but you do not want to do this if you are
   *          running multiple master in a test and you shut down one before end
   *          of the test.
   * @return the master that was stopped
   */
  public JVMClusterUtil.MasterThread stopMaster(int serverNumber,
      final boolean shutdownFS) {
    JVMClusterUtil.MasterThread server = waspCluster.getMasters().get(
        serverNumber);
    LOG.info("Stopping " + server.toString());
    server.getMaster().stop("Stopping master " + serverNumber);
    return server;
  }

  /**
   * Wait for the specified master to stop. Removes this thread from list of
   * running threads.
   * 
   * @param serverNumber
   * @return Name of master that just went down.
   */
  public String waitOnMaster(final int serverNumber) {
    return this.waspCluster.waitOnMaster(serverNumber);
  }

  /**
   * Blocks until there is an active master and that master has completed
   * initialization.
   * 
   * @return true if an active master becomes available. false if there are no
   *         masters left.
   * @throws InterruptedException
   */
  public boolean waitForActiveAndReadyMaster(long timeout) throws IOException {
    List<JVMClusterUtil.MasterThread> mts;
    long start = System.currentTimeMillis();
    while (!(mts = getMasterThreads()).isEmpty()
        && (System.currentTimeMillis() - start) < timeout) {
      for (JVMClusterUtil.MasterThread mt : mts) {
        if (mt.getMaster().isActiveMaster() && mt.getMaster().isInitialized()) {
          return true;
        }
      }

      Threads.sleep(100);
    }
    return false;
  }

  /**
   * @return List of master threads.
   */
  public List<JVMClusterUtil.MasterThread> getMasterThreads() {
    return this.waspCluster.getMasters();
  }

  /**
   * @return List of live master threads (skips the aborted and the killed)
   */
  public List<JVMClusterUtil.MasterThread> getLiveMasterThreads() {
    return this.waspCluster.getLiveMasters();
  }

  /**
   * Wait for Mini HBase Cluster to shut down.
   */
  public void join() {
    this.waspCluster.join();
  }

  /**
   * Shut down the mini HBase cluster
   * 
   * @throws IOException
   */
  public void shutdown() throws IOException {
    if (this.waspCluster != null) {
      this.waspCluster.shutdown();
    }
    FConnectionManager.deleteAllConnections(false);
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public ClusterStatus getClusterStatus() throws IOException {
    FMaster master = getMaster();
    return master == null ? null : master.getClusterStatus();
  }

  /**
   * @return List of entityGroup server threads.
   */
  public List<JVMClusterUtil.FServerThread> getFServerThreads() {
    return this.waspCluster.getFServers();
  }

  /**
   * @return List of live entityGroup server threads (skips the aborted and the
   *         killed)
   */
  public List<JVMClusterUtil.FServerThread> getLiveFServerThreads() {
    return this.waspCluster.getLiveFServers();
  }

  /**
   * Grab a numbered entityGroup server of your choice.
   * 
   * @param serverNumber
   * @return entityGroup server
   */
  public FServer getFServer(int serverNumber) {
    return waspCluster.getFServer(serverNumber);
  }

  public List<EntityGroup> getEntityGroups(byte[] tableName) {
    List<EntityGroup> ret = new ArrayList<EntityGroup>();
    for (JVMClusterUtil.FServerThread rst : getFServerThreads()) {
      FServer fs = rst.getFServer();
      Collection<EntityGroup> egs = fs.getOnlineEntityGroupsLocalContext();
      for (EntityGroup entityGroup : egs) {
        TestCase.assertNotNull(entityGroup);
        TestCase.assertNotNull(entityGroup.getTableDesc());
        TestCase.assertNotNull(entityGroup.getTableDesc().getTableName());
        TestCase.assertNotNull(tableName);
        if (Bytes
            .equals(Bytes.toBytes(entityGroup.getTableDesc().getTableName()),
                tableName)) {
          ret.add(entityGroup);
        }
      }
    }
    return ret;
  }

  /**
   * Get the location of the specified entityGroup
   * 
   * @param entityGroupName
   *          Name of the entityGroup in bytes
   * @return Index into List of {@link MiniWaspCluster#getFServerThreads()} of
   *         HRS carrying .META.. Returns -1 if none found.
   */
  public int getServerWith(byte[] entityGroupName) {
    int index = -1;
    int count = 0;
    for (JVMClusterUtil.FServerThread rst : getFServerThreads()) {
      FServer hrs = rst.getFServer();
      EntityGroup entityGroup = hrs.getOnlineEntityGroup(entityGroupName);
      if (entityGroup != null) {
        index = count;
        break;
      }
      count++;
    }
    return index;
  }

  @Override
  public ServerName getServerHoldingEntityGroup(byte[] entityGroupName)
      throws IOException {
    int index = getServerWith(entityGroupName);
    if (index < 0) {
      return null;
    }
    return getFServer(index).getServerName();
  }

  /**
   * Counts the total numbers of entityGroups being served by the currently
   * online entityGroup servers by asking each how many entityGroups they have.
   * Does not look at META at all. Count includes catalog tables.
   * 
   * @return number of entityGroups being served by all entityGroup servers
   */
  public long countServedEntityGroups() {
    long count = 0;
    for (JVMClusterUtil.FServerThread rst : getLiveFServerThreads()) {
      count += rst.getFServer().getNumberOfOnlineEntityGroups();
    }
    return count;
  }

  /**
   * Do a simulated kill all masters and entityGroups servers. Useful when it is
   * impossible to bring the mini-cluster back for clean shutdown.
   */
  public void killAll() {
    for (JVMClusterUtil.FServerThread rst : getFServerThreads()) {
      rst.getFServer().abort("killAll");
    }
    for (JVMClusterUtil.MasterThread masterThread : getMasterThreads()) {
      masterThread.getMaster().abort("killAll", new Throwable());
    }
  }

  @Override
  public void waitUntilShutDown() {
    this.waspCluster.join();
  }

  protected int getFServerIndex(ServerName serverName) {
    // we have a small number of entityGroup servers, this should be fine for
    // now.
    List<JVMClusterUtil.FServerThread> servers = getFServerThreads();
    for (int i = 0; i < servers.size(); i++) {
      if (servers.get(i).getFServer().getServerName().equals(serverName)) {
        return i;
      }
    }
    return -1;
  }

  protected int getMasterIndex(ServerName serverName) {
    List<JVMClusterUtil.MasterThread> masters = getMasterThreads();
    for (int i = 0; i < masters.size(); i++) {
      if (masters.get(i).getMaster().getServerName().equals(serverName)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public AdminProtocol getAdminProtocol(ServerName serverName)
      throws IOException {
    return getFServer(getFServerIndex(serverName));
  }

  @Override
  public ClientProtocol getClientProtocol(ServerName serverName)
      throws IOException {
    return getFServer(getFServerIndex(serverName));
  }
}
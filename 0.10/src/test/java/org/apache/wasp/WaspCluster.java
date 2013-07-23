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

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.wasp.client.ClientProtocol;
import org.apache.wasp.fserver.AdminProtocol;
import org.apache.wasp.master.FMasterAdminProtocol;
import org.apache.wasp.master.FMasterMonitorProtocol;

/**
 * This class defines methods that can help with managing Wasp clusters from
 * unit tests and system tests. There are 1 type of cluster deployments:
 * <ul>
 * <li><b>MiniWaspCluster:</b> each server is run in the same JVM in separate
 * threads, used by unit tests</li>
 * <p>
 * WaspCluster unifies the way tests interact with the cluster, so that the same
 * test can be run against a mini-cluster during unit test execution.
 * 
 * <p>
 * WaspCluster exposes client-side public interfaces to tests, so that tests
 * does not assume running in a particular mode. Not all the tests are suitable
 * to be run on an actual cluster, and some tests will still need to mock stuff
 * and introspect internal state. For those use cases from unit tests, or if
 * more control is needed, you can use the subclasses directly. In that sense,
 * this class does not abstract away <strong>every</strong> interface that
 * MiniWaspCluster provides.
 */
public abstract class WaspCluster implements Closeable, Configurable {
  static final Log LOG = LogFactory.getLog(WaspCluster.class.getName());
  protected Configuration conf;

  /** the status of the cluster before we begin */
  protected ClusterStatus initialClusterStatus;

  /**
   * Construct an HBaseCluster
   * 
   * @param conf
   *          Configuration to be used for cluster
   */
  public WaspCluster(Configuration conf) {
    setConf(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Returns a ClusterStatus for this HBase cluster.
   * 
   * @see #getInitialClusterStatus()
   */
  public abstract ClusterStatus getClusterStatus() throws IOException;

  /**
   * Returns a ClusterStatus for this HBase cluster as observed at the starting
   * of the HBaseCluster
   */
  public ClusterStatus getInitialClusterStatus() throws IOException {
    return initialClusterStatus;
  }

  /**
   * Returns an {@link FMasterAdminProtocol} to the active master
   */
  public abstract FMasterAdminProtocol getMasterAdmin() throws IOException;

  /**
   * Returns an {@link FMasterMonitorProtocol} to the active master
   */
  public abstract FMasterMonitorProtocol getMasterMonitor() throws IOException;

  /**
   * Returns an AdminProtocol interface to the fserver
   */
  public abstract AdminProtocol getAdminProtocol(ServerName serverName)
      throws IOException;

  /**
   * Returns a ClientProtocol interface to the fserver
   */
  public abstract ClientProtocol getClientProtocol(ServerName serverName)
      throws IOException;

  /**
   * Starts a new fserver on the given hostname or if this is a mini/local
   * cluster, starts a fserver locally.
   * 
   * @param hostname
   *          the hostname to start the fserver on
   * @throws IOException
   *           if something goes wrong
   */
  public abstract void startFServer(String hostname) throws IOException;

  /**
   * Kills the fserver process if this is a distributed cluster, otherwise this
   * causes the fserver to exit doing basic clean up only.
   * 
   * @throws IOException
   *           if something goes wrong
   */
  public abstract void killFServer(ServerName serverName) throws IOException;

  /**
   * Stops the given fserver, by attempting a gradual stop.
   * 
   * @return whether the operation finished with success
   * @throws IOException
   *           if something goes wrong
   */
  public abstract void stopFServer(ServerName serverName) throws IOException;

  /**
   * Wait for the specified fserver to join the cluster
   * 
   * @return whether the operation finished with success
   * @throws IOException
   *           if something goes wrong or timeout occurs
   */
  public void waitForFServerToStart(String hostname, long timeout)
      throws IOException {
    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < timeout) {
      for (ServerName server : getClusterStatus().getServers()) {
        if (server.getHostname().equals(hostname)) {
          return;
        }
      }
      Threads.sleep(100);
    }
    throw new IOException("did timeout waiting for fserver to start:"
        + hostname);
  }

  /**
   * Wait for the specified fserver to stop the thread / process.
   * 
   * @return whether the operation finished with success
   * @throws IOException
   *           if something goes wrong or timeout occurs
   */
  public abstract void waitForFServerToStop(ServerName serverName, long timeout)
      throws IOException;

  /**
   * Starts a new master on the given hostname or if this is a mini/local
   * cluster, starts a master locally.
   * 
   * @param hostname
   *          the hostname to start the master on
   * @return whether the operation finished with success
   * @throws IOException
   *           if something goes wrong
   */
  public abstract void startMaster(String hostname) throws IOException;

  /**
   * Kills the master process if this is a distributed cluster, otherwise, this
   * causes master to exit doing basic clean up only.
   * 
   * @throws IOException
   *           if something goes wrong
   */
  public abstract void killMaster(ServerName serverName) throws IOException;

  /**
   * Stops the given master, by attempting a gradual stop.
   * 
   * @throws IOException
   *           if something goes wrong
   */
  public abstract void stopMaster(ServerName serverName) throws IOException;

  /**
   * Wait for the specified master to stop the thread / process.
   * 
   * @throws IOException
   *           if something goes wrong or timeout occurs
   */
  public abstract void waitForMasterToStop(ServerName serverName, long timeout)
      throws IOException;

  /**
   * Blocks until there is an active master and that master has completed
   * initialization.
   * 
   * @return true if an active master becomes available. false if there are no
   *         masters left.
   * @throws IOException
   *           if something goes wrong or timeout occurs
   */
  public boolean waitForActiveAndReadyMaster() throws IOException {
    return waitForActiveAndReadyMaster(Long.MAX_VALUE);
  }

  /**
   * Blocks until there is an active master and that master has completed
   * initialization.
   * 
   * @param timeout
   *          the timeout limit in ms
   * @return true if an active master becomes available. false if there are no
   *         masters left.
   */
  public abstract boolean waitForActiveAndReadyMaster(long timeout)
      throws IOException;

  /**
   * Wait for HBase Cluster to shut down.
   */
  public abstract void waitUntilShutDown() throws IOException;

  /**
   * Shut down the HBase cluster
   */
  public abstract void shutdown() throws IOException;

  /**
   * Restores the cluster to it's initial state if this is a real cluster,
   * otherwise does nothing.
   */
  public void restoreInitialStatus() throws IOException {
    restoreClusterStatus(getInitialClusterStatus());
  }

  /**
   * Restores the cluster to given state if this is a real cluster, otherwise
   * does nothing.
   */
  public void restoreClusterStatus(ClusterStatus desiredStatus)
      throws IOException {
  }

  /**
   * Get the ServerName of fServer serving the specified entityGroup
   * 
   * @param entityGroupName
   *          Name of the entityGroup in bytes
   * @return ServerName that hosts the entityGroup or null
   */
  public abstract ServerName getServerHoldingEntityGroup(byte[] entityGroupName)
      throws IOException;

  /**
   * @return whether we are interacting with a distributed cluster as opposed to
   *         an in-process mini/local cluster.
   */
  public boolean isDistributedCluster() {
    return false;
  }

  /**
   * Closes all the resources held open for this cluster. Note that this call
   * does not shutdown the cluster.
   * 
   * @see #shutdown()
   */
  @Override
  public abstract void close() throws IOException;
}
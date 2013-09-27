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

import com.alibaba.wasp.client.WaspAdmin;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.fserver.FServer;
import com.alibaba.wasp.master.FMaster;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.util.JVMClusterUtil;
import com.alibaba.wasp.util.JVMClusterUtil.FServerThread;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This class creates a single process Wasp cluster. One thread is created for a
 * master and one per fserver.
 * 
 * Call {@link #startup()} to start the cluster running and {@link #shutdown()}
 * to close it all down. {@link #join} the cluster is you want to wait on
 * shutdown completion.
 * 
 * <p>
 * Runs master on port 60000 by defaul.To use a port other than 60000, set the
 * wasp.master to a value of 'local:PORT': that is 'local', not 'localhost', and
 * the port number the master should use instead of 60000.
 * 
 */
public class LocalWaspCluster {
  static final Log LOG = LogFactory.getLog(LocalWaspCluster.class);
  private final List<JVMClusterUtil.MasterThread> masterThreads = new CopyOnWriteArrayList<JVMClusterUtil.MasterThread>();
  private final List<FServerThread> fserverThreads = new CopyOnWriteArrayList<FServerThread>();
  private final static int DEFAULT_NO = 1;
  /** local mode */
  public static final String LOCAL = "local";
  /** 'local:' */
  public static final String LOCAL_COLON = LOCAL + ":";
  private final Configuration conf;
  private final Class<? extends FMaster> masterClass;
  private final Class<? extends FServer> fserverClass;

  /**
   * Constructor.
   *
   * @param conf
   * @throws java.io.IOException
   */
  public LocalWaspCluster(final Configuration conf) throws IOException {
    this(conf, DEFAULT_NO);
  }

  /**
   * Constructor.
   *
   * @param conf
   *          Configuration to use. Post construction has the master's address.
   * @param noFServers
   *          Count of fservers to start.
   * @throws java.io.IOException
   */
  public LocalWaspCluster(final Configuration conf, final int noFServers)
      throws IOException {
    this(conf, 1, noFServers, getMasterImplementation(conf),
        getFServerImplementation(conf));
  }

  /**
   * Constructor.
   *
   * @param conf
   *          Configuration to use. Post construction has the active master
   *          address.
   * @param noMasters
   *          Count of masters to start.
   * @param noFServers
   *          Count of fservers to start.
   * @throws java.io.IOException
   */
  public LocalWaspCluster(final Configuration conf, final int noMasters,
      final int noFServers) throws IOException {
    this(conf, noMasters, noFServers, getMasterImplementation(conf),
        getFServerImplementation(conf));
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends FServer> getFServerImplementation(
      final Configuration conf) {
    return (Class<? extends FServer>) conf.getClass(FConstants.FSERVER_IMPL,
        FServer.class);
  }

  @SuppressWarnings("unchecked")
  private static Class<? extends FMaster> getMasterImplementation(
      final Configuration conf) {
    return (Class<? extends FMaster>) conf.getClass(FConstants.MASTER_IMPL,
        FMaster.class);
  }

  /**
   * Constructor.
   *
   * @param conf
   *          Configuration to use. Post construction has the master's address.
   * @param noMasters
   *          Count of masters to start.
   * @param noFServers
   *          Count of fservers to start.
   * @param masterClass
   * @param fserverClass
   * @throws java.io.IOException
   */
  @SuppressWarnings("unchecked")
  public LocalWaspCluster(final Configuration conf, final int noMasters,
      final int noFServers, final Class<? extends FMaster> masterClass,
      final Class<? extends FServer> fserverClass) throws IOException {
    this.conf = conf;
    // Always have masters and fservers come up on port '0' so we don't
    // clash over default ports.
    conf.set(FConstants.MASTER_PORT, "0");
    conf.set(FConstants.FSERVER_PORT, "0");
    this.masterClass = (Class<? extends FMaster>) conf.getClass(
        FConstants.MASTER_IMPL, masterClass);
    // Start the FMasters.
    for (int i = 0; i < noMasters; i++) {
      addMaster(new Configuration(conf), i);
    }

    // Wait for master active.
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    // Start the FServers.
    this.fserverClass = (Class<? extends FServer>) conf.getClass(
        FConstants.FSERVER_IMPL, fserverClass);

    for (int i = 0; i < noFServers; i++) {
      addFServer(new Configuration(conf), i);
    }
  }

  public JVMClusterUtil.FServerThread addFServer() throws IOException {
    return addFServer(new Configuration(conf), this.fserverThreads.size());
  }

  public JVMClusterUtil.FServerThread addFServer(final Configuration config,
      final int index) throws IOException {
    try {
      return new PrivilegedExceptionAction<FServerThread>() {
        public JVMClusterUtil.FServerThread run() throws Exception {
          JVMClusterUtil.FServerThread rst = JVMClusterUtil
              .createFServerThread(config, fserverClass, index);
          fserverThreads.add(rst);
          return rst;
        }
      }.run();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public JVMClusterUtil.MasterThread addMaster() throws IOException {
    return addMaster(new Configuration(conf), this.masterThreads.size());
  }

  public JVMClusterUtil.MasterThread addMaster(Configuration c, final int index)
      throws IOException {
    // Create each master with its own Configuration instance so each has
    // its HConnection instance rather than share (see HBASE_INSTANCES down in
    // the guts of HConnectionManager.
    JVMClusterUtil.MasterThread mt = JVMClusterUtil.createMasterThread(c,
        (Class<? extends FMaster>) conf.getClass(FConstants.MASTER_IMPL,
            this.masterClass), index);
    this.masterThreads.add(mt);
    return mt;
  }

  /**
   * @param serverNumber
   * @return fserver
   */
  public FServer getFServer(int serverNumber) {
    return fserverThreads.get(serverNumber).getFServer();
  }

  /**
   * @return Read-only list of fserver threads.
   */
  public List<FServerThread> getFServers() {
    return Collections.unmodifiableList(this.fserverThreads);
  }

  /**
   * @return List of running servers (Some servers may have been killed or
   *         aborted during lifetime of cluster; these servers are not included
   *         in this list).
   */
  public List<FServerThread> getLiveFServers() {
    List<FServerThread> liveServers = new ArrayList<FServerThread>();
    List<FServerThread> list = getFServers();
    for (JVMClusterUtil.FServerThread rst : list) {
      if (rst.isAlive())
        liveServers.add(rst);
      else
        LOG.info("Not alive " + rst.getName());
    }
    return liveServers;
  }

  /**
   * Wait for the specified fserver to stop Removes this thread from list of
   * running threads.
   *
   * @param serverNumber
   * @return Name of fserver that just went down.
   */
  public String waitOnFServer(int serverNumber) {
    JVMClusterUtil.FServerThread fserverThread = this.fserverThreads
        .remove(serverNumber);
    while (fserverThread.isAlive()) {
      try {
        LOG.info("Waiting on " + fserverThread.getFServer().toString());
        fserverThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return fserverThread.getName();
  }

  /**
   * Wait for the specified fserver to stop Removes this thread from list of
   * running threads.
   *
   * @param rst
   * @return Name of fserver that just went down.
   */
  public String waitOnFServer(JVMClusterUtil.FServerThread rst) {
    while (rst.isAlive()) {
      try {
        LOG.info("Waiting on " + rst.getFServer().toString());
        rst.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    for (int i = 0; i < fserverThreads.size(); i++) {
      if (fserverThreads.get(i) == rst) {
        fserverThreads.remove(i);
        break;
      }
    }
    return rst.getName();
  }

  /**
   * @param serverNumber
   * @return the FMaster thread
   */
  public FMaster getMaster(int serverNumber) {
    return masterThreads.get(serverNumber).getMaster();
  }

  /**
   * Gets the current active master, if available. If no active master, returns
   * null.
   *
   * @return the FMaster for the active master
   */
  public FMaster getActiveMaster() {
    for (JVMClusterUtil.MasterThread mt : masterThreads) {
      if (mt.getMaster().isActiveMaster()) {
        // Ensure that the current active master is not stopped.
        // We don't want to return a stopping master as an active master.
        if (mt.getMaster().isActiveMaster() && !mt.getMaster().isStopped()) {
          return mt.getMaster();
        }
      }
    }
    return null;
  }

  /**
   * @return Read-only list of master threads.
   */
  public List<JVMClusterUtil.MasterThread> getMasters() {
    return Collections.unmodifiableList(this.masterThreads);
  }

  /**
   * @return List of running master servers (Some servers may have been killed
   *         or aborted during lifetime of cluster; these servers are not
   *         included in this list).
   */
  public List<JVMClusterUtil.MasterThread> getLiveMasters() {
    List<JVMClusterUtil.MasterThread> liveServers = new ArrayList<JVMClusterUtil.MasterThread>();
    List<JVMClusterUtil.MasterThread> list = getMasters();
    for (JVMClusterUtil.MasterThread mt : list) {
      if (mt.isAlive()) {
        liveServers.add(mt);
      }
    }
    return liveServers;
  }

  /**
   * Wait for the specified master to stop Removes this thread from list of
   * running threads.
   *
   * @param serverNumber
   * @return Name of master that just went down.
   */
  public String waitOnMaster(int serverNumber) {
    JVMClusterUtil.MasterThread masterThread = this.masterThreads
        .remove(serverNumber);
    while (masterThread.isAlive()) {
      try {
        LOG.info("Waiting on "
            + masterThread.getMaster().getServerName().toString());
        masterThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return masterThread.getName();
  }

  /**
   * Wait for the specified master to stop Removes this thread from list of
   * running threads.
   *
   * @param masterThread
   * @return Name of master that just went down.
   */
  public String waitOnMaster(JVMClusterUtil.MasterThread masterThread) {
    while (masterThread.isAlive()) {
      try {
        LOG.info("Waiting on "
            + masterThread.getMaster().getServerName().toString());
        masterThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    for (int i = 0; i < masterThreads.size(); i++) {
      if (masterThreads.get(i) == masterThread) {
        masterThreads.remove(i);
        break;
      }
    }
    return masterThread.getName();
  }

  /**
   * Wait for Mini Wasp Cluster to shut down. Presumes you've already called
   * {@link #shutdown()}.
   */
  public void join() {
    if (this.fserverThreads != null) {
      for (Thread t : this.fserverThreads) {
        if (t.isAlive()) {
          try {
            Threads.threadDumpingIsAlive(t);
          } catch (InterruptedException e) {
            LOG.debug("Interrupted", e);
          }
        }
      }
    }
    if (this.masterThreads != null) {
      for (Thread t : this.masterThreads) {
        if (t.isAlive()) {
          try {
            Threads.threadDumpingIsAlive(t);
          } catch (InterruptedException e) {
            LOG.debug("Interrupted", e);
          }
        }
      }
    }
  }

  /**
   * Start the cluster.
   */
  public void startup() throws IOException {
    JVMClusterUtil.startup(this.masterThreads, this.fserverThreads);
  }

  /**
   * Shut down the mini Wasp cluster
   */
  public void shutdown() throws IOException {
    JVMClusterUtil.shutdown(this.masterThreads, this.fserverThreads);
  }

  /**
   * @param c
   *          Configuration to check.
   * @return True if a 'local' address in wasp.master value.
   */
  public static boolean isLocal(final Configuration c) {
    boolean mode = c.getBoolean(FConstants.CLUSTER_DISTRIBUTED,
        FConstants.DEFAULT_CLUSTER_DISTRIBUTED);
    return (mode == FConstants.CLUSTER_IS_LOCAL);
  }

  /**
   * Test things basically work.
   *
   * @param args
   * @throws java.io.IOException
   */
  public static void main(String[] args) throws IOException {
    Configuration conf = WaspConfiguration.create();
    LocalWaspCluster cluster = new LocalWaspCluster(conf);
    cluster.startup();
    WaspAdmin admin = new WaspAdmin(conf);
    FTable table = new FTable();
    table.setTableName(cluster.getClass().getName());
    admin.createTable(table);
    cluster.shutdown();
  }
}
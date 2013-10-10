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
package com.alibaba.wasp.master;

import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.LocalWaspCluster;
import com.alibaba.wasp.MasterNotRunningException;
import com.alibaba.wasp.ZNodeClearer;
import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.client.WaspAdmin;
import com.alibaba.wasp.fserver.FServer;
import com.alibaba.wasp.util.JVMClusterUtil;
import com.alibaba.wasp.util.ServerCommandLine;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class FMasterCommandLine extends ServerCommandLine {
  private static final Log LOG = LogFactory.getLog(FMasterCommandLine.class);

  private static final String USAGE = "Usage: Master [opts] start|stop|clear\n"
      + " start  Start Master. If local mode, start Master and FServer in same JVM\n"
      + " stop   Start cluster shutdown; Master signals FServer shutdown\n"
      + " clear  Delete the master znode in ZooKeeper after a master crashes\n "
      + " where [opts] are:\n"
      + "   --minServers=<servers>    Minimum FServers needed to host user tables.\n"
      + "   --backup                  Master should start in backup mode";

  private final Class<? extends FMaster> masterClass;

  public FMasterCommandLine(Class<? extends FMaster> masterClass) {
    this.masterClass = masterClass;
  }

  protected String getUsage() {
    return USAGE;
  }

  public int run(String args[]) throws Exception {
    Options opt = new Options();
    opt.addOption("minServers", true,
        "Minimum FServers needed to host user tables");
    opt.addOption("backup", false,
        "Do not try to become FMaster until the primary fails");

    CommandLine cmd;
    try {
      cmd = new GnuParser().parse(opt, args);
    } catch (ParseException e) {
      LOG.error("Could not parse: ", e);
      usage(null);
      return -1;
    }

    if (cmd.hasOption("minServers")) {
      String val = cmd.getOptionValue("minServers");
      getConf().setInt("wasp.fserver.count.min", Integer.valueOf(val));
      LOG.debug("minServers set to " + val);
    }

    // check if we are the backup master - override the conf if so
    if (cmd.hasOption("backup")) {
      getConf().setBoolean(FConstants.MASTER_TYPE_BACKUP, true);
    }

    List<String> remainingArgs = cmd.getArgList();
    if (remainingArgs.size() != 1) {
      usage(null);
      return -1;
    }

    String command = remainingArgs.get(0);

    if ("start".equals(command)) {
      return startMaster();
    } else if ("stop".equals(command)) {
      return stopMaster();
    } else if ("clear".equals(command)) {
      return (ZNodeClearer.clear(getConf()) ? 0 : -1);
    } else {
      usage("Invalid command: " + command);
      return -1;
    }
  }

  private int startMaster() {
    Configuration conf = getConf();
    try {
      // If 'local', defer to LocalWaspCluster instance. Starts master
      // and fserver both in the one JVM.
      if (LocalWaspCluster.isLocal(conf)) {
        final MiniZooKeeperCluster zooKeeperCluster = new MiniZooKeeperCluster();
        File zkDataPath = new File(conf.get(FConstants.ZOOKEEPER_DATA_DIR));
        int zkClientPort = conf.getInt(FConstants.ZOOKEEPER_CLIENT_PORT, 0);
        if (zkClientPort == 0) {
          throw new IOException("No config value for "
              + FConstants.ZOOKEEPER_CLIENT_PORT);
        }
        zooKeeperCluster.setDefaultClientPort(zkClientPort);
        int clientPort = zooKeeperCluster.startup(zkDataPath);
        if (clientPort != zkClientPort) {
          String errorMsg = "Could not start ZK at requested port of "
              + zkClientPort + ".  ZK was started at port: " + clientPort
              + ".  Aborting as clients (e.g. shell) will not be able to find "
              + "this ZK quorum.";
          System.err.println(errorMsg);
          throw new IOException(errorMsg);
        }
        conf.set(FConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(clientPort));
        // Need to have the zk cluster shutdown when master is shutdown.
        // Run a subclass that does the zk cluster shutdown on its way out.
        LocalWaspCluster cluster = new LocalWaspCluster(conf, 1, 1,
            LocalFMaster.class, FServer.class);
        ((LocalFMaster) cluster.getMaster(0)).setZKCluster(zooKeeperCluster);
        cluster.startup();
        waitOnMasterThreads(cluster);
      } else {
        FMaster master = FMaster.constructMaster(masterClass, conf);
        if (master.isStopped()) {
          LOG.info("Won't bring the Master up as a shutdown is requested");
          return -1;
        }
        master.start();
        master.join();
        if (master.isAborted())
          throw new RuntimeException("FMaster Aborted");
      }
    } catch (Throwable t) {
      LOG.error("Failed to start master", t);
      return -1;
    }
    return 0;
  }

  private int stopMaster() {
    WaspAdmin adm = null;
    try {
      Configuration conf = getConf();
      // Don't try more than once
      conf.setInt("wasp.client.retries.number", 1);
      adm = new WaspAdmin(getConf());
    } catch (MasterNotRunningException e) {
      LOG.error("Master not running");
      return -1;
    } catch (ZooKeeperConnectionException e) {
      LOG.error("ZooKeeper not available");
      return -1;
    }
    try {
      adm.shutdown();
    } catch (Throwable t) {
      LOG.error("Failed to stop master", t);
      return -1;
    } finally {
      if (adm != null) {
        try {
          adm.close();
        } catch (IOException e) {
          LOG.error("Failed to close admin.");
          return -1;
        }
      }
    }
    return 0;
  }

  private void waitOnMasterThreads(LocalWaspCluster cluster)
      throws InterruptedException {
    List<JVMClusterUtil.MasterThread> masters = cluster.getMasters();
    List<JVMClusterUtil.FServerThread> fservers = cluster.getFServers();

    if (masters != null) {
      for (JVMClusterUtil.MasterThread t : masters) {
        t.join();
        if (t.getMaster().isAborted()) {
          closeAllFServerThreads(fservers);
          throw new RuntimeException("FMaster Aborted");
        }
      }
    }
  }

  private static void closeAllFServerThreads(
      List<JVMClusterUtil.FServerThread> fservers) {
    for (JVMClusterUtil.FServerThread t : fservers) {
      t.getFServer().stop("FMaster Aborted; Bringing down fservers");
    }
  }

  /*
   * Version of master that will shutdown the passed zk cluster on its way out.
   */
  public static class LocalFMaster extends FMaster {
    private MiniZooKeeperCluster zkcluster = null;

    public LocalFMaster(Configuration conf) throws IOException,
        KeeperException, InterruptedException {
      super(conf);
    }

    @Override
    public void run() {
      super.run();
      if (this.zkcluster != null) {
        try {
          this.zkcluster.shutdown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    void setZKCluster(final MiniZooKeeperCluster zkcluster) {
      this.zkcluster = zkcluster;
    }
  }
}

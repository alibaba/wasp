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
package com.alibaba.wasp.util;

import com.alibaba.wasp.fserver.FServer;
import com.alibaba.wasp.master.FMaster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Utility used running a cluster all in the one JVM.
 */
public class JVMClusterUtil {
  private static final Log LOG = LogFactory.getLog(JVMClusterUtil.class);

  /**
   * Datastructure to hold FServer Thread and FServer instance
   */
  public static class FServerThread extends Thread {
    private final FServer fserver;

    public FServerThread(final FServer r, final int index) {
      super(r, "FServer:" + index + ";" + r.getServerName());
      this.fserver = r;
    }

    /** @return the fserver */
    public FServer getFServer() {
      return this.fserver;
    }

    /**
     * Block until the fserver has come online, indicating it is ready
     * to be used.
     */
    public void waitForServerOnline() {
      // The server is marked online after the init method completes inside of
      // the fserver#run method. fserver#init can fail for whatever entityGroup.
      // In those cases, we'll jump out of the run without setting online flag.
      // Check stopRequested so we don't wait here a flag that will never be
      // flipped.
      fserver.waitForServerOnline();
    }
  }

  /**
   * Creates a {@link FServerThread}.
   * Call 'start' on the returned thread to make it run.
   * @param c Configuration to use.
   * @param hrsc Class to create.
   * @param index Used distinguishing the object returned.
   * @throws java.io.IOException
   * @return FServer added.
   */
  public static JVMClusterUtil.FServerThread createFServerThread(
      final Configuration c, final Class<? extends FServer> hrsc,
      final int index)
  throws IOException {
    FServer server;
    try {
      Constructor<? extends FServer> ctor = hrsc.getConstructor(Configuration.class);
      ctor.setAccessible(true);
      server = ctor.newInstance(c);
    } catch (InvocationTargetException ite) {
      Throwable target = ite.getTargetException();
      throw new RuntimeException("Failed construction of FServer: " +
        hrsc.toString() + ((target.getCause() != null)?
          target.getCause().getMessage(): ""), target);
    } catch (Exception e) {
      IOException ioe = new IOException();
      ioe.initCause(e);
      throw ioe;
    }
    return new JVMClusterUtil.FServerThread(server, index);
  }


  /**
   * Datastructure to hold Master Thread and Master instance
   */
  public static class MasterThread extends Thread {
    private final FMaster master;

    public MasterThread(final FMaster m, final int index) {
      super(m, "Master:" + index + ";" + m.getServerName());
      this.master = m;
    }

    /** @return the master */
    public FMaster getMaster() {
      return this.master;
    }
  }

  /**
   * Creates a {@link MasterThread}.
   * Call 'start' on the returned thread to make it run.
   * @param c Configuration to use.
   * @param hmc Class to create.
   * @param index Used distinguishing the object returned.
   * @throws java.io.IOException
   * @return Master added.
   */
  public static JVMClusterUtil.MasterThread createMasterThread(
      final Configuration c, final Class<? extends FMaster> hmc,
      final int index)
  throws IOException {
    FMaster server;
    try {
      server = hmc.getConstructor(Configuration.class).newInstance(c);
    } catch (InvocationTargetException ite) {
      Throwable target = ite.getTargetException();
      throw new RuntimeException("Failed construction of Master: " +
        hmc.toString() + ((target.getCause() != null)?
          target.getCause().getMessage(): ""), target);
    } catch (Exception e) {
      IOException ioe = new IOException();
      ioe.initCause(e);
      throw ioe;
    }
    return new JVMClusterUtil.MasterThread(server, index);
  }

  private static JVMClusterUtil.MasterThread findActiveMaster(
    List<MasterThread> masters) {
    for (JVMClusterUtil.MasterThread t : masters) {
      if (t.master.isActiveMaster()) {
        return t;
      }
    }

    return null;
  }

  /**
   * Start the cluster.  Waits until there is a primary master initialized
   * and returns its address.
   * @param masters
   * @param fservers
   * @return Address to use contacting primary master.
   */
  public static String startup(final List<MasterThread> masters,
      final List<FServerThread> fservers) throws IOException {

    if (masters == null || masters.isEmpty()) {
      return null;
    }

    for (JVMClusterUtil.MasterThread t : masters) {
      t.start();
    }

    // Wait for an active master
    // having an active master before starting the fserver threads allows
    //  then to succeed on their connection to master
    long startTime = System.currentTimeMillis();
    while (findActiveMaster(masters) == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {
      }
      if (System.currentTimeMillis() > startTime + 30000) {
        throw new RuntimeException("Master not active after 30 seconds");
      }
    }

    if (fservers != null) {
      for (JVMClusterUtil.FServerThread t: fservers) {
        t.start();
      }
    }

    // Wait for an active master to be initialized (implies being master)
    //  with this, when we return the cluster is complete
    startTime = System.currentTimeMillis();
    while (true) {
      JVMClusterUtil.MasterThread t = findActiveMaster(masters);
      if (t != null && t.master.isInitialized()) {
        return t.master.getServerName().toString();
      }
      if (System.currentTimeMillis() > startTime + 200000) {
        throw new RuntimeException("Master not initialized after 200 seconds");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {
        // Keep waiting
      }
    }
  }

  /**
   * @param masters
   * @param fservers
   */
  public static void shutdown(final List<MasterThread> masters,
      final List<FServerThread> fservers) throws IOException {
    LOG.debug("Shutting down HBase Cluster");
    if (masters != null) {
      // Do backups first.
      JVMClusterUtil.MasterThread activeMaster = null;
      for (JVMClusterUtil.MasterThread t : masters) {
        if (!t.master.isActiveMaster()) {
          t.master.stopMaster();
        } else {
          activeMaster = t;
        }
      }
      // Do active after.
      if (activeMaster != null) activeMaster.master.shutdown();
    }
    if (fservers != null) {
      for (FServerThread t : fservers) {
        if (t.isAlive()) {
          try {
            t.getFServer().stop("Shutdown requested");
            t.join();
          } catch (InterruptedException e) {
            // continue
          }
        }
      }
    }
    if (masters != null) {
      for (JVMClusterUtil.MasterThread t : masters) {
        while (t.master.isAlive()) {
          try {
            // The below has been replaced to debug sometime hangs on end of
            // tests.
            // this.master.join():
            Threads.threadDumpingIsAlive(t.master.getThread());
          } catch(InterruptedException e) {
            // continue
          }
        }
      }
    }
    LOG.info("Shutdown of " +
      ((masters != null) ? masters.size() : "0") + " master(s) and " +
      ((fservers != null) ? fservers.size() : "0") +
      " fserver(s) complete");
  }
}

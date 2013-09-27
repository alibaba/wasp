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
package com.alibaba.wasp.fserver;

import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.LocalWaspCluster;
import com.alibaba.wasp.util.ServerCommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Class responsible for parsing the command line and starting the FServer.
 */
public class FServerCommandLine extends ServerCommandLine {
  private static final Log LOG = LogFactory.getLog(FServerCommandLine.class);

  private final Class<? extends FServer> fserverClass;

  private static final String USAGE = "Usage: FServer [-D conf.param=value] start";

  public FServerCommandLine(Class<? extends FServer> clazz) {
    this.fserverClass = clazz;
  }

  protected String getUsage() {
    return USAGE;
  }

  private int start() throws Exception {
    Configuration conf = getConf();

    // If 'local', don't start a fserver here. Defer to
    // LocalWaspCluster. It manages 'local' clusters.
    if (LocalWaspCluster.isLocal(conf)) {
      LOG.warn("Not starting a distinct fserver because "
          + FConstants.CLUSTER_DISTRIBUTED + " is false");
    } else {
      logJVMInfo();
      FServer fs = FServer.constructFServer(fserverClass, conf);
      FServer.startFServer(fs);
    }
    return 0;
  }

  public int run(String args[]) throws Exception {
    if (args.length != 1) {
      usage(null);
      return -1;
    }

    String cmd = args[0];

    if ("start".equals(cmd)) {
      return start();
    } else if ("stop".equals(cmd)) {
      System.err.println("To shutdown the fserver run "
          + "bin/wasp-daemon.sh stop fserver or send a kill signal to"
          + "the fserver pid");
      return -1;
    } else {
      usage("Unknown command: " + args[0]);
      return -1;
    }
  }
}

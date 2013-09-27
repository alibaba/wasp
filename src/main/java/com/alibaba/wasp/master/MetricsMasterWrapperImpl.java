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

import com.alibaba.wasp.ClusterId;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.master.metrics.MetricsMasterWrapper;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;

/**
 * Impl for exposing FMaster Information through JMX
 */
public class MetricsMasterWrapperImpl implements MetricsMasterWrapper {

  private final FMaster master;

  public MetricsMasterWrapperImpl(final FMaster master) {
    this.master = master;
  }

  @Override
  public double getAverageLoad() {
    return master.getAverageLoad();
  }

  @Override
  public String getClusterId() {
    ClusterId clusterId = master.getClusterId();
    if (clusterId == null) {
      return "";
    }
    return clusterId.toString();
  }

  @Override
  public String getZookeeperQuorum() {
    ZooKeeperWatcher zk = master.getZooKeeperWatcher();
    if (zk == null) {
      return "";
    }
    return zk.getQuorum();
  }

  @Override
  public long getStartTime() {
    return master.getMasterStartTime();
  }

  @Override
  public long getActiveTime() {
    return master.getMasterActiveTime();
  }

  @Override
  public int getFServers() {
    FServerManager serverManager = this.master.getFServerManager();
    if (serverManager == null) {
      return 0;
    }
    return serverManager.getOnlineServers().size();
  }

  @Override
  public int getDeadFServers() {
    FServerManager serverManager = this.master.getFServerManager();
    if (serverManager == null) {
      return 0;
    }
    return serverManager.getDeadServers().size();
  }

  @Override
  public String getServerName() {
    ServerName serverName = master.getServerName();
    if (serverName == null) {
      return "";
    }
    return serverName.getServerName();
  }

  @Override
  public boolean getIsActiveMaster() {
    return master.isActiveMaster();
  }
}

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
package com.alibaba.wasp.client;

import java.io.IOException;
import java.util.List;

import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.zookeeper.FServerTracker;
import com.alibaba.wasp.zookeeper.ZKUtil;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

public class FClientFServerTracker extends FServerTracker {

  /**
   * Hacked from FServerTracker.
   * 
   * @param watcher
   */
  public FClientFServerTracker(ZooKeeperWatcher watcher) {
    super(watcher, null, null);
  }

  @Override
  public void nodeDeleted(String path) {
    if (path.startsWith(watcher.fsZNode)) {
      String serverName = ZKUtil.getNodeName(path);
      LOG.info("FServer ephemeral node deleted, processing expiration ["
          + serverName + "]");
      ServerName sn = ServerName.parseServerName(serverName);
      remove(sn);
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(watcher.fsZNode)) {
      try {
        List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher,
            watcher.fsZNode);
        add(servers);
      } catch (IOException e) {
        LOG.warn("Unexpected zk exception getting RS nodes", e);
      } catch (KeeperException e) {
        LOG.warn("Unexpected zk exception getting RS nodes", e);
      }
    }
  }
}

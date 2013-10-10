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
package com.alibaba.wasp.util;

import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.fserver.EntityGroup;
import com.alibaba.wasp.fserver.EntityGroupServices;
import com.alibaba.wasp.fserver.FServerServices;
import com.alibaba.wasp.fserver.GlobalEntityGroup;
import com.alibaba.wasp.fserver.Leases;
import com.alibaba.wasp.storage.StorageActionManager;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;

/**
 * Basic mock fserver services.
 */
public class MockFServerServices implements FServerServices {
  private final Map<String, EntityGroup> entityGroups = new HashMap<String, EntityGroup>();
  private boolean stopping = false;
  private final ConcurrentSkipListMap<byte[], Boolean> eit = new ConcurrentSkipListMap<byte[], Boolean>(
      Bytes.BYTES_COMPARATOR);
  private GlobalEntityGroup globalEntityGroup = new GlobalEntityGroup(this);

  /**
   * @see com.alibaba.wasp.fserver.OnlineEntityGroups#addToOnlineEntityGroups(com.alibaba.wasp.fserver.EntityGroup)
   */
  @Override
  public void addToOnlineEntityGroups(EntityGroup e) {
    this.entityGroups.put(e.getEntityGroupInfo().getEncodedName(), e);
  }

  /**
   * @see com.alibaba.wasp.fserver.OnlineEntityGroups#removeFromOnlineEntityGroups(String)
   */
  @Override
  public boolean removeFromOnlineEntityGroups(String encodedEntityGroupName) {
    return this.entityGroups.remove(encodedEntityGroupName) != null;
  }

  /**
   * @see com.alibaba.wasp.fserver.OnlineEntityGroups#getFromOnlineEntityGroups(String)
   */
  @Override
  public EntityGroup getFromOnlineEntityGroups(String encodedEntityGroupName) {
    return this.entityGroups.get(encodedEntityGroupName);
  }

  @Override
  public boolean isStopping() {
    return this.stopping;
  }

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public void abort(String why, Throwable e) {
    // no-op
  }

  @Override
  public void stop(String why) {
    // no-op
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public boolean isAborted() {
    return false;
  }

  @Override
  public Leases getLeases() {
    return null;
  }

  /**
   * @see com.alibaba.wasp.fserver.OnlineEntityGroups#getOnlineEntityGroups(byte[])
   */
  @Override
  public List<EntityGroup> getOnlineEntityGroups(byte[] tableName)
      throws IOException {
    return null;
  }

  /**
   * @see com.alibaba.wasp.fserver.OnlineEntityGroups#getOnlineEntityGroups()
   */
  @Override
  public Collection<EntityGroup> getOnlineEntityGroups() throws IOException {
    return null;
  }

  /**
   * @see com.alibaba.wasp.fserver.FServerServices#getEntityGroupsInTransitionInFS()
   */
  @Override
  public Map<byte[], Boolean> getEntityGroupsInTransitionInFS() {
    return eit;
  }

  /**
   * @see com.alibaba.wasp.fserver.FServerServices#getThreadPool()
   */
  @Override
  public ExecutorService getThreadPool() {
    return null;
  }

  /**
   * @see com.alibaba.wasp.fserver.FServerServices#postOpenDeployTasks(com.alibaba.wasp.fserver.EntityGroup,
   *      boolean)
   */
  @Override
  public void postOpenDeployTasks(EntityGroup entityGroup, boolean daughter)
      throws IOException {
    addToOnlineEntityGroups(entityGroup);
  }

  /**
   * @see com.alibaba.wasp.fserver.FServerServices#getGlobalEntityGroup()
   */
  @Override
  public EntityGroupServices getGlobalEntityGroup() {
    return globalEntityGroup;
  }

  /**
   * @see com.alibaba.wasp.fserver.FServerServices#getActionManager()
   */
  @Override
  public StorageActionManager getActionManager() {
    return null;
  }

  /**
   * @see com.alibaba.wasp.Server#getZooKeeper()
   */
  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return null;
  }

  /**
   * @see com.alibaba.wasp.Server#getServerName()
   */
  @Override
  public ServerName getServerName() {
    return null;
  }
}

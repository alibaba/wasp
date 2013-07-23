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
package org.apache.wasp.util;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.ServerName;
import org.apache.wasp.fserver.EntityGroup;
import org.apache.wasp.fserver.EntityGroupServices;
import org.apache.wasp.fserver.FServerServices;
import org.apache.wasp.fserver.GlobalEntityGroup;
import org.apache.wasp.fserver.Leases;
import org.apache.wasp.storage.StorageActionManager;
import org.apache.wasp.zookeeper.ZooKeeperWatcher;

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
   * @see org.apache.wasp.fserver.OnlineEntityGroups#addToOnlineEntityGroups(org.apache.wasp.fserver.EntityGroup)
   */
  @Override
  public void addToOnlineEntityGroups(EntityGroup e) {
    this.entityGroups.put(e.getEntityGroupInfo().getEncodedName(), e);
  }

  /**
   * @see org.apache.wasp.fserver.OnlineEntityGroups#removeFromOnlineEntityGroups(java.lang.String)
   */
  @Override
  public boolean removeFromOnlineEntityGroups(String encodedEntityGroupName) {
    return this.entityGroups.remove(encodedEntityGroupName) != null;
  }

  /**
   * @see org.apache.wasp.fserver.OnlineEntityGroups#getFromOnlineEntityGroups(java.lang.String)
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
   * @see org.apache.wasp.fserver.OnlineEntityGroups#getOnlineEntityGroups(byte[])
   */
  @Override
  public List<EntityGroup> getOnlineEntityGroups(byte[] tableName)
      throws IOException {
    return null;
  }

  /**
   * @see org.apache.wasp.fserver.OnlineEntityGroups#getOnlineEntityGroups()
   */
  @Override
  public Collection<EntityGroup> getOnlineEntityGroups() throws IOException {
    return null;
  }

  /**
   * @see org.apache.wasp.fserver.FServerServices#getEntityGroupsInTransitionInFS()
   */
  @Override
  public Map<byte[], Boolean> getEntityGroupsInTransitionInFS() {
    return eit;
  }

  /**
   * @see org.apache.wasp.fserver.FServerServices#getThreadPool()
   */
  @Override
  public ExecutorService getThreadPool() {
    return null;
  }

  /**
   * @see org.apache.wasp.fserver.FServerServices#postOpenDeployTasks(org.apache.wasp.fserver.EntityGroup,
   *      boolean)
   */
  @Override
  public void postOpenDeployTasks(EntityGroup entityGroup, boolean daughter)
      throws IOException {
    addToOnlineEntityGroups(entityGroup);
  }

  /**
   * @see org.apache.wasp.fserver.FServerServices#getGlobalEntityGroup()
   */
  @Override
  public EntityGroupServices getGlobalEntityGroup() {
    return globalEntityGroup;
  }

  /**
   * @see org.apache.wasp.fserver.FServerServices#getActionManager()
   */
  @Override
  public StorageActionManager getActionManager() {
    return null;
  }

  /**
   * @see org.apache.wasp.Server#getZooKeeper()
   */
  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return null;
  }

  /**
   * @see org.apache.wasp.Server#getServerName()
   */
  @Override
  public ServerName getServerName() {
    return null;
  }
}

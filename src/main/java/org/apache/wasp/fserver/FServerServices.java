/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.wasp.fserver;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.wasp.storage.StorageActionManager;

/**
 * Services provided by {@link FServer}
 */
public interface FServerServices extends OnlineEntityGroups {
  /**
   * @return True if this FServer is stopping.
   */
  public boolean isStopping();

  /**
   * Get the entityGroups that are currently being opened or closed in the FS
   * 
   * @return map of entityGroupns in transition in this FS
   */
  public Map<byte[], Boolean> getEntityGroupsInTransitionInFS();

  /**
   * Get the thread pool of wasp server.
   * 
   * @return
   */
  public ExecutorService getThreadPool();

  /**
   * Tasks to perform after entityGroup open to complete deploy of entityGroup
   * on wasp server
   * 
   * @param entityGroup
   * @param daughter
   * @throws IOException
   */
  public void postOpenDeployTasks(EntityGroup entityGroup, boolean daughter)
      throws IOException;

  /**
   * @return the FServer's "Leases" service
   */
  public Leases getLeases();

  /**
   * @return the FServer's GlobalEntityGroup.
   */
  public EntityGroupServices getGlobalEntityGroup();

  /**
   * return storage manager.
   * 
   * @return
   */
  public StorageActionManager getActionManager();
}

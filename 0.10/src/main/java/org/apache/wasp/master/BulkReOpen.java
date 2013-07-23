/**
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
package org.apache.wasp.master;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.Server;
import org.apache.wasp.ServerName;

/**
 * Performs bulk reopen of the list of entityGroups provided to it.
 */
@InterfaceAudience.Private
public class BulkReOpen extends BulkAssigner {
  private final Map<ServerName, List<EntityGroupInfo>> fsToEntityGroups;
  private final AssignmentManager assignmentManager;
  private static final Log LOG = LogFactory.getLog(BulkReOpen.class);

  public BulkReOpen(final Server server,
      final Map<ServerName, List<EntityGroupInfo>> serverToEntityGroups,
      final AssignmentManager am) {
    super(server);
    this.assignmentManager = am;
    this.fsToEntityGroups = serverToEntityGroups;
  }

  /**
   * Unassign all entityGroups, so that they go through the regular entityGroup assignment
   * flow (in assignment manager) and are re-opened.
   */
  @Override
  protected void populatePool(ExecutorService pool) {
    LOG.debug("Creating threads for each entityGroup server ");
    for (Map.Entry<ServerName, List<EntityGroupInfo>> e : fsToEntityGroups
        .entrySet()) {
      final List<EntityGroupInfo> egis = e.getValue();
      // add plans for the entityGroups that need to be reopened
      Map<String, EntityGroupPlan> plans = new HashMap<String, EntityGroupPlan>();
      for (EntityGroupInfo egi : egis) {
        EntityGroupPlan reOpenPlan = assignmentManager
            .getEntityGroupReopenPlan(egi);
        plans.put(egi.getEncodedName(), reOpenPlan);
      }
      assignmentManager.addPlans(plans);
      pool.execute(new Runnable() {
        public void run() {
          assignmentManager.unassign(egis);
        }
      });
    }
  }

  /**
   * Reopen the entityGroups asynchronously, so always returns true immediately.
   * 
   * @return true
   */
  @Override
  protected boolean waitUntilDone(long timeout) {
    return true;
  }

  /**
   * Configuration knobs "hbase.bulk.reopen.threadpool.size" number of entityGroups
   * that can be reopened concurrently. The maximum number of threads the master
   * creates is never more than the number of entityGroup servers. If configuration
   * is not defined it defaults to 20
   */
  protected int getThreadCount() {
    int defaultThreadCount = super.getThreadCount();
    return this.server.getConfiguration().getInt(
        "hbase.bulk.reopen.threadpool.size", defaultThreadCount);
  }

  public boolean bulkReOpen() throws InterruptedException, IOException {
    return bulkAssign();
  }
}

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
package org.apache.wasp.master;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.Server;
import org.apache.wasp.ServerName;


/**
 * Run bulk assign. Does one RCP per fserver passing a batch of entityGroups
 * using {@link SingleServerBulkAssigner}.
 */
public class GeneralBulkAssigner extends BulkAssigner {
  private static final Log LOG = LogFactory.getLog(GeneralBulkAssigner.class);

  private Map<ServerName, List<EntityGroupInfo>> failedPlans = new ConcurrentHashMap<ServerName, List<EntityGroupInfo>>();
  private ExecutorService pool;

  final Map<ServerName, List<EntityGroupInfo>> bulkPlan;
  final AssignmentManager assignmentManager;

  GeneralBulkAssigner(final Server server,
      final Map<ServerName, List<EntityGroupInfo>> bulkPlan,
      final AssignmentManager am) {
    super(server);
    this.bulkPlan = bulkPlan;
    this.assignmentManager = am;
  }

  @Override
  protected String getThreadNamePrefix() {
    return this.server.getServerName() + "-GeneralBulkAssigner";
  }

  @Override
  protected void populatePool(ExecutorService pool) {
    this.pool = pool; // shut it down later in case some assigner hangs
    for (Map.Entry<ServerName, List<EntityGroupInfo>> e : this.bulkPlan.entrySet()) {
      pool.execute(new SingleServerBulkAssigner(e.getKey(), e.getValue(),
          this.assignmentManager, this.failedPlans));
    }
  }

  /**
   * 
   * @param timeout How long to wait.
   * @return true if done.
   */
  @Override
  protected boolean waitUntilDone(final long timeout)
      throws InterruptedException {
    Set<EntityGroupInfo> entityGroupSet = new HashSet<EntityGroupInfo>();
    for (List<EntityGroupInfo> entityGroupList : bulkPlan.values()) {
      entityGroupSet.addAll(entityGroupList);
    }

    pool.shutdown(); // no more task allowed
    int serverCount = bulkPlan.size();
    int entityGroupCount = entityGroupSet.size();
    long startTime = System.currentTimeMillis();
    long rpcWaitTime = startTime + timeout;
    while (!server.isStopped() && !pool.isTerminated()
        && rpcWaitTime > System.currentTimeMillis()) {
      if (failedPlans.isEmpty()) {
        pool.awaitTermination(100, TimeUnit.MILLISECONDS);
      } else {
        reassignFailedPlans();
      }
    }
    if (!pool.isTerminated()) {
      LOG.warn("bulk assigner is still running after "
          + (System.currentTimeMillis() - startTime) + "ms, shut it down now");
      // some assigner hangs, can't wait any more, shutdown the pool now
      List<Runnable> notStarted = pool.shutdownNow();
      if (notStarted != null && !notStarted.isEmpty()) {
        server.abort("some single server assigner hasn't started yet"
            + " when the bulk assigner timed out", null);
        return false;
      }
    }

    int reassigningEntityGroups = 0;
    if (!failedPlans.isEmpty() && !server.isStopped()) {
      reassigningEntityGroups = reassignFailedPlans();
    }

    Configuration conf = server.getConfiguration();
    long perEntityGroupOpenTimeGuesstimate = conf.getLong(
        "wasp.bulk.assignment.perentityGroup.open.time", 1000);
    long endTime = Math.max(System.currentTimeMillis(), rpcWaitTime)
        + perEntityGroupOpenTimeGuesstimate * (reassigningEntityGroups + 1);
    EntityGroupStates entityGroupStates = assignmentManager.getEntityGroupStates();
    // We're not synchronizing on entityGroupsInTransition now because we don't use
    // any iterator.
    while (!entityGroupSet.isEmpty() && !server.isStopped()
        && endTime > System.currentTimeMillis()) {
      Iterator<EntityGroupInfo> entityGroupInfoIterator = entityGroupSet.iterator();
      while (entityGroupInfoIterator.hasNext()) {
        EntityGroupInfo egi = entityGroupInfoIterator.next();
        EntityGroupState state = entityGroupStates.getEntityGroupState(egi);
        if ((!entityGroupStates.isEntityGroupInTransition(egi) && entityGroupStates
            .isEntityGroupAssigned(egi)) || state.isSplit() || state.isSplitting()) {
          entityGroupInfoIterator.remove();
        }
      }
      if (!entityGroupSet.isEmpty()) {
        entityGroupStates.waitForUpdate(100);
      }
    }

    if (LOG.isDebugEnabled()) {
      long elapsedTime = System.currentTimeMillis() - startTime;
      String status = "successfully";
      if (!entityGroupSet.isEmpty()) {
        status = "with " + entityGroupSet.size() + " entityGroups still not assigned yet";
      }
      LOG.debug("bulk assigning total " + entityGroupCount + " entityGroups to "
          + serverCount + " servers, took " + elapsedTime + "ms, " + status);
    }
    return entityGroupSet.isEmpty();
  }

  @Override
  protected long getTimeoutOnRIT() {
    // Guess timeout. Multiply the max number of entityGroups on a server
    // by how long we think one entityGroup takes opening.
    Configuration conf = server.getConfiguration();
    long perEntityGroupOpenTimeGuesstimate = conf.getLong(
        "wasp.bulk.assignment.perentityGroup.open.time", 1000);
    int maxEntityGroupsPerServer = 1;
    for (List<EntityGroupInfo> entityGroupList : bulkPlan.values()) {
      int size = entityGroupList.size();
      if (size > maxEntityGroupsPerServer) {
        maxEntityGroupsPerServer = size;
      }
    }
    long timeout = perEntityGroupOpenTimeGuesstimate
        * maxEntityGroupsPerServer
        + conf.getLong("wasp.fserver.rpc.startup.waittime", 60000)
        + conf.getLong("wasp.bulk.assignment.perfserver.rpc.waittime",
            30000) * bulkPlan.size();
    LOG.debug("Timeout-on-RIT=" + timeout);
    return timeout;
  }

  @Override
  protected UncaughtExceptionHandler getUncaughtExceptionHandler() {
    return new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        LOG.warn("Assigning entityGroups in " + t.getName(), e);
      }
    };
  }

  private int reassignFailedPlans() {
    List<EntityGroupInfo> reassigningEntityGroups = new ArrayList<EntityGroupInfo>();
    for (Map.Entry<ServerName, List<EntityGroupInfo>> e : failedPlans.entrySet()) {
      LOG.info("Failed assigning " + e.getValue().size()
          + " entityGroups to server " + e.getKey() + ", reassigning them");
      reassigningEntityGroups.addAll(failedPlans.remove(e.getKey()));
    }
    for (EntityGroupInfo entityGroup : reassigningEntityGroups) {
      assignmentManager.invokeAssign(entityGroup);
    }
    return reassigningEntityGroups.size();
  }

  /**
   * Manage bulk assigning to a server.
   */
  static class SingleServerBulkAssigner implements Runnable {
    private final ServerName fserver;
    private final List<EntityGroupInfo> entityGroups;
    private final AssignmentManager assignmentManager;
    private final Map<ServerName, List<EntityGroupInfo>> failedPlans;

    SingleServerBulkAssigner(final ServerName fserver,
        final List<EntityGroupInfo> entityGroups, final AssignmentManager am,
        final Map<ServerName, List<EntityGroupInfo>> failedPlans) {
      this.fserver = fserver;
      this.entityGroups = entityGroups;
      this.assignmentManager = am;
      this.failedPlans = failedPlans;
    }

    @Override
    public void run() {
      try {
        if (!assignmentManager.assign(fserver, entityGroups)) {
          failedPlans.put(fserver, entityGroups);
        }
      } catch (Throwable t) {
        LOG.warn("Failed bulking assigning " + entityGroups.size()
            + " entityGroup(s) to " + fserver.getServerName()
            + ", and continue to bulk assign others", t);
        failedPlans.put(fserver, entityGroups);
      }
    }
  }

}

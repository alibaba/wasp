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
package com.alibaba.wasp.master.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.master.AssignmentManager;
import com.alibaba.wasp.master.DeadServer;
import com.alibaba.wasp.master.EntityGroupState;
import com.alibaba.wasp.master.EntityGroupStates;
import com.alibaba.wasp.master.FMasterServices;
import com.alibaba.wasp.meta.FMetaEditor;
import com.alibaba.wasp.meta.FMetaReader;
import com.alibaba.wasp.meta.FMetaScanner;
import com.alibaba.wasp.meta.FMetaVisitor;
import com.alibaba.wasp.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * Process server shutdown. Server-to-handle must be already in the deadservers
 * lists.
 */
public class ServerShutdownHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(ServerShutdownHandler.class);
  private final ServerName serverName;
  private final AssignmentManager assignmentManager;
  private final DeadServer deadServers;

  public ServerShutdownHandler(Server server, FMasterServices services,
      DeadServer deadservers, ServerName serverName) {
    super(server, EventType.M_SERVER_SHUTDOWN);
    this.serverName = serverName;
    this.assignmentManager = services.getAssignmentManager();
    this.deadServers = deadservers;
    if (!this.deadServers.contains(this.serverName)) {
      LOG.warn(this.serverName + " is NOT in deadservers; it should be!");
    }
  }

  @Override
  public void process() throws IOException {
    final ServerName serverName = this.serverName;
    try {
      NavigableMap<EntityGroupInfo, Result> egis = null;
      while (!this.server.isStopped()) {
        try {
          egis = FMetaReader.getServerUserEntityGroups(
              server.getConfiguration(), this.serverName);
          break;
        } catch (IOException ioe) {
          LOG.info(
              "Received exception accessing META during server shutdown of "
                  + serverName + ", retrying META read", ioe);
        }
      }
      if (this.server.isStopped()) {
        throw new IOException("Server is stopped");
      }

      // Clean out anything in entityGroups in transition. Being conservative
      // and
      // doing after log splitting. Could do some states before -- OPENING?
      // OFFLINE? -- and then others after like CLOSING that depend on log
      // splitting.
      List<EntityGroupState> entityGroupsInTransition = assignmentManager
          .processServerShutdown(serverName);
      LOG.info("Reassigning " + ((egis == null) ? 0 : egis.size())
          + " entityGroup(s) that "
          + (serverName == null ? "null" : serverName)
          + " was carrying (skipping " + entityGroupsInTransition.size()
          + " entityGroup(s) that are already in transition)");

      // Iterate entityGroups that were on this server and assign them
      if (egis != null) {
        EntityGroupStates entityGroupStates = assignmentManager
            .getEntityGroupStates();
        List<EntityGroupInfo> toAssignEntityGroups = new ArrayList<EntityGroupInfo>();
        for (Map.Entry<EntityGroupInfo, Result> e : egis.entrySet()) {
          EntityGroupInfo egi = e.getKey();
          EntityGroupState egit = entityGroupStates
              .getEntityGroupTransitionState(egi);
          if (processDeadEntityGroup(egi, e.getValue(), assignmentManager,
              server)) {
            ServerName addressFromAM = entityGroupStates
                .getFServerOfEntityGroup(egi);
            if (addressFromAM != null && !addressFromAM.equals(this.serverName)) {
              // If this entityGroup is in transition on the dead server, it
              // must be
              // opening or pending_open, which is covered by
              // AM#processServerShutdown
              LOG.debug("Skip assigning entityGroup "
                  + egi.getEntityGroupNameAsString()
                  + " because it has been opened in "
                  + addressFromAM.getServerName());
              continue;
            }
            if (egit != null) {
              if (!egit.isOnServer(serverName) || egit.isClosed()
                  || egit.isOpened() || egit.isSplit()) {
                // Skip entityGroups that are in transition on other server,
                // or in state closed/opened/split
                LOG.info("Skip assigning entityGroup " + egit);
                continue;
              }
              try {
                // clean zk node
                LOG.info("Reassigning entityGroup with eg = " + egit
                    + " and deleting zk node if exists");
                ZKAssign.deleteNodeFailSilent(server.getZooKeeper(), egi);
              } catch (KeeperException ke) {
                this.server.abort(
                    "Unexpected ZK exception deleting unassigned node " + egi,
                    ke);
                return;
              }
            }
            toAssignEntityGroups.add(egi);
          } else if (egit != null) {
            if (egit.isSplitting() || egit.isSplit()) {
              // This will happen when the FServer went down and the call back
              // for
              // the SPLIITING or SPLIT
              // has not yet happened for node Deleted event. In that case if
              // the EntityGroup was actually
              // split
              // but the FServer had gone down before completing the split
              // process
              // then will not try to
              // assign the parent EntityGroup again. In that case we should
              // make the
              // EntityGroup offline and
              // also delete the EntityGroup from EGIT.
              assignmentManager.entityGroupOffline(egi);
            } else if ((egit.isClosing() || egit.isPendingClose())
                && assignmentManager.getZKTable().isDisablingOrDisabledTable(
                    egi.getTableNameAsString())) {
              // If the table was partially disabled and the FServer went down,
              // we
              // should clear the EGIT
              // and remove the node for the EntityGroup.
              // The egit that we use may be stale in case the table was in
              // DISABLING state
              // but though we did assign we will not be clearing the znode in
              // CLOSING state.
              // Doing this will have no harm. See HBASE-5927
              assignmentManager.deleteClosingOrClosedNode(egi);
              assignmentManager.entityGroupOffline(egi);
            } else {
              LOG.warn("THIS SHOULD NOT HAPPEN: unexpected entityGroup in transition "
                  + egit + " not to be assigned by SSH of server " + serverName);
            }
          }
        }
        try {
          assignmentManager.assign(toAssignEntityGroups);
        } catch (InterruptedException ie) {
          LOG.error("Caught " + ie + " during round-robin assignment");
          throw new IOException(ie);
        }
      }
    } finally {
      this.deadServers.finish(serverName);
    }
    LOG.info("Finished processing of shutdown of " + serverName);
  }

  /**
   * Process a dead EntityGroup from a dead FServer. Checks if the EntityGroup
   * is disabled or disabling or if the EntityGroup has a partially completed
   * split.
   * 
   * @param egi
   * @param result
   * @param assignmentManager
   * @param server
   * @return Returns true if specified EntityGroup should be assigned, false if
   *         not.
   * @throws IOException
   */
  public boolean processDeadEntityGroup(EntityGroupInfo egi, Result result,
      AssignmentManager assignmentManager, Server server) throws IOException {
    boolean tablePresent = assignmentManager.getZKTable().isTablePresent(
        egi.getTableNameAsString());
    if (!tablePresent) {
      LOG.info("The table " + egi.getTableNameAsString()
          + " was deleted.  Hence not proceeding.");
      return false;
    }
    // If table is not disabled but the EntityGroup is offlined,
    boolean disabled = assignmentManager.getZKTable().isDisabledTable(
        egi.getTableNameAsString());
    if (disabled) {
      LOG.info("The table " + egi.getTableNameAsString()
          + " was disabled.  Hence not proceeding.");
      return false;
    }
    if (egi.isOffline() && egi.isSplit()) {
      LOG.debug("Offlined and split entityGroup "
          + egi.getEntityGroupNameAsString() + "; checking daughter presence");
      if (FMetaReader.getEntityGroupAndLocation(server.getConfiguration(),
          egi.getEntityGroupName()) == null) {
        return false;
      }
      fixupDaughters(result, assignmentManager, server.getConfiguration());
      return false;
    }
    boolean disabling = assignmentManager.getZKTable().isDisablingTable(
        egi.getTableNameAsString());
    if (disabling) {
      LOG.info("The table " + egi.getTableNameAsString()
          + " is disabled.  Hence not assigning entityGroup"
          + egi.getEncodedName());
      return false;
    }
    return true;
  }

  /**
   * Check that daughter entityGroups are up in .FMETA. and if not, add them.
   * 
   * @param result
   *          The contents of the parent row in .META.
   * @return the number of daughters missing and fixed
   * @throws IOException
   */
  public static int fixupDaughters(final Result result,
      final AssignmentManager assignmentManager, final Configuration conf)
      throws IOException {
    PairOfSameType<EntityGroupInfo> daughters = EntityGroupInfo
        .getDaughterEntityGroups(result);
    int fixedA = fixupDaughter(result, daughters.getFirst(), assignmentManager,
        conf);
    int fixedB = fixupDaughter(result, daughters.getSecond(),
        assignmentManager, conf);
    return fixedA + fixedB;
  }

  /**
   * Check individual daughter is up in .FMETA.; fixup if its not.
   * 
   * @param result
   *          The contents of the parent row in .FMETA.
   * @return 1 if the daughter is missing and fixed. Otherwise 0
   * @throws IOException
   */
  static int fixupDaughter(final Result result, EntityGroupInfo daughter,
      final AssignmentManager assignmentManager, final Configuration conf)
      throws IOException {
    if (daughter == null)
      return 0;
    if (isDaughterMissing(conf, daughter)) {
      LOG.info("Fixup; missing daughter "
          + daughter.getEntityGroupNameAsString());
      FMetaEditor.addDaughter(conf, daughter, null);

      // And assign it.
      assignmentManager.assign(daughter, true, true);
      return 1;
    } else {
      LOG.debug("Daughter " + daughter.getEntityGroupNameAsString()
          + " present");
    }
    return 0;
  }

  /**
   * Look for presence of the daughter OR of a split of the daughter in .FMETA.
   * Daughter could have been split over on fserver before a run of the
   * catalogJanitor had chance to clear reference from parent.
   * 
   * @param daughter
   *          Daughter entityGroup to search for.
   * @throws IOException
   */
  private static boolean isDaughterMissing(final Configuration conf,
      final EntityGroupInfo daughter)
      throws IOException {
    FindDaughterVisitor visitor = new FindDaughterVisitor(daughter);
    // Start the scan at what should be the daughter's row in the .FMETA.
    // We will either 1., find the daughter or some derivative split of the
    // daughter (will have same table name and start row at least but will sort
    // after because has larger entityGroupid -- the entityGroupid is timestamp
    // of entityGroup
    // creation), OR, we will not find anything with same table name and start
    // row. If the latter, then assume daughter missing and do fixup.
    byte[] startrow = daughter.getTableName();
    FMetaScanner.fullScan(conf, visitor, startrow, null);
    return !visitor.foundDaughter();
  }

  /**
   * Looks for daughter. Sets a flag if daughter or some progeny of daughter is
   * found up in <code>.FMETA.</code>.
   */
  static class FindDaughterVisitor implements FMetaVisitor {
    private final EntityGroupInfo daughter;
    private boolean found = false;

    FindDaughterVisitor(final EntityGroupInfo daughter) {
      this.daughter = daughter;
    }

    /**
     * @return True if we found a daughter entityGroup during our visiting.
     */
    boolean foundDaughter() {
      return this.found;
    }

    @Override
    public boolean visit(Result r) throws IOException {
      EntityGroupInfo egi = EntityGroupInfo.getEntityGroupInfo(r);
      if (egi == null) {
        LOG.warn("No serialized EntityGroupInfo in " + r);
        return true;
      }
      byte[] value = r.getValue(FConstants.CATALOG_FAMILY,
          FConstants.EGLOCATION);
      // See if daughter is assigned to some server
      if (value == null)
        return false;

      // Now see if we have gone beyond the daughter's startrow.
      if (!Bytes.equals(daughter.getTableName(), egi.getTableName())) {
        // We fell into another table. Stop scanning.
        return false;
      }
      // If our start rows do not compare, move on.
      if (!Bytes.equals(daughter.getStartKey(), egi.getStartKey())) {
        return false;
      }
      // Else, table name and start rows compare. It means that the daughter
      // or some derivative split of the daughter is up in .FMETA. Daughter
      // exists.
      this.found = true;
      return false;
    }
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if (server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid();
  }
}

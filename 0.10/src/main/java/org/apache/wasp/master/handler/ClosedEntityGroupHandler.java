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
package org.apache.wasp.master.handler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.Server;
import org.apache.wasp.executor.EventHandler;
import org.apache.wasp.master.AssignmentManager;
import org.apache.wasp.master.EntityGroupState;

/**
 * Handles CLOSED entityGroup event on FMaster.
 * <p>
 * If table is being disabled, deletes ZK unassigned node and removes from
 * entityGroups in transition.
 * <p>
 * Otherwise, assigns the entityGroup to another server.
 */
public class ClosedEntityGroupHandler extends EventHandler {
  private static final Log LOG = LogFactory
      .getLog(ClosedEntityGroupHandler.class);
  private final AssignmentManager assignmentManager;
  private final EntityGroupInfo entityGroupInfo;

  public ClosedEntityGroupHandler(Server server,
      AssignmentManager assignmentManager, EntityGroupInfo egInfo) {
    super(server, EventType.FSERVER_ZK_ENTITYGROUP_CLOSED);
    this.assignmentManager = assignmentManager;
    this.entityGroupInfo = egInfo;
  }

  @Override
  public void process() {
    LOG.debug("Handling CLOSED event for " + entityGroupInfo.getEncodedName());
    // Check if this table is being disabled or not
    if (this.assignmentManager.getZKTable().isDisablingOrDisabledTable(
        this.entityGroupInfo.getTableNameAsString())) {
      assignmentManager.offlineDisabledEntityGroup(entityGroupInfo);
      return;
    }
    // ZK Node is in CLOSED state, assign it.
    assignmentManager.getEntityGroupStates().updateEntityGroupState(
        entityGroupInfo, EntityGroupState.State.CLOSED, null);
    // This below has to do w/ online enable/disable of a table
    assignmentManager.removeClosedEntityGroup(entityGroupInfo);
    assignmentManager.assign(entityGroupInfo, true);
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

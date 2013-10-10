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
package com.alibaba.wasp.master;

import com.alibaba.wasp.ClusterStatus;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.ServerName;
import org.apache.hadoop.conf.Configurable;

import java.util.List;
import java.util.Map;

/**
 * Makes decisions about the placement and movement of EntityGroups across
 * FServers.
 * 
 * <p>
 * Cluster-wide load balancing will occur only when there are no entityGroups in
 * transition and according to a fixed period of a time using
 * {@link #balanceCluster(java.util.Map)}.
 * 
 * <p>
 * On cluster startup, bulk assignment can be used to determine locations for
 * all EntityGroups in a cluster.
 * 
 * <p>
 * This classes produces plans for the {@link EntityGroupManager} to execute.
 */
public interface LoadBalancer extends Configurable {

  /**
   * Set the current cluster status.  This allows a LoadBalancer to map host name to a server
   * @param st
   */
  public void setClusterStatus(ClusterStatus st);


  /**
   * Set the master service.
   * @param masterServices
   */
  public void setMasterServices(FMasterServices masterServices);

  /**
   * Perform the major balance operation
   * @param clusterState
   * @return List of plans
   */
  public List<EntityGroupPlan> balanceCluster(Map<ServerName, List<EntityGroupInfo>> clusterState);

  /**
   * Perform a Round Robin assignment of entityGroups.
   * @param entityGroups
   * @param servers
   * @return Map of servername to entityGroupinfos
   */
  public Map<ServerName, List<EntityGroupInfo>> roundRobinAssignment(List<EntityGroupInfo> entityGroups, List<ServerName> servers);

  /**
   * Assign entityGroups to the previously hosting fserver
   * @param entityGroups
   * @param servers
   * @return List of plans
   */
  public Map<ServerName, List<EntityGroupInfo>> retainAssignment(Map<EntityGroupInfo, ServerName> entityGroups, List<ServerName> servers);

  /**
   * Sync assign a entityGroup
   * @param entityGroups
   * @param servers
    * @return Map entityGroupinfos to servernames
   */
  public Map<EntityGroupInfo, ServerName> immediateAssignment(List<EntityGroupInfo> entityGroups, List<ServerName> servers);

  /**
   * Get a random fserver from the list
   * @param entityGroupInfo EntityGroup for which this selection is being done.
   * @param servers
   * @return Servername
   */
  public ServerName randomAssignment(EntityGroupInfo entityGroupInfo,
                                     List<ServerName> servers);
}

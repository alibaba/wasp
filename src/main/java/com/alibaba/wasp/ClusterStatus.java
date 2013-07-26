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

package com.alibaba.wasp;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.alibaba.wasp.master.EntityGroupState;import com.alibaba.wasp.protobuf.generated.ClusterStatusProtos;import com.alibaba.wasp.protobuf.generated.WaspProtos;import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.master.EntityGroupState;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.generated.ClusterStatusProtos;
import com.alibaba.wasp.protobuf.generated.ClusterStatusProtos.EntityGroupInTransitionProtos;
import com.alibaba.wasp.protobuf.generated.ClusterStatusProtos.LiveServerInfo;
import com.alibaba.wasp.protobuf.generated.WaspProtos;
import com.alibaba.wasp.protobuf.generated.WaspProtos.EntityGroupSpecifier;
import com.alibaba.wasp.protobuf.generated.WaspProtos.EntityGroupSpecifier.EntityGroupSpecifierType;

import com.google.protobuf.ByteString;

/**
 * Status information on the Wasp cluster.
 * <p>
 * <tt>ClusterStatus</tt> provides clients with information such as:
 * <ul>
 * <li>The count and names of fservers in the cluster.</li>
 * <li>The count and names of dead fservers in the cluster.</li>
 * <li>The name of the active master for the cluster.</li>
 * <li>The name(s) of the backup master(s) for the cluster, if they exist.</li>
 * <li>The average cluster load.</li>
 * <li>The number of entity groups deployed on the cluster.</li>
 * <li>The number of requests since last report.</li>
 * <li>Detailed fserver loading and resource usage information, per server and
 * per entityGroup.</li>
 * <li>entityGroups in transition at master</li>
 * <li>The unique cluster ID</li>
 * </ul>
 */
public class ClusterStatus {
  /**
   * Version for object serialization. Incremented for changes in serialized
   * representation.
   * <dl>
   * <dt>0</dt>
   * <dd>Initial version</dd>
   * <dt>1</dt>
   * <dd>Added cluster ID</dd>
   * <dt>2</dt>
   * <dd>Added Map of ServerName to ServerLoad</dd>
   * <dt>3</dt>
   * <dd>Added master and backupMasters</dd>
   * </dl>
   */

  private Map<ServerName, ServerLoad> liveServers;
  private Collection<ServerName> deadServers;
  private ServerName master;
  private Collection<ServerName> backupMasters;
  private Map<String, EntityGroupState> intransition;
  private String clusterId;
  private boolean balancerOn;

  public ClusterStatus(final String clusterid,
      final Map<ServerName, ServerLoad> servers,
      final Collection<ServerName> deadServers, final ServerName master,
      final Collection<ServerName> backupMasters,
      final Map<String, EntityGroupState> egit, final boolean balancerOn) {
    this.liveServers = servers;
    this.deadServers = deadServers;
    this.master = master;
    this.backupMasters = backupMasters;
    this.intransition = egit;
    this.clusterId = clusterid;
    this.balancerOn = balancerOn;
  }

  /**
   * @return the names of fservers on the dead list
   */
  public Collection<ServerName> getDeadServerNames() {
    return Collections.unmodifiableCollection(deadServers);
  }

  /**
   * @return the number of fservers in the cluster
   */
  public int getServersSize() {
    return liveServers.size();
  }

  /**
   * @return the number of dead fservers in the cluster
   */
  public int getDeadServers() {
    return deadServers.size();
  }

  /**
   * @return the average cluster load
   */
  public double getAverageLoad() {
    int load = getEntityGroupsCount();
    return (double) load / (double) getServersSize();
  }

  /**
   * @return the number of entityGroups deployed on the cluster
   */
  public int getEntityGroupsCount() {
    int count = 0;
    for (Map.Entry<ServerName, ServerLoad> e : this.liveServers.entrySet()) {
      count += e.getValue().getNumberOfEntityGroups();
    }
    return count;
  }

  /**
   * @return the number of requests since last report
   */
  public int getRequestsCount() {
    int count = 0;
    for (Map.Entry<ServerName, ServerLoad> e : this.liveServers.entrySet()) {
      count += e.getValue().getTotalNumberOfRequests();
    }
    return count;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClusterStatus)) {
      return false;
    }
    return this.liveServers.equals(((ClusterStatus) o).liveServers)
        && this.deadServers.containsAll(((ClusterStatus) o).deadServers)
        && this.master.equals(((ClusterStatus) o).master)
        && this.backupMasters.containsAll(((ClusterStatus) o).backupMasters);
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  public int hashCode() {
    return this.liveServers.hashCode() + this.deadServers.hashCode()
        + this.master.hashCode() + this.backupMasters.hashCode();
  }

  //
  // Getters
  //
  public Collection<ServerName> getServers() {
    return Collections.unmodifiableCollection(this.liveServers.keySet());
  }

  /**
   * Returns detailed information about the current master {@link ServerName}.
   * 
   * @return current master information if it exists
   */
  public ServerName getMaster() {
    return this.master;
  }

  /**
   * @return the number of backup masters in the cluster
   */
  public int getBackupMastersSize() {
    return this.backupMasters.size();
  }

  /**
   * @return the names of backup masters
   */
  public Collection<ServerName> getBackupMasters() {
    return Collections.unmodifiableCollection(this.backupMasters);
  }

  /**
   * @param sn
   * @return Server's load or null if not found.
   */
  public ServerLoad getLoad(final ServerName sn) {
    return this.liveServers.get(sn);
  }

  public Map<String, EntityGroupState> getEntityGroupsInTransition() {
    return this.intransition;
  }

  public String getClusterId() {
    return clusterId;
  }

  public boolean isBalancerOn() {
    return balancerOn;
  }

  /**
   * Convert a ClutserStatus to a protobuf ClusterStatus
   * 
   * @return the protobuf ClusterStatus
   */
  public ClusterStatusProtos.ClusterStatus convert() {
    ClusterStatusProtos.ClusterStatus.Builder builder = ClusterStatusProtos.ClusterStatus
        .newBuilder();

    for (Map.Entry<ServerName, ServerLoad> entry : liveServers.entrySet()) {
      ClusterStatusProtos.LiveServerInfo.Builder lsi = ClusterStatusProtos.LiveServerInfo.newBuilder().setServer(
          ProtobufUtil.toServerName(entry.getKey()));
      lsi.setServerLoad(entry.getValue().obtainServerLoadPB());
      builder.addLiveServers(lsi.build());
    }
    for (ServerName deadServer : getDeadServerNames()) {
      builder.addDeadServers(ProtobufUtil.toServerName(deadServer));
    }

    for (Map.Entry<String, EntityGroupState> egit : getEntityGroupsInTransition()
        .entrySet()) {
      ClusterStatusProtos.EntityGroupStateProtos egs = egit.getValue()
          .convert();
      WaspProtos.EntityGroupSpecifier.Builder spec = WaspProtos.EntityGroupSpecifier.newBuilder()
          .setType(WaspProtos.EntityGroupSpecifier.EntityGroupSpecifierType.ENTITYGROUP_NAME);
      spec.setValue(ByteString.copyFrom(Bytes.toBytes(egit.getKey())));

      ClusterStatusProtos.EntityGroupInTransitionProtos pbEGIT = ClusterStatusProtos.EntityGroupInTransitionProtos
          .newBuilder().setSpec(spec.build()).setEntityGroupState(egs).build();
      builder.addEntityGroupsInTransition(pbEGIT);
    }

    builder.setClusterId(new ClusterId(getClusterId()).convert());

    builder.setMaster(ProtobufUtil.toServerName(getMaster()));
    for (ServerName backup : getBackupMasters()) {
      builder.addBackupMasters(ProtobufUtil.toServerName(backup));
    }
    builder.setBalancerOn(balancerOn);
    return builder.build();
  }

  /**
   * Convert a protobuf ClusterStatus to a ClusterStatus
   * 
   * @param proto
   *          the protobuf ClusterStatus
   * @return the converted ClusterStatus
   */
  public static ClusterStatus convert(ClusterStatusProtos.ClusterStatus proto) {
    Map<ServerName, ServerLoad> servers = new HashMap<ServerName, ServerLoad>();
    for (ClusterStatusProtos.LiveServerInfo lsi : proto.getLiveServersList()) {
      servers.put(ProtobufUtil.toServerName(lsi.getServer()), new ServerLoad(
          lsi.getServerLoad()));
    }
    Collection<ServerName> deadServers = new LinkedList<ServerName>();
    for (WaspProtos.ServerName sn : proto.getDeadServersList()) {
      deadServers.add(ProtobufUtil.toServerName(sn));
    }
    Collection<ServerName> backupMasters = new LinkedList<ServerName>();
    for (WaspProtos.ServerName sn : proto.getBackupMastersList()) {
      backupMasters.add(ProtobufUtil.toServerName(sn));
    }
    final Map<String, EntityGroupState> egit = new HashMap<String, EntityGroupState>();
    for (ClusterStatusProtos.EntityGroupInTransitionProtos entityGroup : proto
        .getEntityGroupsInTransitionList()) {
      String key = new String(entityGroup.getSpec().getValue().toByteArray());
      EntityGroupState value = EntityGroupState.convert(entityGroup
          .getEntityGroupState());
      egit.put(key, value);
    }

    return new ClusterStatus(
        ClusterId.convert(proto.getClusterId()).toString(), servers,
        deadServers, ProtobufUtil.toServerName(proto.getMaster()),
        backupMasters, egit, proto.getBalancerOn());
  }
}

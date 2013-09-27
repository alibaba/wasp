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

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.ServerName;
import org.apache.hadoop.classification.InterfaceAudience;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Stores the plan for the move of an individual EntityGroup.
 * 
 * Contains info for the EntityGroup being moved, info for the server the
 * EntityGroup should be moved from, and info for the server the EntityGroup
 * should be moved to.
 * 
 * The comparable implementation of this class compares only the EntityGroup
 * information and not the source/dest server info.
 */
@InterfaceAudience.Private
public class EntityGroupPlan implements Comparable<EntityGroupPlan> {
  private final EntityGroupInfo egi;
  private final ServerName source;
  private ServerName dest;

  public static class EntityGroupPlanComparator implements Comparator<EntityGroupPlan>, Serializable {

    private static final long serialVersionUID = 4213207330485734853L;

    @Override
    public int compare(EntityGroupPlan l, EntityGroupPlan r) {
      long diff = r.getEntityGroupInfo().getEntityGroupId()
          - l.getEntityGroupInfo().getEntityGroupId();
      if (diff < 0) return -1;
      if (diff > 0) return 1;
      return 0;
    }
  }

  /**
   * Instantiate a plan for a entityGroup move, moving the specified entityGroup from
   * the specified source server to the specified destination server.
   *
   * Destination server can be instantiated as null and later set
   * with {@link #setDestination(com.alibaba.wasp.ServerName)}.
   *
   * @param egi entityGroup to be moved
   * @param source fserver entityGroup should be moved from
   * @param dest fserver entityGroup should be moved to
   */
  public EntityGroupPlan(final EntityGroupInfo egi, ServerName source,
      ServerName dest) {
    this.egi = egi;
    this.source = source;
    this.dest = dest;
  }

  /**
   * Set the destination server for the plan for this EntityGroup.
   */
  public void setDestination(ServerName dest) {
    this.dest = dest;
  }

  /**
   * Get the source server for the plan for this EntityGroup.
   * 
   * @return server info for source
   */
  public ServerName getSource() {
    return source;
  }

  /**
   * Get the destination server for the plan for this EntityGroup.
   * 
   * @return server info for destination
   */
  public ServerName getDestination() {
    return dest;
  }

  /**
   * Get the encoded EntityGroup name for the EntityGroup this plan is for.
   * 
   * @return Encoded EntityGroup name
   */
  public String getEntityGroupName() {
    return this.egi.getEncodedName();
  }

  public EntityGroupInfo getEntityGroupInfo() {
    return this.egi;
  }

  /**
   * Compare the EntityGroup info.
   * 
   * @param o
   *          EntityGroup plan you are comparing against
   */
  @Override
  public int compareTo(EntityGroupPlan o) {
    return getEntityGroupName().compareTo(o.getEntityGroupName());
  }

  @Override
  public String toString() {
    return "egi=" + this.egi.getEntityGroupNameAsString() + ", src="
        + (this.source == null ? "" : this.source.toString()) +
      ", dest=" + (this.dest == null? "": this.dest.toString());
  }
}

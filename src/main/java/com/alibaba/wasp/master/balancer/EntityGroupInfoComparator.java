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
package com.alibaba.wasp.master.balancer;

import com.alibaba.wasp.EntityGroupInfo;

import java.util.Comparator;

/**
 * The following comparator assumes that EntityGroupId from EntityGroupInfo can
 * represent the age of the EntityGroup- larger EntityGroupId means the
 * EntityGroup is younger. This comparator is used in balanceCluster() to
 * account for the out-of-band entityGroups which were assigned to the server
 * after some other fserver crashed.
 */
class EntityGroupInfoComparator implements Comparator<EntityGroupInfo> {
  @Override
  public int compare(EntityGroupInfo l, EntityGroupInfo r) {
    long diff = r.getEntityGroupId() - l.getEntityGroupId();
    if (diff < 0)
      return -1;
    if (diff > 0)
      return 1;
    return 0;
  }
}

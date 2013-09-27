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
package com.alibaba.wasp.fserver;

import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.meta.FTable;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * A split policy determines when a entityGroup should be split.
 * 
 */
public abstract class EntityGroupSplitPolicy extends Configured {
  private static final Class<? extends EntityGroupSplitPolicy> DEFAULT_SPLIT_POLICY_CLASS = DefaultEntityGroupSplitPolicy.class;

  /**
   * The entityGroup configured for this split policy.
   */
  protected EntityGroup entityGroup;

  /**
   * Upon construction, this method will be called with the entityGroup to be
   * governed. It will be called once and only once.
   */
  protected void configureForEntityGroup(EntityGroup entityGroup) {
    Preconditions.checkState(this.entityGroup == null,
        "Policy already configured for entityGroup {}", this.entityGroup);

    this.entityGroup = entityGroup;
  }

  /**
   * @return the key at which the entityGroup should be split, or null if it cannot
   *         be split. This will only be called if shouldSplit previously
   *         returned true.
   */
  abstract protected byte[] getSplitPoint(final byte[] splitPoint);

  /**
   * Create the EntityGroupSplitPolicy configured for the given table. Each
   * 
   * @param htd
   * @param conf
   * @return
   * @throws java.io.IOException
   */
  public static EntityGroupSplitPolicy create(EntityGroup entityGroup,
      Configuration conf) throws IOException {

    Class<? extends EntityGroupSplitPolicy> clazz = getSplitPolicyClass(
        entityGroup.getTableDesc(), conf);
    EntityGroupSplitPolicy policy = ReflectionUtils.newInstance(clazz, conf);
    policy.configureForEntityGroup(entityGroup);
    return policy;
  }

  static Class<? extends EntityGroupSplitPolicy> getSplitPolicyClass(
      FTable ftd, Configuration conf) throws IOException {
    String className = conf.get(FConstants.WASP_ENTITYGROUP_SPLIT_POLICY_KEY,
        DEFAULT_SPLIT_POLICY_CLASS.getName());

    try {
      Class<? extends EntityGroupSplitPolicy> clazz = Class.forName(className)
          .asSubclass(EntityGroupSplitPolicy.class);
      return clazz;
    } catch (Exception e) {
      throw new IOException(
          "Unable to load configured entityGroup split policy '" + className
              + "' for table '" + ftd.getTableName() + "'", e);
    }
  }
}
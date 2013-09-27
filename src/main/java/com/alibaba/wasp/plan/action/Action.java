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
package com.alibaba.wasp.plan.action;

import com.alibaba.wasp.EntityGroupLocation;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * basic Action.
 */
public abstract class Action implements Configurable {

  private Configuration conf;

  // ///////////////// used for sql //////////////////
  /** wasp table name **/
  protected String fTableName;
  private EntityGroupLocation entityGroupLocation;
  // ///////////////// used for sql //////////////////
  /**
   * default
   */
  public Action() {
  }

  /**
   * @return the tableName
   */
  public String getFTableName() {
    return fTableName;
  }

  /**
   * @param tableName
   *          the tableName to set
   */
  public void setFTableName(String tableName) {
    this.fTableName = tableName;
  }

  /**
   * 
   * @return
   */
  public EntityGroupLocation getEntityGroupLocation() {
    return entityGroupLocation;
  }

  /**
   * 
   * @param entityGroupLocation
   */
  public void setEntityGroupLocation(EntityGroupLocation entityGroupLocation) {
    this.entityGroupLocation = entityGroupLocation;
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
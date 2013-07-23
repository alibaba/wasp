/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.wasp.fserver.metrics;

/**
 * Interface of class that will wrap an EntityGroup and export numbers so they
 * can be used in MetricsEntityGroupSource
 */
public interface MetricsEntityGroupWrapper {

  /**
   * Get the name of the table the EntityGroup belongs to.
   * 
   * @return The string version of the table name.
   */
  String getTableName();

  /**
   * Get the name of the EntityGroup.
   * 
   * @return The encoded name of the EntityGroup.
   */
  String getEntityGroupName();

  /**
   * Get the total number of read requests that have been issued against this
   * EntityGroup
   */
  long getReadRequestCount();

  /**
   * Get the total number of mutations that have been issued against this
   * EntityGroup.
   */
  long getWriteRequestCount();

  /**
   * Get the uncommitted redo log size of this EntityGroup.
   */
  long getTransactionLogSize();
}

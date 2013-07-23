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

import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.wasp.metrics.BaseSource;


/**
 * This interface will be implemented to allow single entity group to push
 * metrics into MetricsEntityGroupAggregateSource that will in turn push data to
 * the Hadoop metrics system.
 */
public interface MetricsEntityGroupSource extends BaseSource,
    Comparable<MetricsEntityGroupSource> {

  /**
   * The name of the metrics
   */
  static final String METRICS_NAME = "InternalEntityGroup";

  /**
   * The name of the metrics context that metrics will be under.
   */
  static final String METRICS_CONTEXT = "EntityGroup";

  /**
   * Description
   */
  static final String METRICS_DESCRIPTION = "Metrics about EntityGroup";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  static final String METRICS_JMX_CONTEXT = "EntityGroup,sub=" + METRICS_NAME;

  /**
   * Close the entity group's metrics as this entity group is closing.
   */
  void close();

  /**
   * Update related counts of puts.
   */
  void updateInsert();

  /**
   * Update related counts of deletes.
   */
  void updateDelete();

  /**
   * Update related counts of gets.
   */
  void updateSelect();

  /**
   * Update related counts of updates.
   */
  void updateUpdate();

  /**
   * Update the ObtainRowLock time histogram
   * 
   * @param t
   *          time it took
   */
  void updateObtainRowLock(long t);

  /**
   * Increment the number of slow ObtainRowLock that have happened.
   */
  void incrSlowObtainRowLock();

  /**
   * Update the PrepareInsertEntity time histogram
   * 
   * @param t
   *          time it took
   */
  void updatePrepareInsertEntity(long t);

  /**
   * Increment the number of slow PrepareInsertEntity that have happened.
   */
  void incrSlowPrepareInsertEntity();

  /**
   * Update the PrepareUpdateEntity time histogram
   * 
   * @param t
   *          time it took
   */
  void updatePrepareUpdateEntity(long t);

  /**
   * Increment the number of slow PrepareUpdateEntity that have happened.
   */
  void incrSlowPrepareUpdateEntity();

  /**
   * Update the PrepareDeleteEntity time histogram
   * 
   * @param t
   *          time it took
   */
  void updatePrepareDeleteEntity(long t);

  /**
   * Increment the number of slow PrepareDeleteEntity that have happened.
   */
  void incrSlowPrepareDeleteEntity();

  /**
   * Update the RedoLogAppend time histogram
   * 
   * @param t
   *          time it took
   */
  void updateRedoLogAppend(long t);

  /**
   * Increment the number of slow RedoLogAppend that have happened.
   */
  void incrSlowRedoLogAppend();

  /**
   * Update the BackgroundRedoLog time histogram
   * 
   * @param t
   *          time it took
   */
  void updateBackgroundRedoLog(long t);

  /**
   * Increment the number of slow BackgroundRedoLog that have happened.
   */
  void incrSlowBackgroundRedoLog();

  /**
   * Get the aggregate source to which this reports.
   */
  MetricsEntityGroupAggregateSource getAggregateSource();

  /**
   * 
   * @param mrb metricsBuilder Builder to accept metrics
   * @param all push all or only changed?
   */
  void snapshot(MetricsRecordBuilder mrb, boolean all);

  static final String OBTAINROWLOCK_KEY = "obtainRowLock";
  static final String SLOW_OBTAINROWLOCK_KEY = "slowObtainRowLock";
  static final String SLOW_OBTAINROWLOCK_DESC = "The number of obtainRowLock that took over 1000ms to complete";
  static final String PREPAREINSERTENTITY_KEY = "prepareInsertEntity";
  static final String SLOW_PREPAREINSERTENTITY_KEY = "slowPrepareInsertEntity";
  static final String SLOW_PREPAREINSERTENTITY_DESC = "The number of prepareInsertEntity that took over 1000ms to complete";
  static final String PREPAREUPDATEENTITY_KEY = "prepareUpdateEntity";
  static final String SLOW_PREPAREUPDATEENTITY_KEY = "slowPrepareUpdateEntity";
  static final String SLOW_PREPAREUPDATEENTITY_DESC = "The number of prepareUpdateEntity that took over 1000ms to complete";
  static final String PREPAREDELETEENTITY_KEY = "prepareDeleteEntity";
  static final String SLOW_PREPAREDELETEENTITY_KEY = "slowPrepareDeleteEntity";
  static final String SLOW_PREPAREDELETEENTITY_DESC = "The number of prepareDeleteEntity that took over 1000ms to complete";
  static final String REDOLOGAPPEND_KEY = "redoLogAppend";
  static final String SLOW_REDOLOGAPPEND_KEY = "slowRedoLogAppend";
  static final String SLOW_REDOLOGAPPEND_DESC = "The number of redoLogAppend that took over 1000ms to complete";
  static final String BACKGROUNDREDOLOG_KEY = "backgroundRedoLog";
  static final String SLOW_BACKGROUNDREDOLOG_KEY = "slowBackgroundRedoLog";
  static final String SLOW_BACKGROUNDREDOLOG_DESC = "The number of backgroundRedoLog that took over 1000ms to complete";

}

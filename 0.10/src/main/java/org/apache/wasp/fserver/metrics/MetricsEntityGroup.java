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
 * This is the glue between the EntityGroup and whatever hadoop shim layer is
 * loaded.
 */
public class MetricsEntityGroup {

  private MetricsEntityGroupSource source;

  public MetricsEntityGroup(MetricsEntityGroupWrapper wrapper) {
    source = MetricsFServerSourceFactory.createEntityGroup(wrapper);
  }

  public void close() {
    source.close();
  }

  public void updateInsert() {
    source.updateInsert();
  }

  public void updateDelete() {
    source.updateDelete();
  }

  public void updateUpdate() {
    source.updateUpdate();
  }

  public void updateSelect() {
    source.updateSelect();
  }

  public void updateObtainRowLock(long t) {
    if (t > 1000) {
      source.incrSlowObtainRowLock();
    }
    source.updateObtainRowLock(t);
  }

  public void updatePrepareInsertEntity(long t) {
    if (t > 1000) {
      source.incrSlowPrepareInsertEntity();
    }
    source.updatePrepareInsertEntity(t);
  }

  public void updatePrepareUpdateEntity(long t) {
    if (t > 1000) {
      source.incrSlowPrepareUpdateEntity();
    }
    source.updatePrepareUpdateEntity(t);
  }

  public void updatePrepareDeleteEntity(long t) {
    if (t > 1000) {
      source.incrSlowPrepareDeleteEntity();
    }
    source.updatePrepareDeleteEntity(t);
  }

  public void updateRedoLogAppend(long t) {
    if (t > 1000) {
      source.incrSlowRedoLogAppend();
    }
    source.updateRedoLogAppend(t);
  }

  public void updateBackgroundRedoLog(long t) {
    if (t > 1000) {
      source.incrSlowBackgroundRedoLog();
    }
    source.updateBackgroundRedoLog(t);
  }

  MetricsEntityGroupSource getSource() {
    return source;
  }
}

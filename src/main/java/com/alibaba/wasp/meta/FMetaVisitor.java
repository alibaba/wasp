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
package com.alibaba.wasp.meta;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;

/**
 * Implementations 'visit' a catalog table row.
 */
public interface FMetaVisitor {
  /**
   * Visit the catalog table row.
   * 
   * @param r A row from catalog table
   * @return True if we are to proceed scanning the table, else false if we are
   *         to stop now.
   */
  public boolean visit(final Result r) throws IOException;
}

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
package com.alibaba.wasp.storage;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import com.alibaba.wasp.plan.action.DeleteAction;
import com.alibaba.wasp.plan.action.InsertAction;
import com.alibaba.wasp.plan.action.UpdateAction;

/**
 * Storage Services
 * 
 */
public interface StorageServices {

  public Result getRow(String entityTableName, Get get) throws IOException,
      StorageTableNotFoundException;

  public Result getRowBeforeDelete(DeleteAction delete, String entityTableName,
      Get get) throws IOException, StorageTableNotFoundException;

  public void putRow(String entityTableName, Put put) throws IOException,
      StorageTableNotFoundException;

  public void deleteRow(String entityTableName, Delete delete)
      throws IOException, StorageTableNotFoundException;

  // Before insert, check if the row exists
  public void checkRowExistsBeforeInsert(InsertAction insert,
      String entityTableName, Put entityPut) throws IOException,
      StorageTableNotFoundException;

  // Before update, get the row
  public Result getRowBeforeUpdate(UpdateAction update, String entityTableName,
      Get get) throws IOException, StorageTableNotFoundException;

}
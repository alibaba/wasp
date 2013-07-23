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
package org.apache.wasp.storage;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.PrimaryKeyAlreadyExistsException;
import org.apache.wasp.plan.action.DeleteAction;
import org.apache.wasp.plan.action.InsertAction;
import org.apache.wasp.plan.action.UpdateAction;

import java.io.IOException;

/**
 * 
*/
public class StorageServicesImpl implements StorageServices {

  StorageActionManager actionManager;

  public StorageServicesImpl(StorageActionManager actionManager) {
    this.actionManager = actionManager;
  }

  @Override
  public Result getRow(String entityTableName, Get get) throws IOException,
      StorageTableNotFoundException {
    return actionManager.get(entityTableName, get);
  }

  @Override
  public void putRow(String entityTableName, Put put) throws IOException,
      StorageTableNotFoundException {
    actionManager.put(entityTableName, put);
  }

  @Override
  public void deleteRow(String entityTableName, Delete delete)
      throws IOException, StorageTableNotFoundException {
    actionManager.delete(entityTableName, delete);
  }

  @Override
  public void checkRowExistsBeforeInsert(InsertAction insert,
      String entityTableName, Put entityPut) throws IOException,
      StorageTableNotFoundException {
    if (actionManager.exits(entityTableName, new Get(entityPut.getRow()))) {
      throw new PrimaryKeyAlreadyExistsException(Bytes.toString(insert
          .getCombinedPrimaryKey()));
    }
  }

  @Override
  public Result getRowBeforeUpdate(UpdateAction update, String entityTableName,
      Get get) throws IOException, StorageTableNotFoundException {
    Result result = actionManager.get(entityTableName, get);
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.wasp.storage.StorageServices#getRowBeforeDelete(org.apache.wasp
   * .plan.action.DeleteAction, java.lang.String,
   * org.apache.hadoop.hbase.client.Get)
   */
  @Override
  public Result getRowBeforeDelete(DeleteAction delete, String entityTableName,
      Get get) throws IOException, StorageTableNotFoundException {
    Result result = actionManager.get(entityTableName, get);
    return result;
  }
}
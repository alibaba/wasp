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
package org.apache.wasp.fserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.GlobalEntityGroupInfo;
import org.apache.wasp.NotServingEntityGroupException;
import org.apache.wasp.plan.action.ScanAction;
import org.apache.wasp.storage.StorageTableNotFoundException;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Execute global query plan.
 * 
 */
public class GlobalEntityGroup implements Closeable, EntityGroupServices {

  public static final Log LOG = LogFactory.getLog(GlobalEntityGroup.class);

  protected final FServerServices services;

  protected Configuration configuration;

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  final AtomicBoolean close = new AtomicBoolean(false);

  final GlobalEntityGroupInfo entityGroupInfo;

  public GlobalEntityGroup(FServerServices service) {
    this.services = service;
    this.configuration = service.getConfiguration();
    this.entityGroupInfo = new GlobalEntityGroupInfo();
  }

  /**
   * Return an iterator that scans over the EntityGroup, returning the indicated
   * columns and rows specified by the
   * 
   * <p>
   * This Iterator must be closed by the caller.
   * 
   * @param action
   * @return
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  public EntityGroupScanner getScanner(ScanAction action) throws IOException,
      StorageTableNotFoundException {
    startEntityGroupOperation();
    try {
      return instantiateEntityGroupScanner(action);
    } finally {
      closeEntityGroupOperation();
    }
  }

  /**
   * return EntityGroupScanner.
   * 
   * @param action
   * @return
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  protected EntityGroupScanner instantiateEntityGroupScanner(ScanAction action)
      throws StorageTableNotFoundException, IOException {
    return new EntityGroupScanner(this, action, System.currentTimeMillis());
  }

  /**
   * This method needs to be called before any public call that reads or
   * modifies data. It has to be called just before a try. Acquires checks if
   * the entityGroup is close.
   * 
   * @throws org.apache.wasp.NotServingEntityGroupException
   *           when the entityGroup is closing or closed
   */
  private void startEntityGroupOperation()
      throws NotServingEntityGroupException {
    lock.readLock().lock();
    if (this.close.get()) {
      lock.readLock().unlock();
      throw new NotServingEntityGroupException("GlobalEntityGroup is closed");
    }
  }

  private void closeEntityGroupOperation() {
    lock.readLock().unlock();
  }

  @Override
  public void close() {
    close.set(true);
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return this.configuration;
  }

  /**
   * @see org.apache.wasp.fserver.EntityGroupServices#getFServerServices()
   */
  @Override
  public FServerServices getFServerServices() {
    return this.services;
  }

  /**
   * @see org.apache.wasp.fserver.EntityGroupServices#getEntityGroupInfo()
   */
  @Override
  public EntityGroupInfo getEntityGroupInfo() {
    return entityGroupInfo;
  }
}
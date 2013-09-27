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
package com.alibaba.wasp.client;

import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import java.io.IOException;

public class ZooKeeperKeepAliveConnection extends ZooKeeperWatcher {

  /**
   * 
   * @param conf
   * @param descriptor
   * @param conn
   * @throws java.io.IOException
   */
  public ZooKeeperKeepAliveConnection(Configuration conf, String descriptor,
      FConnectionManager.FConnectionImplementation conn) throws IOException {
    super(conf, descriptor, conn);
  }

  /**
   * @param conf
   * @param descriptor
   * @param abortable
   * @throws org.apache.hadoop.hbase.ZooKeeperConnectionException
   * @throws java.io.IOException
   */
  public ZooKeeperKeepAliveConnection(Configuration conf, String descriptor,
      Abortable abortable) throws ZooKeeperConnectionException, IOException {
    super(conf, descriptor, abortable);

  }

  /**
   * @param conf
   * @param descriptor
   * @param abortable
   * @param canCreateBaseZNode
   * @throws java.io.IOException
   * @throws org.apache.hadoop.hbase.ZooKeeperConnectionException
   */
  public ZooKeeperKeepAliveConnection(Configuration conf, String descriptor,
      Abortable abortable, boolean canCreateBaseZNode) throws IOException,
      ZooKeeperConnectionException {
    super(conf, descriptor, abortable, canCreateBaseZNode);
  }

  @Override
  public void close() {
    ((FConnectionManager.FConnectionImplementation) abortable)
        .releaseZooKeeperWatcher(this);
  }

  void internalClose() {
    super.close();
  }
}

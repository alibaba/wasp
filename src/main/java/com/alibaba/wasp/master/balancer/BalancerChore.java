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
package com.alibaba.wasp.master.balancer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Chore;
import com.alibaba.wasp.master.FMaster;

/**
 * Chore that will call FMaster.balance{@link org.apache.wap.master.FMaster#balance()} when
 * needed.
 */
public class BalancerChore extends Chore {
  private static final Log LOG = LogFactory.getLog(BalancerChore.class);
  private final FMaster master;

  public BalancerChore(FMaster master) {
    super(master.getServerName() + "-BalancerChore",
        master.getConfiguration().getInt("wasp.balancer.period", 300000),
        master);
    this.master = master;
  }

  @Override
  protected void chore() {
    try {
      master.balance();
    } catch (IOException ioe) {
      LOG.error("Error invoking balancer", ioe);
    }
  }
}

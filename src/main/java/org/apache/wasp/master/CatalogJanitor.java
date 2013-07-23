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
package org.apache.wasp.master;

import org.apache.hadoop.hbase.Chore;
import org.apache.wasp.Server;

/**
 * A janitor for the catalog tables. Scans the <code>.FMETA.</code> catalog
 * table on a period looking for unused entityGroups to garbage collect.
 */
class CatalogJanitor extends Chore {

  CatalogJanitor(final Server server, final FMasterServices services) {
    super(server.getServerName() + "-CatalogJanitor", server.getConfiguration()
        .getInt("wasp.catalogjanitor.interval", 300000), server);
  }

  public void interrupt() {
  }

  @Override
  protected void chore() {
  }
}
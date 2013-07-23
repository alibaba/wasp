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
package org.apache.wasp.meta;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.wasp.FConstants;
import org.apache.wasp.MasterNotRunningException;
import org.apache.wasp.MetaException;
import org.apache.wasp.ZooKeeperConnectionException;

/**
 * 
 *
 */
public class FMetaUtil {

  public static final Log LOG = LogFactory.getLog(FMetaUtil.class);

  /**
   * Use HBaseAdmin to check the meta table exists. if not exists create it. if
   * exists just return.
   */
  public static void checkAndInit(Configuration conf) throws MetaException {
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      String metaTable = conf.get(FConstants.METASTORE_TABLE,
          FConstants.DEFAULT_METASTORE_TABLE);
      if (!admin.tableExists(metaTable)) {
        // if meta table don't exists just create it.
        HTableDescriptor desc = new HTableDescriptor(metaTable);
        HColumnDescriptor family = new HColumnDescriptor(
            FConstants.CATALOG_FAMILY_STR);
        family.setInMemory(true);
        desc.addFamily(family);
        admin.createTable(desc);
      }
      admin.close();
    } catch (ZooKeeperConnectionException zce) {
      throw new MetaException("FMeta could not connect to zookeeper ", zce);
    } catch (MasterNotRunningException mnre) {
      throw new MetaException("FMeta Could not connect to HBase Master ", mnre);
    } catch (IOException e) {
      throw new MetaException(e);
    }
  }

  /**
   * Use HBaseAdmin to delete the meta table .
   */
  public static boolean clean(Configuration conf) {
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      String metaTable = conf.get(FConstants.METASTORE_TABLE,
          FConstants.DEFAULT_METASTORE_TABLE);
      if (admin.tableExists(metaTable)) {
        admin.disableTable(metaTable);
        admin.deleteTable(metaTable);
      }
      admin.close();
      return true;
    } catch (ZooKeeperConnectionException zce) {
      LOG.error("Could not connect to zookeeper ", zce);
    } catch (MasterNotRunningException mnre) {
      LOG.error("Could not connect to HBase Master ", mnre);
    } catch (IOException e) {
      LOG.error(e);
    }
    return false;
  }
}
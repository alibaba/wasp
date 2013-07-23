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
package org.apache.wasp.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.FConstants;
import org.apache.wasp.storage.StorageActionManager;

public class ResultInHBasePrinter {

  public static void printTablesRs(String type, Configuration conf,
      String tablename, String indexName, Log LOG) throws IOException {
    StorageActionManager manager = new StorageActionManager(conf);

    String indexTableName = FConstants.WASP_TABLE_INDEX_PREFIX + tablename
        + FConstants.TABLE_ROW_SEP + indexName;

    try {
      LOG.info("rs begin " + type);
      // manager.scan(".META.", new Scan());
      // manager.scan(StorageTableNameBuilder.buildEntityTableName("TEST_TABLE"),
      // new Scan());
      // ResultScanner fmeters = manager.scan("_FMETA_", new Scan());
      ResultScanner indexrs = manager.scan(indexTableName, new Scan());
      ResultScanner rs = manager.scan(FConstants.WASP_TABLE_ENTITY_PREFIX
          + tablename, new Scan());
      // LOG.info("rs fmeta");
      // print(fmeters);
      LOG.info("rs table " + type);
      print(rs, LOG);
      LOG.info("rs index, table name = " + indexTableName);
      // WASP_INDEX_TEST_TABLE_test_index
      print(indexrs, LOG);
      LOG.info("rs end");
    } catch (UnsupportedEncodingException e1) {
      e1.printStackTrace();
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    // TEST!!!
  }

  public static void printFMETA(Configuration conf, Log LOG) throws IOException {
    printTable("fmeta", "_FMETA_", conf, LOG);
  }

  public static void printTable(String type, String table, Configuration conf, Log LOG) throws IOException {
    StorageActionManager manager = new StorageActionManager(conf);
    try {
      ResultScanner fmeters = manager.scan(table, new Scan());
      LOG.info("rs " + type);
      print(fmeters, LOG);
      LOG.info("rs end");
    } catch (UnsupportedEncodingException e1) {
      e1.printStackTrace();
    } catch (IOException e1) {
      e1.printStackTrace();
    }
  }

   public static void printMETA(Configuration conf, Log LOG) throws IOException {
    printTable("meta", ".META.", conf, LOG);
  }

  private static void print(ResultScanner indexrs, Log LOG) throws UnsupportedEncodingException {
    for (Result r : indexrs) {
      KeyValue[] kv = r.raw();
      for (int i = 0; i < kv.length; i++) {
        LOG.info("row:" + Bytes.toString(kv[i].getRow()));
        LOG.info("row length:" + kv[i].getRow().length);
        LOG.info("family:" + Bytes.toString(kv[i].getFamily()));
        LOG.info("qualifier:" + Bytes.toString(kv[i].getQualifier()));
        LOG.info("value:" + Bytes.toString(kv[i].getValue()));
        LOG.info("value length:" + kv[i].getValue().length);
        LOG.info("timestamp:" + kv[i].getTimestamp());
      }
    }
  }
}
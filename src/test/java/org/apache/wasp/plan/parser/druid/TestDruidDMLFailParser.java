/**
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.wasp.plan.parser.druid;

import org.apache.hadoop.conf.Configuration;
import org.apache.wasp.ZooKeeperConnectionException;
import org.apache.wasp.conf.WaspConfiguration;
import org.apache.wasp.meta.FMetaTestUtil;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.meta.MemFMetaStore;
import org.apache.wasp.meta.TableSchemaCacheReader;
import org.apache.wasp.plan.parser.ParseContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDruidDMLFailParser {
  private static ParseContext context = new ParseContext();
  private Configuration conf = WaspConfiguration.create();
  private TableSchemaCacheReader reader = null;
  private DruidDDLParser druidDDLParser = null;
  private DruidDMLParser druidDMLParser = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    // Logger.getRootLogger().setLevel(Level.TRACE);
    context.setGenWholePlan(false);
    // Use MemFMetaStore
    // Use MemFMetaStore
    MemFMetaStore fmetaServices = new MemFMetaStore();
    reader = TableSchemaCacheReader.getInstance(conf, fmetaServices);
    context.setTsr(reader);
    fmetaServices.close();
    if (druidDDLParser == null) {
      druidDDLParser = new DruidDDLParser(conf);
    }
    if (druidDMLParser == null) {
      druidDMLParser = new DruidDMLParser(conf);
    }
    DruidParserTestUtil.loadTable(context, druidDDLParser, fmetaServices);
    // when loadData , reader will read some FTable into it
    // reader.clearCache();
  }

  @After
  public void tearDown() throws Exception {
    reader.clearCache();
    reader = TableSchemaCacheReader.getInstance(conf, null);
  }

  @Test
  public void getInsertFailedPlan() throws ZooKeeperConnectionException {
    // table not exists
    String sql1 = "Insert into User1(ID,NAME) values(1,'binlijin');";
    // column not exists
    String sql2 = "Insert into User(ID,NAME2) values(1,'binlijin'),(2,'lars');";
    // primary keys not full
    String sql3 = "Insert into Photo(photo_id,time) values(1, 10000),(2,20000);";
    // Not all Required Field show up
    String sql4 = "Insert into Photo(user_id,photo_id, time) values(1,1000,10000);";
    // Give too much values
    String sql5 = "Insert into User(user_id,name,value) values(1,'binlijin',1.,'aa');";
    // Give too few values
    String sql6 = "Insert into User(user_id,name,value) values(1,'binlijin');";
    String[] sqlList = { sql1, sql2, sql3, sql4, sql5, sql6 };
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidDMLParser);
      Assert.assertFalse(result);
    }
  }

  @Test
  public void getUpdateFailedPlan() throws ZooKeeperConnectionException {
    // table not exists
    String sql1 = "UPDATE User1 SET name = 'Mike' WHERE user_id = 123;";
    // column in set not exists
    String sql2 = "UPDATE User SET name2 = 'Mike' WHERE user_id = 123;";
    // primary keys not full
    String sql3 = "UPDATE Photo SET time=2000 WHERE photo_id = 123";
    // column in where not exists
    String sql4 = "UPDATE User SET name = 'Mike' WHERE user_id22 = 123;";
    // Range not supported
    String sql5 = "UPDATE User SET name = 'Mike' WHERE user_id >= 123;";
    // primary keys not full
    String sql6 = "UPDATE Photo SET photo_id=2000 WHERE user_id = 321 and photo_id = 123";

    String[] sqlList = { sql1, sql2, sql3, sql4, sql5, sql6 };
    // sql1, sql2, sql3, sql4, sql5, sql6
    // DruidDMLParser druidParser = new DruidDMLParser(conf);
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidDMLParser);
      Assert.assertFalse(result);
    }
  }

  @Test
  public void getDeleteFailedPlan() throws ZooKeeperConnectionException {
    // table not exists
    String sql1 = "DELETE FROM User1 WHERE user_id = 123;";
    // column not exists
    String sql2 = "DELETE FROM User WHERE user_id2 = 123;";
    // primary keys not full
    String sql3 = "DELETE FROM Photo WHERE photo_id = 123;";
    // Range not supported
    String sql4 = "DELETE FROM User WHERE user_id >= 123;";
    String[] sqlList = { sql1, sql2, sql3, sql4 };
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidDMLParser);
      Assert.assertFalse(result);
    }
  }

  @Test
  public void printFTable() {
    FTable user = FMetaTestUtil.User;
    FTable photo = FMetaTestUtil.Photo;
    System.out.println(user.toString());
    System.out.println("\n");
    System.out.println(photo.toString());
  }
}

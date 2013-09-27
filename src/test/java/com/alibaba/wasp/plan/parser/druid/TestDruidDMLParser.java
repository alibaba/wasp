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

package com.alibaba.wasp.plan.parser.druid;

import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.meta.FMetaTestUtil;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.MemFMetaStore;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.DeletePlan;
import com.alibaba.wasp.plan.InsertPlan;
import com.alibaba.wasp.plan.Plan;
import com.alibaba.wasp.plan.UpdatePlan;
import com.alibaba.wasp.plan.parser.ParseContext;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDruidDMLParser {
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
    reader.clearCache();
    DruidParserTestUtil.loadTable(context, druidDDLParser, fmetaServices);
    // when loadData , reader will read some FTable into it
  }

  @After
  public void tearDown() throws Exception {
    reader.clearCache();
    TableSchemaCacheReader.getInstance(conf, null);
  }

  @Test
  public void getInsertPlan() throws ZooKeeperConnectionException {
    String sql1 = "Insert into User(user_id,name,value) values(1,'binlijin',1.);";
    // String sql2 = "Insert into User(user_id,name) values(1,'binlijin'),(2,'lars');";
    String sql2 =
        "Insert into Photo(user_id,photo_id, time, full_url) values(1,1000,10000, 'dads');";
    String sql3 =
        "Insert into Photo(user_id,photo_id,time,full_url,date) values(1,1000,10000,'dads',NOW());";
    String[] sqlList = { sql1, sql2, sql3 };
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidDMLParser);
      Assert.assertTrue(result);
      Plan plan = context.getPlan();
      if (plan instanceof InsertPlan) {
      } else {
        Assert.assertTrue(false);
      }
    }
  }

  @Test
  public void getUpdatePlan() throws ZooKeeperConnectionException {
    // UPDATE users SET age = 24 WHERE id = 123;
    // UPDATE users SET age = 24, name = 'Mike' WHERE id = 123;
    String sql1 = "UPDATE User SET name = 'Mike' WHERE user_id = 123;";
    String sql2 = "UPDATE Photo SET date = NOW() WHERE user_id = 123 and photo_id=123;";
    String[] sqlList = { sql1, sql2 };
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidDMLParser);
      Assert.assertTrue(result);
      Plan plan = context.getPlan();
      if (plan instanceof UpdatePlan) {
      } else {
        Assert.assertTrue(false);
      }
    }
  }

  @Test
  public void getDeletePlan() throws ZooKeeperConnectionException {
    // DELETE FROM users WHERE id = 123;
    String sql1 = "DELETE FROM User WHERE user_id = 123;";
    String[] sqlList = { sql1 };
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidDMLParser);
      Assert.assertTrue(result);
      Plan plan = context.getPlan();
      if (plan instanceof DeletePlan) {
      } else {
        Assert.assertTrue(false);
      }
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
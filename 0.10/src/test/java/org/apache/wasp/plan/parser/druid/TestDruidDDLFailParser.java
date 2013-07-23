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
import org.apache.wasp.MetaException;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestDruidDDLFailParser {
  private static ParseContext context = new ParseContext();
  private Configuration conf = WaspConfiguration.create();
  private TableSchemaCacheReader reader = null;
  private DruidDDLParser druidParser = null;

  @Rule
  public TestName testName = new TestName();

  public static void print(String message) {
    System.out.println("\n\n" + message + "\n\n");
  }

  @Before
  public void setUp() throws Exception {
    print(testName.getMethodName());

    // Logger.getRootLogger().setLevel(Level.TRACE);
    context.setGenWholePlan(false);
    // Use MemFMetaStore
    MemFMetaStore fmetaServices = new MemFMetaStore();
    reader = TableSchemaCacheReader.getInstance(conf, fmetaServices);
    context.setTsr(reader);
    fmetaServices.close();
    if (druidParser == null) {
      druidParser = new DruidDDLParser(conf);
    }
    DruidParserTestUtil.loadTable(context, druidParser, fmetaServices);
    // when init , reader will read some FTable into it
    reader.clearCache();
  }

  @After
  public void tearDown() throws Exception {
    reader.clearCache();
    TableSchemaCacheReader.getInstance(conf, null);
  }

  @Test
  public void getCreateIndexFailedPlan() {
    // index name is not legal
    String sql1 = "CREATE index .PhotosByTime. on Photo(user_id, time);";
    // table not exists
    String sql2 = "CREATE index PhotosByTime on Photo1(user_id, time);";
    // Index keys is Primary keys
    String sql3 = "CREATE index PhotosByTime on Photo(user_id, photo_id);";
    // index type is illegal
    String sql4 = "CREATE local index PhotosByTime on Photo(user_id, time);";
    // column is illegal
    String sql5 = "CREATE index PhotosByTime on Photo(user_id, time2);";
    // Duplicate columns
//    String sql6 = "CREATE index PhotosByTime on Photo(user_id, time, time);";
    // null columns
    String sql7 = "CREATE index PhotosByTime on Photo();";

//    String[] sqlList = { sql1, sql2, sql3, sql4, sql5, sql6, sql7 };
    String[] sqlList = { sql1, sql2, sql3, sql4, sql5, sql7 };
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertFalse(result);
    }
  }

  @Test
  public void getDropIndexFailedPlan() {
    // index name is illegal
    String sql1 = "DROP INDEX -PhotosByTime- ON Photo;";
    // table is not exists
    String sql2 = "DROP INDEX PhotosByTime ON Photo1;";
    // table name is illegal
    String sql3 = "DROP INDEX PhotosByTime ON .Photo.;";

    String[] sqlList = { sql1, sql2, sql3 };

    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertFalse(result);
    }
  }

  @Test
  public void getCreateTableFailedPlan() throws MetaException {
    // clear MemFMetaStore

    // Wrong table name
    String sql1 = "CREATE TABLE -User- {Required Int64 user_id; Required String name; } primary key(user_id), entity group root;";
    // null columns
    String sql2 = "CREATE TABLE User { } primary key(), entity group root;";
    // primary key is not in columns
    String sql3 = "CREATE TABLE User { } primary key(user_id), entity group root;";
    // lack of keyword(required,optional,repeated)
    String sql4 = "CREATE TABLE User {Int64 user_id; Required String name;} primary key(user_id), entity group root;";
    // Duplicate column name
    String sql5 = "CREATE TABLE User {Required Int64 user_id; Required String user_id;} primary key(user_id), entity group root;";
    // Unsupported Datatype
    String sql6 = "CREATE TABLE User {Required Int64 user_id; Required longlong user_id;} primary key(user_id), entity group root;";
    // wrong foreign table name
    String sql7 = "CREATE TABLE Photo{Required Int64 user_id columnfamily cf comment 'aaa'; "
        + "Required Int64 time;"
        + "Repeated string tag; } primary key(user_id), "
        + " in table User, Entity Group Key(user_id) references User2; ";
    // wrong foreignKey
    String sql8 = "CREATE TABLE Photo{Required Int64 user_id columnfamily cf comment 'aaa'; "
        + "Required Int32 photo_id comment 'child primary key'; "
        + "Repeated string tag; } primary key(user_id), "
        + " in table User, Entity Group Key(tag) references User; ";
    // wrong columnfamily name
    String sql9 = "CREATE TABLE User {Required Int64 user_id columnfamily .cf.; Required String name;} primary key(user_id), entity group root;";

    String[] sqlList = { sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8, sql9 };

    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertFalse(result);
    }

    String sql10 = DruidParserTestUtil.SEED[0];
    // partition by range only supported by root table
    String sql11 = "CREATE TABLE Photo{Required Int64 user_id columnfamily cf comment 'aaa'; "
        + "Required Int32 photo_id comment 'child primary key'; "
        + "Required Int64 time;"
        + " Required String full_url; Optional String thumbnail_url; Repeated string tag; } primary key(user_id), "
        + " in table User, Entity Group Key(user_id) references User, "
        + " partition by range('aaa', 'zzz', 10);";
    context.setSql(sql10);
    String[] sqlList2 = { sql10 };
    DruidParserTestUtil.loadTable(context, druidParser,
        TableSchemaCacheReader.getService(conf), sqlList2);
    context.setSql(sql11);
    boolean result = DruidParserTestUtil.execute(context, druidParser);
    Assert.assertFalse(result);
  }

  @Test
  public void getDropTableFailedPlan() {
    // table is not exists
    String sql1 = "DROP TABLE Photo2;";
    // table name is illegal
    String sql2 = "DROP TABLE IF EXISTS .Photo.;";

    String[] sqlList = { sql1, sql2 };
    DruidDDLParser druidParser = new DruidDDLParser(conf);
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
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

  @Test
  public void getAlterTableFailedPlan() {
    // table name is illegal
    String sql1 = "ALTER TABLE .Photo. ADD DateOfBirth string";
    // table is not exists
    String sql2 = "ALTER TABLE Photo2 ADD COLUMN dummy2 int32 AFTER thumbnail_url";
    // alter primary key
    String sql3 = "ALTER TABLE Photo CHANGE photo_id photo_id2 INT32";
    // alter a column that not exists
    String sql4 = "ALTER TABLE Photo CHANGE photo_id2 photo_id3 INT32";
    // after add column there are Duplicate columns
    String sql5 = "ALTER TABLE Photo ADD COLUMN tag int32 FIRST thumbnail_url";
    // after change column there are Duplicate columns
    String sql6 = "ALTER TABLE Photo CHANGE COLUMN thumbnail_url tag INT32";
    // drop a column that not exists
    String sql7 = "ALTER TABLE Photo DROP COLUMN full_url1, DROP COLUMN thumbnail_url2;";

    String sql8 = "ALTER TABLE Photo ADD COLUMN dummy2 int32 FIRST thumbnail_url";

    String[] sqlList = { sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8 };

    DruidDDLParser druidParser = new DruidDDLParser(conf);
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertFalse(result);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }
}

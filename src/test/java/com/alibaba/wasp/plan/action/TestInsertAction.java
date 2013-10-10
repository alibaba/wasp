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

package com.alibaba.wasp.plan.action;

import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.MemFMetaStore;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.CreateTablePlan;
import com.alibaba.wasp.plan.InsertPlan;
import com.alibaba.wasp.plan.Plan;
import com.alibaba.wasp.plan.parser.ParseContext;
import com.alibaba.wasp.plan.parser.WaspParser;
import com.alibaba.wasp.plan.parser.druid.DruidDDLParser;
import com.alibaba.wasp.plan.parser.druid.DruidDMLParser;
import com.alibaba.wasp.plan.parser.druid.DruidDQLParser;
import com.alibaba.wasp.protobuf.generated.MetaProtos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestInsertAction {

  private static Configuration conf = WaspConfiguration.create();
  private static ParseContext context = new ParseContext();
  private static TableSchemaCacheReader reader;

  @BeforeClass
  public static void setUp() throws Exception {
    context.setGenWholePlan(false);
    MemFMetaStore fmetaServices = new MemFMetaStore();
    reader = TableSchemaCacheReader.getInstance(conf, fmetaServices);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    reader.clearCache();
    TableSchemaCacheReader.getInstance(conf, null);
  }

  @Test
  public void testGenerateInsertAction() {
    String createTable = "CREATE TABLE User {Required Int64 user_id; Required String name; } "
        + "primary key(user_id), entity group root, entity group key(user_id);";
    String insert = "Insert into User(user_id,name) values(1,'binlijin');";
    try {
      TableSchemaCacheReader reader = TableSchemaCacheReader.getInstance(conf);
      context.setTsr(reader);

      DruidDQLParser dqlParser = new DruidDQLParser(conf, null);
      DruidDDLParser ddlParser = new DruidDDLParser(conf);
      DruidDMLParser dmlParser = new DruidDMLParser(conf, null);
      WaspParser druidParser = new WaspParser(ddlParser, dqlParser, dmlParser);

      // create table
      context.setSql(createTable);
      druidParser.generatePlan(context);
      Plan plan = context.getPlan();
      if (plan instanceof CreateTablePlan) {
        CreateTablePlan createPlan = (CreateTablePlan) plan;
        FTable table = createPlan.getTable();
        TableSchemaCacheReader.getService(conf).createTable(table);
      }
      // insert
      context.setSql(insert);
      druidParser.generatePlan(context);
      plan = context.getPlan();
      if (plan instanceof InsertPlan) {
        InsertPlan insertPlan = (InsertPlan) plan;
        List<InsertAction> actions = insertPlan.getActions();
        Assert.assertEquals(actions.size(), 1);
      }
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testInsertAction() {
    InsertAction insertAction = ActionTestUtil.makeTestInsertAction();
    MetaProtos.InsertActionProto insertActionProto = InsertAction.convert(insertAction);
    InsertAction derInsertAction = InsertAction.convert(insertActionProto);
    Equals(insertAction, derInsertAction);
  }

  public static boolean Equals(InsertAction one, InsertAction two) {
    if (one == two) {
      return true;
    }
    if (!one.getFTableName().equals(two.getFTableName())) {
      return false;
    }
    if (!Bytes.equals(one.getCombinedPrimaryKey(), two.getCombinedPrimaryKey())) {
      return false;
    }
    List<ColumnStruct> oneColumns = one.getColumns();
    List<ColumnStruct> twoColumns = two.getColumns();
    if (oneColumns.size() != twoColumns.size()) {
      return false;
    }
    for (int i = 0; i < oneColumns.size(); i++) {
      if (!oneColumns.get(i).equals(twoColumns.get(i))) {
        return false;
      }
    }
    return true;
  }
}
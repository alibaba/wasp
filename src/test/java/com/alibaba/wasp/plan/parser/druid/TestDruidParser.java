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

import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.meta.FMetaTestUtil;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Index;
import com.alibaba.wasp.meta.MemFMetaStore;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.CreateIndexPlan;
import com.alibaba.wasp.plan.CreateTablePlan;
import com.alibaba.wasp.plan.Plan;
import com.alibaba.wasp.plan.parser.ParseContext;
import com.alibaba.wasp.plan.parser.WaspParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestDruidParser {
  private static final Log LOG = LogFactory.getLog(TestDruidParser.class);

  private static ParseContext context = new ParseContext();
  private Configuration conf = WaspConfiguration.create();
  private TableSchemaCacheReader reader = null;
  private WaspParser druidParser = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    context.setGenWholePlan(false);
    // Use MemFMetaStore
    MemFMetaStore fmetaServices = new MemFMetaStore();
    reader = TableSchemaCacheReader.getInstance(conf, fmetaServices);
    context.setTsr(reader);
    fmetaServices.close();
    if (druidParser == null) {
      druidParser = new WaspParser();
      druidParser.setConf(conf);
    }
    reader.clearCache();
    DruidParserTestUtil.loadTable(context, druidParser.getDruidDDLParser(),
        fmetaServices);
    reader.clearCache();
    // when loadData , reader will read some FTable into it
  }

  @After
  public void tearDown() throws Exception {
    reader.clearCache();
    TableSchemaCacheReader.getInstance(conf, null);
  }

  @Test
  public void printFTable() {
    FTable user = FMetaTestUtil.User;
    FTable photo = FMetaTestUtil.Photo;
    System.out.println(user.toString());
    // System.out.println("\n");
    System.out.println(photo.toString());
  }

  @Test
  public void getUseIndexPlan() {
    StringBuilder sql = new StringBuilder();
    sql.append("CREATE TABLE index2 {REQUIRED INT64 id;");
    sql.append("REQUIRED INT32 age;");
    sql.append("REQUIRED FLOAT scores;");
    sql.append("REQUIRED DOUBLE doudata;");
    sql.append("REQUIRED DATETIME birthday;");
    sql.append("REQUIRED DATETIME birthday1;");
    sql.append("REQUIRED STRING name; }");
    sql.append("PRIMARY KEY(id),");
    sql.append("ENTITY GROUP ROOT,");
    sql.append("ENTITY GROUP KEY(id);");

    String sql2 = "create index MultiIndexsKey on index2(age,scores);";
    String sql3 = "create index SingleIndex on index2(age);";
    String sql4 = "select * from index2 where age=110;";
    String[] sqlList = { sql.toString(), sql2, sql3, sql4 };
    for (String sql1 : sqlList) {
      context.setSql(sql1);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertTrue(result);
      try {
        Plan plan = context.getPlan();
        if (plan instanceof CreateTablePlan) {
          CreateTablePlan createTable = (CreateTablePlan) plan;
          FTable ftable = createTable.getTable();
          TableSchemaCacheReader.getService(conf).createTable(ftable);
          TableSchemaCacheReader.getInstance(conf).addSchema(
              ftable.getTableName(), ftable);
        } else if (plan instanceof CreateIndexPlan) {
          CreateIndexPlan createIndex = (CreateIndexPlan) plan;
          Index index = createIndex.getIndex();
          TableSchemaCacheReader.getService(conf).addIndex(
              index.getDependentTableName(), index);
          TableSchemaCacheReader.getInstance(conf).refreshSchema(
              index.getDependentTableName());
        } else {
        }
      } catch (IOException ioe) {
        // ioe.printStackTrace();
        LOG.error(ioe);
        Assert.assertTrue(false);
      }
    }
  }
}

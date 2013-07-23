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
package org.apache.wasp.plan.parser.druid;

import org.apache.hadoop.conf.Configuration;
import org.apache.wasp.ZooKeeperConnectionException;
import org.apache.wasp.conf.WaspConfiguration;
import org.apache.wasp.meta.MemFMetaStore;
import org.apache.wasp.meta.TableSchemaCacheReader;
import org.apache.wasp.plan.parser.ParseContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.Token;

public class TestDruidDQLParser {
  private static ParseContext context = new ParseContext();
  private Configuration conf = WaspConfiguration.create();
  private TableSchemaCacheReader reader = null;
  private DruidDDLParser druidDDLParser = null;
  private DruidDQLParser druidDQLParser = null;

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
    reader.clearCache();
    context.setTsr(reader);
    fmetaServices.close();
    if (druidDDLParser == null) {
      druidDDLParser = new DruidDDLParser(conf);
    }
    if (druidDQLParser == null) {
      druidDQLParser = new DruidDQLParser(conf);
    }
    DruidParserTestUtil.loadTable(context, druidDDLParser, fmetaServices);
    // when loadData , reader will read some FTable into it
  }

  @After
  public void tearDown() throws Exception {
    reader.clearCache();
    TableSchemaCacheReader.getInstance(conf, null);
  }

  @Test
  public void testLexer() {
    String sql = "SELECT * FROM T WHERE F1 = ? ORDER BY F2";
    Lexer lexer = new Lexer(sql);
    System.out.println("--------------------------------------------");
    for (;;) {
      lexer.nextToken();
      Token tok = lexer.token();

      if (tok == Token.IDENTIFIER) {
        System.out.println(tok.name() + "\t\t" + lexer.stringVal());
      } else {
        System.out.println(tok.name() + "\t\t\t" + tok.name);
      }

      if (tok == Token.EOF) {
        break;
      }
    }
    System.out.println("--------------------------------------------");
  }

  @Test
  public void getSelectPlan() throws ZooKeeperConnectionException {

    String sql1 = "SELECT user_id,name from User where user_id=1 limit 1;";
    String sql2 = "SELECT user_id,photo_id,full_url,thumbnail_url "
        + "from Photo " + "where user_id=99999999999 and photo_id=0.1;"; //
    String sql3 = "SELECT * from User where user_id=1;";

    String[] sqlList = { sql1, sql2, sql3 }; // sql1, sql2, sql3
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidDQLParser);
      Assert.assertTrue(result);
    } // for
  }
}

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
package com.alibaba.wasp.plan.parser.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.meta.FMetaTestUtil;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Index;
import com.alibaba.wasp.meta.MemFMetaStore;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.parser.ParseContext;
import com.alibaba.wasp.plan.parser.QueryInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestFMetaEventOperation {

  private static final Log LOG = LogFactory
      .getLog(TestFMetaEventOperation.class);

  private static ParseContext context = new ParseContext();
  private Configuration conf = WaspConfiguration.create();

  @Test
  public void testCheckAndGetIndex() throws ZooKeeperConnectionException,
      MetaException {
    FTable table = FMetaTestUtil.getTestTable();
    List<String> fields = new ArrayList<String>();
    fields.add("c4");
    fields.add("c5");
    Index i1 = FMetaTestUtil.makeIndex(table, "i1", fields);
    table.addIndex(i1);
    fields = new ArrayList<String>();
    fields.add("c5");
    fields.add("c4");
    Index i2 = FMetaTestUtil.makeIndex(table, "i2", fields);
    table.addIndex(i2);

    String sql1 = "SELECT c1, c2, c3 from test where c4=1 and c5=2;";
    String sql2 = "SELECT c1, c2, c3 from test where c5=2 and c4=1;";

    String[] sqlList = { sql1, sql2 }; // sql1, sql2, sql3

    Index[] indexs = { i1, i2 }; // sql1, sql2, sql3

    DruidDQLParser druidParser = new DruidDQLParser(conf);
    for (int i = 0; i < sqlList.length; i++) {
      String sql = sqlList[i];
      context.setSql(sql);
      try {
        druidParser.parseSqlToStatement(context);
        SQLStatement stmt = context.getStmt();
        if (stmt instanceof SQLSelectStatement) {
          MemFMetaStore fmetaServices = new MemFMetaStore();
          TableSchemaCacheReader reader = TableSchemaCacheReader.getInstance(
              conf, fmetaServices);
          reader.addSchema(table.getTableName(), table);
          MetaEventOperation metaEventOperation = new FMetaEventOperation(
              reader);

          SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) stmt;
          // this is SELECT clause
          SQLSelect select = sqlSelectStatement.getSelect();
          SQLSelectQuery sqlSelectQuery = select.getQuery();
          if (sqlSelectQuery instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
            // Parse The WHERE clause
            SQLExpr where = sqlSelectQueryBlock.getWhere();
            LOG.debug("SELECT SQL:where " + where);
            QueryInfo queryInfo = druidParser.parseWhereClause(table,
                metaEventOperation, where, false);
            Index index = metaEventOperation.checkAndGetIndex(table,
                queryInfo.getAllConditionFieldName());
            // Get the right Index
            Assert.assertEquals(indexs[i].getIndexName(), index.getIndexName());
          }
        }
        Assert.assertTrue(true);
      } catch (IOException ioe) {
        LOG.error(ioe);
        Assert.assertTrue(false);
      } finally {
        TableSchemaCacheReader.getInstance(conf, null);
      }
    } // for
  }
}
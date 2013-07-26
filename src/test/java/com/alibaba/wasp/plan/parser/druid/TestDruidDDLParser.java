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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.meta.FMetaTestUtil;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Index;
import com.alibaba.wasp.meta.MemFMetaStore;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.AlterTablePlan;
import com.alibaba.wasp.plan.CreateIndexPlan;
import com.alibaba.wasp.plan.CreateTablePlan;
import com.alibaba.wasp.plan.DescTablePlan;
import com.alibaba.wasp.plan.DropIndexPlan;
import com.alibaba.wasp.plan.DropTablePlan;
import com.alibaba.wasp.plan.Plan;
import com.alibaba.wasp.plan.ShowIndexesPlan;
import com.alibaba.wasp.plan.ShowTablesPlan;
import com.alibaba.wasp.plan.TruncateTablePlan;
import com.alibaba.wasp.plan.parser.ParseContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDruidDDLParser {
  private static final Log LOG = LogFactory.getLog(TestDruidDDLParser.class);

  private static ParseContext context = new ParseContext();
  private Configuration conf = WaspConfiguration.create();
  private TableSchemaCacheReader reader = null;
  private DruidDDLParser druidParser = null;

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
      druidParser = new DruidDDLParser(conf);
    }
    reader.clearCache();
    DruidParserTestUtil.loadTable(context, druidParser, fmetaServices);
    reader.clearCache();
    // when loadData , reader will read some FTable into it
  }

  @After
  public void tearDown() throws Exception {
    reader.clearCache();
    TableSchemaCacheReader.getInstance(conf, null);
  }

  @Test
  public void getCreateTablePlan() throws MetaException {
    // http://dev.mysql.com/doc/refman/5.0/en/create-table.html
    // MySQL DDL
    // CREATE TABLE t (c CHAR(20) CHARACTER SET utf8 COLLATE utf8_bin);
    // CREATE TABLE lookup (id INT, INDEX USING BTREE (id)) ENGINE = MEMORY;
    TableSchemaCacheReader.getService(conf).close();
    String sql1 = DruidParserTestUtil.SEED[0];
    String sql2 = DruidParserTestUtil.SEED[1];
    String sql3 = "CREATE TABLE User2 {Required Int64 user_id; Required String name; } "
        + " primary key(user_id), "
        + " entity group root, "
        + " entity group key(user_id), "
        + " partition by range('aaa', 'zzz', 10); ";

    String[] sqlList = { sql1, sql2, sql3 }; // sql1, sql2, sql3

    for (String sql : sqlList) {
      context.setSql(sql);
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
        } else {
          Assert.assertTrue(false);
        }
      } catch (IOException ioe) {
        // ioe.printStackTrace();
        LOG.error(ioe);
        Assert.assertTrue(false);
      }
    }
  }

  @Test
  public void getCreateIndexPlan() {
    // http://dev.mysql.com/doc/refman/5.0/en/create-index.html
    // Wasp DDL
    // CREATE local index PhotosByTime on Photo(user_id,time);
    // CREATE global index PhotosByTag on Photo(tag) storing (thumbnail_url);

    // MySQL DDL
    // CREATE TABLE lookup (id INT) ENGINE = MEMORY;
    // CREATE INDEX id_index ON lookup (id) USING BTREE;

    // String sql1 =
    // "CREATE index PhotosByTag on Photo(tag) storing (thumbnail_url);";
    String sql1 = "CREATE index PhotosByTime on Photo(user_id,time);";
    String sql2 = "CREATE index PhotosByTag on Photo(tag);";
    String[] sqlList = { sql1, sql2 };
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertTrue(result);
      try {
        Plan plan = context.getPlan();
        if (plan instanceof CreateIndexPlan) {
          CreateIndexPlan createIndex = (CreateIndexPlan) plan;
          Index index = createIndex.getIndex();
          TableSchemaCacheReader.getService(conf).addIndex(
              index.getDependentTableName(), index);
        } else {
          Assert.assertTrue(false);
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
        LOG.error(ioe);
        Assert.assertTrue(false);
      }
    }
  }

  @Test
  public void getDropIndexPlan() {
    // http://dev.mysql.com/doc/refman/5.0/en/drop-index.html
    // Wasp DDL
    // DROP INDEX index_name ON tbl_name

    // example
    // DROP INDEX PhotosByTime ON Photo;
    String sql1 = "CREATE index PhotosByTime on Photo(user_id,time);";
    String sql2 = "DROP INDEX PhotosByTime ON Photo;";
    String[] sqlList = { sql1, sql2 };
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertTrue(result);
      try {
        Plan plan = context.getPlan();
        if (plan instanceof CreateIndexPlan) {
          CreateIndexPlan createIndex = (CreateIndexPlan) plan;
          Index index = createIndex.getIndex();
          TableSchemaCacheReader.getService(conf).addIndex(
              index.getDependentTableName(), index);
        } else if (plan instanceof DropIndexPlan) {
          DropIndexPlan dropIndex = (DropIndexPlan) plan;
          String tableName = dropIndex.getTableName();
          String indexName = dropIndex.getIndexName();
          Index index = TableSchemaCacheReader.getService(conf).getIndex(
              tableName, indexName);
          TableSchemaCacheReader.getService(conf).deleteIndex(
              index.getDependentTableName(), index);
        } else {
          Assert.assertTrue(false);
        }
        Assert.assertTrue(true);
      } catch (IOException ioe) {
        LOG.error(ioe);
        Assert.assertTrue(false);
      }
    }
  }

  @Test
  public void getShowIndexPlan() {
    // http://dev.mysql.com/doc/refman/5.6/en/show-index.html
    String sql1 = "SHOW INDEXES IN Photo;";
    String sql2 = "SHOW INDEXES FROM Photo;";
    String[] sqlList = { sql1, sql2 };
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertTrue(result);
      Plan plan = context.getPlan();
      if (plan instanceof ShowIndexesPlan) {
        ShowIndexesPlan showIndex = (ShowIndexesPlan) plan;
        String tableName = showIndex.getTableName();
        Assert.assertEquals("Photo", tableName);
      } else {
        Assert.assertTrue(false);
      }
    }
  }

  @Test
  public void getDropTablePlan() {
    // http://dev.mysql.com/doc/refman/5.0/en/drop-table.html
    // An example to drop tables having parent-child relationship is to drop the
    // child tables first and then the parent tables.
    // This can be very helpful when we drop tables and then recreate them in a
    // script.
    // Example:
    // Let's say table A has two children B and C. Then we can use the following
    // syntax to drop all tables.
    // DROP TABLE IF EXISTS B,C,A;
    // This can be placed in the beginning of the script instead of individually
    // dropping each table
    // (somewhat but not exactly similar to CASCADE CONSTRAINTS option in
    // Oracle).

    String sql1 = "DROP TABLE Photo;";
    String sql2 = "DROP TABLE IF EXISTS Photo, User;";

    String[] sqlList = { sql1, sql2 };
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertTrue(result);
      Plan plan = context.getPlan();
      if (plan instanceof DropTablePlan) {
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
    // System.out.println("\n");
    System.out.println(photo.toString());
  }

  @Test
  public void getAlterTablePlan() {
    // http://dev.mysql.com/doc/refman/5.0/en/alter-table.html
    // http://www.w3schools.com/sql/sql_alter.asp

    // Add or delete columns
    // Change the type of existing columns
    // Rename columns or the table itself.
    // Change the table comment?
    // Don't support rename table?
    //

    // example
    // ALTER TABLE t2 DROP COLUMN c, DROP COLUMN d;

    // Rename a column using a CHANGE old_col_name new_col_name
    // column_definition clause.
    // ALTER TABLE t1 CHANGE a b INTEGER;

    // Use MODIFY to change a column's type without renaming it:
    // ALTER TABLE t1 MODIFY b BIGINT NOT NULL;
    // ALTER TABLE t1 MODIFY col1 BIGINT;

    // To add a column at a specific position within a table row, use FIRST or
    // AFTER col_name.
    // ALTER TABLE mytable ADD COLUMN dummy1 VARCHAR(40) AFTER id ADD COLUMN
    // dummy2 VARCHAR(12) AFTER dummy1;

    // ALTER TABLE Persons ADD DateOfBirth date (add in as the last column)
    // ALTER TABLE Persons ALTER COLUMN DateOfBirth year (unsupported)
    // ALTER TABLE Persons DROP COLUMN DateOfBirth

    String sql4 = "ALTER TABLE Photo ADD COLUMN dummy1 int32 FIRST";
    String sql1 = "ALTER TABLE Photo ADD COLUMN DateOfBirth string";
    String sql2 = "ALTER TABLE Photo ADD COLUMN DateOfBirth string columnfamily cf comment 'aaa'";
    String sql3 = "ALTER TABLE Photo ADD COLUMN dummy2 int32 AFTER thumbnail_url";

    String sql5 = "ALTER TABLE Photo CHANGE COLUMN thumbnail_url thumbnail_url INT32";
    String sql6 = "ALTER TABLE Photo CHANGE thumbnail_url thumbnail_url INT32";
    String sql7 = "ALTER TABLE Photo CHANGE thumbnail_url rename_url INT64;";

    String sql8 = "ALTER TABLE Photo DROP COLUMN full_url, DROP COLUMN thumbnail_url;";
    // String sql8 = "ALTER TABLE tbl_name RENAME TO new_tbl_name";
    // String sql9 = "RENAME TABLE newtable to oldtable;";

    String[] sqlList = { sql4, sql1, sql2, sql3, sql5, sql6, sql7, sql8 };
    // sql1, sql2, sql3, sql4, sql5, sql6, sql7

    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertTrue(result);
      Plan plan = context.getPlan();
      if (plan instanceof AlterTablePlan) {
      } else {
        Assert.assertTrue(false);
      }
    }
  }

  @Test
  public void getShowTablesPlan() {
    // http://dev.mysql.com/doc/refman/5.5/en/describe.html
    // {DESCRIBE | DESC} tbl_name [col_name | wild]
    // http://dev.mysql.com/doc/refman/5.5/en/show-tables.html
    // Show tables;
    // SHOW TABLES, SHOW TABLES LIKE 'pattern',
    // SHOW CREATE TABLE tablename
    String sql1 = "SHOW TABLES";
    String sql2 = "SHOW TABLES LIKE 'pattern'";
    String sql3 = "SHOW CREATE TABLE Photo";
    String[] sqlList = { sql1, sql2, sql3 }; // sql1, sql2, sql3
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertTrue(result);
      Plan plan = context.getPlan();
      if (plan instanceof ShowTablesPlan) {
      } else {
        Assert.assertTrue(false);
      }
    }
  }

  @Test
  public void getDescTablePlan() {
    // http://dev.mysql.com/doc/refman/5.5/en/describe.html
    // {DESCRIBE | DESC} tbl_name [col_name | wild]
    String sql1 = "DESCRIBE Photo;";
    String[] sqlList = { sql1 }; // sql1, sql2, sql3
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertTrue(result);
      Plan plan = context.getPlan();
      if (plan instanceof DescTablePlan) {
      } else {
        Assert.assertTrue(false);
      }
    }
  }

  @Test
  public void getTruncateTablePlan() {
    // http://dev.mysql.com/doc/refman/5.0/en/truncate-table.html
    // TRUNCATE TABLE tablename;
    String sql1 = "TRUNCATE TABLE Photo;";
    String[] sqlList = { sql1 }; // sql1, sql2, sql3
    for (String sql : sqlList) {
      context.setSql(sql);
      boolean result = DruidParserTestUtil.execute(context, druidParser);
      Assert.assertTrue(result);
      Plan plan = context.getPlan();
      if (plan instanceof TruncateTablePlan) {
        LOG.info(plan.toString());
      } else {
        Assert.assertTrue(false);
      }
    }
  }

}

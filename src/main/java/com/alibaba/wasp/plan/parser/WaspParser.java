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
package com.alibaba.wasp.plan.parser;

import java.io.IOException;

import com.alibaba.wasp.ZooKeeperConnectionException;import com.alibaba.wasp.client.FConnection;import com.alibaba.wasp.client.FConnectionManager;import com.alibaba.wasp.plan.parser.druid.DruidDDLParser;import com.alibaba.wasp.plan.parser.druid.DruidDMLParser;import com.alibaba.wasp.plan.parser.druid.DruidDQLParser;import com.alibaba.wasp.plan.parser.druid.DruidParser;import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlCreateIndexStatement;import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlCreateTableStatement;import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlDescribeStatement;import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlShowCreateTableStatement;import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlShowIndexesStatement;import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlShowTablesStatement;import org.apache.hadoop.conf.Configuration;
import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.client.FConnection;
import com.alibaba.wasp.client.FConnectionManager;
import com.alibaba.wasp.plan.parser.druid.DruidDDLParser;
import com.alibaba.wasp.plan.parser.druid.DruidDMLParser;
import com.alibaba.wasp.plan.parser.druid.DruidDQLParser;
import com.alibaba.wasp.plan.parser.druid.DruidParser;
import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlCreateIndexStatement;
import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlCreateTableStatement;
import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlDescribeStatement;
import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlShowCreateTableStatement;
import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlShowIndexesStatement;
import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlShowTablesStatement;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableStatement;

/**
 * Wrap class for DruidDDLParser DruidDMLParser
 * 
 */
public class WaspParser extends DruidParser {

  private FConnection connection;

  private DruidDDLParser ddlParser = null;
  private DruidDQLParser dqlParser = null;
  private DruidDMLParser dmlParser = null;

  /**
   * Empty constructor.
   * 
   * @param conf
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   */
  public WaspParser() throws ZooKeeperConnectionException {
  }
  
  /**
   * @param conf
   * @throws ZooKeeperConnectionException
   */
  public WaspParser(DruidDDLParser ddlParser, DruidDQLParser dqlParser,
      DruidDMLParser dmlParser) throws ZooKeeperConnectionException {
    this.ddlParser = ddlParser;
    this.dmlParser = dmlParser;
    this.dqlParser = dqlParser;
  }

  /**
   * @return the connection
   */
  public FConnection getConnection() {
    return connection;
  }

  /**
   * @return the ddlParser
   */
  public DruidDDLParser getDruidDDLParser() {
    return ddlParser;
  }

  /**
   * @throws ZooKeeperConnectionException
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration configuration) {
    super.setConf(configuration);
    try {
      connection = FConnectionManager.getConnection(this.getConf());
      if (ddlParser == null) {
        ddlParser = new DruidDDLParser(this.configuration);
      }
      if (dqlParser == null) {
        dqlParser = new DruidDQLParser(this.configuration);
      }
      if (dmlParser == null) {
        dmlParser = new DruidDMLParser(this.configuration);
      }
    } catch (ZooKeeperConnectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generate plan(insert plan,update plan,delete plan,query plan) in the parse
   * context
   * 
   * @param context
   * @throws IOException
   */
  @Override
  public void generatePlan(ParseContext context) throws IOException {
    parseSqlToStatement(context);
    SQLStatement stmt = context.getStmt();
    boolean isDDL = false;
    boolean isDQL = false;
    if (stmt instanceof WaspSqlCreateTableStatement) {
      // This is a Create Table SQL
      isDDL = true;
    } else if (stmt instanceof WaspSqlCreateIndexStatement) {
      // This is a Create Index SQL
      isDDL = true;
    } else if (stmt instanceof SQLDropTableStatement) {
      // This is a Drop Table SQL
      isDDL = true;
    } else if (stmt instanceof SQLDropIndexStatement) {
      // This is a Drop Index SQL
      isDDL = true;
    } else if (stmt instanceof MySqlAlterTableStatement) {
      // This is a Alter Table SQL
      isDDL = true;
    } else if (stmt instanceof WaspSqlShowTablesStatement) {
      // This is a Show Tables SQL
      isDDL = true;
    } else if (stmt instanceof WaspSqlShowCreateTableStatement) {
      // This is a Show Create Table SQL
      isDDL = true;
    } else if (stmt instanceof WaspSqlDescribeStatement) {
      // This is a DESCRIBE SQL
      isDDL = true;
    } else if (stmt instanceof WaspSqlShowIndexesStatement) {
      // This is a SHOW INDEXES SQL
      isDDL = true;
    } else if (stmt instanceof SQLSelectStatement) {
      // This is a Select SQL
      isDDL = false;
      isDQL = true;
    } else if (stmt instanceof SQLUpdateStatement) {
      // This is a Update SQL
      isDDL = false;
    } else if (stmt instanceof SQLInsertStatement) {
      // This is a Insert SQL
      isDDL = false;
    } else if (stmt instanceof SQLDeleteStatement) {
      // This is a Delete SQL
      isDDL = false;
    } else {
      throw new UnsupportedException("Unsupported SQLStatement " + stmt);
    }
    if (isDDL) {
      ddlParser.generatePlan(context);
    } else if (isDQL) {
      dqlParser.generatePlan(context);
    } else {
      dmlParser.generatePlan(context);
    }
  }
}
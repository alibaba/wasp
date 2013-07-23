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
package org.apache.wasp.plan.parser.druid.dialect;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;

/**
 *
 *
 */
public class WaspSqlCreateTableStatement extends MySqlCreateTableStatement {

  private static final long serialVersionUID = 1L;

  public static enum TableCategory {
    ROOT, CHILD
  }

  private List<SQLExpr> primaryKeys = new ArrayList<SQLExpr>();

  private SQLExpr entityGroupKey;

  private SQLExprTableSource referenceTable; // reference table

  protected SQLExprTableSource parentTableSource;

  private TableCategory category;

  public SQLExpr getEntityGroupKey() {
    return entityGroupKey;
  }

  public void setEntityGroupKey(SQLExpr foreignKey) {
    this.entityGroupKey = foreignKey;
  }

  public SQLExprTableSource getReferenceTable() {
    return referenceTable;
  }

  public void setReferenceTable(SQLName name) {
    this.referenceTable = new SQLExprTableSource(name);
  }

  public TableCategory getCategory() {
    return category;
  }

  public void setCategory(TableCategory category) {
    this.category = category;
  }

  public List<SQLExpr> getPrimaryKeys() {
    return primaryKeys;
  }

  public void setPrimaryKeys(List<SQLExpr> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  public void addPrimaryKey(SQLExpr primaryKey) {
    primaryKeys.add(primaryKey);
  }

  public void setInTableName(SQLName name) {
    parentTableSource = new SQLExprTableSource(name);
  }

  public SQLExprTableSource getInTableName() {
    return parentTableSource;
  }
}
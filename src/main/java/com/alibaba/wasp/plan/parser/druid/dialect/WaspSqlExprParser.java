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
package com.alibaba.wasp.plan.parser.druid.dialect;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSQLColumnDefinition;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.wasp.FieldKeyWord;

public class WaspSqlExprParser extends MySqlExprParser {

  public WaspSqlExprParser(Lexer lexer) {
    super(lexer);
  }

  public WaspSqlExprParser(String sql) {
    super(sql);
  }

  // parse a column
  public SQLColumnDefinition parseCreateColumn() {
    WaspSqlColumnDefinition column = new WaspSqlColumnDefinition();
    SQLName name = name();
    if (name.getSimleName().equalsIgnoreCase(WaspSqlCreateTableParser.REQUIRED)) {
      column.setColumnConstraint(FieldKeyWord.REQUIRED);
    } else if (name.getSimleName().equalsIgnoreCase(
        WaspSqlCreateTableParser.OPTIONAL)) {
      column.setColumnConstraint(FieldKeyWord.OPTIONAL);
    } else if (name.getSimleName().equalsIgnoreCase(
        WaspSqlCreateTableParser.REPEATED)) {
      column.setColumnConstraint(FieldKeyWord.REPEATED);
    } else {
      throw new ParserException("error " + name);
    }
    column.setDataType(parseDataType());
    column.setName(name());
    return parseColumnRest(column);
  }

  public SQLColumnDefinition parseColumn() {
    WaspSqlColumnDefinition column = new WaspSqlColumnDefinition();
    column.setName(name());
    column.setDataType(parseDataType());

    return parseColumnRest(column);
  }

  public SQLColumnDefinition parseColumnRest(SQLColumnDefinition column) {
    if (identifierEquals("AUTO_INCREMENT")) {
      lexer.nextToken();
      if (column instanceof MySqlSQLColumnDefinition) {
        ((MySqlSQLColumnDefinition) column).setAutoIncrement(true);
      }
      return parseColumnRest(column);
    }

    if (identifierEquals("COLUMNFAMILY")) {// columnfamily
      lexer.nextToken();
      if (column instanceof WaspSqlColumnDefinition) {
        ((WaspSqlColumnDefinition) column).setColumnFamily(name());
      }
      return parseColumnRest(column);
    }

    if (identifierEquals("COMMENT")) {// columnfamily
      lexer.nextToken();
      // accept();
      column.setComment(name().getSimleName());
      return parseColumnRest(column);
    }

    if (identifierEquals("PARTITION")) {
      throw new ParserException("syntax error " + lexer.token() + " "
          + lexer.stringVal());
    }

    super.parseColumnRest(column);

    return column;
  }

  public Limit parseLimit() {
    if (lexer.token() == Token.LIMIT) {
      lexer.nextToken();

      Limit limit = new Limit();

      SQLExpr temp = this.expr();
      if (lexer.token() == (Token.COMMA)) {
        limit.setOffset(temp);
        lexer.nextToken();
        limit.setRowCount(this.expr());
      } else if (identifierEquals("OFFSET")) {
        limit.setRowCount(temp);
        lexer.nextToken();
        limit.setOffset(this.expr());
      } else {
        limit.setRowCount(temp);
      }
      return limit;
    }
    return null;
  }
}

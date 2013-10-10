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

import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlSelectParser;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.Token;
import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlCreateTableStatement.TableCategory;

/**
 * Create table dialect.
 * 
 */
public class WaspSqlCreateTableParser extends MySqlCreateTableParser {

  public static final String REQUIRED = "REQUIRED";
  public static final String OPTIONAL = "OPTIONAL";
  public static final String REPEATED = "REPEATED";

  /**
   * @param exprParser
   */
  public WaspSqlCreateTableParser(SQLExprParser exprParser) {
    super(exprParser);
  }

  /**
   * @param sql
   */
  public WaspSqlCreateTableParser(String sql) {
    super(new WaspSqlExprParser(sql));
  }

  public WaspSqlCreateTableStatement parseCrateTable(boolean acceptCreate) {
    if (acceptCreate) {
      accept(Token.CREATE);
    }
    WaspSqlCreateTableStatement stmt = new WaspSqlCreateTableStatement();

    if (identifierEquals("TEMPORARY")) {
      lexer.nextToken();
      stmt.setType(SQLCreateTableStatement.Type.GLOBAL_TEMPORARY);
    }

    accept(Token.TABLE);

    if (lexer.token() == Token.IF || identifierEquals("IF")) {
      lexer.nextToken();
      accept(Token.NOT);
      accept(Token.EXISTS);

      stmt.setIfNotExiists(true);
    }

    stmt.setName(this.exprParser.name());

    Token start = null;
    if (lexer.token() == (Token.LPAREN)) {
      start = Token.LPAREN;
    } else if (lexer.token() == (Token.LBRACE)) {
      start = Token.LBRACE;
    }
    if (start == (Token.LPAREN) || start == (Token.LBRACE)) {
      lexer.nextToken();

      for (;;) {
        if (identifierEquals(REQUIRED) || identifierEquals(OPTIONAL)
            || identifierEquals(REPEATED)) {
          if (exprParser instanceof WaspSqlExprParser) {
            SQLColumnDefinition column = ((WaspSqlExprParser) this.exprParser)
                .parseCreateColumn();
            stmt.getTableElementList().add(column);
          } else {
            SQLColumnDefinition column = this.exprParser.parseColumn();
            stmt.getTableElementList().add(column);
          }
        }

        if (!(lexer.token() == (Token.COMMA) || (lexer.token() == (Token.SEMI)))) {
          break;
        } else {
          lexer.nextToken();
        }
      }
      if (start == (Token.LPAREN)) {
        accept(Token.RPAREN);
      } else if (start == (Token.LBRACE)) {
        accept(Token.RBRACE);
      }
    }

    // Primary Key
    accept(Token.PRIMARY);
    accept(Token.KEY);
    accept(Token.LPAREN);
    for (;;) {
      stmt.getPrimaryKeys().add(this.exprParser.expr());
      if (!(lexer.token() == (Token.COMMA))) {
        break;
      } else {
        lexer.nextToken();
      }
    }
    accept(Token.RPAREN);
    accept(Token.COMMA);

    // In Table
    if (lexer.stringVal().equalsIgnoreCase("IN")) {
      lexer.nextToken();
      accept(Token.TABLE);
      stmt.setInTableName(this.exprParser.name());
      accept(Token.COMMA);
    }

    for (;;) {
      if (identifierEquals("ENTITY")) {
        lexer.nextToken();
        accept(Token.GROUP);
        if (identifierEquals("ROOT")) {
          stmt.setCategory(TableCategory.ROOT);
          lexer.nextToken();
        } else if (lexer.stringVal().equalsIgnoreCase("KEY")) {
          lexer.nextToken();
          accept(Token.LPAREN);
          stmt.setEntityGroupKey(this.exprParser.expr());
          accept(Token.RPAREN);
          if (stmt.getCategory() != TableCategory.ROOT) {
            stmt.setCategory(TableCategory.CHILD);
            accept(Token.REFERENCES);
            stmt.setReferenceTable(this.exprParser.name());
          }
        }
      }

      if (identifierEquals("TYPE")) {
        lexer.nextToken();
        accept(Token.EQ);
        stmt.getTableOptions().put("TYPE", lexer.stringVal());
        lexer.nextToken();
      }

      if (identifierEquals("PARTITION")) {
        lexer.nextToken();
        accept(Token.BY);

        if (lexer.token() == Token.KEY) {
          MySqlPartitionByKey clause = new MySqlPartitionByKey();
          lexer.nextToken();
          accept(Token.LPAREN);
          for (;;) {
            clause.getColumns().add(this.exprParser.name());
            if (lexer.token() == Token.COMMA) {
              lexer.nextToken();
              continue;
            }
            break;
          }
          accept(Token.RPAREN);
          stmt.setPartitioning(clause);

          if (identifierEquals("PARTITIONS")) {
            lexer.nextToken();
            clause.setPartitionCount(this.exprParser.expr());
          }
        } else if (identifierEquals("RANGE")) {
          WaspSqlPartitionByKey clause = new WaspSqlPartitionByKey();
          lexer.nextToken();
          accept(Token.LPAREN);
          clause.setStart(this.exprParser.expr());
          accept(Token.COMMA);
          clause.setEnd(this.exprParser.expr());
          accept(Token.COMMA);
          clause.setPartitionCount(this.exprParser.expr());
          accept(Token.RPAREN);
          stmt.setPartitioning(clause);
        } else {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        }
      }

      if (!(lexer.token() == (Token.COMMA))) {
        break;
      } else {
        lexer.nextToken();
      }

    }
    if (lexer.token() == (Token.ON)) {
      throw new ParserException("TODO");
    }

    if (lexer.token() == (Token.SELECT)) {
      SQLSelect query = new MySqlSelectParser(this.exprParser).select();
      stmt.setQuery(query);
    }

    return stmt;
  }
}
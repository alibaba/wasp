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

import java.util.List;

import org.apache.wasp.plan.parser.druid.dialect.WaspSqlLockTableStatement.LockType;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLLiteralExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLPrimaryKey;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLSelectParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.parser.Token;

/**
 * 
 *
 */
public class WaspSqlStatementParser extends SQLStatementParser {
  private static final String COLLATE2 = "COLLATE";
  private static final String CASCADE = "CASCADE";
  private static final String RESTRICT = "RESTRICT";
  private static final String CHARACTER = "CHARACTER";
  private static final String SESSION = "SESSION";
  private static final String GLOBAL = "GLOBAL";
  private static final String VARIABLES = "VARIABLES";
  private static final String ERRORS = "ERRORS";
  private static final String STATUS = "STATUS";
  private static final String IGNORE = "IGNORE";
  private static final String DESCRIBE = "DESCRIBE";
  private static final String DESC = "DESC";
  private static final String WRITE = "WRITE";
  private static final String READ = "READ";
  private static final String LOCAL = "LOCAL";
  private static final String TABLES = "TABLES";
  private static final String TEMPORARY = "TEMPORARY";
  private static final String USER = "USER";
  private static final String SPATIAL = "SPATIAL";
  private static final String FULLTEXT = "FULLTEXT";
  private static final String REPLACE = "REPLACE";
  private static final String DELAYED = "DELAYED";
  private static final String LOW_PRIORITY = "LOW_PRIORITY";

  /**
   * @param lexer
   */
  public WaspSqlStatementParser(Lexer lexer) {
    super(new WaspSqlExprParser(lexer));
  }

  public WaspSqlStatementParser(String sql) {
    super(new WaspSqlExprParser(sql));
  }

  public SQLStatement parseCreate() {
    accept(Token.CREATE);
    List<SQLCommentHint> hints = this.exprParser.parseHints();
    if (lexer.token() == Token.TABLE || identifierEquals(TEMPORARY)) {
      WaspSqlCreateTableParser parser = new WaspSqlCreateTableParser(
          this.exprParser);
      WaspSqlCreateTableStatement stmt = parser.parseCrateTable(false);
      stmt.setHints(hints);
      return stmt;
    }
    if (lexer.token() == Token.DATABASE) {
      return parseCreateDatabase();
    }
    if (lexer.token() == Token.UNIQUE || lexer.token() == Token.INDEX
        || identifierEquals(FULLTEXT) || identifierEquals(SPATIAL)) {
      return parseCreateIndex();
    }

    if (identifierEquals(USER)) {
      return parseCreateUser();
    }
    throw new ParserException("TODO " + lexer.token() + " " + lexer.stringVal());
  }

  public SQLStatement parseCreateIndex() {
    WaspSqlCreateIndexStatement stmt = new WaspSqlCreateIndexStatement();
    if (lexer.token() == Token.UNIQUE) {
      stmt.setType("UNIQUE");
      lexer.nextToken();
    } else if (identifierEquals(FULLTEXT)) {
      stmt.setType(FULLTEXT);
      lexer.nextToken();
    } else if (identifierEquals(SPATIAL)) {
      stmt.setType(SPATIAL);
      lexer.nextToken();
    }
    accept(Token.INDEX);
    stmt.setName(this.exprParser.name());
    parseCreateIndexUsing(stmt);
    accept(Token.ON);
    stmt.setTable(this.exprParser.name());
    accept(Token.LPAREN);
    for (;;) {
      SQLSelectOrderByItem item = this.exprParser.parseSelectOrderByItem();
      stmt.getItems().add(item);
      if (lexer.token() == Token.COMMA) {
        lexer.nextToken();
        continue;
      }
      break;
    }
    accept(Token.RPAREN);
    parseCreateIndexUsing(stmt);
    return stmt;
  }

  private void parseCreateIndexUsing(WaspSqlCreateIndexStatement stmt) {
    if (identifierEquals("USING")) {
      lexer.nextToken();
      if (identifierEquals("BTREE")) {
        stmt.setUsing("BTREE");
        lexer.nextToken();
      } else if (identifierEquals("HASH")) {
        stmt.setUsing("HASH");
        lexer.nextToken();
      } else {
        throw new ParserException("TODO " + lexer.token() + " "
            + lexer.stringVal());
      }
    } else if (identifierEquals("STORING")) {
      lexer.nextToken();
      accept(Token.LPAREN);
      for (;;) {
        stmt.getStoringCols().add(this.exprParser.expr());
        if (!(lexer.token() == (Token.COMMA))) {
          break;
        } else {
          lexer.nextToken();
        }
      }
      accept(Token.RPAREN);
    }
  }

  public SQLCreateTableStatement parseCreateTable() {
    WaspSqlCreateTableParser parser = new WaspSqlCreateTableParser(
        this.exprParser);
    return parser.parseCrateTable(false);
  }

  public SQLSelectStatement parseSelect() {
    return new SQLSelectStatement(
        new WaspSqlSelectParser(this.exprParser).select());
  }

  public SQLUpdateStatement parseUpdateStatement() {
    WaspSqlUpdateStatement stmt = createUpdateStatement();
    if (lexer.token() == Token.UPDATE) {
      lexer.nextToken();
      if (identifierEquals(LOW_PRIORITY)) {
        lexer.nextToken();
        stmt.setLowPriority(true);
      }
      if (identifierEquals(IGNORE)) {
        lexer.nextToken();
        stmt.setIgnore(true);
      }
      SQLTableSource tableSource = this.exprParser.createSelectParser()
          .parseTableSource();
      stmt.setTableSource(tableSource);
    }
    accept(Token.SET);
    for (;;) {
      SQLUpdateSetItem item = new SQLUpdateSetItem();
      item.setColumn(this.exprParser.name());
      if (lexer.token() == Token.EQ) {
        lexer.nextToken();
      } else {
        accept(Token.COLONEQ);
      }
      item.setValue(this.exprParser.expr());
      stmt.getItems().add(item);
      if (lexer.token() == (Token.COMMA)) {
        lexer.nextToken();
        continue;
      }
      break;
    }
    if (lexer.token() == (Token.WHERE)) {
      lexer.nextToken();
      stmt.setWhere(this.exprParser.expr());
    }
    stmt.setOrderBy(this.exprParser.parseOrderBy());
    stmt.setLimit(parseLimit());
    return stmt;
  }

  protected WaspSqlUpdateStatement createUpdateStatement() {
    return new WaspSqlUpdateStatement();
  }

  public WaspSqlDeleteStatement parseDeleteStatement() {
    WaspSqlDeleteStatement deleteStatement = new WaspSqlDeleteStatement();
    if (lexer.token() == Token.DELETE) {
      lexer.nextToken();
      if (identifierEquals(LOW_PRIORITY)) {
        deleteStatement.setLowPriority(true);
        lexer.nextToken();
      }
      if (identifierEquals("QUICK")) {
        deleteStatement.setQuick(true);
        lexer.nextToken();
      }
      if (identifierEquals(IGNORE)) {
        deleteStatement.setIgnore(true);
        lexer.nextToken();
      }
      if (lexer.token() == Token.IDENTIFIER) {
        deleteStatement.setTableSource(createSQLSelectParser()
            .parseTableSource());
        if (lexer.token() == Token.FROM) {
          lexer.nextToken();
          SQLTableSource tableSource = createSQLSelectParser()
              .parseTableSource();
          deleteStatement.setFrom(tableSource);
        }
      } else {
        if (lexer.token() == Token.FROM) {
          lexer.nextToken();
          deleteStatement.setTableSource(createSQLSelectParser()
              .parseTableSource());
        }
      }
      if (identifierEquals("USING")) {
        lexer.nextToken();
        SQLTableSource tableSource = createSQLSelectParser().parseTableSource();
        deleteStatement.setUsing(tableSource);
      }
    }

    if (lexer.token() == (Token.WHERE)) {
      lexer.nextToken();
      SQLExpr where = this.exprParser.expr();
      deleteStatement.setWhere(where);
    }
    if (lexer.token() == (Token.ORDER)) {
      SQLOrderBy orderBy = exprParser.parseOrderBy();
      deleteStatement.setOrderBy(orderBy);
    }
    deleteStatement.setLimit(parseLimit());
    return deleteStatement;
  }

  public SQLStatement parseCreateUser() {
    if (lexer.token() == Token.CREATE) {
      lexer.nextToken();
    }
    acceptIdentifier(USER);
    WaspSqlCreateUserStatement stmt = new WaspSqlCreateUserStatement();
    for (;;) {
      WaspSqlCreateUserStatement.UserSpecification userSpec = new WaspSqlCreateUserStatement.UserSpecification();
      SQLExpr expr = exprParser.primary();
      userSpec.setUser(expr);
      if (lexer.token() == Token.IDENTIFIED) {
        lexer.nextToken();
        if (lexer.token() == Token.BY) {
          lexer.nextToken();
          if (identifierEquals("PASSWORD")) {
            lexer.nextToken();
          }
          SQLCharExpr password = (SQLCharExpr) this.exprParser.expr();
          userSpec.setPassword(password);
        } else if (lexer.token() == Token.WITH) {
          lexer.nextToken();
          SQLCharExpr text = (SQLCharExpr) this.exprParser.expr();
          userSpec.setAuthPlugin(text);
        }
      }
      stmt.getUsers().add(userSpec);
      if (lexer.token() == Token.COMMA) {
        lexer.nextToken();
        continue;
      }
      break;
    }
    return stmt;
  }

  public SQLStatement parseKill() {
    accept(Token.KILL);
    WaspSqlKillStatement stmt = new WaspSqlKillStatement();
    if (identifierEquals("CONNECTION")) {
      stmt.setType(WaspSqlKillStatement.Type.CONNECTION);
      lexer.nextToken();
    } else if (identifierEquals("QUERY")) {
      stmt.setType(WaspSqlKillStatement.Type.QUERY);
      lexer.nextToken();
    } else {
      throw new ParserException("not support kill type " + lexer.token());
    }
    SQLExpr threadId = this.exprParser.expr();
    stmt.setThreadId(threadId);
    return stmt;
  }

  public boolean parseStatementListDialect(List<SQLStatement> statementList) {
    if (lexer.token() == Token.KILL) {
      SQLStatement stmt = parseKill();
      statementList.add(stmt);
      return true;
    }

    if (identifierEquals("PREPARE")) {
      WaspSqlPrepareStatement stmt = parsePrepare();
      statementList.add(stmt);
      return true;
    }

    if (identifierEquals("EXECUTE")) {
      WaspSqlExecuteStatement stmt = parseExecute();
      statementList.add(stmt);
      return true;
    }

    if (identifierEquals("LOAD")) {
      SQLStatement stmt = parseLoad();
      statementList.add(stmt);
      return true;
    }

    if (identifierEquals("START")) {
      WaspSqlStartTransactionStatement stmt = parseStart();
      statementList.add(stmt);
      return true;
    }

    if (identifierEquals("SHOW")) {
      SQLStatement stmt = parseShow();
      statementList.add(stmt);
      return true;
    }

    if (identifierEquals("HELP")) {
      lexer.nextToken();
      WaspSqlHelpStatement stmt = new WaspSqlHelpStatement();
      stmt.setContent(this.exprParser.primary());
      statementList.add(stmt);
      return true;
    }

    if (identifierEquals(DESC) || identifierEquals(DESCRIBE)) {
      SQLStatement stmt = parseDescribe();
      statementList.add(stmt);
      return true;
    }

    if (lexer.token() == Token.LOCK) {
      lexer.nextToken();
      acceptIdentifier(TABLES);

      WaspSqlLockTableStatement stmt = new WaspSqlLockTableStatement();
      stmt.setTableSource(this.exprParser.name());

      if (identifierEquals(READ)) {
        lexer.nextToken();
        if (identifierEquals(LOCAL)) {
          lexer.nextToken();
          stmt.setLockType(LockType.READ_LOCAL);
        } else {
          stmt.setLockType(LockType.READ);
        }
      } else if (identifierEquals(WRITE)) {
        stmt.setLockType(LockType.WRITE);
      } else if (identifierEquals(LOW_PRIORITY)) {
        lexer.nextToken();
        acceptIdentifier(WRITE);
        stmt.setLockType(LockType.LOW_PRIORITY_WRITE);
      }

      statementList.add(stmt);
      return true;
    }

    if (identifierEquals("UNLOCK")) {
      lexer.nextToken();
      acceptIdentifier(TABLES);
      statementList.add(new WaspSqlUnlockTablesStatement());
      return true;
    }

    return false;
  }

  public WaspSqlDescribeStatement parseDescribe() {
    if (lexer.token() == Token.DESC || identifierEquals(DESCRIBE)) {
      lexer.nextToken();
    } else {
      throw new ParserException("expect DESC, actual " + lexer.token());
    }

    WaspSqlDescribeStatement stmt = new WaspSqlDescribeStatement();
    stmt.setObject(this.exprParser.name());

    return stmt;
  }

  public SQLStatement parseShow() {
    acceptIdentifier("SHOW");
    if (lexer.token() == Token.FULL) {
      lexer.nextToken();
      if (identifierEquals("PROCESSLIST")) {
        lexer.nextToken();
        WaspSqlShowProcessListStatement stmt = new WaspSqlShowProcessListStatement();
        stmt.setFull(true);
        return stmt;
      }
      acceptIdentifier("COLUMNS");
      WaspSqlShowColumnsStatement stmt = parseShowColumns();
      stmt.setFull(true);
      return stmt;
    }
    if (identifierEquals("COLUMNS")) {
      lexer.nextToken();
      WaspSqlShowColumnsStatement stmt = parseShowColumns();
      return stmt;
    }
    if (identifierEquals(TABLES)) {
      lexer.nextToken();
      WaspSqlShowTablesStatement stmt = parseShowTabless();
      return stmt;
    }
    if (identifierEquals("DATABASES")) {
      lexer.nextToken();
      WaspSqlShowDatabasesStatement stmt = parseShowDatabases();
      return stmt;
    }
    if (identifierEquals("WARNINGS")) {
      lexer.nextToken();
      WaspSqlShowWarningsStatement stmt = parseShowWarnings();
      return stmt;
    }
    if (identifierEquals("COUNT")) {
      lexer.nextToken();
      accept(Token.LPAREN);
      accept(Token.STAR);
      accept(Token.RPAREN);
      if (identifierEquals(ERRORS)) {
        lexer.nextToken();
        WaspSqlShowErrorsStatement stmt = new WaspSqlShowErrorsStatement();
        stmt.setCount(true);
        return stmt;
      } else {
        acceptIdentifier("WARNINGS");
        WaspSqlShowWarningsStatement stmt = new WaspSqlShowWarningsStatement();
        stmt.setCount(true);
        return stmt;
      }
    }
    if (identifierEquals(ERRORS)) {
      lexer.nextToken();
      WaspSqlShowErrorsStatement stmt = new WaspSqlShowErrorsStatement();
      stmt.setLimit(parseLimit());
      return stmt;
    }
    if (identifierEquals(STATUS)) {
      lexer.nextToken();
      WaspSqlShowStatusStatement stmt = parseShowStatus();
      return stmt;
    }
    if (identifierEquals(VARIABLES)) {
      lexer.nextToken();
      WaspSqlShowVariantsStatement stmt = parseShowVariants();
      return stmt;
    }
    if (identifierEquals(GLOBAL)) {
      lexer.nextToken();
      if (identifierEquals(STATUS)) {
        lexer.nextToken();
        WaspSqlShowStatusStatement stmt = parseShowStatus();
        stmt.setGlobal(true);
        return stmt;
      }
      if (identifierEquals(VARIABLES)) {
        lexer.nextToken();
        WaspSqlShowVariantsStatement stmt = parseShowVariants();
        stmt.setGlobal(true);
        return stmt;
      }
    }
    if (identifierEquals(SESSION)) {
      lexer.nextToken();
      if (identifierEquals(STATUS)) {
        lexer.nextToken();
        WaspSqlShowStatusStatement stmt = parseShowStatus();
        stmt.setSession(true);
        return stmt;
      }
      if (identifierEquals(VARIABLES)) {
        lexer.nextToken();
        WaspSqlShowVariantsStatement stmt = parseShowVariants();
        stmt.setSession(true);
        return stmt;
      }
    }
    if (identifierEquals(CHARACTER)) {
      lexer.nextToken();
      accept(Token.SET);
      WaspSqlShowCharacterSetStatement stmt = new WaspSqlShowCharacterSetStatement();
      if (lexer.token() == Token.LIKE) {
        lexer.nextToken();
        stmt.setPattern(this.exprParser.expr());
      }
      if (lexer.token() == Token.WHERE) {
        lexer.nextToken();
        stmt.setWhere(this.exprParser.expr());
      }
      return stmt;
    }
    if (lexer.token() == Token.CREATE) {
      lexer.nextToken();
      if (lexer.token() == Token.DATABASE) {
        lexer.nextToken();
        WaspSqlShowCreateDatabaseStatement stmt = new WaspSqlShowCreateDatabaseStatement();
        stmt.setDatabase(this.exprParser.name());
        return stmt;
      }
      if (lexer.token() == Token.TABLE) {
        lexer.nextToken();
        WaspSqlShowCreateTableStatement stmt = new WaspSqlShowCreateTableStatement();
        stmt.setName(this.exprParser.name());
        return stmt;
      }
      throw new ParserException("TODO " + lexer.stringVal());
    }
    if (identifierEquals("GRANTS")) {
      lexer.nextToken();
      WaspSqlShowGrantsStatement stmt = new WaspSqlShowGrantsStatement();
      if (lexer.token() == Token.FOR) {
        lexer.nextToken();
        stmt.setUser(this.exprParser.expr());
      }
      return stmt;
    }
    if (lexer.token() == Token.INDEX || identifierEquals("INDEXES")) {
      lexer.nextToken();
      WaspSqlShowIndexesStatement stmt = new WaspSqlShowIndexesStatement();
      if (lexer.token() == Token.FROM || lexer.token() == Token.IN) {
        lexer.nextToken();
        SQLName table = exprParser.name();
        stmt.setTable(table);
        if (lexer.token() == Token.FROM || lexer.token() == Token.IN) {
          lexer.nextToken();
          SQLName database = exprParser.name();
          stmt.setDatabase(database);
        }
      }
      return stmt;
    }
    if (identifierEquals("OPEN")) {
      lexer.nextToken();
      acceptIdentifier(TABLES);
      WaspSqlShowOpenTablesStatement stmt = new WaspSqlShowOpenTablesStatement();
      if (lexer.token() == Token.FROM || lexer.token() == Token.IN) {
        lexer.nextToken();
        stmt.setDatabase(this.exprParser.name());
      }
      if (lexer.token() == Token.LIKE) {
        lexer.nextToken();
        stmt.setLike(this.exprParser.expr());
      }
      if (lexer.token() == Token.WHERE) {
        lexer.nextToken();
        stmt.setWhere(this.exprParser.expr());
      }
      return stmt;
    }
    if (identifierEquals("PROFILES")) {
      lexer.nextToken();
      WaspSqlShowProfilesStatement stmt = new WaspSqlShowProfilesStatement();
      return stmt;
    }

    if (identifierEquals("PROFILE")) {
      lexer.nextToken();
      WaspSqlShowProfileStatement stmt = new WaspSqlShowProfileStatement();
      for (;;) {
        if (lexer.token() == Token.ALL) {
          stmt.getTypes().add(WaspSqlShowProfileStatement.Type.ALL);
          lexer.nextToken();
        } else if (identifierEquals("BLOCK")) {
          lexer.nextToken();
          acceptIdentifier("IO");
          stmt.getTypes().add(WaspSqlShowProfileStatement.Type.BLOCK_IO);
        } else if (identifierEquals("CONTEXT")) {
          lexer.nextToken();
          acceptIdentifier("SWITCHES");
          stmt.getTypes()
              .add(WaspSqlShowProfileStatement.Type.CONTEXT_SWITCHES);
        } else if (identifierEquals("CPU")) {
          lexer.nextToken();
          stmt.getTypes().add(WaspSqlShowProfileStatement.Type.CPU);
        } else if (identifierEquals("IPC")) {
          lexer.nextToken();
          stmt.getTypes().add(WaspSqlShowProfileStatement.Type.IPC);
        } else if (identifierEquals("MEMORY")) {
          lexer.nextToken();
          stmt.getTypes().add(WaspSqlShowProfileStatement.Type.MEMORY);
        } else if (identifierEquals("PAGE")) {
          lexer.nextToken();
          acceptIdentifier("FAULTS");
          stmt.getTypes().add(WaspSqlShowProfileStatement.Type.PAGE_FAULTS);
        } else if (identifierEquals("SOURCE")) {
          lexer.nextToken();
          stmt.getTypes().add(WaspSqlShowProfileStatement.Type.SOURCE);
        } else if (identifierEquals("SWAPS")) {
          lexer.nextToken();
          stmt.getTypes().add(WaspSqlShowProfileStatement.Type.SWAPS);
        } else {
          break;
        }

        if (lexer.token() == Token.COMMA) {
          lexer.nextToken();
          continue;
        }
        break;
      }

      if (lexer.token() == Token.FOR) {
        lexer.nextToken();
        acceptIdentifier("QUERY");
        stmt.setForQuery(this.exprParser.primary());
      }

      stmt.setLimit(this.parseLimit());

      return stmt;
    }
    if (lexer.token() == Token.TABLE) {
      lexer.nextToken();
      acceptIdentifier(STATUS);
      WaspSqlShowTableStatusStatement stmt = new WaspSqlShowTableStatusStatement();
      if (lexer.token() == Token.FROM || lexer.token() == Token.IN) {
        lexer.nextToken();
        stmt.setDatabase(this.exprParser.name());
      }
      if (lexer.token() == Token.LIKE) {
        lexer.nextToken();
        stmt.setLike(this.exprParser.expr());
      }
      if (lexer.token() == Token.WHERE) {
        lexer.nextToken();
        stmt.setWhere(this.exprParser.expr());
      }
      return stmt;
    }
    throw new ParserException("TODO " + lexer.stringVal());
  }

  private WaspSqlShowStatusStatement parseShowStatus() {
    WaspSqlShowStatusStatement stmt = new WaspSqlShowStatusStatement();
    if (lexer.token() == Token.LIKE) {
      lexer.nextToken();
      SQLExpr like = exprParser.expr();
      stmt.setLike(like);
    }
    if (lexer.token() == Token.WHERE) {
      lexer.nextToken();
      SQLExpr where = exprParser.expr();
      stmt.setWhere(where);
    }
    return stmt;
  }

  private WaspSqlShowVariantsStatement parseShowVariants() {
    WaspSqlShowVariantsStatement stmt = new WaspSqlShowVariantsStatement();
    if (lexer.token() == Token.LIKE) {
      lexer.nextToken();
      SQLExpr like = exprParser.expr();
      stmt.setLike(like);
    }
    if (lexer.token() == Token.WHERE) {
      lexer.nextToken();
      SQLExpr where = exprParser.expr();
      stmt.setWhere(where);
    }
    return stmt;
  }

  private WaspSqlShowWarningsStatement parseShowWarnings() {
    WaspSqlShowWarningsStatement stmt = new WaspSqlShowWarningsStatement();
    stmt.setLimit(parseLimit());
    return stmt;
  }

  private WaspSqlShowDatabasesStatement parseShowDatabases() {
    WaspSqlShowDatabasesStatement stmt = new WaspSqlShowDatabasesStatement();
    if (lexer.token() == Token.LIKE) {
      lexer.nextToken();
      SQLExpr like = exprParser.expr();
      stmt.setLike(like);
    }
    if (lexer.token() == Token.WHERE) {
      lexer.nextToken();
      SQLExpr where = exprParser.expr();
      stmt.setWhere(where);
    }
    return stmt;
  }

  private WaspSqlShowTablesStatement parseShowTabless() {
    WaspSqlShowTablesStatement stmt = new WaspSqlShowTablesStatement();
    if (lexer.token() == Token.FROM) {
      lexer.nextToken();
      SQLName database = exprParser.name();
      stmt.setDatabase(database);
    }
    if (lexer.token() == Token.LIKE) {
      lexer.nextToken();
      SQLExpr like = exprParser.expr();
      stmt.setLike(like);
    }
    if (lexer.token() == Token.WHERE) {
      lexer.nextToken();
      SQLExpr where = exprParser.expr();
      stmt.setWhere(where);
    }
    return stmt;
  }

  private WaspSqlShowColumnsStatement parseShowColumns() {
    WaspSqlShowColumnsStatement stmt = new WaspSqlShowColumnsStatement();
    if (lexer.token() == Token.FROM) {
      lexer.nextToken();
      SQLName table = exprParser.name();
      stmt.setTable(table);
      if (lexer.token() == Token.FROM) {
        lexer.nextToken();
        SQLName database = exprParser.name();
        stmt.setDatabase(database);
      }
    }
    if (lexer.token() == Token.LIKE) {
      lexer.nextToken();
      SQLExpr like = exprParser.expr();
      stmt.setLike(like);
    }
    if (lexer.token() == Token.WHERE) {
      lexer.nextToken();
      SQLExpr where = exprParser.expr();
      stmt.setWhere(where);
    }
    return stmt;
  }

  public WaspSqlStartTransactionStatement parseStart() {
    acceptIdentifier("START");
    acceptIdentifier("TRANSACTION");
    WaspSqlStartTransactionStatement stmt = new WaspSqlStartTransactionStatement();
    if (identifierEquals("WITH")) {
      lexer.nextToken();
      acceptIdentifier("CONSISTENT");
      acceptIdentifier("SNAPSHOT");
      stmt.setConsistentSnapshot(true);
    }
    if (identifierEquals("BEGIN")) {
      lexer.nextToken();
      stmt.setBegin(true);
      if (identifierEquals("WORK")) {
        lexer.nextToken();
        stmt.setWork(true);
      }
    }
    return stmt;
  }

  protected SQLStatement parseLoad() {
    acceptIdentifier("LOAD");
    if (identifierEquals("DATA")) {
      SQLStatement stmt = parseLoadDataInFile();
      return stmt;
    }
    if (identifierEquals("XML")) {
      SQLStatement stmt = parseLoadXml();
      return stmt;
    }
    throw new ParserException("TODO");
  }

  protected WaspSqlLoadXmlStatement parseLoadXml() {
    acceptIdentifier("XML");
    WaspSqlLoadXmlStatement stmt = new WaspSqlLoadXmlStatement();
    if (identifierEquals(LOW_PRIORITY)) {
      stmt.setLowPriority(true);
      lexer.nextToken();
    }
    if (identifierEquals("CONCURRENT")) {
      stmt.setConcurrent(true);
      lexer.nextToken();
    }
    if (identifierEquals(LOCAL)) {
      stmt.setLocal(true);
      lexer.nextToken();
    }
    acceptIdentifier("INFILE");
    SQLLiteralExpr fileName = (SQLLiteralExpr) exprParser.expr();
    stmt.setFileName(fileName);
    if (identifierEquals(REPLACE)) {
      stmt.setReplicate(true);
      lexer.nextToken();
    }
    if (identifierEquals(IGNORE)) {
      stmt.setIgnore(true);
      lexer.nextToken();
    }
    accept(Token.INTO);
    accept(Token.TABLE);
    SQLName tableName = exprParser.name();
    stmt.setTableName(tableName);
    if (identifierEquals(CHARACTER)) {
      lexer.nextToken();
      accept(Token.SET);
      if (lexer.token() != Token.LITERAL_CHARS) {
        throw new ParserException("syntax error, illegal charset");
      }
      String charset = lexer.stringVal();
      lexer.nextToken();
      stmt.setCharset(charset);
    }
    if (identifierEquals("ROWS")) {
      lexer.nextToken();
      accept(Token.IDENTIFIED);
      accept(Token.BY);
      SQLExpr rowsIdentifiedBy = exprParser.expr();
      stmt.setRowsIdentifiedBy(rowsIdentifiedBy);
    }
    if (identifierEquals(IGNORE)) {
      throw new ParserException("TODO");
    }
    if (lexer.token() == Token.SET) {
      throw new ParserException("TODO");
    }
    return stmt;
  }

  protected WaspSqlLoadDataInFileStatement parseLoadDataInFile() {
    acceptIdentifier("DATA");
    WaspSqlLoadDataInFileStatement stmt = new WaspSqlLoadDataInFileStatement();
    if (identifierEquals(LOW_PRIORITY)) {
      stmt.setLowPriority(true);
      lexer.nextToken();
    }
    if (identifierEquals("CONCURRENT")) {
      stmt.setConcurrent(true);
      lexer.nextToken();
    }
    if (identifierEquals(LOCAL)) {
      stmt.setLocal(true);
      lexer.nextToken();
    }
    acceptIdentifier("INFILE");
    SQLLiteralExpr fileName = (SQLLiteralExpr) exprParser.expr();
    stmt.setFileName(fileName);
    if (identifierEquals(REPLACE)) {
      stmt.setReplicate(true);
      lexer.nextToken();
    }
    if (identifierEquals(IGNORE)) {
      stmt.setIgnore(true);
      lexer.nextToken();
    }
    accept(Token.INTO);
    accept(Token.TABLE);
    SQLName tableName = exprParser.name();
    stmt.setTableName(tableName);
    if (identifierEquals(CHARACTER)) {
      lexer.nextToken();
      accept(Token.SET);
      if (lexer.token() != Token.LITERAL_CHARS) {
        throw new ParserException("syntax error, illegal charset");
      }
      String charset = lexer.stringVal();
      lexer.nextToken();
      stmt.setCharset(charset);
    }
    if (identifierEquals("FIELDS") || identifierEquals("COLUMNS")) {
      throw new ParserException("TODO");
    }
    if (identifierEquals("LINES")) {
      throw new ParserException("TODO");
    }
    if (identifierEquals(IGNORE)) {
      throw new ParserException("TODO");
    }
    if (lexer.token() == Token.SET) {
      throw new ParserException("TODO");
    }
    return stmt;
  }

  public WaspSqlPrepareStatement parsePrepare() {
    acceptIdentifier("PREPARE");
    SQLName name = exprParser.name();
    accept(Token.FROM);
    SQLExpr from = exprParser.expr();
    return new WaspSqlPrepareStatement(name, from);
  }

  public WaspSqlExecuteStatement parseExecute() {
    acceptIdentifier("EXECUTE");
    WaspSqlExecuteStatement stmt = new WaspSqlExecuteStatement();
    SQLName statementName = exprParser.name();
    stmt.setStatementName(statementName);
    if (identifierEquals("USING")) {
      lexer.nextToken();
      exprParser.exprList(stmt.getParameters());
    }
    return stmt;
  }

  public SQLInsertStatement parseInsert() {
    WaspSqlInsertStatement insertStatement = new WaspSqlInsertStatement();
    if (lexer.token() == Token.INSERT) {
      lexer.nextToken();
      if (identifierEquals(LOW_PRIORITY)) {
        insertStatement.setLowPriority(true);
        lexer.nextToken();
      }
      if (identifierEquals(DELAYED)) {
        insertStatement.setDelayed(true);
        lexer.nextToken();
      }
      if (identifierEquals("HIGH_PRIORITY")) {
        insertStatement.setHighPriority(true);
        lexer.nextToken();
      }
      if (identifierEquals(IGNORE)) {
        insertStatement.setIgnore(true);
        lexer.nextToken();
      }
      if (lexer.token() == Token.INTO) {
        lexer.nextToken();
      }
      SQLName tableName = this.exprParser.name();
      insertStatement.setTableName(tableName);
      if (lexer.token() == Token.IDENTIFIER && !identifierEquals("VALUE")) {
        insertStatement.setAlias(lexer.stringVal());
        lexer.nextToken();
      }
    }
    if (lexer.token() == (Token.LPAREN)) {
      lexer.nextToken();
      if (lexer.token() == (Token.SELECT)) {
        SQLSelect select = this.exprParser.createSelectParser().select();
        select.setParent(insertStatement);
        insertStatement.setQuery(select);
      } else {
        this.exprParser.exprList(insertStatement.getColumns());
      }
      accept(Token.RPAREN);
    }
    if (lexer.token() == Token.VALUES || identifierEquals("VALUE")) {
      lexer.nextToken();
      for (;;) {
        accept(Token.LPAREN);
        SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
        this.exprParser.exprList(values.getValues());
        insertStatement.getValuesList().add(values);
        accept(Token.RPAREN);
        if (lexer.token() == Token.COMMA) {
          lexer.nextToken();
          continue;
        } else {
          break;
        }
      }
    } else if (lexer.token() == Token.SET) {
      lexer.nextToken();
      SQLInsertStatement.ValuesClause values = new SQLInsertStatement.ValuesClause();
      insertStatement.getValuesList().add(values);
      for (;;) {
        SQLName name = this.exprParser.name();
        insertStatement.getColumns().add(name);
        if (lexer.token() == Token.EQ) {
          lexer.nextToken();
        } else {
          accept(Token.COLONEQ);
        }
        values.getValues().add(this.exprParser.expr());
        if (lexer.token() == Token.COMMA) {
          lexer.nextToken();
          continue;
        }
        break;
      }
    } else if (lexer.token() == (Token.SELECT)) {
      SQLSelect select = this.exprParser.createSelectParser().select();
      select.setParent(insertStatement);
      insertStatement.setQuery(select);
    } else if (lexer.token() == (Token.LPAREN)) {
      lexer.nextToken();
      SQLSelect select = this.exprParser.createSelectParser().select();
      select.setParent(insertStatement);
      insertStatement.setQuery(select);
      accept(Token.RPAREN);
    }
    if (lexer.token() == Token.ON) {
      lexer.nextToken();
      acceptIdentifier("DUPLICATE");
      accept(Token.KEY);
      accept(Token.UPDATE);
      exprParser.exprList(insertStatement.getDuplicateKeyUpdate());
    }

    return insertStatement;
  }

  public SQLStatement parseDropUser() {
    acceptIdentifier(USER);
    WaspSqlDropUser stmt = new WaspSqlDropUser();
    for (;;) {
      SQLExpr expr = this.exprParser.expr();
      stmt.getUsers().add(expr);
      if (lexer.token() == Token.COMMA) {
        lexer.nextToken();
        continue;
      }
      break;
    }
    return stmt;
  }

  protected SQLDropTableStatement parseDropTable(boolean acceptDrop) {
    if (acceptDrop) {
      accept(Token.DROP);
    }
    WaspSqlDropTableStatement stmt = new WaspSqlDropTableStatement();
    if (identifierEquals(TEMPORARY)) {
      lexer.nextToken();
      stmt.setTemporary(true);
    }
    accept(Token.TABLE);
    if (lexer.token() == Token.IF) {
      lexer.nextToken();
      accept(Token.EXISTS);
      stmt.setIfExists(true);
    }
    for (;;) {
      SQLName name = this.exprParser.name();
      stmt.addTableSource(name);
      if (lexer.token() == Token.COMMA) {
        lexer.nextToken();
        continue;
      }
      break;
    }
    if (identifierEquals(RESTRICT)) {
      stmt.setOption(RESTRICT);
      lexer.nextToken();
    } else if (identifierEquals(CASCADE)) {
      stmt.setOption(CASCADE);
      lexer.nextToken();
    }
    return stmt;
  }

  public SQLSelectParser createSQLSelectParser() {
    return new WaspSqlSelectParser(this.exprParser);
  }

  public Limit parseLimit() {
    return ((WaspSqlExprParser) this.exprParser).parseLimit();
  }

  public SQLStatement parseAlter() {
    accept(Token.ALTER);
    boolean ignore = false;
    if (identifierEquals(IGNORE)) {
      ignore = true;
      lexer.nextToken();
    }
    if (lexer.token() == Token.TABLE) {
      lexer.nextToken();
      WaspSqlAlterTableStatement stmt = new WaspSqlAlterTableStatement();
      stmt.setIgnore(ignore);
      stmt.setName(this.exprParser.name());
      for (;;) {
        if (identifierEquals("ADD")) {
          lexer.nextToken();
          if (identifierEquals("COLUMN")) {
            lexer.nextToken();
            WaspSqlAlterTableAddColumn item = new WaspSqlAlterTableAddColumn();
            SQLColumnDefinition columnDef = this.exprParser.parseColumn();
            item.getColumns().add(columnDef);
            if (identifierEquals("AFTER")) {
              lexer.nextToken();
              item.setAfterColumn(this.exprParser.name());
            } else if (identifierEquals("FIRST")) {
              lexer.nextToken();
              item.setFirst(true);
            }
            stmt.getItems().add(item);
          } else if (lexer.token() == Token.INDEX) {
            lexer.nextToken();
            WaspSqlAlterTableAddIndex item = new WaspSqlAlterTableAddIndex();

            if (lexer.token() == Token.LPAREN) {
              lexer.nextToken();
            } else {
              item.setName(this.exprParser.name());
              accept(Token.LPAREN);
            }
            for (;;) {
              SQLSelectOrderByItem column = this.exprParser
                  .parseSelectOrderByItem();
              item.getItems().add(column);
              if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
              }
              break;
            }
            accept(Token.RPAREN);
            stmt.getItems().add(item);
          } else if (lexer.token() == Token.UNIQUE) {
            lexer.nextToken();
            if (lexer.token() == Token.INDEX) {
              lexer.nextToken();
            }
            WaspSqlAlterTableAddUnique item = new WaspSqlAlterTableAddUnique();
            if (lexer.token() == Token.LPAREN) {
              lexer.nextToken();
            } else {
              item.setName(this.exprParser.name());
              accept(Token.LPAREN);
            }
            for (;;) {
              SQLSelectOrderByItem column = this.exprParser
                  .parseSelectOrderByItem();
              item.getItems().add(column);
              if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
              }
              break;
            }
            accept(Token.RPAREN);
            stmt.getItems().add(item);
          } else if (lexer.token() == Token.PRIMARY) {
            SQLPrimaryKey primaryKey = ((WaspSqlExprParser) this.exprParser)
                .parsePrimaryKey();
            SQLAlterTableAddPrimaryKey item = new SQLAlterTableAddPrimaryKey();
            item.setPrimaryKey(primaryKey);
            stmt.getItems().add(item);
          } else if (lexer.token() == Token.KEY) {
            throw new ParserException("TODO " + lexer.token() + " "
                + lexer.stringVal());
          } else if (lexer.token() == Token.CONSTRAINT) {
            throw new ParserException("TODO " + lexer.token() + " "
                + lexer.stringVal());
          } else if (identifierEquals(FULLTEXT)) {
            throw new ParserException("TODO " + lexer.token() + " "
                + lexer.stringVal());
          } else if (identifierEquals(SPATIAL)) {
            throw new ParserException("TODO " + lexer.token() + " "
                + lexer.stringVal());
          } else {
            WaspSqlAlterTableAddColumn item = new WaspSqlAlterTableAddColumn();
            SQLColumnDefinition columnDef = this.exprParser.parseColumn();
            item.getColumns().add(columnDef);
            if (identifierEquals("AFTER")) {
              lexer.nextToken();
              item.setAfterColumn(this.exprParser.name());
            }
            stmt.getItems().add(item);
          }
        } else if (identifierEquals("ALTER")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("CHANGE")) {
          lexer.nextToken();
          if (identifierEquals("COLUMN")) {
            lexer.nextToken();
          }
          WaspSqlAlterTableChangeColumn item = new WaspSqlAlterTableChangeColumn();
          item.setColumnName(this.exprParser.name());
          item.setNewColumnDefinition(this.exprParser.parseColumn());
          if (identifierEquals("AFTER")) {
            lexer.nextToken();
            item.setAfterColumn(this.exprParser.name());
          } else if (identifierEquals("FIRST")) {
            lexer.nextToken();
            if (lexer.token() == Token.IDENTIFIER) {
              item.setFirstColumn(this.exprParser.name());
            } else {
              item.setFirst(true);
            }
          }
          stmt.getItems().add(item);
        } else if (identifierEquals("MODIFY")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (lexer.token() == Token.DROP) {
          lexer.nextToken();
          if (lexer.token() == Token.INDEX) {
            lexer.nextToken();
            SQLName indexName = this.exprParser.name();
            SQLAlterTableDropIndex item = new SQLAlterTableDropIndex();
            item.setIndexName(indexName);
            stmt.getItems().add(item);
          } else {
            if (identifierEquals("COLUMN")) {
              lexer.nextToken();
            }
            SQLAlterTableDropColumnItem item = new SQLAlterTableDropColumnItem();
            item.setColumnName(this.exprParser.name());
            stmt.getItems().add(item);
          }
        } else if (identifierEquals("DISABLE")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("ENABLE")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("RENAME")) {
          lexer.nextToken();
          WaspSqlRenameTableStatement renameStmt = new WaspSqlRenameTableStatement();
          WaspSqlRenameTableStatement.Item item = new WaspSqlRenameTableStatement.Item();
          item.setName(stmt.getTableSource().getExpr());
          item.setTo(this.exprParser.name());
          renameStmt.getItems().add(item);

          return renameStmt;
        } else if (lexer.token() == Token.ORDER) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("CONVERT")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (lexer.token() == Token.DEFAULT) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("DISCARD")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("IMPORT")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("FORCE")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("TRUNCATE")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("COALESCE")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());

        } else if (identifierEquals("REORGANIZE")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("EXCHANGE")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("ANALYZE")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("CHECK")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("OPTIMIZE")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("REBUILD")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("REPAIR")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals("REMOVE")) {
          throw new ParserException("TODO " + lexer.token() + " "
              + lexer.stringVal());
        } else if (identifierEquals(CHARACTER)) {
          lexer.nextToken();
          accept(Token.SET);
          accept(Token.EQ);
          WaspSqlAlterTableCharacter item = new WaspSqlAlterTableCharacter();
          item.setCharacterSet(this.exprParser.primary());
          if (lexer.token() == Token.COMMA) {
            lexer.nextToken();
            acceptIdentifier(COLLATE2);
            accept(Token.EQ);
            item.setCollate(this.exprParser.primary());
          }
          stmt.getItems().add(item);
        } else {
          break;
        }

        if (lexer.token() == Token.COMMA) {
          lexer.nextToken();
          continue;
        } else {
          break;
        }
      }

      return stmt;
    }
    throw new ParserException("TODO " + lexer.token() + " " + lexer.stringVal());
  }

  public SQLStatement parseRename() {
    WaspSqlRenameTableStatement stmt = new WaspSqlRenameTableStatement();
    acceptIdentifier("RENAME");
    accept(Token.TABLE);
    for (;;) {
      WaspSqlRenameTableStatement.Item item = new WaspSqlRenameTableStatement.Item();
      item.setName(this.exprParser.name());
      acceptIdentifier("TO");
      item.setTo(this.exprParser.name());
      stmt.getItems().add(item);
      if (lexer.token() == Token.COMMA) {
        lexer.nextToken();
        continue;
      }
      break;
    }
    return stmt;
  }

  public SQLStatement parseCreateDatabase() {
    if (lexer.token() == Token.CREATE) {
      lexer.nextToken();
    }
    accept(Token.DATABASE);
    SQLCreateDatabaseStatement stmt = new SQLCreateDatabaseStatement();
    stmt.setName(this.exprParser.name());
    if (lexer.token() == Token.DEFAULT) {
      lexer.nextToken();
    }
    return stmt;
  }
}
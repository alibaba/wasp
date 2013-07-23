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
package org.apache.wasp.plan.parser.druid;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.DataType;
import org.apache.wasp.meta.Field;
import org.apache.wasp.plan.parser.ParseContext;
import org.apache.wasp.plan.parser.Parser;
import org.apache.wasp.plan.parser.UnsupportedException;
import org.apache.wasp.plan.parser.druid.dialect.WaspSqlParserUtils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.parser.SQLStatementParser;

/**
 * Use Druid (https://github.com/AlibabaTech/druid) to parse the sql and
 * generate QueryPlan
 * 
 */
public abstract class DruidParser extends Parser implements Configurable {
  private static final Log LOG = LogFactory.getLog(DruidParser.class);

  protected Configuration configuration;

  public DruidParser() {
    super();
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Parse SQL into SQLStatement, assume just one SQLStatement, so return one
   * SQLStatement result.
   * 
   * @see org.apache.wasp.plan.parser.Parser#parse(org.apache.wasp.plan.parser.ParseContext)
   */
  @Override
  public void parseSqlToStatement(ParseContext context) throws IOException {
    LOG.debug("Parsing SQL " + context.getSql());
    SQLStatementParser parser = WaspSqlParserUtils.createSQLStatementParser(
        context.getSql(), WaspSqlParserUtils.WASP); // JdbcUtils.MYSQL
                                                    // JdbcUtils.HBASE
    // WaspSqlParserUtils.WASP
    List<SQLStatement> stmtList = parser.parseStatementList();
    if (stmtList == null) {
      throw new UnsupportedException("Non SQLStatement");
    }
    if (stmtList.size() > 1) {
      throw new UnsupportedException("Multi SQLStatement Unsupported"
          + ", there is " + stmtList.size() + " SQLStatement");
    }
    // StmtList could be a list,but we only use an element.
    context.setStmt(stmtList.get(0));
  }

  /**
   * Parse The FROM clause. The FROM clause which indicates the table(s) from
   * which data is to be retrieved.
   */
  public String parseFromClause(SQLTableSource from)
      throws UnsupportedException {
    // only support SQLExprTableSource now
    if (from instanceof SQLExprTableSource) {
      SQLExprTableSource fromExpr = (SQLExprTableSource) from;
      SQLExpr expr = fromExpr.getExpr(); // SQLIdentifierExpr
      return parseName(expr);
    } else if (from instanceof SQLJoinTableSource) {
      // TODO impl join clause
      throw new UnsupportedException("Join clause Unsupported");
    } else if (from instanceof SQLSubqueryTableSource) {
      throw new UnsupportedException("Subquery clause Unsupported");
    } else {
      throw new UnsupportedException("Unsupported");
    }
  }

  public static String parseName(SQLExpr name) throws UnsupportedException {
    if (name instanceof SQLIdentifierExpr) {
      SQLIdentifierExpr id = (SQLIdentifierExpr) name;
      return id.getName();
    } else {
      throw new UnsupportedException(
          "Non SQLIdentifierExpr Unsupported, this is " + name);
    }
  }

  /**
   * Fetch Field instance from columns used by column name.
   * 
   * @param name
   * @param columns
   * @return
   */
  public static Field get(String name, List<Field> columns) {
    for (Field field : columns) {
      if (field.getName().equals(name)) {
        return field;
      }
    }
    return null;
  }

  public static String parseString(SQLExpr value) throws UnsupportedException {
    if (value instanceof SQLCharExpr) {
      SQLCharExpr charexpr = (SQLCharExpr) value;
      return ((SQLCharExpr) charexpr).getText();
    } else {
      throw new UnsupportedException(
          "Non SQLIdentifierExpr Unsupported, this is " + value);
    }
  }

  /**
   * Parse one column, get the column's name(String type)
   */
  public String parseColumn(SQLExpr column) throws UnsupportedException {
    return parseName(column);
  }

  /**
   * Parse the SQLExpr value into byte value
   * 
   */
  public static byte[] convert(Field column, SQLExpr expr)
      throws UnsupportedException {
    if (expr instanceof SQLIntegerExpr) {// int, long
      Number number = ((SQLIntegerExpr) expr).getNumber();
      if (column != null) {
        return convert(column.getType(), number);
      } else {
        return convert(null, number);
      }
    } else if (expr instanceof SQLCharExpr) {// String
      String value = ((SQLCharExpr) expr).getText();
      if (column != null && column.getType() == DataType.DATETIME) {
        long timestamp = -1;
        try {
          SimpleDateFormat formatter = new SimpleDateFormat(
              "yyyy-MM-dd HH:mm:ss");
          timestamp = formatter.parse(value).getTime();
        } catch (ParseException e) {
          //TODO what should i do with the exception?
          e.printStackTrace();
        }
        return Bytes.toBytes(timestamp);
      } else {
        return Bytes.toBytes(value);
      }
    } else if (expr instanceof SQLNumberExpr) {// float, double
      Number number = ((SQLNumberExpr) expr).getNumber();
      if (column != null) {
        return convert(column.getType(), number);
      } else {
        return convert(null, number);
      }
    }
    return null;
  }

  public static int convertToInt(SQLExpr expr) throws UnsupportedException {
    if (expr instanceof SQLIntegerExpr) {// int, long
      Number number = ((SQLIntegerExpr) expr).getNumber();
      return number.intValue();
    } else {
      throw new UnsupportedException("Input is not int or long, it is " + expr);
    }
  }

  public void checkType(Field field, SQLExpr value) throws UnsupportedException {
    DataType type = field.getType();
    if (type == DataType.INT32 || type == DataType.INT64) {
      if (value instanceof SQLIntegerExpr) {
        return; // OK
      }
    } else if (type == DataType.DOUBLE || type == DataType.FLOAT) {
      if (value instanceof SQLNumberExpr || value instanceof SQLIntegerExpr) {
        return; // OK
      }
    } else if (type == DataType.STRING) {
      if (value instanceof SQLCharExpr) {
        return; // OK
      }
    } else if (type == DataType.DATETIME) {
      if (value instanceof SQLCharExpr) {
        return; // OK
      }
    }
    throw new UnsupportedException("Unsupported DataType " + type
        + ", input is " + value);
  }

  /**
   * The abstract class Number is the superclass of classes BigDecimal,
   * BigInteger, Byte, Double, Float, Integer, Long, and Short. Subclasses of
   * Number must provide methods to convert the represented numeric value to
   * byte, double, float, int, long, and short.
   */
  public static byte[] convert(DataType type, Number number)
      throws UnsupportedException {
    // BigDecimal, BigInteger, Byte, Double, Float, Integer, Long, and Short.
    if (number instanceof BigDecimal) {
      if (type == DataType.FLOAT) {
        return Bytes.toBytes(number.floatValue());
      } else if (type == DataType.DOUBLE) {
        return Bytes.toBytes(number.doubleValue());
      } else if (type == DataType.INT32) {
        return Bytes.toBytes(number.intValue());
      } else if (type == DataType.INT64) {
        return Bytes.toBytes(number.longValue());
      }
      return Bytes.toBytes((BigDecimal) number);
    } else if (number instanceof BigInteger) {
      throw new UnsupportedException(" BigInteger " + number + " Unsupported");
    } else if (number instanceof Byte) {
      return Bytes.toBytes((Byte) number);
    } else if (number instanceof Double) {
      double value = number.doubleValue();
      if (type == DataType.FLOAT) {
        return Bytes.toBytes((float) value);
      } else if (type == DataType.DOUBLE) {
        return Bytes.toBytes(value);
      } else if (type == DataType.INT32) {
        int iv = (int) value;
        return Bytes.toBytes(iv);
      } else if (type == DataType.INT64) {
        long lv = (long) value;
        return Bytes.toBytes(lv);
      }
    } else if (number instanceof Float) {
      float value = number.floatValue();
      if (type == DataType.FLOAT) {
        return Bytes.toBytes(value);
      } else if (type == DataType.DOUBLE) {
        return Bytes.toBytes((float) value);
      } else if (type == DataType.INT32) {
        int iv = (int) value;
        return Bytes.toBytes(iv);
      } else if (type == DataType.INT64) {
        long lv = (long) value;
        return Bytes.toBytes(lv);
      }
    } else if (number instanceof Integer) {
      int value = number.intValue();
      if (type == DataType.INT32) {
        return Bytes.toBytes((Integer) value);
      } else if (type == DataType.INT64) {
        return Bytes.toBytes((long) value);
      } else if (type == DataType.FLOAT) {
        float fv = (float) value;
        return Bytes.toBytes(fv);
      } else if (type == DataType.DOUBLE) {
        double fv = (double) value;
        return Bytes.toBytes(fv);
      }
    } else if (number instanceof Long) {
      long value = number.longValue();
      if (type == DataType.INT32) {
        return Bytes.toBytes((int) value);
      } else if (type == DataType.INT64) {
        return Bytes.toBytes(value);
      } else if (type == DataType.FLOAT) {
        float fv = (float) value;
        return Bytes.toBytes(fv);
      } else if (type == DataType.DOUBLE) {
        double fv = (double) value;
        return Bytes.toBytes(fv);
      }
    } else if (number instanceof Short) {
      return Bytes.toBytes((Short) number);
    }
    throw new UnsupportedException("Unknown Number:" + number + " Type:" + type
        + " Unsupported ");
  }
}
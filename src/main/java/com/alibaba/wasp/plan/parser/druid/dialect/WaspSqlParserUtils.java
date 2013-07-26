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

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.parser.SQLExprParser;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcUtils;

/**
 * Local wasp parser tools,which extends SQLParserUtils.
 */
public class WaspSqlParserUtils extends SQLParserUtils {

  public static final String WASP = "wasp";

  public static SQLStatementParser createSQLStatementParser(String sql,
      String dbType) {
    if (WASP.equals(dbType) || JdbcUtils.HBASE.equals(dbType)) {
      return new WaspSqlStatementParser(sql);
    }
    return SQLParserUtils.createSQLStatementParser(sql, dbType);
  }

  public static SQLExprParser createExprParser(String sql, String dbType) {
    if (WASP.equals(dbType) || JdbcUtils.HBASE.equals(dbType)) {
      return new MySqlExprParser(sql);
    }
    return SQLParserUtils.createExprParser(sql, dbType);
  }
}
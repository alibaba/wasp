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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.alibaba.wasp.meta.FMetaServices;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.CreateTablePlan;
import com.alibaba.wasp.plan.Plan;
import com.alibaba.wasp.plan.parser.ParseContext;

/**
 * Util for DruidParser Test
 * 
 */
public class DruidParserTestUtil {

  private static final Log LOG = LogFactory.getLog(DruidParserTestUtil.class);

  public static String[] SEED = {
      "CREATE TABLE User { Required Int64 user_id; Required String name; Optional DOUBLE value; } "
          + " primary key(user_id), "
          + " entity group root,"
          + " entity group key(user_id);",
      "CREATE TABLE Photo { Required Int64 user_id columnfamily cf comment 'aaa'; "
          + " Required Int32 photo_id comment 'child primary key'; "
          + " Required Int64 time;" + " Required String full_url; "
          + " Optional String thumbnail_url;" + " Repeated string tag; } "
          + " primary key(user_id,photo_id), " + " in table User, "
          + " Entity Group Key(user_id) references User; " };

  public static String[] INDEX_SEED = {
      "CREATE index PhotosByTime on Photo(user_id,time);",
      "CREATE index PhotosByTag on Photo(tag);" };

  public static void loadTable(ParseContext context,
      DruidDDLParser druidParser, FMetaServices fmetaServices) {
    loadTable(context, druidParser, fmetaServices, SEED);
  }

  public static void loadTable(ParseContext context,
      DruidDDLParser druidParser, FMetaServices fmetaServices, String[] seed) {
    for (String sql : seed) {
      context.setSql(sql);
      try {
        druidParser.parseSqlToStatement(context);
        druidParser.generatePlan(context);
        Plan plan = context.getPlan();
        if (plan instanceof CreateTablePlan) {
          CreateTablePlan createTable = (CreateTablePlan) plan;
          FTable ftable = createTable.getTable();
          fmetaServices.createTable(ftable);
          TableSchemaCacheReader reader = TableSchemaCacheReader
              .getInstance(fmetaServices.getConf());
          reader.addSchema(ftable.getTableName(), ftable);
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }

  public static boolean execute(ParseContext context, DruidParser druidParser) {
    try {
      druidParser.parseSqlToStatement(context);
      druidParser.generatePlan(context);
      return true;
    } catch (Exception ioe) {
      LOG.error("execute", ioe);
      return false;
    }
  }
}
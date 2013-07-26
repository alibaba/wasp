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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import com.alibaba.wasp.EntityGroupLocation;
import com.alibaba.wasp.NotMatchPrimaryKeyException;
import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.client.FConnection;
import com.alibaba.wasp.client.FConnectionManager;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Field;
import com.alibaba.wasp.meta.RowBuilder;
import com.alibaba.wasp.plan.DeletePlan;
import com.alibaba.wasp.plan.InsertPlan;
import com.alibaba.wasp.plan.UpdatePlan;
import com.alibaba.wasp.plan.action.ColumnStruct;
import com.alibaba.wasp.plan.action.DeleteAction;
import com.alibaba.wasp.plan.action.InsertAction;
import com.alibaba.wasp.plan.action.UpdateAction;
import com.alibaba.wasp.plan.parser.Condition;
import com.alibaba.wasp.plan.parser.ParseContext;
import com.alibaba.wasp.plan.parser.UnsupportedException;
import com.alibaba.wasp.plan.parser.druid.dialect.WaspSqlInsertStatement;
import com.alibaba.wasp.util.ParserUtils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;

/**
 * Use Druid (https://github.com/AlibabaTech/druid) to parse the sql and
 * generate QueryPlan
 * 
 */
public class DruidDMLParser extends DruidParser {
  private static final Log LOG = LogFactory.getLog(DruidDMLParser.class);

  private final FConnection connection;

  /**
   * @param conf
   * @throws ZooKeeperConnectionException
   */
  public DruidDMLParser(Configuration conf) throws ZooKeeperConnectionException {
    this(conf, FConnectionManager.getConnection(conf));
  }

  /**
   * @param conf
   */
  public DruidDMLParser(Configuration conf, FConnection connection) {
    super();
    this.setConf(conf);
    this.connection = connection;
  }

  /**
   * Parse sql and generate QueryPlan SELECT,UPDATE,INSERT,DELETE
   */
  @Override
  public void generatePlan(ParseContext context) throws IOException {
    SQLStatement stmt = context.getStmt();
    MetaEventOperation metaEventOperation = new FMetaEventOperation(
        context.getTsr());
    if (stmt instanceof SQLUpdateStatement) {
      // This is a Update SQL
      getUpdatePlan(context, (SQLUpdateStatement) stmt, metaEventOperation);
    } else if (stmt instanceof SQLInsertStatement) {
      // This is a Insert SQL
      getInsertPlan(context, (SQLInsertStatement) stmt, metaEventOperation);
    } else if (stmt instanceof SQLDeleteStatement) {
      // This is a Delete SQL
      getDeletePlan(context, (SQLDeleteStatement) stmt, metaEventOperation);
    } else {
      throw new UnsupportedException("Unsupported SQLStatement " + stmt);
    }
  }

  /**
   * Process Insert Statement and generate QueryPlan
   * 
   */
  private void getInsertPlan(ParseContext context,
      SQLInsertStatement sqlInsertStatement,
      MetaEventOperation metaEventOperation) throws IOException {

    // Parse The FROM clause
    String fTableName = parseFromClause(sqlInsertStatement.getTableSource());
    LOG.debug("INSERT SQL TableSource " + sqlInsertStatement.getTableSource());
    LOG.debug("From table " + fTableName);
    // check if table exists and get Table info
    FTable table = metaEventOperation.checkAndGetTable(fTableName, false);

    // Parse The columns
    LinkedHashSet<String> insertColumns = parseInsertColumns(sqlInsertStatement
        .getColumns());
    LOG.debug("INSERT SQL Insert columns " + sqlInsertStatement.getColumns());
    // check if table has this columns
    metaEventOperation.checkAndGetFields(table, insertColumns);
    metaEventOperation.checkRequiredFields(table, insertColumns);

    // Before insert a row , it should not be exists, we do not check exists
    // here but check it in execution.
    List<InsertAction> actions = new ArrayList<InsertAction>();
    if (sqlInsertStatement instanceof WaspSqlInsertStatement) {
      // For MySQL, INSERT statements that use VALUES syntax can insert multiple
      // rows. see http://dev.mysql.com/doc/refman/5.5/en/insert.html
      List<ValuesClause> valuesList = ((WaspSqlInsertStatement) sqlInsertStatement)
          .getValuesList();
      LOG.debug("INSERT SQL Insert Values List " + valuesList);
      for (ValuesClause values : valuesList) {
        actions.add(genInsertAction(metaEventOperation, table, insertColumns,
            values));
      }
    } else if (sqlInsertStatement instanceof SQLInsertStatement) {
      // INSERT statements that use VALUES syntax insert one row
      ValuesClause values = sqlInsertStatement.getValues();
      LOG.debug("INSERT SQL Insert Values " + values);
      actions.add(genInsertAction(metaEventOperation, table, insertColumns,
          values));
    }

    if (context.isGenWholePlan()) {
      for (InsertAction insertAction : actions) {
        // Get entityGroupLocation according to entity group key
        EntityGroupLocation entityGroupLocation = this.connection
            .locateEntityGroup(Bytes.toBytes(table.getTableName()),
                insertAction.getValueOfEntityGroupKey());
        insertAction.setEntityGroupLocation(entityGroupLocation);
      }
    }
    InsertPlan insertPlan = new InsertPlan(actions);
    context.setPlan(insertPlan);
    LOG.debug("InsertPlan " + insertPlan.toString());
  }

  private InsertAction genInsertAction(MetaEventOperation metaEventOperation,
      FTable table, LinkedHashSet<String> insertColumns, ValuesClause values)
      throws IOException {
    Pair<List<Pair<String, byte[]>>, List<ColumnStruct>> pair = buildFieldsPair(
        metaEventOperation, table, insertColumns, values);
    byte[] primaryKey = RowBuilder.build().genRowkey(pair.getFirst());
    return new InsertAction(table.getTableName(), pair.getFirst().get(0)
        .getSecond(), primaryKey, pair.getSecond());
  }

  /**
   * Process Delete Statement and generate QueryPlan
   * 
   */
  private void getDeletePlan(ParseContext context,
      SQLDeleteStatement sqlDeleteStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    // DELETE FROM users WHERE id = 123;

    // Parse The FROM clause
    String wtableName = parseFromClause(sqlDeleteStatement.getTableSource());
    LOG.debug("UPDATE SQL From clause " + sqlDeleteStatement.getTableSource());
    // check if table exists and get Table info
    FTable table = metaEventOperation.checkAndGetTable(wtableName, false);

    // Parse The WHERE clause
    SQLExpr where = sqlDeleteStatement.getWhere();
    LOG.debug("UPDATE SQL where " + where);
    LinkedHashMap<String, Condition> eqConditions = new LinkedHashMap<String, Condition>();
    List<Condition> ranges = new ArrayList<Condition>(5);
    ParserUtils.parse(where, eqConditions, ranges);
    if (ranges.size() > 0) {
      throw new UnsupportedException("RANGE is not supported!");
    }

    // check if table has this columns
    metaEventOperation.checkAndGetFields(table, eqConditions.keySet());

    List<Pair<String, byte[]>> primaryKeyPairs = metaEventOperation
        .getPrimaryKeyPairList(table, eqConditions, null);
    if (primaryKeyPairs == null) {
      throw new NotMatchPrimaryKeyException("Not match primary key.");
    }

    byte[] primayKey = RowBuilder.build().genRowkey(primaryKeyPairs);
    DeleteAction action = new DeleteAction(wtableName, primayKey);
    if (context.isGenWholePlan()) {
      Condition entityGroupKeyCondition = ParserUtils.getCondition(table
          .getEntityGroupKey().getName(), eqConditions);
      // Get entityGroupLocation according to entity group key
      EntityGroupLocation entityGroupLocation = this.connection
          .locateEntityGroup(Bytes.toBytes(table.getTableName()), DruidParser
              .convert(table.getColumn(entityGroupKeyCondition.getFieldName()),
                  entityGroupKeyCondition.getValue()));
      action.setEntityGroupLocation(entityGroupLocation);
    }
    List<DeleteAction> actions = new ArrayList<DeleteAction>();
    actions.add(action);
    DeletePlan deletePlan = new DeletePlan(actions);
    context.setPlan(deletePlan);
    LOG.debug("DeletePlan " + deletePlan.toString());
  }

  /**
   * Process UPDATE Statement and generate UpdatePlan
   * 
   */
  private void getUpdatePlan(ParseContext context,
      SQLUpdateStatement sqlUpdateStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    // UPDATE users SET age = 24, name = 'Mike' WHERE id = 123;

    // Parse The FROM clause
    String fTableName = parseFromClause(sqlUpdateStatement.getTableSource());
    LOG.debug("UPDATE SQL From clause " + sqlUpdateStatement.getTableSource());
    // check if table exists and get Table info
    FTable table = metaEventOperation.checkAndGetTable(fTableName, false);

    // Parse The WHERE clause
    SQLExpr where = sqlUpdateStatement.getWhere();
    LOG.debug("UPDATE SQL where " + where);

    LinkedHashMap<String, Condition> conditions = new LinkedHashMap<String, Condition>();
    List<Condition> ranges = new ArrayList<Condition>(5);
    ParserUtils.parse(where, conditions, ranges);
    if (ranges.size() > 0) {
      throw new UnsupportedException(
          "RANGE is not supported by update operation!");
    }

    Set<String> conditionColumns = ParserUtils.getColumns(conditions);
    // check if table has this columns
    metaEventOperation.checkAndGetFields(table, conditionColumns);
    // check if where clause is primary keys
    metaEventOperation.checkIsPrimaryKey(table, conditionColumns);

    List<Pair<String, byte[]>> primaryKeyPairs = metaEventOperation
        .getPrimaryKeyPairList(table, conditions, null);
    if (primaryKeyPairs == null) {
      throw new IOException("Not match primary key.");
    }

    byte[] primayKey = RowBuilder.build().genRowkey(primaryKeyPairs);

    UpdateAction action = new UpdateAction(fTableName, primayKey);
    // Parse Update Item
    List<SQLUpdateSetItem> updateItems = sqlUpdateStatement.getItems();
    for (SQLUpdateSetItem updateItem : updateItems) {
      String columnName = parseColumn(updateItem.getColumn());
      // check this FTable has the column and not pk
      metaEventOperation.checkFieldNotInPrimaryKeys(table, columnName);
      Field field = table.getColumn(columnName);
      byte[] value = convert(field, updateItem.getValue());
      String familyName = metaEventOperation.getColumnFamily(fTableName,
          columnName);
      action.addEntityColumn(fTableName, familyName, columnName,
          field.getType(), value);
    }
    if (context.isGenWholePlan()) {
      Condition entityGroupKeyCondition = ParserUtils.getCondition(table
          .getEntityGroupKey().getName(), conditions);
      // Get entityGroupLocation according to entity group key
      EntityGroupLocation entityGroupLocation = this.connection
          .locateEntityGroup(Bytes.toBytes(table.getTableName()), DruidParser
              .convert(table.getColumn(entityGroupKeyCondition.getFieldName()),
                  entityGroupKeyCondition.getValue()));
      action.setEntityGroupLocation(entityGroupLocation);
    }
    List<UpdateAction> actions = new ArrayList<UpdateAction>();
    actions.add(action);
    UpdatePlan plan = new UpdatePlan(actions);
    context.setPlan(plan);
    LOG.debug("UpdatePlan " + plan);
  }

  private LinkedHashSet<String> parseInsertColumns(List<SQLExpr> columns)
      throws UnsupportedException {
    LinkedHashSet<String> columnsItem = new LinkedHashSet<String>(
        columns.size());
    for (SQLExpr item : columns) {
      String columnName = parseColumn(item);
      LOG.debug(" SQLInsertItem " + columnName);
      columnsItem.add(columnName);
    }
    return columnsItem;
  }

  public Pair<List<Pair<String, byte[]>>, List<ColumnStruct>> buildFieldsPair(
      MetaEventOperation metaEventOperation, FTable table,
      LinkedHashSet<String> columns, ValuesClause values) throws IOException {
    // Convert a row's each field into byte[] value
    List<SQLExpr> exprValues = values.getValues();
    if (exprValues.size() != columns.size()) {
      throw new IOException("Insert clause " + columns.size() + " columns " + " not match "
          + exprValues.size() + " values ");
    }
    Pair<String, byte[]>[] array = new Pair[table.getPrimaryKeys().size()];
    // Construct all ColumnAction
    List<ColumnStruct> cols = new ArrayList<ColumnStruct>(columns.size());
    assert (columns.size() == exprValues.size());
    Iterator<String> iter = columns.iterator();
    int i = 0;
    while (iter.hasNext()) {
      String columnName = iter.next();
      // Get the column's info
      Field column = metaEventOperation.getColumnInfo(table, columnName);
      byte[] value = convert(column, exprValues.get(i));
      Iterator<Entry<String, Field>> pkIter = table.getPrimaryKeys().entrySet()
          .iterator();
      int j = 0;
      while (pkIter.hasNext()) {
        if (pkIter.next().getKey().equalsIgnoreCase(columnName)) {
          array[j] = new Pair<String, byte[]>(columnName, value);
          break;
        }
        j++;
      }
      // Check the input is the same as DataType
      checkType(column, exprValues.get(i));
      ColumnStruct columnAction = new ColumnStruct(table.getTableName(),
          column.getFamily(), columnName, column.getType(), value);
      cols.add(columnAction);
      i++;
    }

    return new Pair<List<Pair<String, byte[]>>, List<ColumnStruct>>(
        Arrays.asList(array), cols);
  }
}
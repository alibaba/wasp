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
package org.apache.wasp.plan.parser.druid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.wasp.EntityGroupLocation;
import org.apache.wasp.FConstants;
import org.apache.wasp.ZooKeeperConnectionException;
import org.apache.wasp.client.FConnection;
import org.apache.wasp.client.FConnectionManager;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.meta.Field;
import org.apache.wasp.meta.Index;
import org.apache.wasp.meta.RowBuilder;
import org.apache.wasp.meta.StorageTableNameBuilder;
import org.apache.wasp.plan.DQLPlan;
import org.apache.wasp.plan.GlobalQueryPlan;
import org.apache.wasp.plan.LocalQueryPlan;
import org.apache.wasp.plan.action.Action;
import org.apache.wasp.plan.action.ColumnStruct;
import org.apache.wasp.plan.action.GetAction;
import org.apache.wasp.plan.action.ScanAction;
import org.apache.wasp.plan.parser.Condition;
import org.apache.wasp.plan.parser.Condition.ConditionType;
import org.apache.wasp.plan.parser.ParseContext;
import org.apache.wasp.plan.parser.QueryInfo;
import org.apache.wasp.plan.parser.UnsupportedException;
import org.apache.wasp.util.ParserUtils;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

/**
 * Use Druid (https://github.com/AlibabaTech/druid) to parse the sql and
 * generate QueryPlan
 * 
 */
public class DruidDQLParser extends DruidParser {

  private static final Log LOG = LogFactory.getLog(DruidDQLParser.class);

  private final FConnection connection;

  /**
   * @param conf
   * @throws ZooKeeperConnectionException
   */
  public DruidDQLParser(Configuration conf) throws ZooKeeperConnectionException {
    this(conf, FConnectionManager.getConnection(conf));
  }

  /**
   * @param conf
   */
  public DruidDQLParser(Configuration conf, FConnection connection) {
    super();
    this.setConf(conf);
    this.connection = connection;
  }

  /**
   * @see org.apache.wasp.plan.parser.Parser#genPlan(org.apache.wasp.plan.parser.ParseContext)
   */
  @Override
  public void generatePlan(ParseContext context) throws IOException {
    SQLStatement stmt = context.getStmt();
    MetaEventOperation metaEventOperation = new FMetaEventOperation(
        context.getTsr());
    if (stmt instanceof SQLSelectStatement) {
      // This is a select SQL
      getSelectPlan(context, (SQLSelectStatement) stmt, metaEventOperation);
    } else {
      throw new UnsupportedException("Unsupported SQLStatement " + stmt);
    }
  }

  /**
   * Process select Statement and generate QueryPlan
   * 
   */
  private void getSelectPlan(ParseContext context,
      SQLSelectStatement sqlSelectStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    SQLSelect select = sqlSelectStatement.getSelect();
    SQLSelectQuery sqlSelectQuery = select.getQuery();
    if (sqlSelectQuery instanceof MySqlSelectQueryBlock) {
      MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) sqlSelectQuery;
      // SELECT
      // FROM
      // WHERE

      // Parse The FROM clause
      String fTableName = parseFromClause(sqlSelectQueryBlock.getFrom());
      LOG.debug("SELECT SQL TableSource " + sqlSelectQueryBlock.getFrom());
      // check if table exists and get Table info(WTable)
      FTable table = metaEventOperation.checkAndGetTable(fTableName, false);

      LinkedHashSet<String> selectItem = null;
      // Parse The SELECT clause
      if (sqlSelectQueryBlock.getSelectList().size() == 1
          && sqlSelectQueryBlock.getSelectList().get(0).getExpr() instanceof SQLAllColumnExpr) {
        // This is SELECT * clause
        selectItem = parseFTable(table);
      } else {
        selectItem = parseSelectClause(sqlSelectQueryBlock.getSelectList());
      }
      LOG.debug("SELECT SQL:Select columns "
          + sqlSelectQueryBlock.getSelectList());
      // check if table has this columns
      metaEventOperation.checkAndGetFields(table, selectItem);

      // Parse The WHERE clause
      SQLExpr where = sqlSelectQueryBlock.getWhere();
      LOG.debug("SELECT SQL:where " + where);
      QueryInfo actionInfo = parseWhereClause(table, metaEventOperation, where);
      LOG.debug("ActionInfo " + actionInfo.toString());

      // Parse The Limit clause
      SQLExpr rowCount = null; 
      if (sqlSelectQueryBlock.getLimit() != null) {
        rowCount = sqlSelectQueryBlock.getLimit().getRowCount();
      }
      int limit = -1;
      if (rowCount != null) {
        limit = convertToInt(rowCount);
      }
      // Convert to QueryPlan
      convertToQueryPlan(table, context, actionInfo, metaEventOperation,
          selectItem, limit);
    } else if (sqlSelectQuery instanceof SQLUnionQuery) {
      throw new UnsupportedException("Union clause Unsupported");
    }
  }

  /**
   * Convert to QueryPlan
   * 
   * @param table
   * @param context
   * @param queryInfo
   * @param metaEventOperation
   * @param selectItem
   * @throws IOException
   */
  private void convertToQueryPlan(FTable table, ParseContext context,
      QueryInfo queryInfo, MetaEventOperation metaEventOperation,
      LinkedHashSet<String> selectItem, int limit) throws IOException {
    // should check the fields in where clause
    List<String> conditionColumns = queryInfo.getAllConditionFieldName();
    // check if table has this columns
    metaEventOperation.checkAndGetFields(table, conditionColumns);

    List<Pair<String, byte[]>> primaryKeyPairs = metaEventOperation
        .getPrimaryKeyPairList(table, queryInfo.getEqConditions(),
            queryInfo.getRangeCondition());

    if ((queryInfo.getEqConditions().size() + (queryInfo.getRangeCondition() != null ? 1
        : 0)) == 0) {
      throw new UnsupportedException("Unsupported null condition.");
    }

    if (primaryKeyPairs == null || primaryKeyPairs.isEmpty()) {
      // conditions do not contain PK.
      queryInfo.setType(QueryInfo.QueryType.SCAN);
    } else {
      queryInfo.setType(QueryInfo.QueryType.GET);
      if (primaryKeyPairs.size() != conditionColumns.size()) {
        throw new UnsupportedException(
            "When you have specified the pk, you'd better not to specify additional filter conditions.");
      }
    }

    Action action = null;
    DQLPlan qp = null;

    Condition entityGroupKeyCondition = queryInfo.getField(table
        .getEntityGroupKey().getName());
    if (queryInfo.getType() == QueryInfo.QueryType.GET) {
      byte[] primaryKey = RowBuilder.build().genRowkey(primaryKeyPairs);
      // check if the column is table's primary key.
      action = new GetAction(context.getReadModel(), table.getTableName(),
          primaryKey, this.buildEntityColumnsForGet(table, metaEventOperation,
              selectItem));
      if (context.isGenWholePlan()) {
        // get entityGroupLocation according to entity group key.
        EntityGroupLocation entityGroupLocation = this.connection
            .locateEntityGroup(Bytes.toBytes(table.getTableName()), DruidParser
                .convert(
                    table.getColumn(entityGroupKeyCondition.getFieldName()),
                    entityGroupKeyCondition.getValue()));
        action.setEntityGroupLocation(entityGroupLocation);
      }
      qp = new LocalQueryPlan((GetAction) action);
      LOG.debug(QueryInfo.QueryType.GET + "  "
          + Bytes.toStringBinary(primaryKey) + " from " + table.getTableName());
    } else if (queryInfo.getType() == QueryInfo.QueryType.SCAN) {
      Index index = metaEventOperation.checkAndGetIndex(table,
          queryInfo.getAllConditionFieldName());

      if (index == null) {
        throw new UnsupportedException("Don't get a Index!");
      }
      Pair<byte[], byte[]> startKeyAndEndKey = metaEventOperation
          .getStartkeyAndEndkey(index, queryInfo);

      Pair<List<ColumnStruct>, List<ColumnStruct>> columnActionPair = this
          .buildEntityColumnsForScan(table, index, metaEventOperation,
              selectItem);
      List<ColumnStruct> selectEntityColumns = columnActionPair.getFirst();
      List<ColumnStruct> selectStoringColumns = columnActionPair.getSecond();

      // instance scan action.
      action = new ScanAction(context.getReadModel(),
          StorageTableNameBuilder.buildIndexTableName(index),
          table.getTableName(), startKeyAndEndKey.getFirst(),
          startKeyAndEndKey.getSecond(), selectEntityColumns);
      ((ScanAction) action).setStoringColumns(selectStoringColumns);
      ((ScanAction) action).setLimit(limit);

      if (entityGroupKeyCondition != null
          && entityGroupKeyCondition.getType() == ConditionType.EQUAL) {
        if (context.isGenWholePlan()) {
          EntityGroupLocation entityGroupLocation = this.connection
              .locateEntityGroup(Bytes.toBytes(table.getTableName()),
                  DruidParser.convert(
                      table.getColumn(entityGroupKeyCondition.getFieldName()),
                      entityGroupKeyCondition.getValue()));
          action.setEntityGroupLocation(entityGroupLocation);
        }
        qp = new LocalQueryPlan((ScanAction) action);
        LOG.debug(QueryInfo.QueryType.SCAN + " startKey "
            + Bytes.toStringBinary(startKeyAndEndKey.getFirst()) + " endKey "
            + Bytes.toStringBinary(startKeyAndEndKey.getSecond()));
      } else {
        qp = new GlobalQueryPlan((ScanAction) action, table);
      }
    }
    qp.setQueryInfo(queryInfo);
    LOG.debug("QueryPlan " + qp);
    context.setPlan(qp);
  }

  /**
   * Build the query field list that get operation requires.
   * 
   */
  private List<ColumnStruct> buildEntityColumnsForGet(FTable table,
      MetaEventOperation metaEventOperation, LinkedHashSet<String> selectItem)
      throws IOException {
    List<ColumnStruct> selectEntityColumns = new ArrayList<ColumnStruct>();
    for (String item : selectItem) {
      // Get the column's familyName
      String familyName = metaEventOperation.getColumnFamily(
          table.getTableName(), item);
      Field field = table.getColumn(item);
      ColumnStruct column = new ColumnStruct(table.getTableName(), familyName,
          item, field.getType());
      selectEntityColumns.add(column);
    }
    return selectEntityColumns;
  }

  /**
   * Separation storing query and entity query.
   * 
   */
  private Pair<List<ColumnStruct>, List<ColumnStruct>> buildEntityColumnsForScan(
      FTable table, Index index, MetaEventOperation metaEventOperation,
      LinkedHashSet<String> selectItem) throws IOException {
    List<ColumnStruct> selectStoringColumns = new ArrayList<ColumnStruct>();
    List<ColumnStruct> selectEntityColumns = new ArrayList<ColumnStruct>();
    String familyName = "";
    for (String item : selectItem) {
      // Get the column's familyName
      familyName = metaEventOperation.getColumnFamily(
          table.getTableName(), item);
      Field field = table.getColumn(item);
      if (index.getStoring().containsKey(item)) {
        ColumnStruct column = new ColumnStruct(table.getTableName(),
            FConstants.INDEX_STORING_FAMILY_STR, item, field.getType());
        selectStoringColumns.add(column);
      } else {
        ColumnStruct column = new ColumnStruct(table.getTableName(),
            familyName, item, field.getType());
        selectEntityColumns.add(column);
      }
    }
    // if not only query storing columns, will be query the column with others.
    if(selectEntityColumns.size() > 0) {
      for (ColumnStruct selectStoringColumn : selectStoringColumns) {
        ColumnStruct column = new ColumnStruct(table.getTableName(),
            familyName, selectStoringColumn.getColumnName(), selectStoringColumn.getDataType());
        selectEntityColumns.add(column);
      }
    }
    return new Pair<List<ColumnStruct>, List<ColumnStruct>>(
        selectEntityColumns, selectStoringColumns);
  }

  /**
   * Parse The WHERE Clause.
   * 
   */
  QueryInfo parseWhereClause(FTable table,
      MetaEventOperation metaEventOperation, SQLExpr where) throws IOException {
    LinkedHashMap<String, Condition> conditions = new LinkedHashMap<String, Condition>();
    List<Condition> ranges = new ArrayList<Condition>(5);
    ParserUtils.parse(where, conditions, ranges);
    return new QueryInfo(null, conditions, ranges);
  }

  /**
   * Parse The SQL SELECT Statement. Get which columns should be return
   * 
   */
  private LinkedHashSet<String> parseSelectClause(List<SQLSelectItem> select)
      throws UnsupportedException {
    LinkedHashSet<String> selectItem = new LinkedHashSet<String>(select.size());
    for (SQLSelectItem item : select) {
      SQLExpr expr = item.getExpr();
      String columnName = parseColumn(expr);
      selectItem.add(columnName);
      LOG.debug(" SQLSelectItem " + columnName);
    }
    return selectItem;
  }

  private LinkedHashSet<String> parseFTable(FTable ftable)
      throws UnsupportedException {
    LinkedHashSet<String> selectItem = new LinkedHashSet<String>(ftable
        .getColumns().size());
    for (Field item : ftable.getColumns().values()) {
      selectItem.add(item.getName());
      LOG.debug(" SQLSelectItem " + item.getName());
    }
    return selectItem;
  }
}

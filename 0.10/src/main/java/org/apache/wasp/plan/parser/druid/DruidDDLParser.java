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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.DataType;
import org.apache.wasp.FConstants;
import org.apache.wasp.FieldKeyWord;
import org.apache.wasp.TableNotFoundException;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.meta.FTable.TableType;
import org.apache.wasp.meta.Field;
import org.apache.wasp.meta.Index;
import org.apache.wasp.meta.TableSchemaCacheReader;
import org.apache.wasp.plan.AlterTablePlan;
import org.apache.wasp.plan.CreateIndexPlan;
import org.apache.wasp.plan.CreateTablePlan;
import org.apache.wasp.plan.DescTablePlan;
import org.apache.wasp.plan.DropIndexPlan;
import org.apache.wasp.plan.DropTablePlan;
import org.apache.wasp.plan.ShowIndexesPlan;
import org.apache.wasp.plan.ShowTablesPlan;
import org.apache.wasp.plan.ShowTablesPlan.ShowTablesType;
import org.apache.wasp.plan.TruncateTablePlan;
import org.apache.wasp.plan.parser.ParseContext;
import org.apache.wasp.plan.parser.UnsupportedException;
import org.apache.wasp.plan.parser.druid.dialect.WaspSqlColumnDefinition;
import org.apache.wasp.plan.parser.druid.dialect.WaspSqlCreateIndexStatement;
import org.apache.wasp.plan.parser.druid.dialect.WaspSqlCreateTableStatement;
import org.apache.wasp.plan.parser.druid.dialect.WaspSqlCreateTableStatement.TableCategory;
import org.apache.wasp.plan.parser.druid.dialect.WaspSqlDescribeStatement;
import org.apache.wasp.plan.parser.druid.dialect.WaspSqlDropTableStatement;
import org.apache.wasp.plan.parser.druid.dialect.WaspSqlPartitionByKey;
import org.apache.wasp.plan.parser.druid.dialect.WaspSqlShowCreateTableStatement;
import org.apache.wasp.plan.parser.druid.dialect.WaspSqlShowIndexesStatement;
import org.apache.wasp.plan.parser.druid.dialect.WaspSqlShowTablesStatement;

import com.alibaba.druid.sql.ast.SQLDataType;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLPartitioningClause;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.ast.statement.SQLTruncateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableAddColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableStatement;

/**
 * Use Druid (https://github.com/AlibabaTech/druid) to parse the sql and
 * generate Execute Plan
 * 
 */
public class DruidDDLParser extends DruidParser {

  private static final Log LOG = LogFactory.getLog(DruidDDLParser.class);

  /**
   * @param conf
   */
  public DruidDDLParser(Configuration conf) {
    super();
    this.setConf(conf);
  }

  /**
   * Parse sql and generate Execute Plan:CREATE,DROP,ALTER
   * 
   */
  @Override
  public void generatePlan(ParseContext context) throws IOException {
    SQLStatement stmt = context.getStmt();
    MetaEventOperation metaEventOperation = new FMetaEventOperation(
        context.getTsr());
    if (stmt instanceof WaspSqlCreateTableStatement) {
      // This is a Create Table SQL
      getCreateTablePlan(context, (WaspSqlCreateTableStatement) stmt,
          metaEventOperation);
    } else if (stmt instanceof WaspSqlCreateIndexStatement) {
      // This is a Create Index SQL
      getCreateIndexPlan(context, (WaspSqlCreateIndexStatement) stmt,
          metaEventOperation);
    } else if (stmt instanceof SQLDropTableStatement) {
      // This is a Drop Table SQL
      getDropTablePlan(context, (SQLDropTableStatement) stmt,
          metaEventOperation);
    } else if (stmt instanceof SQLDropIndexStatement) {
      // This is a Drop Index SQL
      getDropIndexPlan(context, (SQLDropIndexStatement) stmt,
          metaEventOperation);
    } else if (stmt instanceof MySqlAlterTableStatement) {
      // This is a Alter Table SQL
      getAlterTablePlan(context, (MySqlAlterTableStatement) stmt,
          metaEventOperation);
    } else if (stmt instanceof WaspSqlShowTablesStatement) {
      // This is a Show Tables SQL
      getShowTablesPlan(context, (WaspSqlShowTablesStatement) stmt,
          metaEventOperation);
    } else if (stmt instanceof WaspSqlShowCreateTableStatement) {
      // This is a Show Create Table SQL
      getShowCreateTablePlan(context, (WaspSqlShowCreateTableStatement) stmt,
          metaEventOperation);
    } else if (stmt instanceof WaspSqlDescribeStatement) {
      // This is a DESCRIBE SQL
      getDescribePlan(context, (WaspSqlDescribeStatement) stmt,
          metaEventOperation);
    } else if (stmt instanceof WaspSqlShowIndexesStatement) {
      // This is a SHOW INDEXES SQL
      getShowIndexesPlan(context, (WaspSqlShowIndexesStatement) stmt,
          metaEventOperation);
    } else if (stmt instanceof SQLTruncateStatement) {
      // This is a TRUNCATE TABLE SQL
      getTruncatePlan(context, (SQLTruncateStatement) stmt, metaEventOperation);
    } else {
      throw new UnsupportedException("Unsupported SQLStatement " + stmt);
    }
  }

  /**
   * Process TRUNCATE Table Statement and generate Execute Plan
   * 
   */
  private void getTruncatePlan(ParseContext context,
      SQLTruncateStatement sqlTruncateStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    List<SQLExprTableSource> tableSources = sqlTruncateStatement
        .getTableSources();
    List<String> tableNames = new ArrayList<String>();
    for (SQLExprTableSource tableSource : tableSources) {
      String tablename = parseFromClause(tableSource);
      // check if table exists and get Table info
      metaEventOperation.checkAndGetTable(tablename, true);
      tableNames.add(tablename);
    }
    TruncateTablePlan truncateTable = new TruncateTablePlan(tableNames);
    context.setPlan(truncateTable);
    LOG.debug("TruncateTablePlan " + truncateTable.toString());
  }

  /**
   * Process SHOW INDEXES Statement and generate Execute Plan
   * 
   */
  private void getShowIndexesPlan(ParseContext context,
      WaspSqlShowIndexesStatement sqlShowIndexesStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    SQLName object = sqlShowIndexesStatement.getTable();
    String tablename = parseName(object);
    // check if table exists and get Table info
    metaEventOperation.checkAndGetTable(tablename, true);

    ShowIndexesPlan showIndexesTable = new ShowIndexesPlan(tablename);
    context.setPlan(showIndexesTable);
    LOG.debug("ShowIndexesPlan " + showIndexesTable.toString());
  }

  /**
   * Process Describe Table Statement and generate Execute Plan
   * 
   */
  private void getDescribePlan(ParseContext context,
      WaspSqlDescribeStatement sqlDescribeStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    SQLName object = sqlDescribeStatement.getObject();
    String tablename = parseName(object);
    // check if table exists and get Table info
    metaEventOperation.checkAndGetTable(tablename, true);

    DescTablePlan descTable = new DescTablePlan(tablename);
    context.setPlan(descTable);
    LOG.debug("DescTablePlan " + descTable.toString());
  }

  /**
   * Process Show Tables Statement and generate Execute Plan
   * 
   */
  private void getShowTablesPlan(ParseContext context,
      WaspSqlShowTablesStatement sqlShowTablesStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    ShowTablesPlan showTables = null;
    SQLExpr like = sqlShowTablesStatement.getLike();
    SQLExpr where = sqlShowTablesStatement.getWhere();
    if (like == null && where == null) {
      showTables = new ShowTablesPlan(ShowTablesType.ALL);
    } else if (like != null) {
      showTables = new ShowTablesPlan(ShowTablesType.LIKE);
      String likePattern = parseString(like);
      showTables.setLikePattern(likePattern);
    } else if (where != null) {
      throw new UnsupportedException("Show tables where not Unsupported!");
    }
    context.setPlan(showTables);
    LOG.debug("ShowTablesPlan " + showTables.toString());
  }

  /**
   * Process Show Create Table Statement and generate Execute Plan
   * 
   */
  private void getShowCreateTablePlan(ParseContext context,
      WaspSqlShowCreateTableStatement sqlShowCreateTableStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    ShowTablesPlan showTables = new ShowTablesPlan(ShowTablesType.CREATE);
    SQLExpr name = sqlShowCreateTableStatement.getName();
    String tablename = parseName(name);
    // check if table exists and get Table info
    metaEventOperation.checkAndGetTable(tablename, true);

    showTables.setTablename(tablename);
    context.setPlan(showTables);
    LOG.debug("ShowTablesPlan " + showTables.toString());
  }

  /**
   * Process Alter Table Statement and generate Execute Plan
   * 
   */
  private void getAlterTablePlan(ParseContext context,
      MySqlAlterTableStatement sqlAlterTableStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    SQLExprTableSource tableSource = sqlAlterTableStatement.getTableSource();
    String tableName = parseFromClause(tableSource);
    // check if table exists and get Table info
    FTable oldTable = metaEventOperation.checkAndGetTable(tableName, true);

    FTable newTable = FTable.clone(oldTable);

    List<SQLAlterTableItem> items = sqlAlterTableStatement.getItems();
    for (SQLAlterTableItem item : items) {
      if (item instanceof MySqlAlterTableChangeColumn) {
        // Alter Table Change Column
        MySqlAlterTableChangeColumn changeColumn = (MySqlAlterTableChangeColumn) item;
        SQLName columnName = changeColumn.getColumnName();
        LinkedHashMap<String, Field> ftableColumns = newTable.getColumns();
        String oldColumnName = parseName(columnName);
        // Table have this column and this column is not primary key
        metaEventOperation.checkFieldExists(oldTable, oldColumnName);
        metaEventOperation.checkFieldNotInPrimaryKeys(oldTable, oldColumnName);
        // Check column not in a index
        metaEventOperation.checkColumnNotInIndex(oldTable, oldColumnName);

        // Which column(index) to change
        Field field = ftableColumns.get(oldColumnName); // Change this Field
        SQLColumnDefinition newColumnDefinition = changeColumn
            .getNewColumnDefinition();
        if (newColumnDefinition.getDataType() != null) {
          field.setType(parse(newColumnDefinition.getDataType()));
        }
        String newColumnName = parseName(newColumnDefinition.getName());
        if (!oldColumnName.equals(newColumnName)) { // Change column name
          for (Field f : ftableColumns.values()) {
            if (f.getName().equalsIgnoreCase(newColumnName)) {
              throw new UnsupportedException(
                  "Unsupported. Rename one column to a column that already column "
                      + newColumnName);
            }
          }
          field.setName(newColumnName);
        }
      } else if (item instanceof MySqlAlterTableAddColumn) {
        // Alter Table Add Column
        MySqlAlterTableAddColumn addColumn = (MySqlAlterTableAddColumn) item;
        List<SQLColumnDefinition> columns = addColumn.getColumns();
        boolean first = addColumn.isFirst();
        SQLName afterColumn = addColumn.getAfterColumn();
        LinkedHashMap<String, Field> ftableColumns = newTable.getColumns();

        List<Field> addFields = convertColumnDefForAlterTable(columns);
        // check Duplicate column name
        metaEventOperation.areLegalTableColumns(ftableColumns.values(),
            addFields);

        if (first) {
          this.addFieldByPosition(-1, addFields, ftableColumns, newTable);
        } else if (afterColumn != null) {
          int index = getIndex(parseName(afterColumn), ftableColumns);
          this.addFieldByPosition(index, addFields, ftableColumns, newTable);
        } else {
          int index = ftableColumns.size() - 1;
          this.addFieldByPosition(index, addFields, ftableColumns, newTable);
        }
      } else if (item instanceof SQLAlterTableDropColumnItem) {
        // Alter Table Drop Column
        SQLAlterTableDropColumnItem dropColumn = (SQLAlterTableDropColumnItem) item;
        SQLName columnName = dropColumn.getColumnName();
        String cname = parseName(columnName);
        // This column is not primary key
        metaEventOperation.checkFieldNotInPrimaryKeys(oldTable, cname);
        // Check column not in a index, if you want to drop the column you
        // should drop the index first
        metaEventOperation.checkColumnNotInIndex(oldTable, cname);

        LinkedHashMap<String, Field> ftableColumns = newTable.getColumns();
        Field field = ftableColumns.remove(cname);
        if (field == null) {
          throw new UnsupportedException("Unsupported Do not find this column "
              + ((SQLAlterTableDropColumnItem) item).getColumnName());
        }
        newTable.setColumns(ftableColumns);
      } else {
        throw new UnsupportedException(item + " SQLAlterTableItem Unsupported");
      }
    }

    AlterTablePlan alterTable = new AlterTablePlan(oldTable, newTable);
    context.setPlan(alterTable);
    LOG.debug("AlterTablePlan " + alterTable.toString());
  }

  private void addFieldByPosition(int index, List<Field> addFields,
      LinkedHashMap<String, Field> ftableColumns, FTable newTable) {
    LinkedHashMap<String, Field> finalColumns = new LinkedHashMap<String, Field>();
    int i = 0;
    if (index < 0) {
      for (Field addField : addFields) {
        finalColumns.put(addField.getName(), addField);
      }
    }
    for (Field field : ftableColumns.values()) {
      if (index == i) {
        finalColumns.put(field.getName(), field);
        for (Field addField : addFields) {
          finalColumns.put(addField.getName(), addField);
        }
      } else {
        finalColumns.put(field.getName(), field);
      }
      i++;
    }
    newTable.setColumns(finalColumns);
  }

  /**
   * Process Drop Table Statement and generate Execute Plan
   * 
   */
  private void getDropTablePlan(ParseContext context,
      SQLDropTableStatement sqlDropTableStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    List<SQLExprTableSource> tableSources = sqlDropTableStatement
        .getTableSources();
    List<String> tableNames = new ArrayList<String>(tableSources.size());
    boolean ifExists = ((WaspSqlDropTableStatement) sqlDropTableStatement)
        .isIfExists();
    for (SQLExprTableSource tableSource : tableSources) {
      String tableName = parseFromClause(tableSource);
      // check if table exists and get Table info
      try {
        metaEventOperation.checkAndGetTable(tableName, true);
      } catch (TableNotFoundException e) {
        if (!ifExists) {
          throw e;
        } else {
          continue;
        }
      }
      tableNames.add(tableName);
    }

    DropTablePlan dropTable = new DropTablePlan(tableNames);
    context.setPlan(dropTable);
    LOG.debug("DropTablePlan " + dropTable.toString());
  }

  /**
   * Process Create Table Statement and generate Execute Plan
   * 
   */
  private void getCreateTablePlan(ParseContext context,
      WaspSqlCreateTableStatement waspSqlCreateTableStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    /**
     * example String sql3 = "CREATE TABLE User {Required Int64 user_id;
     * Required String name; Optional String phone;} primary key(user_id),ENTITY
     * GROUP ROOT, Entity Group Key(user_id);" ; String sql4 = "CREATE TABLE
     * Photo { Required Int64 user_id columnfamily cf comment 'aaa'; Required
     * Int32 photo_id comment 'child primary key'; Required Int64 time; Required
     * String full_url; Optional String thumbnail_url; Repeated String tag; }
     * primary key(user_id, photo_id) IN TABLE user,ENTITY GROUP KEY(user_id)
     * references User;";
     */

    // Table Info
    SQLExprTableSource tableSource = waspSqlCreateTableStatement
        .getTableSource();
    String tableName = parseFromClause(tableSource);
    // Check Table Name is legal.
    metaEventOperation.isLegalTableName(tableName);
    // Check if the table exists
    metaEventOperation.checkTableNotExists(tableName, true);

    // Table category.
    TableCategory category = waspSqlCreateTableStatement.getCategory();
    TableType tableType = TableType.CHILD;
    if (category != null && category == TableCategory.ROOT) {
      tableType = TableType.ROOT;
    }

    // Primary Key.
    List<SQLExpr> primaryKeysSQLExpr = waspSqlCreateTableStatement
        .getPrimaryKeys();
    // table columns.
    List<SQLTableElement> tableElementList = waspSqlCreateTableStatement
        .getTableElementList(); // columns info
    LinkedHashMap<String, Field> columns = new LinkedHashMap<String, Field>();
    for (SQLTableElement element : tableElementList) {
      Field field = parse(element);
      columns.put(field.getName(), field);
    }
    // Check if columns are legal.
    metaEventOperation.areLegalTableColumns(null, columns.values());
    checkFamilyLegal(columns.values(), metaEventOperation);

    // Primary keys check will be done in this following method
    LinkedHashMap<String, Field> primaryKeys = parse(primaryKeysSQLExpr,
        columns);

    long createTime = System.currentTimeMillis();
    long lastAccessTime = createTime;
    String owner = "me";
    FTable table = new FTable(null, tableName, tableType, owner, createTime,
        lastAccessTime, columns, primaryKeys, primaryKeys.entrySet().iterator()
            .next().getValue());
    SQLExpr entityGroupKeySQLExpr = waspSqlCreateTableStatement
        .getEntityGroupKey();
    Field entityGroupKey = primaryKeys.get(parseName(entityGroupKeySQLExpr));
    if (entityGroupKey == null) {
      throw new UnsupportedException(entityGroupKeySQLExpr
          + " is ForeignKey, but don't in primaryKeys.");
    }
    table.setEntityGroupKey(entityGroupKey);

    if (tableType == TableType.CHILD) {
      String parentName = parseFromClause(waspSqlCreateTableStatement
          .getInTableName());
      table.setParentName(parentName);
      if (!parentName.equals(parseFromClause(waspSqlCreateTableStatement
          .getReferenceTable()))) {
        throw new UnsupportedException(" in table "
            + waspSqlCreateTableStatement.getInTableName()
            + " != references table "
            + waspSqlCreateTableStatement.getReferenceTable());
      }

      // Check parent's EGK equals child's EGK.
      TableSchemaCacheReader reader = TableSchemaCacheReader
          .getInstance(configuration);
      FTable parentTable = reader.getSchema(parentName);
      if (parentTable == null) {
        parentTable = TableSchemaCacheReader.getService(reader.getConf())
            .getTable(tableName);
      }
      if (parentTable == null) {
        throw new TableNotFoundException("Not found parent table:" + parentName);
      }

      if (!parentTable.getEntityGroupKey().getName()
          .equals(table.getEntityGroupKey().getName())) {
        throw new UnsupportedException("Parent" + parentName
            + "'s egk doesn't equals Child" + tableName + "'s egk.");
      }

      // Check child's PKS contains parent's PKS.
      for (Field parentPrimaryKey : parentTable.getPrimaryKeys().values()) {
        boolean found = table.getPrimaryKeys().containsKey(
            parentPrimaryKey.getName());
        if (!found) {
          throw new UnsupportedException(
              "Child's pks must contains parent's pks.");
        }
      }
    }

    SQLPartitioningClause partitioning = waspSqlCreateTableStatement
        .getPartitioning();
    byte[][] splitKeys = null;
    if (partitioning != null) {
      if (table.isRootTable()) {
        if (partitioning instanceof WaspSqlPartitionByKey) {
          WaspSqlPartitionByKey partitionKey = (WaspSqlPartitionByKey) partitioning;
          byte[] start = convert(null, partitionKey.getStart());
          byte[] end = convert(null, partitionKey.getEnd());
          int partitionCount = convertToInt(partitionKey.getPartitionCount());
          splitKeys = Bytes.split(start, end, partitionCount - 3);
        } else {
          throw new UnsupportedException("Unsupported SQLPartitioningClause "
              + partitioning);
        }
      } else {
        throw new UnsupportedException(
            "Partition by only supported for Root Table");
      }
    }
    CreateTablePlan createTable = new CreateTablePlan(table, splitKeys);
    context.setPlan(createTable);
    LOG.debug("CreateTablePlan " + createTable.toString());
  }

  private void checkFamilyLegal(Collection<Field> newColumns,
      MetaEventOperation metaEventOperation) throws IOException {
    for (Field newColumn : newColumns) {
      if (!newColumn.getFamily().equals(FConstants.COLUMNFAMILYNAME_STR)) {
        metaEventOperation.isLegalFamilyName(newColumn.getFamily());
      }
    }
  }

  private LinkedHashMap<String, Field> parse(List<SQLExpr> particularColumns,
      LinkedHashMap<String, Field> columns) throws UnsupportedException {
    LinkedHashMap<String, Field> particularFields = new LinkedHashMap<String, Field>(
        particularColumns.size());
    for (SQLExpr expr : particularColumns) {
      String name = parseName(expr);
      Field field = columns.get(name);
      if (field == null) {
        throw new UnsupportedException(
            "Unsupported table don't have this primaryKey " + expr);
      }
      particularFields.put(name, field);
    }
    return particularFields;
  }

  private Field parse(SQLTableElement tableElement) throws UnsupportedException {
    Field field = null;
    FieldKeyWord keyWord = null;
    if (tableElement instanceof WaspSqlColumnDefinition) {
      WaspSqlColumnDefinition column = (WaspSqlColumnDefinition) tableElement;
      if (column.getColumnConstraint() == FieldKeyWord.REQUIRED) {
        keyWord = FieldKeyWord.REQUIRED;
      } else if (column.getColumnConstraint() == FieldKeyWord.OPTIONAL) {
        keyWord = FieldKeyWord.OPTIONAL;
      } else if (column.getColumnConstraint() == FieldKeyWord.REPEATED) {
        keyWord = FieldKeyWord.REPEATED;
      } else {
        throw new UnsupportedException("Unsupported ColumnConstraint "
            + column.getColumnConstraint());
      }
      SQLName name = column.getName();
      SQLDataType dataType = column.getDataType();
      SQLName columnFamilyName = column.getColumnFamily();
      String columnFamily = FConstants.COLUMNFAMILYNAME_STR;
      if (columnFamilyName != null) {
        columnFamily = parseName(columnFamilyName);
      }
      field = new Field(keyWord, columnFamily, parseName(name),
          parse(dataType), column.getComment());
      return field;
    } else {
      throw new UnsupportedException("Unsupported SQLColumnDefinition "
          + tableElement);
    }
  }

  /**
   * Process Drop Index Statement and generate Execute Plan
   * 
   */
  private void getDropIndexPlan(ParseContext context,
      SQLDropIndexStatement sqlDropIndexStatement,
      MetaEventOperation metaEventOperation) throws IOException {
    SQLExpr indexNameExpr = sqlDropIndexStatement.getIndexName();
    SQLExpr table = sqlDropIndexStatement.getTableName();
    String tableName = parseName(table);

    // check if table exists and get Table info
    FTable ftable = metaEventOperation.checkAndGetTable(tableName, true);

    String indexName = parseName(indexNameExpr);
    metaEventOperation.checkIndexExists(ftable, indexName);

    DropIndexPlan dropIndex = new DropIndexPlan(indexName, tableName);
    context.setPlan(dropIndex);
    LOG.debug("DropIndexPlan " + dropIndex.toString());
  }

  /**
   * Process Create Index Statement and generate Execute Plan
   * 
   */
  private void getCreateIndexPlan(ParseContext context,
      WaspSqlCreateIndexStatement sqlCreateIndexStatement,
      MetaEventOperation metaEventOperation) throws IOException {

    // Index Name
    SQLName name = sqlCreateIndexStatement.getName();
    String indexName = parseName(name);
    metaEventOperation.isLegalIndexName(indexName);
    LOG.debug("Create Index SQL IndexName " + name);

    // Table Name
    SQLName table = sqlCreateIndexStatement.getTable();
    String tableName = parseName(table);
    LOG.debug("Create Index SQL TableName " + table);

    // check if table exists and get Table info
    FTable fTable = metaEventOperation.checkAndGetTable(tableName, true);

    // check the index not exists
    metaEventOperation.checkIndexNotExists(fTable, indexName);

    // Field
    List<SQLSelectOrderByItem> items = sqlCreateIndexStatement.getItems();
    LinkedHashSet<String> columns = new LinkedHashSet<String>(items.size());
    List<String> desc = new ArrayList<String>();
    for (SQLSelectOrderByItem item : items) {
      String columnName = parseName(item.getExpr());
      columns.add(columnName);
      if (item.getType() == SQLOrderingSpecification.DESC) {
        desc.add(columnName);
      }
    }

    if (!metaEventOperation.isLegalDescFields(fTable, desc)) {
      throw new UnsupportedException(
          "Currently we only support the ascending and descending time field.");
    }

    List<String> colList = new ArrayList<String>();
    colList.addAll(columns);
    if (metaEventOperation.arePrimaryKeys(fTable, colList)) {
      throw new UnsupportedException("Index keys is Primary Keys.");
    }

    LinkedHashMap<String, Field> indexKeys = metaEventOperation
        .checkAndGetFields(fTable, columns);
    // Check the indexKeys whether have Duplicate column name
    metaEventOperation.areLegalTableColumns(null, indexKeys.values());

    Index index = new Index(indexName, tableName, indexKeys);
    // Check if two index have the same columns and the same columns order
    metaEventOperation.checkTwoIndexWithSameColumn(fTable, index);

    index.setDesc(desc);
    index.setStoring(parse(sqlCreateIndexStatement.getStoringCols(),
        fTable.getColumns()));
    CreateIndexPlan createIndexPlan = new CreateIndexPlan(index);

    context.setPlan(createIndexPlan);
    LOG.debug("CreateIndexPlan " + createIndexPlan.toString());
  }

  public int getIndex(String name, LinkedHashMap<String, Field> columns)
      throws UnsupportedException {
    Iterator<Entry<String, Field>> iter = columns.entrySet().iterator();
    int i = 0;
    while (iter.hasNext()) {
      Field field = iter.next().getValue();
      if (field.getName().equals(name)) {
        return i;
      }
      i++;
    }
    throw new UnsupportedException(name + " is not in set");
  }

  public static List<Field> convertColumnDefForAlterTable(
      List<SQLColumnDefinition> columns) throws UnsupportedException {
    List<Field> addFields = new ArrayList<Field>();
    for (SQLColumnDefinition sqlColumnDefinition : columns) {
      addFields.add(convertColumnDefForAlterTable(sqlColumnDefinition));
    }
    return addFields;
  }

  private static Field convertColumnDefForAlterTable(SQLColumnDefinition column)
      throws UnsupportedException {
    Field field = new Field();
    if (column instanceof WaspSqlColumnDefinition) {
      WaspSqlColumnDefinition waspColumn = (WaspSqlColumnDefinition) column;
      if (waspColumn.getComment() != null) {
        field.setComment(waspColumn.getComment());
      }
      if (waspColumn.getColumnFamily() != null) {
        field.setFamily(parseName(waspColumn.getColumnFamily()));
      } else {
        field.setFamily(FConstants.COLUMNFAMILYNAME_STR);
      }
      field.setKeyWord(FieldKeyWord.OPTIONAL);
      field.setName(parseName(column.getName()));
      field.setType(parse(column.getDataType()));
    } else {
      field.setComment(column.getComment());
      field.setFamily(FConstants.COLUMNFAMILYNAME_STR);
      field.setKeyWord(FieldKeyWord.OPTIONAL);
      field.setName(parseName(column.getName()));
      field.setType(parse(column.getDataType()));
    }
    return field;
  }

  public static DataType parse(SQLDataType dataType)
      throws UnsupportedException {
    String typeName = dataType.getName();
    if (typeName.equalsIgnoreCase("INT32")) {
      return DataType.INT32;
    } else if (typeName.equalsIgnoreCase("INT64")) {
      return DataType.INT64;
    } else if (typeName.equalsIgnoreCase("STRING")) {
      return DataType.STRING;
    } else if (typeName.equalsIgnoreCase("FLOAT")) {
      return DataType.FLOAT;
    } else if (typeName.equalsIgnoreCase("DOUBLE")) {
      return DataType.DOUBLE;
    } else if (typeName.equalsIgnoreCase("PROTOBUF")) {
      return DataType.PROTOBUF;
    } else if (typeName.equalsIgnoreCase("DATETIME")) {
      return DataType.DATETIME;
    } else {
      throw new UnsupportedException("Unsupported SQLDataType " + dataType);
    }
  }

}

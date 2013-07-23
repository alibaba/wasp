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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.wasp.DataType;
import org.apache.wasp.FieldKeyWord;
import org.apache.wasp.TableExistsException;
import org.apache.wasp.TableNotFoundException;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.meta.Field;
import org.apache.wasp.meta.Index;
import org.apache.wasp.meta.RowBuilder;
import org.apache.wasp.meta.TableSchemaCacheReader;
import org.apache.wasp.plan.parser.Condition;
import org.apache.wasp.plan.parser.QueryInfo;
import org.apache.wasp.plan.parser.UnsupportedException;
import org.apache.wasp.util.ParserUtils;

/**
 * Wasp meta operation.
 * 
 */
public class FMetaEventOperation implements MetaEventOperation {

  private TableSchemaCacheReader reader;

  public FMetaEventOperation(TableSchemaCacheReader reader) {
    this.reader = reader;
  }

  /**
   * Get the table as the specified name if exists, else throw exception
   * 
   * @param tableName
   * @return the table
   * @throws IOException
   */
  @Override
  public FTable checkAndGetTable(String tableName, boolean fetch)
      throws IOException {
    FTable table = reader.getSchema(tableName);
    if (table == null) {
      if (fetch) {
        table = TableSchemaCacheReader.getService(reader.getConf()).getTable(
            tableName);
      }
    }
    if (table == null) {
      throw new TableNotFoundException(tableName + " not exists!");
    }
    return table;
  }

  @Override
  public void checkTableNotExists(String tableName, boolean fetch)
      throws IOException {
    FTable table = reader.getSchema(tableName);
    if (table == null) {
      if (fetch) {
        table = TableSchemaCacheReader.getService(reader.getConf()).getTable(
            tableName);
      }
    }
    if (table != null) {
      throw new TableExistsException(tableName + " is already exists!");
    }
  }

  /**
   * Get fields as the field names under the specified table, throw exception if
   * one does not exist
   * 
   * @param table
   * @param fields
   * @return a list of fields
   * @throws IOException
   */
  @Override
  public LinkedHashMap<String, Field> checkAndGetFields(FTable table,
      Collection<String> fields) throws IOException {
    LinkedHashMap<String, Field> foundColumns = new LinkedHashMap<String, Field>(
        fields.size());
    for (String field : fields) {
      Field foundColumn = table.getColumn(field);
      if (foundColumn == null) {
        throw new UnsupportedException(table.getTableName() + " does not have column '"
            + field + "'!");
      }
      foundColumns.put(foundColumn.getName(), foundColumn);
    }
    return foundColumns;
  }

  /**
   * check whether the given field is primary key, throw exception if not
   * 
   * @param table
   * @param field
   * @throws IOException
   */
  @Override
  public void checkIsPrimaryKey(FTable table, String field) throws IOException {
    LinkedHashMap<String, Field> primaryKeys = table.getPrimaryKeys();
    if (primaryKeys.size() == 1
        && primaryKeys.entrySet().iterator().next().getValue().getName()
            .equals(field)) {
    } else {
      throw new UnsupportedException(field + " is not Primary Key !");
    }
  }

  /**
   * check whether the given fields are primary key, throw exception if not
   * 
   * @param table
   * @param fields
   * @throws IOException
   */
  @Override
  public void checkIsPrimaryKey(FTable table, Set<String> fields)
      throws IOException {
    LinkedHashMap<String, Field> primaryKeys = table.getPrimaryKeys();
    if (primaryKeys.size() != fields.size()) {
      throw new UnsupportedException(fields + " is not Primary Key !");
    }

    for (String fieldName : fields) {
      boolean exists = false;
      if (primaryKeys.containsKey(fieldName)) {
        exists = true;
      }

      if (!exists) {
        throw new UnsupportedException("Does not have primary key '" + fieldName + "'!");
      }
    }
  }

  /**
   * Check whether the given field is exist, throw exception if it is not exist
   * 
   * @param table
   * @param field
   * @throws IOException
   */
  @Override
  public void checkFieldExists(FTable table, String field) throws IOException {
    LinkedHashMap<String, Field> columns = table.getColumns();
    if (!inSet(columns, field)) {
      throw new UnsupportedException("Field '" + field + "' not exists!");
    }
  }

  /**
   * Check whether the given field is not a primary key, throw exception if it
   * is
   * 
   * @param table
   * @param field
   * @throws IOException
   */
  @Override
  public void checkFieldNotInPrimaryKeys(FTable table, String field)
      throws IOException {
    LinkedHashMap<String, Field> primaryKeys = table.getPrimaryKeys();
    if (inSet(primaryKeys, field)) {
      throw new UnsupportedException(field + " is primary key!");
    }
  }

  /**
   * Check whether the given fields are primary keys under the specified table
   * 
   * @param table
   * @param fields
   * @return true if they are primary keys
   * @throws IOException
   */
  @Override
  public boolean arePrimaryKeys(FTable table, List<String> fields) {
    LinkedHashMap<String, Field> primaryKeys = table.getPrimaryKeys();
    if (primaryKeys.size() == fields.size()) {
      for (int i = 0; i < fields.size(); i++) {
        if (!inSet(primaryKeys, fields.get(i))) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }

  public boolean inSet(LinkedHashMap<String, Field> columns, String field) {
    return columns.containsKey(field);
  }

  /**
   * Check whether the given column not belong to a index, if it is, throw
   * Exception, for example alter table change column, the changed column should
   * not be in a index.
   * 
   * @param table
   * @param field
   * @throws IOException
   */
  @Override
  public void checkColumnNotInIndex(FTable table, String field)
      throws IOException {
    LinkedHashMap<String, Index> allIndexs = table.getIndex();
    if (allIndexs != null && allIndexs.size() > 0) {
      for (Index index : allIndexs.values()) {
        if (index.getIndexKeys().containsKey(field)) {
          throw new UnsupportedException("Column " + field + " in Index "
              + index.getIndexName());
        }
      }
    }
  }

  /**
   * Get the index, throw exception if not exist
   * 
   * @param table
   * @param fields
   * @return the index if exists
   * @throws IOException
   */
  @Override
  public Index checkAndGetIndex(FTable table, List<String> fields)
      throws IOException {
    StringBuilder egkb = new StringBuilder();
    StringBuilder negkb = new StringBuilder();
    for (String field : fields) {
      egkb.append(field);
      if (!field.equalsIgnoreCase(table.getEntityGroupKey().getName())) {
        negkb.append(field);
      }
    }

    List<Index> indexs = reader.getIndexsByComposite(table.getTableName(),
        egkb.toString());

    if (indexs == null) {
      indexs = reader.getIndexsByComposite(table.getTableName(),
          negkb.toString());
    }

    // Not supported no-index query!
    if (indexs == null) {
      throw new UnsupportedException("Don't get a Index!");
    } else if (indexs.size() > 1) {
      return optimizationIndex(indexs, fields);
    } else if (indexs.size() == 1) {
      return indexs.get(0);
    } else {
      return null;
    }
  }

  private Index optimizationIndex(List<Index> indexs, List<String> fields) {
    // TODO optimization index.
    for (Index index : indexs) {
      if (index.getIndexKeys().size() == fields.size()) {
        return index;
      }
    }
    return indexs.get(0);
  }

  /**
   * Get the start key and end key of the given query
   * 
   * @param index
   * @param queryInfo
   * @return a pair of start key and end key
   * @throws IOException
   */
  @Override
  public Pair<byte[], byte[]> getStartkeyAndEndkey(Index index,
      QueryInfo queryInfo) throws IOException {
    return RowBuilder.build().buildStartkeyAndEndkey(index, queryInfo);
  }

  /**
   * check whether all required fields(include primary keys) have shown up
   * 
   * @param table
   * @param fields
   * @throws IOException
   */
  @Override
  public void checkRequiredFields(FTable table, LinkedHashSet<String> fields)
      throws IOException {
    LinkedHashMap<String, Field> primaryKeys = table.getPrimaryKeys();

    int count = 0;
    for (String fieldName : fields) {
      if (primaryKeys.containsKey(fieldName)) {
        count++;
      }
    }

    if (count != primaryKeys.size()) {
      throw new UnsupportedException("PrimaryKeys have " + primaryKeys.size()
          + " Fields, but only find " + count + " Fields");
    }

    LinkedHashMap<String, Field> columns = table.getColumns();
    for (Field field : columns.values()) {
      if (field.getKeyWord() == FieldKeyWord.REQUIRED) {
        if (!inSet(fields, field)) {
          throw new UnsupportedException("Field " + field.getName()
              + " is REQUIRED, but don't show up ");
        }
      }
    }
  }

  public boolean inSet(LinkedHashSet<String> columns, Field field) {
    return columns.contains(field.getName());
  }

  /**
   * Get the column's familyName in FTable
   * 
   * @param wtableName
   * @param columnName
   * @return column family name
   * @throws IOException
   */
  @Override
  public String getColumnFamily(String fTableName, String columnName)
      throws IOException {
    FTable table = reader.getSchema(fTableName);
    if (table == null) {
      throw new UnsupportedException(fTableName + " is not exists!");
    }
    Field column = table.getColumn(columnName);
    if (column != null) {
      return column.getFamily();
    }
    throw new UnsupportedException(fTableName + " don't have column '" + columnName
        + "'");
  }

  /**
   * Get the column's Field in FTable
   * 
   * @param ftable
   * @param columnName
   * @return Field
   * @throws IOException
   */
  @Override
  public Field getColumnInfo(FTable fTable, String columnName)
      throws IOException {
    if (fTable == null) {
      throw new UnsupportedException(" Table is not exists!");
    }
    Field column = fTable.getColumn(columnName);
    if (column != null) {
      return column;
    }
    throw new UnsupportedException(fTable.getTableName() + " don't have column '"
        + columnName + "'");
  }

  /**
   * Check whether the index exists, throw exception if not exist
   * 
   * @param table
   * @param indexName
   * @throws IOException
   */
  @Override
  public void checkIndexExists(FTable table, String indexName)
      throws IOException {
    LinkedHashMap<String, Index> allIndexs = table.getIndex();
    if (allIndexs != null && allIndexs.size() > 0) {
      if (!isIndexExists(table, indexName)) {
        throw new UnsupportedException("Table " + table.getTableName()
            + " doesn't have this index '" + indexName + "'");
      }
    } else {
      throw new UnsupportedException("Table " + table.getTableName()
          + " doesn't have any index.");
    }
  }

  /**
   * Check whether the index not exist, throw exception if exist
   * 
   * @param table
   * @param indexName
   * @throws IOException
   */
  @Override
  public void checkIndexNotExists(FTable table, String indexName)
      throws IOException {
    if (isIndexExists(table, indexName)) {
      throw new UnsupportedException("Table " + table.getTableName()
          + " has already a same name index.");
    }
  }

  /**
   * Check whether two indexs have same fields
   * 
   * @param table
   * @param index
   * @throws IOException
   */
  @Override
  public void checkTwoIndexWithSameColumn(FTable table, Index index)
      throws IOException {
    LinkedHashMap<String, Index> allIndexs = table.getIndex();
    if (allIndexs != null && allIndexs.size() > 0) {
      for (Index exist : allIndexs.values()) {
        LinkedHashMap<String, Field> indexKeys = exist.getIndexKeys();
        LinkedHashMap<String, Field> toCheck = index.getIndexKeys();
        if (indexKeys.size() == toCheck.size()) {
          boolean allSame = true;
          Iterator<Entry<String, Field>> iterIndexKeys = indexKeys.entrySet()
              .iterator();
          Iterator<Entry<String, Field>> iterToCheck = toCheck.entrySet()
              .iterator();
          while (iterIndexKeys.hasNext()) {
            if (!iterIndexKeys.next().getValue().getName()
                .equals(iterToCheck.next().getValue().getName())) {
              allSame = false;
            }
          }
          if (allSame) {
            throw new UnsupportedException("Two index: " + index.getIndexName()
                + " and " + exist.getIndexName() + " have the same columns.");
          }
        }
      }
    }
  }

  private boolean isIndexExists(FTable table, String indexName) {
    return table.getIndex().containsKey(indexName);
  }

  /**
   * Check whether the given columns are primary keys
   * 
   * @param table
   * @param columns
   * @throws IOException
   */
  @Override
  public void checkIsPrimaryKeyOrIndex(FTable table, List<String> columns)
      throws IOException {
    if (arePrimaryKeys(table, columns)) {
      return;
    } else {
      Index index = checkAndGetIndex(table, columns);
      if (index == null) {
        throw new UnsupportedException("Not a PrimaryKey or a Index");
      }
    }
  }

  /**
   * Get primary keys
   * 
   * @param table
   * @param condition
   * @return a list of pair of pk and its value
   * @throws IOException
   */
  @Override
  public List<Pair<String, byte[]>> getPrimaryKeyPairList(FTable table,
      LinkedHashMap<String, Condition> conditions, Condition rangeCondition)
      throws IOException {
    List<Pair<String, byte[]>> primaryKeys = new ArrayList<Pair<String, byte[]>>();
    for (Field primaryKey : table.getPrimaryKeys().values()) {
      Condition condition = ParserUtils.getCondition(primaryKey.getName(),
          conditions);
      if (condition != null) {
        primaryKeys.add(new Pair<String, byte[]>(condition.getFieldName(),
            DruidParser.convert(table.getColumn(condition.getFieldName()),
                condition.getValue())));
      } else {
        primaryKeys = null;
        break;
      }
    }
    return primaryKeys;
  }

  /**
   * Check whether the given table name is legal, throw exception if not legal
   * 
   * @param tableName
   * @throws IOException
   */
  @Override
  public void isLegalTableName(String tableName) throws IOException {
    try {
      FTable.isLegalTableName(tableName);
    } catch (Exception e) {
      throw new UnsupportedException("Unsupported TableName " + tableName, e);
    }
  }

  /**
   * Check whether the given index name is legal, throw exception if not
   * 
   * @param indexName
   * @throws IOException
   */
  @Override
  public void isLegalIndexName(String indexName) throws IOException {
    isLegalTableName(indexName);
  }

  /**
   * Check whether the table has duplicate column name after adding the new
   * columns
   * 
   * @param existedColumns
   * @param addColumns
   * @throws IOException
   */
  @Override
  public void areLegalTableColumns(Collection<Field> existedColumns,
      Collection<Field> newColumns) throws IOException {
    HashSet<String> existedColumnNames = new HashSet<String>();
    if (existedColumns != null && !existedColumns.isEmpty()) {
      for (Field existedColumn : existedColumns) {
        existedColumnNames.add(existedColumn.getName());
      }
    }
    for (Field newColumn : newColumns) {
      if (existedColumnNames.contains(newColumn.getName())) {
        throw new UnsupportedException("Duplicate column name "
            + newColumn.getName() + " in the table definition.");
      }
      existedColumnNames.add(newColumn.getName());
    }
  }

  /**
   * check whether the column family name is legal, throw exception if not
   * 
   * @param columnFamily
   * @throws IOException
   */
  @Override
  public void isLegalFamilyName(String columnFamily) throws IOException {
    try {
      HColumnDescriptor.isLegalFamilyName(Bytes.toBytes(columnFamily));
    } catch (Exception e) {
      throw new UnsupportedException("Unsupported ColumnFamily name "
          + columnFamily, e);
    }
  }

  /**
   * Check whether the columns are legal under the given table
   * 
   * @param table
   * @param columns
   * @return true if legal
   */
  @Override
  public boolean isLegalDescFields(FTable table, List<String> columns) {
    for (String columnName : columns) {
      if (table.getColumn(columnName).getType() != DataType.DATETIME) {
        return false;
      }
    }
    return true;
  }
}
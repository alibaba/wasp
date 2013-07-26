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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.wasp.meta.FTable;import com.alibaba.wasp.meta.Index;import org.apache.hadoop.hbase.util.Pair;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Field;
import com.alibaba.wasp.meta.Index;
import com.alibaba.wasp.plan.parser.Condition;
import com.alibaba.wasp.plan.parser.QueryInfo;

/**
 * do some meta related event and check
 */
public interface MetaEventOperation {

  /* Check legality */

  /**
   * Check whether the given table name is legal, throw exception if not
   * 
   * @param tableName
   * @throws IOException
   */
  public void isLegalTableName(String tableName) throws IOException;

  /**
   * Check whether the columns are legal under the given table
   * 
   * @param table
   * @param columns
   * @return true if legal
   */
  public boolean isLegalDescFields(FTable table, List<String> columns);

  /**
   * Check whether the given index name is legal, throw exception if not
   * 
   * @param indexName
   * @throws IOException
   */
  public void isLegalIndexName(String indexName) throws IOException;

  /**
   * Check whether the table has duplicate column name after adding the new
   * columns
   * 
   * @param existedColumns
   * @param addColumns
   * @throws IOException
   */
  public void areLegalTableColumns(Collection<Field> existedColumns,
      Collection<Field> newColumns) throws IOException;

  /**
   * check whether the column family name is legal, throw exception if not
   * 
   * @param columnFamily
   * @throws IOException
   */
  public void isLegalFamilyName(String columnFamily) throws IOException;

  /* Check schema */

  /**
   * Get the table as the specified name if exists, else throw exception
   * 
   * @param tableName
   * @return the table
   * @throws IOException
   */
  public FTable checkAndGetTable(String tableName, boolean fetch)
      throws IOException;

  /**
   * check the table is already exits, if exits, throw exception
   * 
   * @param tableName
   * @throws IOException
   */
  public void checkTableNotExists(String tableName, boolean fetch)
      throws IOException;

  /**
   * check the table has this field
   * 
   * @param tableName
   * @throws IOException
   */
  public void checkFieldExists(FTable table, String field) throws IOException;

  /**
   * Get fields as the field names under the specified table
   * 
   * @param table
   * @param fields
   * @return a list of fields
   * @throws IOException
   */
  public LinkedHashMap<String, Field> checkAndGetFields(FTable table,
      Collection<String> fields) throws IOException;

  /**
   * check whether all required fields(include primary keys) have shown up
   * 
   * @param table
   * @param fields
   * @throws IOException
   */
  public void checkRequiredFields(FTable table, LinkedHashSet<String> fields)
      throws IOException;

  /**
   * check whether the given field is primary key, throw exception if not
   * 
   * @param table
   * @param field
   * @throws IOException
   */
  public void checkIsPrimaryKey(FTable table, String field) throws IOException;

  /**
   * check whether the given fields are primary key, throw exception if not
   * 
   * @param table
   * @param fields
   * @throws IOException
   */
  public void checkIsPrimaryKey(FTable table, Set<String> fields)
      throws IOException;

  /**
   * Check whether the given column not belong to a index, if it is, throw
   * Exception, for example alter table change column, the changed column should
   * not be in a index.
   * 
   * @param table
   * @param field
   * @throws IOException
   */
  public void checkColumnNotInIndex(FTable table, String field)
      throws IOException;

  /**
   * Get primary keys
   * 
   * @param table
   * @param condition
   * @return a list of pair of pk and its value
   * @throws IOException
   */
  public List<Pair<String, byte[]>> getPrimaryKeyPairList(FTable table,
      LinkedHashMap<String, Condition> condition, Condition rangeCondition)
      throws IOException;

  /**
   * Check whether the given fields are primary keys under the specified table
   * 
   * @param table
   * @param fields
   * @return true if they are primary keys
   * @throws IOException
   */
  public boolean arePrimaryKeys(FTable table, List<String> fields)
      throws IOException;

  /**
   * Check whether the given field is not a primary key, throw exception if it
   * is
   * 
   * @param table
   * @param field
   * @throws IOException
   */
  public void checkFieldNotInPrimaryKeys(FTable table, String field)
      throws IOException;

  /**
   * Get the index, throw exception if not exist
   * 
   * @param table
   * @param fields
   * @return the index if exists
   * @throws IOException
   */
  public Index checkAndGetIndex(FTable table, List<String> fields)
      throws IOException;

  /**
   * Check whether the index exists, throw exception if not exist
   * 
   * @param table
   * @param indexName
   * @throws IOException
   */
  public void checkIndexExists(FTable table, String indexName)
      throws IOException;

  /**
   * Check whether the index not exist, throw exception if exist
   * 
   * @param table
   * @param indexName
   * @throws IOException
   */
  public void checkIndexNotExists(FTable table, String indexName)
      throws IOException;

  /**
   * Check whether two indexs have same fields
   * 
   * @param table
   * @param index
   * @throws IOException
   */
  public void checkTwoIndexWithSameColumn(FTable table, Index index)
      throws IOException;

  /**
   * Get the start key and end key of the given query
   * 
   * @param index
   * @param queryInfo
   * @return a pair of start key and end key
   * @throws IOException
   */
  public Pair<byte[], byte[]> getStartkeyAndEndkey(Index index,
      QueryInfo queryInfo) throws IOException;

  /**
   * Get the column's familyName in FTable
   * 
   * @param wtableName
   * @param columnName
   * @return column family name
   * @throws IOException
   */
  public String getColumnFamily(String wtableName, String columnName)
      throws IOException;

  /**
   * Get the column's Field in FTable
   * 
   * @param ftable
   * @param columnName
   * @return field
   * @throws IOException
   */
  public Field getColumnInfo(FTable ftable, String columnName)
      throws IOException;

  /**
   * Check whether the given columns are primary keys
   * 
   * @param table
   * @param columns
   * @throws IOException
   */
  public void checkIsPrimaryKeyOrIndex(FTable table, List<String> columns)
      throws IOException;
}
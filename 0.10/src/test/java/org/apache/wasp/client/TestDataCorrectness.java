/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.wasp.client;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.wasp.DataType;
import org.apache.wasp.FieldKeyWord;
import org.apache.wasp.WaspTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDataCorrectness {
  private static final Log LOG = LogFactory.getLog(TestDataCorrectness.class);

  private final static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  private static FClient client;
  public static final String parent_Table_Name = "TestDataCorrectness";
  public static final String child_Table_Name = "TestDataCorrectness_Child";
  public static String FAMILY = "cf";

  public static String[] parent_columns = { "p0", "p1", "p2", "p3", "p4", "p5" };
  public static String[] child_columns = { "p0", "c1", "c2", "c3", "c4", "c5" };

  public static String[][] parent_indexs = {
      { parent_columns[0], parent_columns[2] },
      { parent_columns[0], parent_columns[3], parent_columns[4] },
      { parent_columns[5] } };

  public static String[][] child_indexs = {
      { child_columns[0], child_columns[2] },
      { child_columns[0], child_columns[3], child_columns[4] },
      { child_columns[5] } };

  public static FieldKeyWord[] field_Keys = { FieldKeyWord.REQUIRED,
      FieldKeyWord.OPTIONAL, FieldKeyWord.OPTIONAL, FieldKeyWord.OPTIONAL,
      FieldKeyWord.OPTIONAL, FieldKeyWord.REPEATED };

  public static DataType[] data_Types = { DataType.INT32, DataType.INT32,
      DataType.INT64, DataType.FLOAT, DataType.DOUBLE, DataType.STRING };

  private final static SecureRandom random = new SecureRandom();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    client = new FClient(TEST_UTIL.getConfiguration());
    creatTable();
    TEST_UTIL.getWaspAdmin().disableTable(child_Table_Name);
    TEST_UTIL.getWaspAdmin().disableTable(parent_Table_Name);
    createIndex();
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(parent_Table_Name.getBytes());
    TEST_UTIL.getWaspAdmin().enableTable(parent_Table_Name);
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(child_Table_Name.getBytes());
    TEST_UTIL.getWaspAdmin().enableTable(child_Table_Name);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Create a parent and child table with given static
   * {@link TestDataCorrectness#parent_columns},
   * {@link TestDataCorrectness#child_columns},
   * {@link TestDataCorrectness#field_Keys},
   * {@link TestDataCorrectness#data_Types}
   * 
   * @throws Exception
   */
  private static void creatTable() throws Exception {
    String createParentTableSql = generateCreateTableSql(null,
        parent_Table_Name, parent_columns, field_Keys, data_Types,
        new String[] { parent_columns[0] }, parent_columns[0]);
    String createChildTableSql = generateCreateTableSql(parent_Table_Name,
        child_Table_Name, child_columns, field_Keys, data_Types, new String[] {
            parent_columns[0], child_columns[0] }, parent_columns[0]);
    LOG.info("Executing sql:" + createParentTableSql);
    client.execute(createParentTableSql);
    TEST_UTIL.waitTableEnabled(Bytes.toBytes(parent_Table_Name), 15 * 1000);
    LOG.info("Executing sql:" + createChildTableSql);
    client.execute(createChildTableSql);
    TEST_UTIL.waitTableEnabled(Bytes.toBytes(child_Table_Name), 15 * 1000);
  }

  /**
   * Create index for parent and child table as
   * {@link TestDataCorrectness#parent_indexs},
   * {@link TestDataCorrectness#child_indexs},
   * 
   * @throws Exception
   */
  private static void createIndex() throws Exception {
    String indexNamePrefix = "index";
    int indexNum = 0;
    for (String[][] indexs : new String[][][] { parent_indexs, child_indexs }) {
      for (String[] index : indexs) {
        String createIndexSql = generateCreateIndexSql(
            ((indexs == parent_indexs) ? parent_Table_Name : child_Table_Name),
            indexNamePrefix + indexNum, index);
        indexNum++;
        LOG.info("Executing sql:" + createIndexSql);
        TEST_UTIL.getWaspAdmin().waitTableNotLocked(
            ((indexs == parent_indexs) ? parent_Table_Name.getBytes()
                : child_Table_Name.getBytes()));
        client.execute(createIndexSql);
      }
    }
  }

  /**
   * Generate a create index sql
   * 
   * @param tableName
   * @param indexName
   * @param indexedColumns
   * @return create index sql
   */
  private static String generateCreateIndexSql(String tableName,
      String indexName, String[] indexedColumns) {
    StringBuffer sql = new StringBuffer("CREATE index " + indexName + " on "
        + tableName + "(");
    for (int i = 0; i < indexedColumns.length; i++) {
      sql.append(indexedColumns[i]);
      if (i < indexedColumns.length - 1) {
        sql.append(",");
      }
    }
    sql.append(")");
    return sql.toString();
  }

  /**
   * Generate a create table sql
   * 
   * @param parentName
   * @param tableName
   * @param columns
   * @param fieldKeys
   * @param dataTypes
   * @param primaryKeys
   * @param egKey
   * @return create table sql
   */
  private static String generateCreateTableSql(String parentName,
      String tableName, String[] columns, FieldKeyWord[] fieldKeys,
      DataType[] dataTypes, String[] primaryKeys, String egKey) {
    assert (columns.length == fieldKeys.length)
        && (fieldKeys.length == dataTypes.length);
    StringBuffer sql = new StringBuffer("CREATE TABLE " + tableName + "{");
    for (int i = 0; i < columns.length; i++) {
      sql.append(fieldKeys[i].toString() + " " + dataTypes[i].toString() + " "
          + columns[i] + ";");
    }
    sql.append("} PRIMARY KEY(");
    for (int i = 0; i < primaryKeys.length; i++) {
      sql.append(primaryKeys[i]);
      if (i < primaryKeys.length - 1) {
        sql.append(",");
      }
    }
    sql.append(")");
    if (parentName != null) {
      sql.append(" ,IN TABLE " + parentName);
    }
    sql.append(",");
    if (parentName == null) {
      sql.append("ENTITY GROUP ROOT,");
    }
    sql.append("ENTITY GROUP KEY(" + egKey + ")");
    if (parentName != null) {
      sql.append(" REFERENCES " + parentName);
    }
    sql.append(";");
    return sql.toString();
  }

  /**
   * Generate insert SQLs as the given row count, columns, filed keys and data
   * types
   * 
   * @param rowNum
   * @param columns
   * @param fieldKeys
   * @param dataTypes
   * @return a pair of a list of insert SQLs and a list of corresponding
   *         QueryResult
   */
  private static Pair<List<String>, List<QueryResult>> generateInsertSQLs(
      String tableName, int rowNum, String[] columns, FieldKeyWord[] fieldKeys,
      DataType[] dataTypes) {
    List<String> insertSqls = new ArrayList<String>();
    List<QueryResult> queryResults = new ArrayList<QueryResult>();
    for (int i = 0; i < rowNum; i++) {
      StringBuffer sql = new StringBuffer("Insert into " + tableName + "(");
      for (int k = 0; k < columns.length; k++) {
        sql.append(columns[k]);
        if (k < columns.length - 1) {
          sql.append(",");
        }
      }
      sql.append(") values(");
      Map<String, Pair<DataType, byte[]>> results = new HashMap<String, Pair<DataType, byte[]>>();
      for (int k = 0; k < columns.length; k++) {
        byte[] value = (k == 0 ? getDataValueAsType(dataTypes[k], i)
            : getRandomValueAsDataType(dataTypes[k]));
        results
            .put(columns[k], new Pair<DataType, byte[]>(dataTypes[k], value));
        sql.append(DataType.valueToStringAsDataType(dataTypes[k], value));
        if (k < columns.length - 1) {
          sql.append(",");
        }
      }
      sql.append(");");
      queryResults.add(new QueryResult(results));
      insertSqls.add(sql.toString());
    }
    return new Pair<List<String>, List<QueryResult>>(insertSqls, queryResults);
  }

  /**
   * Get the byte data of random value with given data type
   * 
   * @param dataType
   * @return the byte data of random value with given data type
   */
  private static byte[] getRandomValueAsDataType(DataType dataType) {
    int maxValue = 5;
    if (dataType == DataType.INT32) {
      return Bytes.toBytes(random.nextInt(maxValue));
    } else if (dataType == DataType.INT64) {
      return Bytes.toBytes(Long.valueOf(random.nextInt(maxValue)));
    } else if (dataType == DataType.FLOAT) {
      return Bytes.toBytes(random.nextFloat());
    } else if (dataType == DataType.DOUBLE) {
      return Bytes.toBytes(random.nextDouble());
    } else if (dataType == DataType.STRING) {
      return Bytes.toBytes(Integer.toString(random.nextInt(maxValue)));
    } else if (dataType == DataType.PROTOBUF) {
      byte[] bytes = new byte[5];
      random.nextBytes(bytes);
      return bytes;
    } else {
      fail("Unsupport data type:" + dataType);
      return null;
    }
  }

  /**
   * Get the byte data of a seq id as given data type
   * 
   * @param dataType
   * @param seqId
   * @return the byte data of given seqId and data type
   */
  private static byte[] getDataValueAsType(DataType dataType, int seqId) {
    if (dataType == DataType.INT32) {
      return Bytes.toBytes(Integer.valueOf(seqId));
    } else if (dataType == DataType.INT64) {
      return Bytes.toBytes(Long.valueOf(seqId));
    } else if (dataType == DataType.FLOAT) {
      return Bytes.toBytes(Float.valueOf(seqId));
    } else if (dataType == DataType.DOUBLE) {
      return Bytes.toBytes(Double.valueOf(seqId));
    } else if (dataType == DataType.STRING) {
      return Bytes.toBytes(String.format("%07d", seqId));
    } else {
      fail("Unsupport data type:" + dataType);
      return null;
    }
  }

  /**
   * Generate random select SQLs as the given QueryResults
   * 
   * @param tableName
   * @param inputResults
   * @param indexs
   * @param percentage
   * @return a pair of list of select SQLs and a list of corresponding
   *         QueryResult
   */
  private static Pair<List<String>, List<QueryResult>> generateSelectSQLsAsResults(
      String tableName, List<QueryResult> inputResults, String[][] indexs,
      int percentage) {
    List<String> selectSqls = new ArrayList<String>();
    List<QueryResult> outputResults = new ArrayList<QueryResult>();
    for (QueryResult queryResult : inputResults) {
      if (random.nextInt(100) > percentage)
        continue;
      outputResults.add(queryResult);
      Map<String, Pair<DataType, byte[]>> columnsInRow = queryResult.getMap();
      StringBuffer sql = new StringBuffer("SELECT * from " + tableName
          + " where ");
      String[] index = indexs[random.nextInt(indexs.length)];
      boolean begin = true;
      for (String indexColumn : index) {
        Pair<DataType, byte[]> columnData = columnsInRow.get(indexColumn);
        assertTrue("Row:" + columnsInRow + " doesn't contain index columns :"
            + indexColumn, columnData != null);
        if (begin) {
          begin = false;
        } else {
          sql.append(" and ");
        }
        sql.append(indexColumn
            + "="
            + DataType.valueToStringAsDataType(columnData.getFirst(),
                columnData.getSecond()));
      }
      sql.append(";");
      selectSqls.add(sql.toString());
    }
    return new Pair<List<String>, List<QueryResult>>(selectSqls, outputResults);
  }

  @Test
  public void testSelectFromParentTableUsingRandomIndex() {
    int insertRowNum = 10;
    Pair<List<String>, List<QueryResult>> insertsAndResults = generateInsertSQLs(
        parent_Table_Name, insertRowNum, parent_columns, field_Keys, data_Types);
    for (String insertSql : insertsAndResults.getFirst()) {
      System.out.println(insertSql);
    }
    for (QueryResult result : insertsAndResults.getSecond()) {
      System.out.println(result);
    }
    String[][] indexs = new String[parent_indexs.length + 1][];
    for (int i = 0; i < parent_indexs.length; i++) {
      indexs[i] = parent_indexs[i];
    }
    indexs[parent_indexs.length] = new String[] { parent_columns[0] };
    Pair<List<String>, List<QueryResult>> selectsAndResults = generateSelectSQLsAsResults(
        parent_Table_Name, insertsAndResults.getSecond(), indexs, 80);
    for (String selectSql : selectsAndResults.getFirst()) {
      System.out.println(selectSql);
    }
    for (QueryResult result : selectsAndResults.getSecond()) {
      System.out.println(result);
    }
  }

  @Test
  public void testSelectFromChildTableUsingRandomIndex() {
    int insertRowNum = 10;
    Pair<List<String>, List<QueryResult>> insertsAndResults = generateInsertSQLs(
        child_Table_Name, insertRowNum, child_columns, field_Keys, data_Types);
    for (String insertSql : insertsAndResults.getFirst()) {
      System.out.println(insertSql);
    }
    for (QueryResult result : insertsAndResults.getSecond()) {
      System.out.println(result);
    }
    String[][] indexs = new String[child_indexs.length + 1][];
    for (int i = 0; i < child_indexs.length; i++) {
      indexs[i] = child_indexs[i];
    }
    indexs[child_indexs.length] = new String[] { child_columns[0] };
    Pair<List<String>, List<QueryResult>> selectsAndResults = generateSelectSQLsAsResults(
        child_Table_Name, insertsAndResults.getSecond(), indexs, 80);
    for (String selectSql : selectsAndResults.getFirst()) {
      System.out.println(selectSql);
    }
    for (QueryResult result : selectsAndResults.getSecond()) {
      System.out.println(result);
    }
  }

//  public static void main(String[] args) {
//    String sql = generateCreateTableSql(parent_Table_Name, child_Table_Name,
//        child_columns, field_Keys, data_Types, new String[] {
//            parent_columns[0], child_columns[0] }, parent_columns[0]);
//    System.out.println();
//  }
}
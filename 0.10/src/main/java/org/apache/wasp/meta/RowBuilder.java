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
package org.apache.wasp.meta;

import com.alibaba.druid.sql.ast.SQLExpr;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.wasp.DataType;
import org.apache.wasp.FConstants;
import org.apache.wasp.MetaException;
import org.apache.wasp.plan.action.ColumnStruct;
import org.apache.wasp.plan.action.InsertAction;
import org.apache.wasp.plan.action.UpdateAction;
import org.apache.wasp.plan.parser.Condition;
import org.apache.wasp.plan.parser.QueryInfo;
import org.apache.wasp.plan.parser.UnsupportedException;
import org.apache.wasp.plan.parser.druid.DruidParser;
import org.apache.wasp.util.ParserUtils;
import org.apache.wasp.util.Utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

public class RowBuilder {

  private final static RowBuilder instance = new RowBuilder();

  private RowBuilder() {
  }

  /** single **/
  public static RowBuilder build() {
    return instance;
  }

  /**
   * Build start key and end key by queryInfo.
   * 
   * @param index
   * @param queryInfo
   * @return
   * @throws UnsupportedException
   */
  public Pair<byte[], byte[]> buildStartkeyAndEndkey(Index index,
      QueryInfo queryInfo) throws UnsupportedException {
    List<IndexField> indexFields = new ArrayList<IndexField>();
    Condition range = queryInfo.getRangeCondition();

    for (Field field : index.getIndexKeys().values()) {
      if (range == null || !field.getName().equals(range.getFieldName())) {
        LinkedHashMap<String, Condition> eqConditions = queryInfo
            .getEqConditions();
        Condition entry = ParserUtils.getCondition(field.getName(),
            eqConditions);
        addToIndexFields(DruidParser.convert(field, entry.getValue()),
            indexFields, field);
      }
    }

    byte[] prefixKey = genRowkey(index, indexFields).getFirst();
    if (range == null) {// no range condition
      return buildStartEndKeyWithoutRange(prefixKey);
    } else {// has range condition
      return buildStartEndKeyWithRange(prefixKey, range, index);
    }
  }

  private Pair<byte[], byte[]> buildStartEndKeyWithRange(byte[] prefixKey,
      Condition rangeCondition, Index index) throws UnsupportedException {
    Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
    boolean isDesc = index.isDesc(rangeCondition.getFieldName());
    SQLExpr left = rangeCondition.getLeft();
    SQLExpr right = rangeCondition.getRight();
    byte[] prefixKeyWithRowSep = prefixKey.length == 0 ? prefixKey : Bytes.add(
        prefixKey, FConstants.DATA_ROW_SEP_STORE);
    try {
      if (left != null) {
        pair.setFirst(Bytes.add(prefixKeyWithRowSep,
            parseByte(index, isDesc, left, rangeCondition.getFieldName())));
      } else {
        pair.setFirst(Bytes.add(prefixKey, FConstants.DATA_ROW_SEP_STORE));
      }
      if (right != null) {
        parseByte(index, isDesc, right, rangeCondition.getFieldName());
        pair.setSecond(Bytes.add(prefixKeyWithRowSep,
            parseByte(index, isDesc, right, rangeCondition.getFieldName())));
      } else {
        pair.setSecond(Bytes.add(prefixKey, FConstants.DATA_ROW_SEP_QUERY));
      }
    } catch (ParseException e) {
      throw new UnsupportedException(e.getMessage(), e);
    }
    // if desc the startkey and stop key will be swap
    if (isDesc) {
      return Pair.newPair(pair.getSecond(), pair.getFirst());
    }
    return pair;
  }

  private byte[] parseByte(Index index, boolean isDesc, SQLExpr range,
      String fieldName) throws UnsupportedException, ParseException {
    LinkedHashMap<String, Field> indexs = index.getIndexKeys();
    Field field = indexs.get(fieldName);
    byte[] value = null;
    if (field.getType() == DataType.DATETIME) {
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.fff");
      long time = formatter.parse(DruidParser.parseString(range)).getTime();
      if (isDesc) {
        time = descLong(time);
      }
      value = Bytes.toBytes(time);
    } else {
      value = DruidParser.convert(field, range);
    }
    return value;
  }

  private Pair<byte[], byte[]> buildStartEndKeyWithoutRange(byte[] prefixKey) {
    return new Pair<byte[], byte[]>(Bytes.add(prefixKey,
        FConstants.DATA_ROW_SEP_STORE), Bytes.add(prefixKey,
        FConstants.DATA_ROW_SEP_QUERY));
  }

  /**
   * 
   * Build a entity row key by sorted primary key conditions.
   * 
   * @param primaryKeyPairs
   * @return
   * @throws UnsupportedException
   */
  public byte[] genRowkey(List<Pair<String, byte[]>> primaryKeyPairs)
      throws UnsupportedException {
    byte[] rowKey = new byte[0];
    Iterator<Pair<String, byte[]>> iter = primaryKeyPairs.iterator();
    boolean first = true;
    while (iter.hasNext()) {
      Pair<String, byte[]> pair = iter.next();
      if (first) {
        rowKey = Bytes.add(rowKey, pair.getSecond());
        first = false;
      } else {
        rowKey = Bytes.add(rowKey, FConstants.DATA_ROW_SEP_STORE,
            pair.getSecond());
      }
    }
    return rowKey;
  }

  /**
   * Build a row key by using one index schema.
   * 
   * @param index
   * @param indexFields
   * @return
   */
  private Pair<byte[], String> genRowkey(Index index,
      List<IndexField> indexFields) {
    byte[] rowKey = new byte[0];
    boolean first = true;
    for (IndexField field : indexFields) {
      if (field.getValue() == null) {
        return null;
      }
      if (index.isDesc(field.getName())) {// is desc?
        field.setValue(descLong(field.getValue()));
      }
      if (first) {
        rowKey = Bytes.add(rowKey, field.getValue());
        first = false;
      } else {
        rowKey = Bytes.add(rowKey, FConstants.DATA_ROW_SEP_STORE,
            field.getValue());
      }
    }
    return new Pair<byte[], String>(rowKey,
        StorageTableNameBuilder.buildIndexTableName(index));
  }

  public byte[] descLong(byte[] value) {
    return Bytes.toBytes(descLong(Bytes.toLong(value)));
  }

  public long descLong(long value) {
    return Long.MAX_VALUE - value;
  }

  /**
   * Convert values to index key/value.
   * 
   * @param index
   * @param result
   * @return
   */
  public Pair<byte[], String> buildIndexKey(Index index,
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> result,
      byte[] primaryKey) {
    List<IndexField> indexFieldLists = getIndexFields(index, result);
    Pair<byte[], String> pair = genRowkey(index, indexFieldLists);
    if (pair == null) {
      return null;
    }
    pair.setFirst(Bytes.add(pair.getFirst(), FConstants.DATA_ROW_SEP_STORE,
        primaryKey));
    return pair;
  }

  /**
   * Build a variety of indexes.
   * 
   * @param index
   * @param values
   * @return
   */
  public static List<IndexField> getIndexFields(Index index,
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> values) {
    List<IndexField> indexFields = new ArrayList<IndexField>();
    for (Field field : index.getIndexKeys().values()) {
      addToIndexFields(values, indexFields, field);
    }
    return indexFields;
  }

  /**
   * @param values
   * @param indexFields
   * @param field
   */
  private static void addToIndexFields(
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> values,
      List<IndexField> indexFields, Field field) {
    String name = field.getName();
    byte[] value = getValue(values, field.getFamily(), name);
    IndexField indexField = new IndexField(field.getFamily(), name, value,
        isNumericDataType(field));
    indexFields.add(indexField);
  }

  /**
   * Tool method.
   * 
   * @param value
   * @param indexFields
   * @param field
   */
  private void addToIndexFields(byte[] value, List<IndexField> indexFields,
      Field field) {
    IndexField indexField = new IndexField(field.getFamily(), field.getName(),
        value, isNumericDataType(field));
    indexFields.add(indexField);
  }

  /**
   * Return current family:column value.
   * 
   * @param results
   * @param family
   * @param name
   * @return current family:column value
   */
  private static byte[] getValue(
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> results,
      String family, String name) {
    if (results == null) {
      return null;
    }
    NavigableMap<byte[], byte[]> values = results.get(Bytes.toBytes(family));
    if (values == null) {
      return null;
    }
    return values.get(Bytes.toBytes(name));
  }

  /**
   * Return true which is INT32 or INT64 or FLOAT or DOUBLE.
   * 
   * @param indexField
   * @return
   */
  static boolean isNumericDataType(Field indexField) {
    return indexField.getType() == DataType.INT32
        || indexField.getType() == DataType.INT64
        || indexField.getType() == DataType.FLOAT
        || indexField.getType() == DataType.DOUBLE;
  }

  /**
   * Convert update action to put.
   * 
   * @param action
   *          UpdateAction
   * @return
   * @throws MetaException
   */
  public Put buildPut(UpdateAction action) throws MetaException {
    return buildPut(
        this.buildEntityRowKey(action.getConf(), action.getFTableName(),
            action.getCombinedPrimaryKey()), action.getColumns());
  }

  /**
   * Convert insert action to put.
   * 
   * @param action
   *          InsertAction
   * @return
   * @throws MetaException
   */
  public Put buildPut(InsertAction action) throws MetaException {
    return buildPut(
        this.buildEntityRowKey(action.getConf(), action.getFTableName(),
            action.getCombinedPrimaryKey()), action.getColumns());
  }

  private Put buildPut(byte[] primayKey, List<ColumnStruct> cols) {
    Put put = new Put(primayKey);
    for (ColumnStruct actionColumn : cols) {
      put.add(Bytes.toBytes(actionColumn.getFamilyName()),
          Bytes.toBytes(actionColumn.getColumnName()), actionColumn.getValue());
    }
    return put;
  }

  public static Set<String> buildFamilyName(FTable ftable) {
    HashSet<String> hs = new HashSet<String>();
    for (Field field : ftable.getColumns().values()) {
      hs.add(field.getFamily());
    }
    hs.add(FConstants.COLUMNFAMILYNAME_STR);
    return hs;
  }

  /**
   * Return entity row key.
   * 
   * @param tableName
   * @param primaryKey
   * @return
   */
  public byte[] buildEntityRowKey(Configuration conf, String tableName,
      byte[] primaryKey) throws MetaException {
    TableSchemaCacheReader reader = TableSchemaCacheReader.getInstance(conf);
    FTable tableDescriptor = reader.getSchema(tableName);
    if (tableDescriptor.isChildTable()) {
      String parentName = tableDescriptor.getParentName();
      return Bytes.add(
          Bytes.toBytes(parentName + FConstants.TABLE_ROW_SEP + tableName
              + FConstants.TABLE_ROW_SEP), primaryKey);
    } else {
      return Bytes.add(Bytes.toBytes(tableName + FConstants.TABLE_ROW_SEP),
          primaryKey);
    }
  }

  /**
   * Convert index key to entity row key.
   * 
   * @param result
   * @return
   */
  public byte[] buildEntityRowKey(Result result) {
    return result.getValue(FConstants.INDEX_STORING_FAMILY_BYTES,
        FConstants.INDEX_STORE_ROW_QUALIFIER);
  }
}
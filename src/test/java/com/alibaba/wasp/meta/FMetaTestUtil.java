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

package com.alibaba.wasp.meta;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import com.alibaba.wasp.DataType;
import com.alibaba.wasp.FieldKeyWord;
import com.alibaba.wasp.meta.FTable.TableType;

public class FMetaTestUtil {

  /**
   * CREATE SCHEMA PhotoApp; CREATE TABLE User { Required Int64 user_id;
   * Required String name; } primary key(user_id),entity group root;
   * 
   * CREATE TABLE Photo{ Required Int64 user_id; Required Int32 photo_id;
   * Required Int64 time; Required String full_url; Optional String
   * thumbnail_url; Repeated string tag; } primary key(user_id,photo_id), in
   * table User, Entity Group Key(user_id) references User;
   * 
   * CREATE local index PhotosByTime on Photo(user_id,time); CREATE global index
   * PhotosByTag on Photo(tag) storing (thumbnail_url);
   */
  public static String CF = "default";
  public static String CF2 = "default2";

  public static FTable User = new FTable(); // Table User
  public static Field user_id = new Field(FieldKeyWord.REQUIRED, CF, "user_id",
      DataType.INT64, "none");
  public static Field name = new Field(FieldKeyWord.REQUIRED, CF, "name",
      DataType.STRING, "none");

  public static FTable Photo = new FTable(); // Table Photo
  public static Field photo_id = new Field(FieldKeyWord.REQUIRED, CF,
      "photo_id", DataType.INT32, "none");
  public static Field time = new Field(FieldKeyWord.REQUIRED, CF, "time",
      DataType.INT64, "none");
  public static Field full_url = new Field(FieldKeyWord.REQUIRED, CF,
      "full_url", DataType.STRING, "none");
  public static Field thumbnail_url = new Field(FieldKeyWord.OPTIONAL, CF,
      "thumbnail_url", DataType.STRING, "none");
  public static Field tag = new Field(FieldKeyWord.REPEATED, CF, "tag",
      DataType.STRING, "none");

  public static Index PhotosByTime = new Index(); // Index PhotosByTime
  public static Index PhotosByTag = new Index(); // Index PhotosByTag

  static {
    // create User table info
    LinkedHashMap<String, Field> columns = new LinkedHashMap<String, Field>();
    columns.put(user_id.getName(), user_id);
    columns.put(name.getName(), name);
    LinkedHashMap<String, Field> primaryKeys = new LinkedHashMap<String, Field>();
    primaryKeys.put(user_id.getName(), user_id);
    User.setTableName("User");
    User.setOwner("default");
    User.setCreateTime(now());
    User.setLastAccessTime(now());
    User.setColumns(columns);
    User.setPrimaryKeys(primaryKeys);
    User.setTableType(TableType.ROOT);
    User.setEntityGroupKey(user_id);

    // create Photo table info
    columns = new LinkedHashMap<String, Field>();
    columns.put(user_id.getName(), user_id);
    columns.put(photo_id.getName(), photo_id);
    columns.put(time.getName(), time);
    columns.put(full_url.getName(), full_url);
    columns.put(thumbnail_url.getName(), thumbnail_url);
    columns.put(tag.getName(), tag);
    primaryKeys = new LinkedHashMap<String, Field>();
    primaryKeys.put(user_id.getName(), user_id);
    primaryKeys.put(photo_id.getName(), photo_id);
    Photo.setTableName("Photo");
    Photo.setOwner("default");
    Photo.setCreateTime(now());
    Photo.setLastAccessTime(now());
    Photo.setColumns(columns);
    Photo.setPrimaryKeys(primaryKeys);
    Photo.setTableType(TableType.CHILD);
    Field foreignKey = user_id;
    Photo.setEntityGroupKey(foreignKey);
    Photo.setParentName("User");

    // Index PhotosByTime info
    PhotosByTime.setIndexName("PhotosByTime");
    PhotosByTime.setDependentTableName("Photo");
    LinkedHashMap<String, Field> indexKeys = new LinkedHashMap<String, Field>();
    indexKeys.put(user_id.getName(), user_id);
    indexKeys.put(time.getName(), time);
    PhotosByTime.setIndexKeys(indexKeys);

    // Index PhotosByTag info
    PhotosByTag.setIndexName("PhotosByTag");
    PhotosByTag.setDependentTableName("Photo");
    indexKeys = new LinkedHashMap<String, Field>();
    indexKeys.put(tag.getName(), tag);
    PhotosByTag.setIndexKeys(indexKeys);
  }

  public static long now() {
    return System.currentTimeMillis();
  }

  public static FTable makeTable(String tableName) {
    List<Field> columns = getColumns();
    LinkedHashMap<String, Field> primaryKeys = new LinkedHashMap<String, Field>();
    primaryKeys.put(columns.get(0).getName(), columns.get(0));
    primaryKeys.put(columns.get(1).getName(), columns.get(1));
    LinkedHashMap<String, Field> finalColumns = new LinkedHashMap<String, Field>();
    for (Field field : columns) {
      finalColumns.put(field.getName(), field);
    }
    return new FTable(null, tableName, TableType.ROOT, finalColumns,
        primaryKeys, columns.get(0));
  }

  public static String[] COLUMNS = { "column1", "column2", "column3", "column4", "column5",
      "column6", "column7", "column8" };

  public static FieldKeyWord[] KEYWORDS = { FieldKeyWord.REQUIRED, FieldKeyWord.REQUIRED,
      FieldKeyWord.REQUIRED, FieldKeyWord.OPTIONAL, FieldKeyWord.OPTIONAL, FieldKeyWord.OPTIONAL,
      FieldKeyWord.REPEATED, FieldKeyWord.OPTIONAL };

  public static DataType[] TYPES = { DataType.INT32, DataType.INT64, DataType.STRING,
      DataType.FLOAT, DataType.DOUBLE, DataType.DATETIME, DataType.PROTOBUF, DataType.STRING };

  public static String[] COMMENTS =
      { "This is column1", "This is column2", "This is column3", "This is column4",
          "This is column5", "This is column6", "This is column7", "This is column8" };

  public static List<Field> getColumns() {
    List<Field> columns = new ArrayList<Field>();
    for (int i = 0; i < COLUMNS.length; i++) {
      Field column = new Field(KEYWORDS[i], CF, COLUMNS[i], TYPES[i],
          COMMENTS[i]);
      columns.add(column);
    }
    return columns;
  }

  public static FTable makeTable(String parentName, String tableName) {
    List<Field> columns = getColumns();
    LinkedHashMap<String, Field> primaryKeys = new LinkedHashMap<String, Field>();
    primaryKeys.put(columns.get(0).getName(), columns.get(0));
    primaryKeys.put(columns.get(1).getName(), columns.get(1));
    primaryKeys.put(columns.get(2).getName(), columns.get(2));
    LinkedHashMap<String, Field> finalColumns = new LinkedHashMap<String, Field>();
    for (Field field : columns) {
      finalColumns.put(field.getName(), field);
    }
    return new FTable(parentName, tableName, TableType.CHILD, finalColumns,
        primaryKeys, columns.get(0));
  }

  public static Index makeIndex(FTable ftable, String indexName,
      List<String> fields) {
    Index index = new Index();
    index.setIndexName(indexName);
    index.setDependentTableName(ftable.getTableName());
    LinkedHashMap<String, Field> indexKeys = new LinkedHashMap<String, Field>();
    for (String field : fields) {
      Field f = getColumn(ftable, field);
      indexKeys.put(f.getName(), f);
    }
    index.setIndexKeys(indexKeys);
    return index;
  }

  public static Field getColumn(FTable ftable, String field) {
    return ftable.getColumn(field);
  }

  public static Field makeColumn(String field, FieldKeyWord keyWord,
      DataType type) {
    Field column = new Field(keyWord, CF, field, type, "Null Comment");
    return column;
  }

  // INT32 OPTIONAL
  public static Field makeIntOptionalColumn(String field) {
    Field column = new Field(FieldKeyWord.OPTIONAL, CF, field, DataType.INT32,
        "Null Comment");
    return column;
  }

  public static FTable makeRootTable(String tableName, List<Field> columns,
      List<Field> primaryKeys) {
    LinkedHashMap<String, Field> cols = new LinkedHashMap<String, Field>();
    for (Field field : columns) {
      cols.put(field.getName(), field);
    }
    LinkedHashMap<String, Field> pk = new LinkedHashMap<String, Field>();
    for (Field field : primaryKeys) {
      pk.put(field.getName(), field);
    }
    return new FTable(null, tableName, TableType.ROOT, cols, pk,
        primaryKeys.get(0));
  }

  public static FTable getTestTable() {
    List<Field> columns = new ArrayList<Field>();
    for (int i = 0; i < 10; i++) {
      columns.add(makeIntOptionalColumn("c" + i));
    }
    List<Field> primaryKeys = new ArrayList<Field>();
    primaryKeys.add(columns.get(0));
    FTable ftable = makeRootTable("test", columns, primaryKeys);
    return ftable;
  }
}
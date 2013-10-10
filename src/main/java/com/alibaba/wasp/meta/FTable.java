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
package com.alibaba.wasp.meta;

import com.alibaba.wasp.DeserializationException;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.generated.MetaProtos.ColumnSchema;
import com.alibaba.wasp.protobuf.generated.MetaProtos.TableSchema;
import com.alibaba.wasp.protobuf.generated.MetaProtos.TableSchema.TableTypeProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Information about a table, Contains Table name, column fields, primary keys,
 * index, entityGroupInfos, etc.
 */
public class FTable {

  private static final byte VERSION = 2;

  private String tableName;

  private String owner;

  private long createTime;

  private long lastAccessTime;

  private LinkedHashMap<String, Field> columns;

  private LinkedHashMap<String, Field> primaryKeys;

  private LinkedHashMap<String, Index> index;

  private Map<String, String> parameters;

  public static enum TableType {
    ROOT,
      CHILD
  }

  /**
   * Megastore tables are either entity group root tables or child tables.
   */
  private TableType tableType;

  private String parentName;

  /**
   * Megastore: Providing Scalable, Highly Available Storage for Interactive
   * Services 3.2 Data Model
   * 
   * Each child table must declare a single distinguished foreign key
   * referencing a root table, illustrated by the ENTITY GROUP KEY annotation
   */
  private Field entityGroupKey;

  public FTable() {
  }

  public FTable(String parentName, String tableName, TableType tableType,
      LinkedHashMap<String, Field> columns,
      LinkedHashMap<String, Field> primaryKeys, Field entityGroupKey) {
    this(parentName, tableName, tableType, "root", columns, primaryKeys,
        entityGroupKey);
  }

  public FTable(String parentName, String tableName, TableType tableType,
      String owner, LinkedHashMap<String, Field> columns,
      LinkedHashMap<String, Field> primaryKeys, Field entityGroupKey) {
    this(parentName, tableName, tableType, owner, System.currentTimeMillis(),
        System.currentTimeMillis(), columns, primaryKeys, entityGroupKey);
  }

  public FTable(String parentName, String tableName, TableType tableType,
      String owner, long createTime, long lastAccessTime,
      LinkedHashMap<String, Field> columns,
      LinkedHashMap<String, Field> primaryKeys, Field entityGroupKey) {
    this.parentName = parentName;
    this.tableName = tableName;
    this.tableType = tableType;
    this.owner = owner;
    this.createTime = createTime;
    this.lastAccessTime = lastAccessTime;
    this.columns = columns;
    this.primaryKeys = primaryKeys;
    this.entityGroupKey = entityGroupKey;
  }

  /** @return the object version number */
  public byte getVersion() {
    return VERSION;
  }

  /**
   * @return the tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName
   *          the tableName to set
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return the columns
   */
  public LinkedHashMap<String, Field> getColumns() {
    return columns;
  }

  /**
   * @param columns
   *          the columns to set
   */
  public void setColumns(LinkedHashMap<String, Field> columns) {
    this.columns = columns;
  }

  /**
   * Return field using by column name.
   * 
   * @param column
   * @return
   */
  public Field getColumn(String column) {
    return this.columns.get(column);
  }

  /**
   * @return the primaryKeys
   */
  public LinkedHashMap<String, Field> getPrimaryKeys() {
    return primaryKeys;
  }

  /**
   * @param primaryKeys
   *          the primaryKeys to set
   */
  public void setPrimaryKeys(LinkedHashMap<String, Field> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  /**
   * @return the index
   */
  public LinkedHashMap<String, Index> getIndex() {
    if (index == null) {
      return new LinkedHashMap<String, Index>(0);
    }
    return index;
  }

  /**
   * @param index
   *          the index to set
   */
  public void setIndex(LinkedHashMap<String, Index> index) {
    this.index = index;
  }

  /**
   * add index to index list.
   * 
   * @param index
   */
  public void addIndex(Index index) {
    if (this.index == null) {
      this.index = new LinkedHashMap<String, Index>();
    }
    this.index.put(index.getIndexName(), index);
  }

  /**
   * remove index from index list.
   * 
   * @param indexName
   */
  public void removeIndex(String indexName) {
    if (index != null) {
      index.remove(indexName);
    }
  }

  public Index getIndex(String indexName) {
    if (index != null) {
      return index.get(indexName);
    }
    return null;
  }

  /**
   * @return the parameters
   */
  public Map<String, String> getParameters() {
    return parameters;
  }

  /**
   * @param parameters
   *          the parameters to set
   */
  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  /**
   * @return the owner
   */
  public String getOwner() {
    return owner;
  }

  /**
   * @param owner
   *          the owner to set
   */
  public void setOwner(String owner) {
    this.owner = owner;
  }

  /**
   * @return the createTime
   */
  public long getCreateTime() {
    return createTime;
  }

  /**
   * @param createTime
   *          the createTime to set
   */
  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  /**
   * @param lastAccessTime
   *          the lastAccessTime to set
   */
  public void setLastAccessTime(long lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
  }

  /**
   * @return the lastAccessTime
   */
  public long getLastAccessTime() {
    return lastAccessTime;
  }

  /**
   * @param tableType
   *          the tableType to set
   */
  public void setTableType(TableType tableType) {
    this.tableType = tableType;
  }

  /**
   * @return the tableType
   */
  public TableType getTableType() {
    return tableType;
  }

  /**
   * @param parentName
   *          the parentName to set
   */
  public void setParentName(String parentName) {
    this.parentName = parentName;
  }

  /**
   * @return the parentName
   */
  public String getParentName() {
    return parentName;
  }

  /**
   * @return true if this table is a ROOT table
   */
  public boolean isRootTable() {
    return tableType == TableType.ROOT;
  }

  /**
   * @return true if this table is a CHILD table
   */
  public boolean isChildTable() {
    return tableType == TableType.CHILD;
  }

  /**
   * @param entityGroupKey
   *          the foreignKey to set
   */
  public void setEntityGroupKey(Field entityGroupKey) {
    if (tableType == null) {
      throw new RuntimeException("TableType can't be NULL!");
    }
    this.entityGroupKey = entityGroupKey;
  }

  /**
   * @return the foreignKey
   */
  public Field getEntityGroupKey() {
    if (tableType == TableType.ROOT) {
      return entityGroupKey;
    } else if (tableType == TableType.CHILD) {
      if (this.parentName == null) {
        throw new NullPointerException("Parent is Null.");
      }
      return entityGroupKey;
    } else {
      throw new RuntimeException("Unkown TableType!");
    }
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FTable)) {
      return false;
    }
    return tableName.equals(((FTable) o).getTableName());
  }

  /**
   * convert to PB object
   * 
   * @throws com.alibaba.wasp.MetaException
   */
  public TableSchema convert() throws MetaException {
    try {
      TableSchema.Builder builder = TableSchema.newBuilder();
      builder.setTableName(this.getTableName());
      builder.setOwner(this.getOwner());
      builder.setCreateTime(this.getCreateTime());
      builder.setLastAccessTime(this.getLastAccessTime());

      Iterator<Entry<String, Field>> iter = this.getColumns().entrySet()
          .iterator();

      int i = 0;
      while (iter.hasNext()) {
        Entry<String, Field> entry = iter.next();
        builder.addColumns(i, entry.getValue().convert());
        i++;
      }

      iter = this.getPrimaryKeys().entrySet().iterator();
      i = 0;
      while (iter.hasNext()) {
        Entry<String, Field> entry = iter.next();
        builder.addPrimaryKeys(i, entry.getValue().convert());
        i++;
      }

      builder.setTableType(TableTypeProtos.valueOf(tableType.name()));
      if (getEntityGroupKey() != null) {
        builder.setForeignKey(this.getEntityGroupKey().convert());
      }
      if (getParentName() != null) {
        builder.setParentName(this.getParentName());
      }
      if (getParameters() != null) {
        builder.addAllParameters(ProtobufUtil
            .toStringStringPairList(getParameters()));
      }
      return builder.build();
    } catch (Throwable e) {
      throw new MetaException(e);
    }
  }

  /**
   * convert PB object to byte[]
   *
   * @throws com.alibaba.wasp.MetaException
   */
  public byte[] toByte() throws MetaException {
    return convert().toByteArray();
  }

  /**
   * convert byte[] to Table Java Object
   *
   * @throws java.io.IOException
   */
  public static FTable convert(byte[] value) throws MetaException {
    if (value == null || value.length <= 0) {
      return null;
    }
    try {
      return parseTableFrom(value);
    } catch (DeserializationException e) {
      throw new MetaException(e);
    }
  }

  /**
   * @param bytes
   *          A pb TableSchema serialized.
   * @return A deserialized {@link FTable}
   * @throws com.alibaba.wasp.DeserializationException
   */
  private static FTable parseTableFrom(final byte[] bytes)
      throws DeserializationException {
    try {
      TableSchema ts = TableSchema.newBuilder()
          .mergeFrom(bytes, 0, bytes.length).build();
      return convert(ts);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
  }

  /**
   * convert PB Object to Table Java Object
   */
  public static FTable convert(TableSchema proto) {
    if (proto == null) {
      return null;
    }
    FTable table = new FTable();
    table.setTableName(proto.getTableName());
    table.setOwner(proto.getOwner());
    table.setCreateTime(proto.getCreateTime());
    table.setLastAccessTime(proto.getLastAccessTime());

    // repeated ColumnSchema columns = 5;
    List<ColumnSchema> columns = proto.getColumnsList();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<String, Field>();
    for (ColumnSchema cs : columns) {
      Field field = Field.convert(cs);
      fields.put(field.getName(), field);
    }
    table.setColumns(fields);

    // repeated ColumnSchema primaryKeys = 6;
    List<ColumnSchema> primaryKeysList = proto.getPrimaryKeysList();
    LinkedHashMap<String, Field> primaryKeys = new LinkedHashMap<String, Field>();
    for (ColumnSchema pk : primaryKeysList) {
      Field field = Field.convert(pk);
      primaryKeys.put(field.getName(), field);
    }
    table.setPrimaryKeys(primaryKeys);

    table.setTableType(TableType.valueOf((proto.getTableType().name())));
    // optional ColumnSchema foreignKey = 8;
    if (proto.hasForeignKey()) {
      table.setEntityGroupKey(Field.convert(proto.getForeignKey()));
    }
    if (proto.hasParentName()) {
      table.setParentName(proto.getParentName());
    }
    table.setParameters(ProtobufUtil.toMap(proto.getParametersList()));

    return table;
  }

  /**
   * Check passed byte buffer, "tableName", is legal user-space table name.
   * 
   * @return Returns passed <code>tableName</code> param
   * @throws NullPointerException
   *           If passed <code>tableName</code> is null
   * @throws IllegalArgumentException
   *           if passed a tableName that is made of other than 'word'
   *           characters or underscores: i.e. <code>[a-zA-Z_0-9].
   */
  public static String isLegalTableName(String tableName) {
    if (tableName == null || tableName.length() == 0) {
      throw new IllegalArgumentException("Name is null or empty");
    }
    byte[] tableNameBytes = Bytes.toBytes(tableName);
    if (tableNameBytes == null || tableNameBytes.length <= 0) {
      throw new IllegalArgumentException("Name is null or empty");
    }
    if (tableNameBytes[0] == '.' || tableNameBytes[0] == '-') {
      throw new IllegalArgumentException("Illegal first character <"
          + tableNameBytes[0]
          + "> at 0. User-space table names can only start with 'word "
          + "characters': i.e. [a-zA-Z_0-9]: " + tableName);
    }
    if (FConstants.CLUSTER_ID_FILE_NAME.equalsIgnoreCase(tableName)
        || FConstants.VERSION_FILE_NAME.equalsIgnoreCase(tableName)) {
      throw new IllegalArgumentException(tableName
          + " conflicted with system reserved words");
    }
    for (int i = 0; i < tableNameBytes.length; i++) {
      if (Character.isLetterOrDigit(tableNameBytes[i])
          || tableNameBytes[i] == '_' || tableNameBytes[i] == '-'
          || tableNameBytes[i] == '.') {
        continue;
      }
      throw new IllegalArgumentException("Illegal character <"
          + tableNameBytes[i] + "> at " + i
          + ". User-space table names can only contain "
          + "'word characters': i.e. [a-zA-Z_0-9-.]: " + tableName);
    }
    return tableName;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ");
    sb.append(tableName);
    sb.append(" { ");
    sb.append("\n");
    for (Field field : columns.values()) {
      sb.append("  ");
      sb.append(field.toString());
      sb.append(";");
      sb.append("\n");
    }
    sb.append("} \n");
    if (isRootTable()) {
      // PRIMARY KEY(user_id), ENTITY GROUP ROOT;
      // PRIMARY KEY(user_id), ENTITY GROUP ROOT, ENTITY GROUP KEY(user_id);
      sb.append("PRIMARY KEY(");
      sb.append(primaryKeys.entrySet().iterator().next().getKey());
      sb.append("),\n");
      sb.append("ENTITY GROUP ROOT,");
      sb.append("\n");
      sb.append("ENTITY GROUP KEY(");
      sb.append(entityGroupKey.getName());
      sb.append(");\n");
    } else {
      sb.append("PRIMARY KEY(");
      int i = 0;
      Iterator<Entry<String, Field>> iterator = primaryKeys.entrySet()
          .iterator();
      while (iterator.hasNext()) {
        Field field = iterator.next().getValue();
        if (i == primaryKeys.size() - 1) {
          sb.append(field.getName());
        } else {
          sb.append(field.getName());
          sb.append(", ");
        }
      }
      // IN TABLE User,
      sb.append("),\n");
      sb.append(" IN TABLE ");
      sb.append(parentName);
      sb.append(",\n");
      // ENTITY GROUP KEY(user_id) REFERENCES User;
      sb.append(" ENTITY GROUP KEY(");
      sb.append(entityGroupKey.getName());
      sb.append(") REFERENCES ");
      sb.append(parentName);
      sb.append(";\n");
    }
    return sb.toString();
  }

  public static FTable clone(FTable table) {
    FTable clone = new FTable();
    clone.setTableName(table.getTableName());
    clone.setOwner(table.getOwner());
    clone.setCreateTime(table.getCreateTime());
    clone.setLastAccessTime(table.getLastAccessTime());
    clone.setColumns(clone(table.getColumns()));
    clone.setPrimaryKeys(clone(table.getPrimaryKeys()));
    clone.setTableType(table.getTableType());
    clone.setEntityGroupKey(Field.clone(table.getEntityGroupKey()));
    clone.setParentName(table.getParentName());
    return clone;
  }

  private static LinkedHashMap<String, Field> clone(
      LinkedHashMap<String, Field> fields) {
    LinkedHashMap<String, Field> clone = new LinkedHashMap<String, Field>();
    Iterator<Entry<String, Field>> iterator = fields.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, Field> entry = iterator.next();
      clone.put(entry.getKey(), Field.clone(entry.getValue()));
    }
    return clone;
  }
}
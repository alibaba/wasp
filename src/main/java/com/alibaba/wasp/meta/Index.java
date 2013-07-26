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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.alibaba.wasp.DeserializationException;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.generated.MetaProtos.ColumnSchema;
import com.alibaba.wasp.protobuf.generated.MetaProtos.IndexSchema;

/**
 * Index schema.
 * 
 */
public class Index {

  private String indexName;

  /** the name of the table which the index dependent on **/
  private String dependentTableName;

  private LinkedHashMap<String, Field> indexKeys;

  private Map<String, Field> storing = new HashMap<String, Field>();

  private Set<String> desc = new HashSet<String>();

  public Index() {
  }

  public Index(String indexName, String dependentTableName,
      LinkedHashMap<String, Field> indexKeys) {
    this.indexName = indexName;
    this.dependentTableName = dependentTableName;
    this.indexKeys = indexKeys;
  }

  private Map<String, String> parameters;

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public LinkedHashMap<String, Field> getIndexKeys() {
    return indexKeys;
  }

  public void setIndexKeys(LinkedHashMap<String, Field> indexKeys) {
    this.indexKeys = indexKeys;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  /**
   * @return the tableName
   */
  public String getDependentTableName() {
    return dependentTableName;
  }

  public boolean isStoringColumn(String column) {
    return storing.containsKey(column);
  }

  /**
   * @return the storing
   */
  public Map<String, Field> getStoring() {
    return storing;
  }

  /**
   * @param storing
   *          the storing to set
   */
  public void setStoring(Map<String, Field> storing) {
    this.storing = storing;
  }

  /**
   * @param storing
   *          the storing to set
   */
  public void setStoring(List<Field> storing) {
    for (Field field : storing) {
      this.storing.put(field.getName(), field);
    }
  }

  /**
   * @return the desc
   */
  public Set<String> getDesc() {
    return desc;
  }

  /**
   * Is this field desc in the index?
   * 
   * @param columnName
   * @return
   */
  public boolean isDesc(String columnName) {
    return desc.contains(columnName);
  }

  /**
   * @param desc
   *          the desc to set
   */
  public void setDesc(List<String> desc) {
    this.desc.clear();
    this.desc.addAll(desc);
  }

  /**
   * @param dependentTableName
   *          the tableName to set
   */
  public void setDependentTableName(String dependentTableName) {
    this.dependentTableName = dependentTableName;
  }

  public IndexSchema convert() {
    IndexSchema.Builder builder = IndexSchema.newBuilder();
    builder.setIndexName(this.getIndexName());
    builder.setTableName(this.dependentTableName);
    LinkedHashMap<String, Field> indexKeys = this.getIndexKeys();
    Iterator<Entry<String, Field>> iterIndexKeys = indexKeys.entrySet()
        .iterator();
    int i = 0;
    while (iterIndexKeys.hasNext()) {
      builder.addIndexKeys(i, iterIndexKeys.next().getValue().convert());
      i++;
    }
    if (this.getParameters() != null) {
      builder.addAllParameters(ProtobufUtil.toStringStringPairList(this
          .getParameters()));
    }
    for (Field field : this.getStoring().values()) {
      builder.addStoringKeys(field.convert());
    }
    for (String desc : this.getDesc()) {
      builder.addDesc(desc);
    }
    return builder.build();
  }

  public byte[] toByte() {
    return convert().toByteArray();
  }

  public static Index convert(byte[] value) {
    if (value == null || value.length <= 0)
      return null;
    try {
      return parseIndexFrom(value);
    } catch (DeserializationException e) {
      return null;
    }
  }

  /**
   * @param bytes
   *          A pb TableSchema serialized with a pb magic prefix.
   * @return A deserialized {@link FTable}
   * @throws DeserializationException
   * @see {@link #toByteArray()}
   */
  public static Index parseIndexFrom(final byte[] bytes)
      throws DeserializationException {
    try {
      IndexSchema is = IndexSchema.newBuilder()
          .mergeFrom(bytes, 0, bytes.length).build();
      return convert(is);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
  }

  public static Index convert(IndexSchema proto) {
    if (proto == null)
      return null;
    Index index = new Index();
    index.setDependentTableName(proto.getTableName());
    index.setIndexName(proto.getIndexName());
    LinkedHashMap<String, Field> indexKeys = new LinkedHashMap<String, Field>();
    for (ColumnSchema ik : proto.getIndexKeysList()) {
      Field field = Field.convert(ik);
      indexKeys.put(field.getName(), field);
    }
    index.setIndexKeys(indexKeys);
    index.setParameters(ProtobufUtil.toMap(proto.getParametersList()));
    HashMap<String, Field> storing = new HashMap<String, Field>();
    for (ColumnSchema sk : proto.getStoringKeysList()) {
      Field field = Field.convert(sk);
      storing.put(field.getName(), field);
    }
    index.setStoring(storing);
    List<String> desc = new ArrayList<String>();
    for (String descName : proto.getDescList()) {
      desc.add(descName);
    }
    index.setDesc(desc);
    return index;
  }
}
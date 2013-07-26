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

import com.alibaba.wasp.DataType;import com.alibaba.wasp.FieldKeyWord;import com.alibaba.wasp.protobuf.generated.MetaProtos;import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.DataType;
import com.alibaba.wasp.FieldKeyWord;
import com.alibaba.wasp.protobuf.generated.MetaProtos.ColumnSchema;

/**
 * Represent a column or a type of a table or object
 */
public class Field {
  private FieldKeyWord keyWord;
  private String family;
  private String name;
  private DataType type;
  private String comment;

  public Field() {
  }

  /**
   * @param comment
   * @param name
   * @param type
   */
  public Field(FieldKeyWord keyWord, String family, String name, DataType type,
      String comment) {
    this.keyWord = keyWord;
    this.family = family;
    this.comment = comment;
    this.name = name;
    this.type = type;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name
   *          the name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the comment
   */
  public String getComment() {
    return comment;
  }

  /**
   * @param comment
   *          the comment to set
   */
  public void setComment(String comment) {
    this.comment = comment;
  }

  /**
   * @return the type
   */
  public DataType getType() {
    return type;
  }

  /**
   * @param type
   *          the type to set
   */
  public void setType(DataType type) {
    this.type = type;
  }

  /**
   * @return the keyWord
   */
  public FieldKeyWord getKeyWord() {
    return keyWord;
  }

  /**
   * @param keyWord
   *          the keyWord to set
   */
  public void setKeyWord(FieldKeyWord keyWord) {
    this.keyWord = keyWord;
  }

  /**
   * @return the family
   */
  public String getFamily() {
    return family;
  }

  /**
   * @param family
   *          the family to set
   */
  public void setFamily(String family) {
    this.family = family;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Field)) {
      return false;
    }
    return name.equals(((Field) o).getName());
  }

  public MetaProtos.ColumnSchema convert() {
    MetaProtos.ColumnSchema.Builder builder = MetaProtos.ColumnSchema.newBuilder();
    builder.setFamily(this.getFamily());
    builder.setName(this.getName());
    builder.setType(DataType.convertDataTypeToDataTypeProtos(this.getType()));
    if (this.getComment() != null) {
      builder.setComment(this.getComment());
    }
    builder.setKeyWord(MetaProtos.ColumnSchema.FieldKeyWord.valueOf(keyWord.getValue()));
    return builder.build();
  }

  public static Field convert(MetaProtos.ColumnSchema col) {
    return new Field(FieldKeyWord.returnValue(col.getKeyWord().name()),
        col.getFamily(), col.getName(),
        DataType.convertDataTypeProtosToDataType(col.getType()),
        col.getComment());
  }

  /**
   * @param b
   *          Family name.
   * @return <code>b</code>
   * @throws IllegalArgumentException
   *           If not null and not a legitimate family name: i.e. 'printable'
   *           and ends in a ':' (Null passes are allowed because <code>b</code>
   *           can be null when deserializing). Cannot start with a '.' either.
   */
  public static byte[] isLegalFamilyName(final byte[] b) {
    if (b == null) {
      return b;
    }
    if (b[0] == '.') {
      throw new IllegalArgumentException("Family names cannot start with a "
          + "period: " + Bytes.toString(b));
    }
    for (int i = 0; i < b.length; i++) {
      if (Character.isISOControl(b[i]) || b[i] == ':') {
        throw new IllegalArgumentException("Illegal character <" + b[i]
            + ">. Family names cannot contain control characters or colons: "
            + Bytes.toString(b));
      }
    }
    return b;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(keyWord);
    sb.append(" ");
    sb.append(type);
    sb.append(" ");
    sb.append(name);
    if (family != null) {
      sb.append(" ");
      sb.append("COLUMNFAMILY");
      sb.append(" ");
      sb.append(family);
    }
    return sb.toString();
  }

  public static Field clone(Field field) {
    if (field == null) {
      return null;
    }
    Field clone = new Field();
    clone.setComment(field.getComment());
    clone.setFamily(field.getFamily());
    clone.setKeyWord(field.getKeyWord());
    clone.setName(field.getName());
    clone.setType(field.getType());
    return clone;
  }

  @Override
  public int hashCode() {
    int result = keyWord != null ? keyWord.hashCode() : 0;
    result = 31 * result + (family != null ? family.hashCode() : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (type != null ? type.hashCode() : 0);
    result = 31 * result + (comment != null ? comment.hashCode() : 0);
    return result;
  }
}

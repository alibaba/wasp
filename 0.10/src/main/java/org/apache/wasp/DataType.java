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
package org.apache.wasp;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.jdbc.JdbcException;
import org.apache.wasp.jdbc.value.Value;
import org.apache.wasp.jdbc.value.ValueBoolean;
import org.apache.wasp.jdbc.value.ValueByte;
import org.apache.wasp.jdbc.value.ValueBytes;
import org.apache.wasp.jdbc.value.ValueDate;
import org.apache.wasp.jdbc.value.ValueDecimal;
import org.apache.wasp.jdbc.value.ValueDouble;
import org.apache.wasp.jdbc.value.ValueFloat;
import org.apache.wasp.jdbc.value.ValueInt;
import org.apache.wasp.jdbc.value.ValueLong;
import org.apache.wasp.jdbc.value.ValueNull;
import org.apache.wasp.jdbc.value.ValueProtobufObject;
import org.apache.wasp.jdbc.value.ValueShort;
import org.apache.wasp.jdbc.value.ValueString;
import org.apache.wasp.jdbc.value.ValueStringFixed;
import org.apache.wasp.jdbc.value.ValueTime;
import org.apache.wasp.jdbc.value.ValueTimestamp;
import org.apache.wasp.plan.parser.UnsupportedException;
import org.apache.wasp.protobuf.generated.MetaProtos.DataTypeProtos;
import org.apache.wasp.session.SessionInterface;
import org.apache.wasp.util.New;

/**
 * Wasp's based data model.
 * 
 */
public enum DataType {
  UNKNOW(-1),
    INT32(1),
    INT64(2),
    STRING(3),
    FLOAT(4),
    DOUBLE(5),
    PROTOBUF(6),
 DATETIME(
      7);

  int type;

  DataType(int type) {
    this.type = type;
  }

  /**
   * The SQL type.
   */
  public int sqlType;

  private static final ArrayList<DataType> TYPES_BY_VALUE_TYPE = New
      .arrayList();

  static {
    TYPES_BY_VALUE_TYPE.addAll(Arrays.asList(DataType.values()));
  }

  public static DataType getDataType(int type) {
    if (type == Value.UNKNOWN) {
      throw JdbcException.get(SQLErrorCode.UNKNOWN_DATA_TYPE_1, "?");
    }
    if (type == DataType.UNKNOW.type) {
      return DataType.UNKNOW;
    }
    DataType dt = TYPES_BY_VALUE_TYPE.get(type);
    if (dt == null) {
      dt = TYPES_BY_VALUE_TYPE.get(Value.NULL);
    }
    return dt;
  }

  public static int convertTypeToSQLType(int type) {
    return getDataType(type).sqlType;
  }

  public static int convertSQLTypeToValueType(int sqlType)
      throws UnsupportedException {
    throw new UnsupportedException("Not implemented.");
  }

  public static Value convertToValue(SessionInterface session, Object x,
      int type) {
    if (x == null) {
      return ValueNull.INSTANCE;
    }
    if (type == Value.JAVA_OBJECT) {
      return ValueProtobufObject.get(x);
    }
    if (x instanceof String) {
      return ValueString.get((String) x);
    } else if (x instanceof Value) {
      return (Value) x;
    } else if (x instanceof Long) {
      return ValueLong.get(((Long) x).longValue());
    } else if (x instanceof Integer) {
      return ValueInt.get(((Integer) x).intValue());
    } else if (x instanceof BigDecimal) {
      return ValueDecimal.get((BigDecimal) x);
    } else if (x instanceof Boolean) {
      return ValueBoolean.get(((Boolean) x).booleanValue());
    } else if (x instanceof Byte) {
      return ValueByte.get(((Byte) x).byteValue());
    } else if (x instanceof Short) {
      return ValueShort.get(((Short) x).shortValue());
    } else if (x instanceof Float) {
      return ValueFloat.get(((Float) x).floatValue());
    } else if (x instanceof Double) {
      return ValueDouble.get(((Double) x).doubleValue());
    } else if (x instanceof byte[]) {
      return ValueBytes.get((byte[]) x);
    } else if (x instanceof Date) {
      return ValueDate.get((Date) x);
    } else if (x instanceof Time) {
      return ValueTime.get((Time) x);
    } else if (x instanceof Timestamp) {
      return ValueTimestamp.get((Timestamp) x);
    } else if (x instanceof java.util.Date) {
      return ValueTimestamp.get(new Timestamp(((java.util.Date) x).getTime()));
    } else if (x instanceof java.io.Reader) {
      return null;
    } else if (x instanceof java.sql.Clob) {
      return null;
    } else if (x instanceof java.io.InputStream) {
      return null;
    } else if (x instanceof java.sql.Blob) {
      return null;
    } else if (x instanceof ResultSet) {
      return null;
    } else if (x instanceof UUID) {
      return null;
    } else if (x instanceof Object[]) {
      return null;
    } else if (x instanceof Character) {
      return ValueStringFixed.get(((Character) x).toString());
    } else {
      return ValueProtobufObject.get(x);
    }
  }

  public static DataTypeProtos convertDataTypeToDataTypeProtos(DataType type) {
    switch (type) {
    case INT32:
      return DataTypeProtos.TYPE_INT32;
    case INT64:
      return DataTypeProtos.TYPE_INT64;
    case STRING:
      return DataTypeProtos.TYPE_STRING;
    case FLOAT:
      return DataTypeProtos.TYPE_FLOAT;
    case DOUBLE:
      return DataTypeProtos.TYPE_DOUBLE;
    case PROTOBUF:
      return DataTypeProtos.TYPE_PROTOBUF;
    case DATETIME:
      return DataTypeProtos.TYPE_DATETIME;
    default:
      return null;
    }
  }

  public static DataType convertDataTypeProtosToDataType(DataTypeProtos type) {
    switch (type) {
    case TYPE_INT32:
      return DataType.INT32;
    case TYPE_INT64:
      return DataType.INT64;
    case TYPE_STRING:
      return DataType.STRING;
    case TYPE_FLOAT:
      return DataType.FLOAT;
    case TYPE_DOUBLE:
      return DOUBLE;
    case TYPE_PROTOBUF:
      return DataType.PROTOBUF;
    case TYPE_DATETIME:
      return DataType.DATETIME;
    }
    return null;
  }

  /**
   * Get the string of given value as its data type
   * 
   * @param dataType
   * @param value
   * @return a string of value
   */
  public static String valueToStringAsDataType(DataType dataType, byte[] value) {
    if (dataType == DataType.INT32) {
      return Integer.toString(Bytes.toInt(value));
    } else if (dataType == DataType.INT64) {
      return Long.toString(Bytes.toLong(value));
    } else if (dataType == DataType.FLOAT) {
      return Float.toString(Bytes.toFloat(value));
    } else if (dataType == DataType.DOUBLE) {
      return Double.toString(Bytes.toDouble(value));
    } else if (dataType == DataType.STRING) {
      return "'" + Bytes.toString(value) + "'";
    } else if (dataType == DataType.PROTOBUF) {
      return "'" + Bytes.toString(value) + "'";
    } else {
      throw new RuntimeException("Unsupport data type:" + dataType);
    }
  }
}
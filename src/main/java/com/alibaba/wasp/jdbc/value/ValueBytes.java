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
package com.alibaba.wasp.jdbc.value;

import com.alibaba.wasp.util.MathUtils;import com.alibaba.wasp.util.Utils;import com.alibaba.wasp.util.MathUtils;
import com.alibaba.wasp.util.StringUtils;
import com.alibaba.wasp.util.Utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Implementation of the BINARY data type. It is also the base class for
 * ValueJavaObject.
 */
public class ValueBytes extends Value {

  private static final ValueBytes EMPTY = new ValueBytes(Utils.EMPTY_BYTES);

  /**
   * The value.
   */
  protected byte[] value;

  /**
   * The hash code.
   */
  protected int hash;

  protected ValueBytes(byte[] v) {
    this.value = v;
  }

  /**
   * Get or create a bytes value for the given byte array. Clone the data.
   * 
   * @param b
   *          the byte array
   * @return the value
   */
  public static ValueBytes get(byte[] b) {
    if (b.length == 0) {
      return EMPTY;
    }
    b = Utils.cloneByteArray(b);
    return getNoCopy(b);
  }

  /**
   * Get or create a bytes value for the given byte array. Do not clone the
   * date.
   * 
   * @param b
   *          the byte array
   * @return the value
   */
  public static ValueBytes getNoCopy(byte[] b) {
    if (b.length == 0) {
      return EMPTY;
    }
    ValueBytes obj = new ValueBytes(b);
    return obj;
  }

  public int getType() {
    return Value.BYTES;
  }

  public String getSQL() {
    return "X'" + StringUtils.convertBytesToHex(getBytesNoCopy()) + "'";
  }

  public byte[] getBytesNoCopy() {
    return value;
  }

  public byte[] getBytes() {
    return Utils.cloneByteArray(getBytesNoCopy());
  }

  protected int compareSecure(Value v, CompareMode mode) {
    byte[] v2 = ((ValueBytes) v).value;
    return Utils.compareNotNull(value, v2);
  }

  public String getString() {
    return StringUtils.convertBytesToHex(value);
  }

  public long getPrecision() {
    return value.length;
  }

  public int hashCode() {
    if (hash == 0) {
      hash = Utils.getByteArrayHash(value);
    }
    return hash;
  }

  public Object getObject() {
    return getBytes();
  }

  public void set(PreparedStatement prep, int parameterIndex)
      throws SQLException {
    prep.setBytes(parameterIndex, value);
  }

  public int getDisplaySize() {
    return MathUtils.convertLongToInt(value.length * 2L);
  }

  public int getMemory() {
    return value.length + 24;
  }

  public boolean equals(Object other) {
    return other instanceof ValueBytes
        && Utils.compareNotNull(value, ((ValueBytes) other).value) == 0;
  }

  public Value convertPrecision(long precision, boolean force) {
    if (value.length <= precision) {
      return this;
    }
    int len = MathUtils.convertLongToInt(precision);
    byte[] buff = new byte[len];
    System.arraycopy(value, 0, buff, 0, len);
    return get(buff);
  }

}

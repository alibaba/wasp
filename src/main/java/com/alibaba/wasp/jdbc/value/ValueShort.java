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

import com.alibaba.wasp.SQLErrorCode;import com.alibaba.wasp.jdbc.JdbcException;import com.alibaba.wasp.util.MathUtils;import com.alibaba.wasp.SQLErrorCode;
import com.alibaba.wasp.jdbc.JdbcException;
import com.alibaba.wasp.util.MathUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Implementation of the SMALLINT data type.
 */
public class ValueShort extends Value {

  /**
   * The precision in digits.
   */
  static final int PRECISION = 5;

  /**
   * The maximum display size of a short. Example: -32768
   */
  static final int DISPLAY_SIZE = 6;

  private final short value;

  public static final ValueShort ZERO = new ValueShort((short)0);

  private ValueShort(short value) {
    this.value = value;
  }

  public Value add(Value v) {
    ValueShort other = (ValueShort) v;
    return checkRange(value + other.value);
  }

  private static ValueShort checkRange(int x) {
    if (x < Short.MIN_VALUE || x > Short.MAX_VALUE) {
      throw JdbcException.get(SQLErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1,
          Integer.toString(x));
    }
    return ValueShort.get((short) x);
  }

  public int getSignum() {
    return Integer.signum(value);
  }

  public Value negate() {
    return checkRange(-(int) value);
  }

  public Value subtract(Value v) {
    ValueShort other = (ValueShort) v;
    return checkRange(value - other.value);
  }

  public Value multiply(Value v) {
    ValueShort other = (ValueShort) v;
    return checkRange(value * other.value);
  }

  public Value divide(Value v) {
    ValueShort other = (ValueShort) v;
    if (other.value == 0) {
      throw JdbcException.get(SQLErrorCode.DIVISION_BY_ZERO_1, getSQL());
    }
    return ValueShort.get((short) (value / other.value));
  }

  public Value modulus(Value v) {
    ValueShort other = (ValueShort) v;
    if (other.value == 0) {
      throw JdbcException.get(SQLErrorCode.DIVISION_BY_ZERO_1, getSQL());
    }
    return ValueShort.get((short) (value % other.value));
  }

  public String getSQL() {
    return getString();
  }

  public int getType() {
    return Value.SHORT;
  }

  public short getShort() {
    return value;
  }

  protected int compareSecure(Value o, CompareMode mode) {
    ValueShort v = (ValueShort) o;
    return MathUtils.compareInt(value, v.value);
  }

  public String getString() {
    return String.valueOf(value);
  }

  public long getPrecision() {
    return PRECISION;
  }

  public int hashCode() {
    return value;
  }

  public Object getObject() {
    return Short.valueOf(value);
  }

  public void set(PreparedStatement prep, int parameterIndex)
      throws SQLException {
    prep.setShort(parameterIndex, value);
  }

  /**
   * Get or create a short value for the given short.
   * 
   * @param i
   *          the short
   * @return the value
   */
  public static ValueShort get(short i) {
    return new ValueShort(i);
  }

  public int getDisplaySize() {
    return DISPLAY_SIZE;
  }

  public boolean equals(Object other) {
    return other instanceof ValueShort && value == ((ValueShort) other).value;
  }

}

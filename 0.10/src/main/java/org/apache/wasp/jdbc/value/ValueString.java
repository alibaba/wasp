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
package org.apache.wasp.jdbc.value;

import org.apache.wasp.util.MathUtils;
import org.apache.wasp.util.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Implementation of the VARCHAR data type. It is also the base class for other
 * ValueString* classes.
 */
public class ValueString extends Value {

  private static final ValueString EMPTY = new ValueString("");

  /**
   * The string data.
   */
  protected final String value;

  protected ValueString(String value) {
    this.value = value;
  }

  public String getSQL() {
    return StringUtils.quoteStringSQL(value);
  }

  public boolean equals(Object other) {
    return other instanceof ValueString
        && value.equals(((ValueString) other).value);
  }

  protected int compareSecure(Value o, CompareMode mode) {
    // compatibility: the other object could be another type
    ValueString v = (ValueString) o;
    return mode.compareString(value, v.value, false);
  }

  public String getString() {
    return value;
  }

  public long getPrecision() {
    return value.length();
  }

  public Object getObject() {
    return value;
  }

  public void set(PreparedStatement prep, int parameterIndex)
      throws SQLException {
    prep.setString(parameterIndex, value);
  }

  public int getDisplaySize() {
    return value.length();
  }

  public int getMemory() {
    return value.length() * 2 + 48;
  }

  public Value convertPrecision(long precision, boolean force) {
    if (precision == 0 || value.length() <= precision) {
      return this;
    }
    int p = MathUtils.convertLongToInt(precision);
    return getNew(value.substring(0, p));
  }

  public int hashCode() {
    // by hashing the size and a few characters
    return value.hashCode();
  }

  public int getType() {
    return Value.STRING;
  }

  /**
   * Get or create a string value for the given string.
   * 
   * @param s
   *          the string
   * @return the value
   */
  public static ValueString get(String s) {
    if (s.length() == 0) {
      return EMPTY;
    }
    ValueString obj = new ValueString(s);
    return (obj);
    // this saves memory, but is really slow
    // return new ValueString(s.intern());
  }

  /**
   * Create a new String value of the current class. This method is meant to be
   * overridden by subclasses.
   * 
   * @param s
   *          the string
   * @return the value
   */
  protected ValueString getNew(String s) {
    return ValueString.get(s);
  }

}

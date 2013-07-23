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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Implementation of the OBJECT by PB data type.
 */
public class ValueProtobufObject extends Value {
  /**
   * The string data.
   */
  protected final Object value;

  protected ValueProtobufObject(Object value) {
    this.value = value;
  }

  @Override
  public String getSQL() {
    return null; // To change body of implemented methods use File | Settings |
                 // File Templates.
  }

  public int getType() {
    return Value.JAVA_OBJECT;
  }

  @Override
  public long getPrecision() {
    return 0; // To change body of implemented methods use File | Settings |
              // File Templates.
  }

  @Override
  public String getString() {
    return null; // To change body of implemented methods use File | Settings |
                 // File Templates.
  }

  @Override
  public Object getObject() {
    return null; // To change body of implemented methods use File | Settings |
                 // File Templates.
  }

  public void set(PreparedStatement prep, int parameterIndex)
      throws SQLException {
    prep.setObject(parameterIndex, value, Types.JAVA_OBJECT);
  }

  @Override
  public int hashCode() {
    return 0; // To change body of implemented methods use File | Settings |
              // File Templates.
  }

  @Override
  public boolean equals(Object other) {
    return false; // To change body of implemented methods use File | Settings |
                  // File Templates.
  }

  @Override
  protected int compareSecure(Value v, CompareMode mode) {
    return 0;
  }

  public static Value get(Object o) {
    if (o == null) {
      return null;
    }
    ValueProtobufObject obj = new ValueProtobufObject(o);
    return (obj);
  }
}

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

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Implementation of the BOOLEAN data type.
 */
public class ValueBoolean extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 1;

    /**
     * The maximum display size of a boolean.
     * Example: FALSE
     */
    public static final int DISPLAY_SIZE = 5;

    /**
     * Of type Object so that Tomcat doesn't set it to null.
     */
    private static final Object TRUE = new ValueBoolean(true);
    private static final Object FALSE = new ValueBoolean(false);

    private final Boolean value;

    private ValueBoolean(boolean value) {
        this.value = Boolean.valueOf(value);
    }

    public int getType() {
        return Value.BOOLEAN;
    }

    public String getSQL() {
        return getString();
    }

    public String getString() {
        return value.booleanValue() ? "TRUE" : "FALSE";
    }

    public Value negate() {
        return (ValueBoolean) (value.booleanValue() ? FALSE : TRUE);
    }

    public Boolean getBoolean() {
        return value;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        boolean v2 = ((ValueBoolean) o).value.booleanValue();
        boolean v = value.booleanValue();
        return (v == v2) ? 0 : (v ? 1 : -1);
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int hashCode() {
        return value.booleanValue() ? 1 : 0;
    }

    public Object getObject() {
        return value;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setBoolean(parameterIndex, value.booleanValue());
    }

    /**
     * Get the boolean value for the given boolean.
     *
     * @param b the boolean
     * @return the value
     */
    public static ValueBoolean get(boolean b) {
        return (ValueBoolean) (b ? TRUE : FALSE);
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public boolean equals(Object other) {
        // there are only ever two instances, so the instance must match
        return this == other;
    }

}

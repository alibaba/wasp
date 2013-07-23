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

import org.apache.wasp.SQLErrorCode;
import org.apache.wasp.jdbc.JdbcException;
import org.apache.wasp.util.DateTimeUtils;
import org.apache.wasp.util.MathUtils;
import org.apache.wasp.util.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

/**
 * Implementation of the TIME data type.
 */
public class ValueTime extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 6;

    /**
     * The display size of the textual representation of a time.
     * Example: 10:00:00
     */
    static final int DISPLAY_SIZE = 8;

    private final long nanos;

    private ValueTime(long nanos) {
        this.nanos = nanos;
    }

    /**
     * Get or create a time value.
     *
     * @param nanos the nanoseconds
     * @return the value
     */
    public static ValueTime fromNanos(long nanos) {
        return new ValueTime(nanos);
    }

    /**
     * Get or create a time value for the given time.
     *
     * @param time the time
     * @return the value
     */
    public static ValueTime get(Time time) {
        return fromNanos(DateTimeUtils.nanosFromDate(time.getTime()));
    }

    /**
     * Parse a string to a ValueTime.
     *
     * @param s the string to parse
     * @return the time
     */

    public static ValueTime parse(String s) {
        try {
            return fromNanos(DateTimeUtils.parseTimeNanos(s, 0, s.length(), false));
        } catch (Exception e) {
            throw JdbcException.get(SQLErrorCode.INVALID_DATETIME_CONSTANT_2,
                e, "TIME", s);
        }
    }

    public long getNanos() {
        return nanos;
    }

    public Time getTime() {
        return DateTimeUtils.convertNanoToTime(nanos);
    }

    public int getType() {
        return Value.TIME;
    }

    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        appendTime(buff, nanos, false);
        return buff.toString();
    }

    public String getSQL() {
        return "TIME '" + getString() + "'";
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        return MathUtils.compareLong(nanos, ((ValueTime) o).nanos);
    }

    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ValueTime && nanos == (((ValueTime) other).nanos);
    }

    public int hashCode() {
        return (int) (nanos ^ (nanos >>> 32));
    }

    public Object getObject() {
        return getTime();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setTime(parameterIndex, getTime());
    }

    public Value multiply(Value v) {
        return ValueTime.fromNanos((long) (nanos * v.getDouble()));
    }

    public Value divide(Value v) {
        return ValueTime.fromNanos((long) (nanos / v.getDouble()));
    }

    public int getSignum() {
        return Long.signum(nanos);
    }

    public Value negate() {
        return ValueTime.fromNanos(-nanos);
    }

    /**
     * Append a time to the string builder.
     *
     * @param buff the target string builder
     * @param nanos the time in nanoseconds
     * @param alwaysAddMillis whether to always add at least ".0"
     */
    static void appendTime(StringBuilder buff, long nanos, boolean alwaysAddMillis) {
        if (nanos < 0) {
            buff.append('-');
            nanos = -nanos;
        }
        long ms = nanos / 1000000;
        nanos -= ms * 1000000;
        long s = ms / 1000;
        ms -= s * 1000;
        long m = s / 60;
        s -= m * 60;
        long h = m / 60;
        m -= h * 60;
        StringUtils.appendZeroPadded(buff, 2, h);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append(':');
        StringUtils.appendZeroPadded(buff, 2, s);
        if (alwaysAddMillis || ms > 0 || nanos > 0) {
            buff.append('.');
            int start = buff.length();
            StringUtils.appendZeroPadded(buff, 3, ms);
            if (nanos > 0) {
                StringUtils.appendZeroPadded(buff, 6, nanos);
            }
            for (int i = buff.length() - 1; i > start; i--) {
                if (buff.charAt(i) != '0') {
                    break;
                }
                buff.deleteCharAt(i);
            }
        }
    }

}

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

import com.alibaba.wasp.util.MathUtils;import com.alibaba.wasp.SQLErrorCode;
import com.alibaba.wasp.jdbc.JdbcException;
import com.alibaba.wasp.util.DateTimeUtils;
import com.alibaba.wasp.util.MathUtils;
import com.alibaba.wasp.util.StringUtils;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Implementation of the DATE data type.
 */
public class ValueDate extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 8;

    /**
     * The display size of the textual representation of a date.
     * Example: 2000-01-02
     */
    static final int DISPLAY_SIZE = 10;

    private final long dateValue;

    private ValueDate(long dateValue) {
        this.dateValue = dateValue;
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value
     * @return the value
     */
    public static ValueDate fromDateValue(long dateValue) {
        return new ValueDate(dateValue);
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param date the date
     * @return the value
     */
    public static ValueDate get(Date date) {
        return fromDateValue(DateTimeUtils.dateValueFromDate(date.getTime()));
    }

    /**
     * Parse a string to a ValueDate.
     *
     * @param s the string to parse
     * @return the date
     */
    public static ValueDate parse(String s) {
        try {
            return fromDateValue(DateTimeUtils.parseDateValue(s, 0, s.length()));
        } catch (Exception e) {
            throw JdbcException.get(SQLErrorCode.INVALID_DATETIME_CONSTANT_2,
                e, "DATE", s);
        }
    }

    public long getDateValue() {
        return dateValue;
    }

    public Date getDate() {
        return DateTimeUtils.convertDateValueToDate(dateValue);
    }

    public int getType() {
        return Value.DATE;
    }

    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        appendDate(buff, dateValue);
        return buff.toString();
    }

    public String getSQL() {
        return "DATE '" + getString() + "'";
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        return MathUtils.compareLong(dateValue, ((ValueDate) o).dateValue);
    }

    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ValueDate && dateValue == (((ValueDate) other).dateValue);
    }

    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32));
    }

    public Object getObject() {
        return getDate();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setDate(parameterIndex, getDate());
    }

    /**
     * Append a date to the string builder.
     *
     * @param buff the target string builder
     * @param dateValue the date value
     */
    static void appendDate(StringBuilder buff, long dateValue) {
        int y = DateTimeUtils.yearFromDateValue(dateValue);
        int m = DateTimeUtils.monthFromDateValue(dateValue);
        int d = DateTimeUtils.dayFromDateValue(dateValue);
        if (y > 0 && y < 10000) {
            StringUtils.appendZeroPadded(buff, 4, y);
        } else {
            buff.append(y);
        }
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, d);
    }

}

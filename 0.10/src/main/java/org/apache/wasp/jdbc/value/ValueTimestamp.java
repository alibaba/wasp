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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Implementation of the TIMESTAMP data type.
 */
public class ValueTimestamp extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 23;

    /**
     * The display size of the textual representation of a timestamp.
     * Example: 2001-01-01 23:59:59.000
     */
    static final int DISPLAY_SIZE = 23;

    /**
     * The default scale for timestamps.
     */
    static final int DEFAULT_SCALE = 10;

    private final long dateValue;
    private final long nanos;

    private ValueTimestamp(long dateValue, long nanos) {
        this.dateValue = dateValue;
        this.nanos = nanos;
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value
     * @param nanos the nanoseconds
     * @return the value
     */
    public static ValueTimestamp fromDateValueAndNanos(long dateValue, long nanos) {
        return new ValueTimestamp(dateValue, nanos);
    }

    /**
     * Get or create a timestamp value for the given timestamp.
     *
     * @param timestamp the timestamp
     * @return the value
     */
    public static ValueTimestamp get(Timestamp timestamp) {
        long ms = timestamp.getTime();
        long nanos = timestamp.getNanos() % 1000000;
        long dateValue = DateTimeUtils.dateValueFromDate(ms);
        nanos += DateTimeUtils.nanosFromDate(ms);
        return fromDateValueAndNanos(dateValue, nanos);
    }

    /**
     * Parse a string to a ValueTimestamp. This method supports the format
     * +/-year-month-day hour:minute:seconds.fractional and an optional timezone
     * part.
     *
     * @param s the string to parse
     * @return the date
     */
    public static ValueTimestamp parse(String s) {
        try {
            return parseTry(s);
        } catch (Exception e) {
            throw JdbcException.get(SQLErrorCode.INVALID_DATETIME_CONSTANT_2,
                e, "TIMESTAMP", s);
        }
    }

    private static ValueTimestamp parseTry(String s) {
        int dateEnd = s.indexOf(' ');
        if (dateEnd < 0) {
            // ISO 8601 compatibility
            dateEnd = s.indexOf('T');
        }
        int timeStart;
        if (dateEnd < 0) {
            dateEnd = s.length();
            timeStart = -1;
        } else {
            timeStart = dateEnd + 1;
        }
        long dateValue = DateTimeUtils.parseDateValue(s, 0, dateEnd);
        long nanos;
        if (timeStart < 0) {
            nanos = 0;
        } else {
            int timeEnd = s.length();
            TimeZone tz = null;
            if (s.endsWith("Z")) {
                tz = TimeZone.getTimeZone("UTC");
                timeEnd--;
            } else {
                int timeZoneStart = s.indexOf('+', dateEnd);
                if (timeZoneStart < 0) {
                    timeZoneStart = s.indexOf('-', dateEnd);
                }
                if (timeZoneStart >= 0) {
                    String tzName = "GMT" + s.substring(timeZoneStart);
                    tz = TimeZone.getTimeZone(tzName);
                    if (!tz.getID().startsWith(tzName)) {
                        throw new IllegalArgumentException(tzName);
                    }
                    timeEnd = timeZoneStart;
                } else {
                    timeZoneStart = s.indexOf(' ', dateEnd + 1);
                    if (timeZoneStart > 0) {
                        String tzName = s.substring(timeZoneStart + 1);
                        tz = TimeZone.getTimeZone(tzName);
                        if (!tz.getID().startsWith(tzName)) {
                            throw new IllegalArgumentException(tzName);
                        }
                        timeEnd = timeZoneStart;
                    }
                }
            }
            nanos = DateTimeUtils.parseTimeNanos(s, dateEnd + 1, timeEnd, true);
            if (tz != null) {
                int year = DateTimeUtils.yearFromDateValue(dateValue);
                int month = DateTimeUtils.monthFromDateValue(dateValue);
                int day = DateTimeUtils.dayFromDateValue(dateValue);
                long ms = nanos / 1000000;
                nanos -= ms * 1000000;
                long second = ms / 1000;
                ms -= second * 1000;
                int minute = (int) (second / 60);
                second -= minute * 60;
                int hour = minute / 60;
                minute -= hour * 60;
                long millis = DateTimeUtils.getMillis(tz, year, month, day, hour, minute, (int) second, (int) ms);
                ms = DateTimeUtils.convertToLocal(new Date(millis), Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                long md = DateTimeUtils.MILLIS_PER_DAY;
                long absoluteDay = (ms >= 0 ? ms : ms - md + 1) / md;
                ms -= absoluteDay * md;
                nanos += ms * 1000000;
            }
        }
        return ValueTimestamp.fromDateValueAndNanos(dateValue, nanos);
    }

    public long getDateValue() {
        return dateValue;
    }

    public long getNanos() {
        return nanos;
    }

    public Timestamp getTimestamp() {
        return DateTimeUtils.convertDateValueToTimestamp(dateValue, nanos);
    }

    public int getType() {
        return Value.TIMESTAMP;
    }

    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        ValueDate.appendDate(buff, dateValue);
        buff.append(' ');
        ValueTime.appendTime(buff, nanos, true);
        return buff.toString();
    }

    public String getSQL() {
        return "TIMESTAMP '" + getString() + "'";
    }

    public long getPrecision() {
        return PRECISION;
    }

    public int getScale() {
        return DEFAULT_SCALE;
    }

    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    public Value convertScale(boolean onlyToSmallerScale, int targetScale) {
        if (targetScale >= DEFAULT_SCALE) {
            return this;
        }
        if (targetScale < 0) {
            throw JdbcException.getInvalidValueException("scale", targetScale);
        }
        long n = nanos;
        BigDecimal bd = BigDecimal.valueOf(n);
        bd = bd.movePointLeft(9);
        bd = MathUtils.setScale(bd, targetScale);
        bd = bd.movePointRight(9);
        long n2 = bd.longValue();
        if (n2 == n) {
            return this;
        }
        return fromDateValueAndNanos(dateValue, n2);
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueTimestamp t = (ValueTimestamp) o;
        int c = MathUtils.compareLong(dateValue, t.dateValue);
        if (c != 0) {
            return c;
        }
        return MathUtils.compareLong(nanos, t.nanos);
    }

    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof ValueTimestamp)) {
            return false;
        }
        ValueTimestamp x = (ValueTimestamp) other;
        return dateValue == x.dateValue && nanos == x.nanos;
    }

    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32) ^ nanos ^ (nanos >>> 32));
    }

    public Object getObject() {
        return getTimestamp();
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setTimestamp(parameterIndex, getTimestamp());
    }


}

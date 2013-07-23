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
import org.apache.wasp.util.MathUtils;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Implementation of the DECIMAL data type.
 */
public class ValueDecimal extends Value {

    /**
     * The value 'zero'.
     */
    public static final Object ZERO = new ValueDecimal(BigDecimal.ZERO);

    /**
     * The value 'one'.
     */
    public static final Object ONE = new ValueDecimal(BigDecimal.ONE);

    /**
     * The default precision for a decimal value.
     */
    static final int DEFAULT_PRECISION = 65535;

    /**
     * The default scale for a decimal value.
     */
    static final int DEFAULT_SCALE = 32767;

    /**
     * The default display size for a decimal value.
     */
    static final int DEFAULT_DISPLAY_SIZE = 65535;

    private static final int DIVIDE_SCALE_ADD = 25;

    private final BigDecimal value;
    private String valueString;
    private int precision;

    private ValueDecimal(BigDecimal value) {
        if (value == null) {
            throw new IllegalArgumentException();
        } else if (!value.getClass().equals(BigDecimal.class)) {
            throw JdbcException.get(SQLErrorCode.INVALID_CLASS_2,
                BigDecimal.class.getName(), value.getClass().getName());
        }
        this.value = value;
    }

    public Value add(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        return ValueDecimal.get(value.add(dec.value));
    }

    public Value subtract(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        return ValueDecimal.get(value.subtract(dec.value));
    }

    public Value negate() {
        return ValueDecimal.get(value.negate());
    }

    public Value multiply(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        return ValueDecimal.get(value.multiply(dec.value));
    }

    public Value divide(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        if (dec.value.signum() == 0) {
            throw JdbcException.get(SQLErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        BigDecimal bd = value.divide(dec.value, value.scale() + DIVIDE_SCALE_ADD, BigDecimal.ROUND_HALF_DOWN);
        if (bd.signum() == 0) {
            bd = BigDecimal.ZERO;
        } else if (bd.scale() > 0) {
            if (!bd.unscaledValue().testBit(0)) {
                String s = bd.toString();
                int i = s.length() - 1;
                while (i >= 0 && s.charAt(i) == '0') {
                    i--;
                }
                if (i < s.length() - 1) {
                    s = s.substring(0, i + 1);
                    bd = new BigDecimal(s);
                }
            }
        }
        return ValueDecimal.get(bd);
    }

    public ValueDecimal modulus(Value v) {
        ValueDecimal dec = (ValueDecimal) v;
        if (dec.value.signum() == 0) {
            throw JdbcException.get(SQLErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        BigDecimal bd = value.remainder(dec.value);
        return ValueDecimal.get(bd);
    }

    public String getSQL() {
        return getString();
    }

    public int getType() {
        return Value.DECIMAL;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueDecimal v = (ValueDecimal) o;
        return value.compareTo(v.value);
    }

    public int getSignum() {
        return value.signum();
    }

    public BigDecimal getBigDecimal() {
        return value;
    }

    public String getString() {
        if (valueString == null) {
            String p = value.toPlainString();
            if (p.length() < 40) {
                valueString = p;
            } else {
                valueString = value.toString();
            }
        }
        return valueString;
    }

    public long getPrecision() {
        if (precision == 0) {
            precision = value.precision();
        }
        return precision;
    }

    public boolean checkPrecision(long prec) {
        if (prec == DEFAULT_PRECISION) {
            return true;
        }
        return getPrecision() <= prec;
    }

    public int getScale() {
        return value.scale();
    }

    public int hashCode() {
        return value.hashCode();
    }

    public Object getObject() {
        return value;
    }

    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setBigDecimal(parameterIndex, value);
    }

    public Value convertScale(boolean onlyToSmallerScale, int targetScale) {
        if (value.scale() == targetScale) {
            return this;
        }
        if (onlyToSmallerScale || targetScale >= DEFAULT_SCALE) {
            if (value.scale() < targetScale) {
                return this;
            }
        }
        BigDecimal bd = MathUtils.setScale(value, targetScale);
        return ValueDecimal.get(bd);
    }

    public Value convertPrecision(long precision, boolean force) {
        if (getPrecision() <= precision) {
            return this;
        }
        if (force) {
            return get(BigDecimal.valueOf(value.doubleValue()));
        }
        throw JdbcException.get(SQLErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, Long.toString(precision));
    }

    /**
     * Get or create big decimal value for the given big decimal.
     *
     * @param dec the bit decimal
     * @return the value
     */
    public static ValueDecimal get(BigDecimal dec) {
        if (BigDecimal.ZERO.equals(dec)) {
            return (ValueDecimal) ZERO;
        } else if (BigDecimal.ONE.equals(dec)) {
            return (ValueDecimal) ONE;
        }
        return new ValueDecimal(dec);
    }

    public int getDisplaySize() {
        // add 2 characters for '-' and '.'
        return MathUtils.convertLongToInt(getPrecision() + 2);
    }

    public boolean equals(Object other) {
        // Two BigDecimal objects are considered equal only if they are equal in
        // value and scale (thus 2.0 is not equal to 2.00 when using equals;
        // however -0.0 and 0.0 are). Can not use compareTo because 2.0 and 2.00
        // have different hash codes
        return other instanceof ValueDecimal && value.equals(((ValueDecimal) other).value);
    }

    public int getMemory() {
        return value.precision() + 120;
    }

}

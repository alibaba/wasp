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

import com.alibaba.wasp.util.StringUtils;

/**
 * Implementation of the VARCHAR_IGNORECASE data type.
 */
public class ValueStringIgnoreCase extends ValueString {

    private static final ValueStringIgnoreCase EMPTY = new ValueStringIgnoreCase("");
    private int hash;

    protected ValueStringIgnoreCase(String value) {
        super(value);
    }

    public int getType() {
        return STRING_IGNORECASE;
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueStringIgnoreCase v = (ValueStringIgnoreCase) o;
        return mode.compareString(value, v.value, true);
    }

    public boolean equals(Object other) {
        return other instanceof ValueString && value.equalsIgnoreCase(((ValueString) other).value);
    }

    public int hashCode() {
        if (hash == 0) {
            // this is locale sensitive
            hash = value.toUpperCase().hashCode();
        }
        return hash;
    }

    public String getSQL() {
        return "CAST(" + StringUtils.quoteStringSQL(value) + " AS VARCHAR_IGNORECASE)";
    }

    /**
     * Get or create a case insensitive string value for the given string.
     * The value will have the same case as the passed string.
     *
     * @param s the string
     * @return the value
     */
    public static ValueStringIgnoreCase get(String s) {
        if (s.length() == 0) {
            return EMPTY;
        }
        ValueStringIgnoreCase obj = new ValueStringIgnoreCase(s);
        return obj;
    }

    protected ValueString getNew(String s) {
        return ValueStringIgnoreCase.get(s);
    }

}

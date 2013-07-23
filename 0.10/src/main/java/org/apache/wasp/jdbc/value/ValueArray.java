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

import org.apache.wasp.util.New;
import org.apache.wasp.util.StatementBuilder;

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * Implementation of the ARRAY data type.
 */
public class ValueArray extends Value {

    private final Class<?> componentType;
    private final Value[] values;
    private int hash;

    private ValueArray(Class<?> componentType, Value[] list) {
        this.componentType = componentType;
        this.values = list;
    }

    private ValueArray(Value[] list) {
        this(Object.class, list);
    }

    /**
     * Get or create a array value for the given value array.
     * Do not clone the data.
     *
     * @param list the value array
     * @return the value
     */
    public static ValueArray get(Value[] list) {
        return new ValueArray(list);
    }

    /**
     * Get or create a array value for the given value array.
     * Do not clone the data.
     *
     * @param componentType the array class (null for Object[])
     * @param list the value array
     * @return the value
     */
    public static ValueArray get(Class<?> componentType, Value[] list) {
        return new ValueArray(componentType, list);
    }

    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        int h = 1;
        for (Value v : values) {
            h = h * 31 + v.hashCode();
        }
        hash = h;
        return h;
    }

    public Value[] getList() {
        return values;
    }

    public int getType() {
        return Value.ARRAY;
    }

    public Class<?> getComponentType() {
        return componentType;
    }

    public long getPrecision() {
        long p = 0;
        for (Value v : values) {
            p += v.getPrecision();
        }
        return p;
    }

    public String getString() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            buff.append(v.getString());
        }
        return buff.append(')').toString();
    }

    protected int compareSecure(Value o, CompareMode mode) {
        ValueArray v = (ValueArray) o;
        if (values == v.values) {
            return 0;
        }
        int l = values.length;
        int ol = v.values.length;
        int len = Math.min(l, ol);
        for (int i = 0; i < len; i++) {
            Value v1 = values[i];
            Value v2 = v.values[i];
            int comp = v1.compareTo(v2, mode);
            if (comp != 0) {
                return comp;
            }
        }
        return l > ol ? 1 : l == ol ? 0 : -1;
    }

    public Object getObject() {
        int len = values.length;
        Object[] list = (Object[]) Array.newInstance(componentType, len);
        for (int i = 0; i < len; i++) {
            list[i] = values[i].getObject();
        }
        return list;
    }

    public void set(PreparedStatement prep, int parameterIndex) {
        throw throwUnsupportedExceptionForType("PreparedStatement.set");
    }

    public String getSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            buff.append(v.getSQL());
        }
        if (values.length == 1) {
            buff.append(',');
        }
        return buff.append(')').toString();
    }

    public String getTraceSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            buff.append(v.getTraceSQL());
        }
        return buff.append(')').toString();
    }

    public boolean equals(Object other) {
        if (!(other instanceof ValueArray)) {
            return false;
        }
        ValueArray v = (ValueArray) other;
        if (values == v.values) {
            return true;
        }
        int len = values.length;
        if (len != v.values.length) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (!values[i].equals(v.values[i])) {
                return false;
            }
        }
        return true;
    }


    public Value convertPrecision(long precision, boolean force) {
        if (!force) {
            return this;
        }
        ArrayList<Value> list = New.arrayList();
        for (Value v : values) {
            v = v.convertPrecision(precision, true);
            // empty byte arrays or strings have precision 0
            // they count as precision 1 here
            precision -= Math.max(1, v.getPrecision());
            if (precision < 0) {
                break;
            }
            list.add(v);
        }
        Value[] array = new Value[list.size()];
        list.toArray(array);
        return get(array);
    }

}

/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.wasp.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class implements a small LRU object cache.
 *
 * @param <K> the key
 * @param <V> the value
 */
public class SmallLRUCache<K, V> extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 1L;
    private int size;

    private SmallLRUCache(int size) {
        super(size, (float) 0.75, true);
        this.size = size;
    }

    /**
     * Create a new object with all elements of the given collection.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param size the number of elements
     * @return the object
     */
    public static <K, V> SmallLRUCache<K, V> newInstance(int size) {
        return new SmallLRUCache<K, V>(size);
    }

    public void setMaxSize(int size) {
        this.size = size;
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > size;
    }

}

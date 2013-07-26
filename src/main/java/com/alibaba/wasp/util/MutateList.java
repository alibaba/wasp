/**
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
package com.alibaba.wasp.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.protobuf.generated.WaspProtos.Mutate;

public class MutateList {

  private NavigableMap<byte[], Map<String, Mutate>> putAndDeletes = new TreeMap<byte[], Map<String, Mutate>>(
      Bytes.BYTES_COMPARATOR);

  /**
   * @return the mutates
   */
  public List<Mutate> getMutates() {
    List<Mutate> mutates = new ArrayList<Mutate>();
    for (Map<String, Mutate> map : putAndDeletes.values()) {
      mutates.addAll(map.values());
    }
    return mutates;
  }

  /**
   * @param mutates
   *          the mutates to set
   */
  public void setMutates(List<Mutate> mutates) {
    putAndDeletes = new TreeMap<byte[], Map<String, Mutate>>(
        Bytes.BYTES_COMPARATOR);
    for (Mutate mutate : mutates) {
      add(mutate);
    }
  }

  /**
   * 
   * @param mutate
   */
  public void add(Mutate mutate) {
    byte[] rowkey = mutate.getRow().toByteArray();
    Map<String, Mutate> map = putAndDeletes.get(rowkey);
    if (map == null) {
      map = new HashMap<String, Mutate>();
      putAndDeletes.put(rowkey, map);
    }
    map.put(mutate.getTableName(), mutate);
  }
}

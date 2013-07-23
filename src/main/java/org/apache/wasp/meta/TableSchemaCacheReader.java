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
package org.apache.wasp.meta;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.wasp.MetaException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * Local cache for table schema information, used by FServer.
 * 
 * If the meta is NOT fresh,fServer will received the news,then update the
 * TableSchemaReader.
 * 
 */
public class TableSchemaCacheReader extends AbstractMetaService implements
    Configurable {

  private static Log LOG = LogFactory.getLog(TableSchemaCacheReader.class);

  private static ConcurrentHashMap<String, FTable> tableSchemas = new ConcurrentHashMap<String, FTable>();

  private static ConcurrentHashMap<String, ConcurrentHashMap<String, List<Index>>> key2Index = new ConcurrentHashMap<String, ConcurrentHashMap<String, List<Index>>>();

  private static ConcurrentHashMap<String, ConcurrentHashMap<String, List<Index>>> compositeIndex = new ConcurrentHashMap<String, ConcurrentHashMap<String, List<Index>>>();

  private static ConcurrentHashMap<String, ConcurrentHashMap<String, List<Index>>> storing2Index = new ConcurrentHashMap<String, ConcurrentHashMap<String, List<Index>>>();

  private static TableSchemaCacheReader tableSchemaReader;

  private Configuration conf;

  /**
   * Load all table schemas from hbase into {@link #tableSchemas}
   * 
   * @throws MetaException
   */
  private void init() throws MetaException {
    if (getService(conf) == null) {
      throw new IllegalStateException(
          "Could not instantiate a TableSchemaCacheReader instance. "
              + "as a result of FMetagetService(conf) is null");
    }
    try {
      List<FTable> tables = getService(conf).getAllTables();
      for (FTable table : tables) {
        addSchema(table.getTableName(), table);
      }
    } catch (MetaException e) {
      if (LOG.isErrorEnabled()) {
        LOG.error("MetaException occur when init TableSchemaReader.", e);
      }
    }
  }

  private TableSchemaCacheReader(Configuration configuration,
      FMetaServices fMetaservice) throws MetaException {
    globalFMetaservice = fMetaservice;
    this.conf = configuration;
    init();
  }

  private TableSchemaCacheReader(Configuration configuration)
      throws MetaException {
    this.conf = configuration;
    init();
  }

  public static synchronized TableSchemaCacheReader getInstance(
      Configuration configuration, FMetaServices fMetaservice)
      throws MetaException {
    if (tableSchemaReader == null) {
      tableSchemaReader = new TableSchemaCacheReader(configuration,
          fMetaservice);
    } else {
      AbstractMetaService.globalFMetaservice = fMetaservice;
    }
    return tableSchemaReader;
  }

  public static synchronized TableSchemaCacheReader getInstance(
      Configuration configuration) throws MetaException {
    if (tableSchemaReader == null) {
      tableSchemaReader = new TableSchemaCacheReader(configuration);
    }
    return tableSchemaReader;
  }

  public FTable removeSchema(String tableName) {
    key2Index.remove(tableName);
    storing2Index.remove(tableName);
    compositeIndex.remove(tableName);
    return tableSchemas.remove(tableName);
  }

  public FTable addSchema(String tableName, FTable tableSchema) {
    if (tableSchema == null) {
      return null;
    }
    LinkedHashMap<String, Index> indexs = tableSchema.getIndex();
    if (indexs != null) {
      ConcurrentHashMap<String, List<Index>> col2IndexMap = new ConcurrentHashMap<String, List<Index>>();
      ConcurrentHashMap<String, List<Index>> storing2IndexMap = new ConcurrentHashMap<String, List<Index>>();
      for (Index index : indexs.values()) {
        for (Field field : index.getIndexKeys().values()) {
          mapping(index, field, col2IndexMap);
        }
        for (Field field : index.getStoring().values()) {
          mapping(index, field, storing2IndexMap);
        }
        mapping(index, tableName);
      }
      key2Index.put(tableName, col2IndexMap);
      storing2Index.put(tableName, storing2IndexMap);
    }
    return tableSchemas.put(tableName, tableSchema);
  }

  private void mapping(Index index, String tableName) {
    ConcurrentHashMap<String, List<Index>> indexs = compositeIndex
        .get(tableName);
    if (indexs == null) {
      indexs = new ConcurrentHashMap<String, List<Index>>();
      compositeIndex.put(tableName, indexs);
    }
    StringBuilder start = new StringBuilder();
    for (Field field : index.getIndexKeys().values()) {
      start.append(field.getName());
      List<Index> indexList = indexs.get(start.toString());
      if (indexList == null) {
        indexList = new ArrayList<Index>();
        indexs.put(start.toString(), indexList);
      }
      indexList.add(index);
    }
  }

  private void mapping(Index index, Field field,
      Map<String, List<Index>> col2Index) {
    List<Index> listIndexs = col2Index.get(field.getName());
    if (listIndexs == null) {
      listIndexs = new ArrayList<Index>();
    }
    listIndexs.add(index);
    col2Index.put(field.getName(), listIndexs);
  }

  public FTable getSchema(String tableName) throws MetaException {
    return tableSchemas.get(tableName);
  }

  public List<Index> getIndexsByComposite(String tableName, String compositeName) {
    ConcurrentHashMap<String, List<Index>> tableIndexes = compositeIndex
        .get(tableName);
    if (tableIndexes == null) {
      return null;
    }
    return tableIndexes.get(compositeName);
  }

  public Set<Index> getIndexsByField(String tableName, String column) {
    List<Index> indexsByKeyField = this.getIndexsByKeyField(tableName, column);
    List<Index> indexsByStoringField = this.getIndexsByStoringField(tableName,
        column);
    Set<Index> indexs = new HashSet<Index>((indexsByStoringField == null ? 0
        : indexsByStoringField.size())
        + (indexsByKeyField == null ? 0 : indexsByKeyField.size()));
    if (indexsByStoringField != null) {
      indexs.addAll(indexsByStoringField);
    }
    if (indexsByKeyField != null) {
      indexs.addAll(indexsByKeyField);
    }
    return indexs;
  }

  private List<Index> getIndexsByKeyField(String tableName, String column) {
    ConcurrentHashMap<String, List<Index>> col2indexs = key2Index
        .get(tableName);
    if (col2indexs == null) {
      return null;
    }
    return col2indexs.get(column);
  }

  private List<Index> getIndexsByStoringField(String tableName, String column) {
    ConcurrentHashMap<String, List<Index>> col2indexs = storing2Index
        .get(tableName);
    if (col2indexs == null) {
      return null;
    }
    return col2indexs.get(column);
  }

  public void refreshSchema(String tableName) throws MetaException {
    FTable schema = getService(conf).getTable(tableName);
    if (schema != null) {
      removeSchema(tableName);
    }
    addSchema(tableName, schema);
  }

  public void clearCache() {
    tableSchemas.clear();
    key2Index.clear();
    storing2Index.clear();
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
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
package com.alibaba.wasp.jdbc.result;

import com.alibaba.wasp.meta.Field;

import java.util.List;

public class ResultTransfer {

  private List<Field> fields;

  //private QueryResult result;

  private String tablename;

  private int commandType;

  private int counts;

  public ResultTransfer(List<Field> fields,
      String tablename, int commandType) {
    this.fields = fields;
    //this.result = result;
    this.tablename = tablename;
    this.commandType = commandType;
  }

  public List<Field> getFields() {
    return fields;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

//  public QueryResult getResult() {
//    return result;
//  }
//
//  public void setResult(QueryResult result) {
//    this.result = result;
//  }

  public String getTablename() {
    return tablename;
  }

  public void setTablename(String tablename) {
    this.tablename = tablename;
  }

  public int getCommandType() {
    return commandType;
  }

  public int getCounts() {
    return counts;
  }
}
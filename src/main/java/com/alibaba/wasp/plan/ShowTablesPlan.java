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
package com.alibaba.wasp.plan;


/**
 * The plan to SHOW TABLES. Use case<SHOW TABLES, SHOW TABLES LIKE 'pattern',
 * SHOW CREATE TABLE tablename>
 * 
 */
public class ShowTablesPlan extends DDLPlan {

  // ALL is SHOW TABLES
  // LIKE is SHOW TABLES LIKE 'pattern'
  // CREATE is SHOW CREATE TABLE tablename
  public static enum ShowTablesType {
    ALL, LIKE, CREATE
  }

  private ShowTablesType type;

  // For SHOW TABLES LIKE 'pattern'
  private String likePattern;

  // For SHOW CREATE TABLE tablename
  private String tablename;

  public ShowTablesPlan(ShowTablesType type) {
    this.type = type;
  }

  public ShowTablesType getType() {
    return type;
  }

  public void setType(ShowTablesType type) {
    this.type = type;
  }

  public String getLikePattern() {
    return likePattern;
  }

  public void setLikePattern(String likePattern) {
    this.likePattern = likePattern;
  }

  public String getTablename() {
    return tablename;
  }

  public void setTablename(String tablename) {
    this.tablename = tablename;
  }

  /**
   * @see Object#toString()
   */
  @Override
  public String toString() {
    return "ShowTablesPlan ";
  }
}
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
package com.alibaba.wasp.plan.parser;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.Plan;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;

/**
 * Parse Context: The current parse context for parsing sql
 * 
 */
public class ParseContext {

  public ParseContext() {
  }

  /** string command **/
  public String sql = "";

  /** formated string command **/
  public String formatedSQL = "";

  /** Druid's SQLParser parse a sql into SQLStatement **/
  protected SQLStatement stmt;

  /** use to read table schema **/
  protected TableSchemaCacheReader tsr;

  protected ReadModel readModel = ReadModel.SNAPSHOT;

  /** execute plan **/
  private Plan plan;

  private static SimpleDateFormat format = new SimpleDateFormat(
      "yyyy-MM-dd_HH-mm-ss_SSS");

  private static Random rand = new Random();

  private boolean genWholePlan = true;

  /**
   * Generate a unique executionId. An executionId, together with user name and
   * the configuration, will determine the temporary locations of all
   * intermediate files.
   * 
   * In the future, users can use the executionId to resume a query.
   */
  public static String generateExecutionId() {
    String executionId = "wasp_" + format.format(new Date()) + "_"
        + Math.abs(rand.nextLong());
    return executionId;
  }

  private SQLStatementParser sp;

  public SQLStatement getStmt() {
    return stmt;
  }

  public void setStmt(SQLStatement stmt) {
    this.stmt = stmt;
  }

  public TableSchemaCacheReader getTsr() {
    return tsr;
  }

  public void setTsr(TableSchemaCacheReader tsr) {
    this.tsr = tsr;
  }

  public String getFormatedSQL() {
    return formatedSQL;
  }

  public void setFormatedSQL(String formatedSQL) {
    this.formatedSQL = formatedSQL;
  }

  public boolean isGenWholePlan() {
    return genWholePlan;
  }

  public void setGenWholePlan(boolean genWholePlan) {
    this.genWholePlan = genWholePlan;
  }

  /**
   * @return the sql
   */
  public String getSql() {
    return sql;
  }

  /**
   * @param sql
   *          the sql to set
   */
  public void setSql(String sql) {
    this.sql = sql;
  }

  /**
   * @return the sp
   */
  public SQLStatementParser getSp() {
    return sp;
  }

  /**
   * @param sp
   *          the sp to set
   */
  public void setSp(SQLStatementParser sp) {
    this.sp = sp;
  }

  /**
   * @return the readModel
   */
  public ReadModel getReadModel() {
    return readModel;
  }

  /**
   * @param readModel
   *          the readModel to set
   */
  public void setReadModel(ReadModel readModel) {
    this.readModel = readModel;
  }

  /**
   * @return the plan
   */
  public Plan getPlan() {
    return plan;
  }

  /**
   * @param plan
   *          the plan to set
   */
  public void setPlan(Plan plan) {
    this.plan = plan;
  }
}

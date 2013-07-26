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

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;

/**
 * Supported value in where cause.
 * 
 */
public class Condition {
  public static enum ConditionType {
    RANGE,
    EQUAL
  }

  final private ConditionType type;

  /** Field's name **/
  final private String fieldName;

  /** Supported type: Integer , Long, Float, String **/
  private SQLExpr value;

  /** Supported type: Integer , Long, Float, String **/
  private SQLExpr left;

  /** GreaterThan, GreaterThanOrEqual **/
  private SQLBinaryOperator leftOperator;

  /** Supported type: Integer , Long, Float, String **/
  private SQLExpr right;

  /** LessThan, LessThanOrEqual **/
  private SQLBinaryOperator rightOperator;


  /**
   * Constructor.
   * 
   * @param fieldName
   * @param type
   * @param value
   */
  public Condition(String fieldName, ConditionType type, SQLExpr value) {
    this(fieldName, type, value, null);
  }

  /**
   * Constructor.
   * 
   * @param fieldName
   * @param type
   * @param value
   * @param operator
   */
  public Condition(String fieldName, ConditionType type, SQLExpr value,
      SQLBinaryOperator operator) {
    this.fieldName = fieldName;
    this.type = type;
    if (operator == null) {
      this.value = value;
    } else if (operator == SQLBinaryOperator.GreaterThan
        || operator == SQLBinaryOperator.GreaterThanOrEqual) {
      this.left = value;
      this.leftOperator = operator;
    } else {
      this.right = value;
      this.rightOperator = operator;
    }
  }

  public ConditionType getType() {
    return type;
  }

  public String getFieldName() {
    return fieldName;
  }

  public SQLExpr getValue() {
    return value;
  }

  public SQLExpr getLeft() {
    return left;
  }


  public SQLBinaryOperator getLeftOperator() {
    return leftOperator;
  }

  public SQLExpr getRight() {
    return right;
  }

  public SQLBinaryOperator getRightOperator() {
    return rightOperator;
  }

  public void resetValue(SQLExpr value, SQLBinaryOperator operator) {
    if (operator == SQLBinaryOperator.GreaterThan
        || operator == SQLBinaryOperator.GreaterThanOrEqual) {
      this.left = value;
      this.leftOperator = operator;
    } else {
      this.right = value;
      this.rightOperator = operator;
    }
  }

  public Object convert(SQLExpr expr) {
    if (expr instanceof SQLIntegerExpr) {// Integer,Long
      Number number = ((SQLIntegerExpr) expr).getNumber();
      return number;
    } else if (expr instanceof SQLCharExpr) {// String
      String value = ((SQLCharExpr) expr).getText();
      return value;
    } else if (expr instanceof SQLNumberExpr) {// Float, Double
      Number number = ((SQLNumberExpr) expr).getNumber();
      return number;
    }
    return null;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Field ");
    sb.append(fieldName);
    sb.append(' ');
    sb.append(" Type ");
    sb.append(type);
    sb.append(" Value ");
    if (type == ConditionType.EQUAL) {
      sb.append(convert(value));
    } else {
      if (leftOperator == SQLBinaryOperator.GreaterThan) {
        sb.append("(");
      } else {
        sb.append("[");
      }
      sb.append(convert(left));
      sb.append(", ");
      sb.append(convert(right));
      if (rightOperator == SQLBinaryOperator.LessThan) {
        sb.append(")");
      } else {
        sb.append("]");
      }
    }
    return sb.toString();
  }
}
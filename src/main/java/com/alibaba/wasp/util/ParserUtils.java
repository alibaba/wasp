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

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.wasp.plan.parser.Condition;
import com.alibaba.wasp.plan.parser.UnsupportedException;
import org.apache.hadoop.hbase.filter.CompareFilter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.druid.sql.ast.expr.SQLBinaryOperator.Equality;

/**
 * SQL Parser Utils.
 */
public class ParserUtils {

  /**
   * Parse The WHERE Clause into <name, value> pairs. For int, long, float type,
   * it is can be range. Others should be equality.
   */
  public static void parse(SQLExpr where,
      LinkedHashMap<String, Condition> conditions, LinkedHashMap<String, Condition> ranges)
      throws UnsupportedException {
    if (where instanceof SQLBinaryOpExpr) {
      SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) where;
      SQLBinaryOperator operator = binaryOpExpr.getOperator();
      if (operator == Equality) {// one pair
        SQLIdentifierExpr left = (SQLIdentifierExpr) binaryOpExpr.getLeft();
        Condition value = new Condition(left.getName(),
            Condition.ConditionType.EQUAL, binaryOpExpr.getRight());
        conditions.put(value.getFieldName(), value);
      } else if (operator == SQLBinaryOperator.BooleanAnd) {// multi pair
        SQLExpr left = binaryOpExpr.getLeft();
        SQLExpr right = binaryOpExpr.getRight();
        parse(left, conditions, ranges);
        parse(right, conditions, ranges);
      } else if (operator == SQLBinaryOperator.GreaterThan
          || operator == SQLBinaryOperator.GreaterThanOrEqual
          || operator == SQLBinaryOperator.LessThan
          || operator == SQLBinaryOperator.LessThanOrEqual) {
        SQLIdentifierExpr left = (SQLIdentifierExpr) binaryOpExpr.getLeft();
        String fieldName = left.getName();
        Condition value = getCondition(fieldName, ranges);
        if (value == null) {
          value = new Condition(left.getName(), Condition.ConditionType.RANGE,
              binaryOpExpr.getRight(), operator);
          ranges.put(value.getFieldName(), value);
        } else {
          value.resetValue(binaryOpExpr.getRight(), operator);
        }
      } else {
        throw new UnsupportedException("where clause '" + where + " has '"
            + operator + "' , current this is Unsupported");
      }
    } else {
      throw new UnsupportedException("where clause '" + where + "' Unsupported");
    }
  }

  public static Condition getCondition(String fieldName,
      Map<String, Condition> conditions) throws UnsupportedException {
    if (conditions == null || conditions.size() == 0) {
      return null;
    }
    return conditions.get(fieldName);
  }

  public static Condition getCondition(String fieldName,
      List<Condition> conditions) throws UnsupportedException {
    if (conditions == null || conditions.size() == 0) {
      return null;
    }
    for (Condition condition : conditions) {
      if (condition.getFieldName().equals(fieldName)) {
        return condition;
      }
    }
    return null;
  }

  public interface OperatorOpValue {
    public static int EQUAL = 1;
    public static int GREATER = 2;
    public static int GREATER_OR_EQUAL = 3;
    public static int LESS = 4;
    public static int LESS_OR_EQUAL = 5;
    public static int NO_OP = -1;
  }

  public static int parseSQLBinaryOperatorToIntValue(SQLBinaryOperator operator){
    switch (operator) {
      case Equality : return OperatorOpValue.EQUAL;
      case GreaterThan : return OperatorOpValue.GREATER;
      case GreaterThanOrEqual : return OperatorOpValue.GREATER_OR_EQUAL;
      case LessThan : return OperatorOpValue.LESS;
      case LessThanOrEqual : return OperatorOpValue.LESS_OR_EQUAL;
    }
    return OperatorOpValue.NO_OP;
  }

  public static CompareFilter.CompareOp convertIntValueToCompareOp(int value) {
    switch (value) {
      case OperatorOpValue.EQUAL:
        return CompareFilter.CompareOp.EQUAL;
      case OperatorOpValue.GREATER:
        return CompareFilter.CompareOp.GREATER;
      case OperatorOpValue.GREATER_OR_EQUAL:
        return CompareFilter.CompareOp.GREATER_OR_EQUAL;
      case OperatorOpValue.LESS:
        return CompareFilter.CompareOp.LESS;
      case OperatorOpValue.LESS_OR_EQUAL:
        return CompareFilter.CompareOp.LESS_OR_EQUAL;
      case OperatorOpValue.NO_OP:
        return CompareFilter.CompareOp.NO_OP;
      default:
        return CompareFilter.CompareOp.NO_OP;
    }
  }

  /**
   * List condition column name.
   * 
   * @param conditions
   * @return
   */
  public static Set<String> getColumns(
      LinkedHashMap<String, Condition> conditions) {
    return conditions.keySet();
  }
}
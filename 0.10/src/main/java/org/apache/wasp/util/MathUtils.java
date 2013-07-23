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
package org.apache.wasp.util;

import org.apache.wasp.jdbc.JdbcException;

import java.math.BigDecimal;


public class MathUtils {

  /**
   * The maximum scale of a BigDecimal value.
   */
  private static final int BIG_DECIMAL_SCALE_MAX = 100000;

  /**
   * if value is greater than Integer.MAX_VALUE,this method will return
   * Integer.MAX_VALUE.
   * 
   * @param value
   * @return
   */
  public static int convertLongToInt(long value) {
    if (value <= Integer.MIN_VALUE) {
      return Integer.MIN_VALUE;
    } else if (value >= Integer.MAX_VALUE) {
      return Integer.MAX_VALUE;
    } else {
      return (int) value;
    }
  }

  /**
   * Compare two values. Returns -1 if the first value is smaller, 1 if bigger,
   * and 0 if equal.
   * 
   * @param a
   *          the first value
   * @param b
   *          the second value
   * @return the result
   */
  public static int compareInt(int a, int b) {
    return a == b ? 0 : a < b ? -1 : 1;
  }

  /**
   * Compare two values. Returns -1 if the first value is smaller, 1 if bigger,
   * and 0 if equal.
   * 
   * @param a
   *          the first value
   * @param b
   *          the second value
   * @return the result
   */
  public static int compareLong(long a, long b) {
    return a == b ? 0 : a < b ? -1 : 1;
  }

  /**
   * Set the scale of a BigDecimal value.
   * 
   * @param bd
   *          the BigDecimal value
   * @param scale
   *          the new scale
   * @return the scaled value
   */
  public static BigDecimal setScale(BigDecimal bd, int scale) {
    if (scale > BIG_DECIMAL_SCALE_MAX || scale < -BIG_DECIMAL_SCALE_MAX) {
      throw JdbcException.getInvalidValueException("scale", scale);
    }
    return bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
  }
}

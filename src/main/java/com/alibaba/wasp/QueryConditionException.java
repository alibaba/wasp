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
package com.alibaba.wasp;

public class QueryConditionException extends ClientConcernedException {

  /**
   *
   */
  private static final long serialVersionUID = -710818159811010988L;

  /**
   *
   */
  public QueryConditionException() {
  }

  /**
   * @param message
   */
  public QueryConditionException(String message) {
    super(message);
  }

  /**
   * @see ClientConcernedException#getErrorCode()
   */
  @Override
  public int getErrorCode() {
    return SQLErrorCode.QUERY_CONDITION_ERROR;
  }
}
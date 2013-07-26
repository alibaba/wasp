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

import com.alibaba.wasp.ClientConcernedException;import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.ClientConcernedException;
import com.alibaba.wasp.SQLErrorCode;

public class UnsupportedException extends ClientConcernedException {
  private static final long serialVersionUID = 1L << 17 - 1L;

  /** default constructor */
  public UnsupportedException() {
    super();
  }

  /**
   * Constructor
   * 
   * @param s
   *          message
   */
  public UnsupportedException(String s) {
    super(s);
  }

  /**
   * Constructor
   * 
   * @param s
   *          message
   */
  public UnsupportedException(final byte[] s) {
    super(Bytes.toString(s));
  }

  public UnsupportedException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * @see com.alibaba.wasp.ClientConcernedException#getErrorCode()
   */
  @Override
  public int getErrorCode() {
    return SQLErrorCode.NOT_SUPPORTED;
  }
}
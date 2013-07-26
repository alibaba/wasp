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
package com.alibaba.wasp.fserver.redo;

import java.io.IOException;

public class AlreadyCommitTransactionException extends IOException {

  private static final long serialVersionUID = 3573918344025960683L;

  public AlreadyCommitTransactionException() {
    super();
  }

  public AlreadyCommitTransactionException(String arg0, Throwable arg1) {
    super(arg0, arg1);
  }

  public AlreadyCommitTransactionException(String arg0) {
    super(arg0);
  }

  public AlreadyCommitTransactionException(Throwable arg0) {
    super(arg0);
  }

  public AlreadyCommitTransactionException(Transaction t) {
    super(t.getKeyStr());
  }
}

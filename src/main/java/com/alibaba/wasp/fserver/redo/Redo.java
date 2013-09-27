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

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.plan.action.Primary;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;

public abstract class Redo implements Closeable {

  protected final EntityGroupInfo egi;

  protected final Configuration conf;

  /**
   * Default constructor.
   * 
   * @param egi
   *          entityGroupInfo
   * @param conf
   *          passed by entityGroup.
   * 
   * @throws java.io.IOException
   */
  public Redo(EntityGroupInfo egi, Configuration conf) {
    this.conf = conf;
    this.egi = egi;
  }

  /**
   * initlize
   *
   * @throws java.io.IOException
   */
  abstract public void initlize() throws IOException;

  /**
   * Append transaction.
   *
   * @param t
   * @throws java.io.IOException
   */
  abstract public void append(Primary action, Transaction t) throws IOException;

  /**
   * return the last commited transaction.
   *
   * @return
   * @throws java.io.IOException
   */
  abstract public WALEdit lastCommitedTransaction() throws IOException;

  /**
   * Return the last transaction.
   *
   * @return
   * @throws java.io.IOException
   */
  abstract public WALEdit lastUnCommitedTransaction()
      throws InterruptedException, NotInitlizedRedoException,
      RedoLogNotServingException;

  /**
   *
   *
   * @return
   */
  abstract public WALEdit peekLastUnCommitedTransaction() throws IOException;

  /**
   * Commit the transaction.
   *
   * @param t
   * @return
   * @throws java.io.IOException
   */
  abstract public boolean commit(WALEdit edit) throws IOException;

  /**
   * redo
   *
   * @throws java.io.IOException
   */
  abstract public void redo() throws IOException;

  /**
   * 
   */
  abstract public int size();
}
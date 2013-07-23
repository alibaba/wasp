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
package org.apache.wasp.fserver.redo;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.plan.action.Primary;

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
   * @throws IOException
   */
  public Redo(EntityGroupInfo egi, Configuration conf) {
    this.conf = conf;
    this.egi = egi;
  }

  /**
   * initlize
   * 
   * @throws IOException
   */
  abstract public void initlize() throws IOException;

  /**
   * Append transaction.
   * 
   * @param t
   * @throws IOException
   */
  abstract public void append(Primary action, Transaction t) throws IOException;

  /**
   * return the last commited transaction.
   * 
   * @return
   * @throws IOException
   */
  abstract public WALEdit lastCommitedTransaction() throws IOException;

  /**
   * Return the last transaction.
   * 
   * @return
   * @throws IOException
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
   * @throws IOException
   */
  abstract public boolean commit(WALEdit edit) throws IOException;

  /**
   * redo
   * 
   * @throws IOException
   */
  abstract public void redo() throws IOException;

  /**
   * 
   */
  abstract public int size();
}
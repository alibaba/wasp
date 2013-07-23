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
package org.apache.wasp.fserver.redo;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.meta.StorageTableNameBuilder;
import org.apache.wasp.plan.action.Primary;

/**
 * Wasp uses this class to support transaction feature.
 * 
 */
public class RedoLog extends Redo {

  public static final Log LOG = LogFactory.getLog(RedoLog.class);

  /** transaction table **/
  private HTable transcationTable;

  /** column family of transaction table **/
  private final byte[] fEntityValue = Bytes.toBytes("i"); // info

  /** info column name **/
  private final byte[] cEntityValue = Bytes.toBytes("i"); // info

  /** commit column name **/
  private final byte[] cCommit = Bytes.toBytes("c"); // commit

  private final byte[] emptyStringByte = Bytes.toBytes("");

  /** Uncommited is 0 **/
  private final byte[] unCommited = Bytes.toBytes(0);

  /** Commited is 1 **/
  private final byte[] commited = Bytes.toBytes(1);

  private WALEdit lastCommitedTransaction;

  protected BlockingQueue<WALEdit> editQueue = new LinkedBlockingQueue<WALEdit>(
      Runtime.getRuntime().availableProcessors() * 100);

  private boolean initlized = false;

  private volatile long maxTransactionID = Long.MIN_VALUE;

  /**
   * Default constructor.
   */
  public RedoLog(EntityGroupInfo egi, Configuration conf) throws IOException {
    super(egi, conf);
  }

  /**
   * 
   * @see org.apache.wasp.fserver.redo.Redo#initlize()
   */
  public void initlize() throws IOException {
    HBaseAdmin admin = new HBaseAdmin(this.conf);
    String tTableName = StorageTableNameBuilder.buildTransactionTableName(egi
        .getEncodedName());
    if (!admin.tableExists(tTableName)) {
      HColumnDescriptor family = new HColumnDescriptor(fEntityValue);
      family.setCompressionType(Algorithm.GZ);
      HTableDescriptor tableDes = new HTableDescriptor(tTableName);
      tableDes.addFamily(family);
      admin.createTable(tableDes);
    }
    admin.close();
    this.transcationTable = new HTable(this.conf, tTableName);
    redo();
    this.initlized = true;
    if (lastCommitedTransaction != null) {
      maxTransactionID = lastCommitedTransaction.getT().getTransactionID();
    }
    if (editQueue.size() > 0) {
      maxTransactionID = editQueue.peek().getT().getTransactionID();
    }
    LOG.info("Init " + egi.toString() + " redolog, maxTransactionID = "
        + maxTransactionID);
  }

  /**
   * 
   * @see org.apache.wasp.fserver.redo.Redo#append(org.apache.wasp.fserver.redo.Transaction)
   */
  @Override
  public void append(Primary action, Transaction t)
      throws NotInitlizedRedoException, RedoLogNotServingException, IOException {
    checkInit();
    if (maxTransactionID >= t.getTransactionID()) {
      throw new AlreadyExsitsTransactionIdException("TransactionId:"
          + t.getTransactionID() + " <=  lastAppendTransaction "
          + maxTransactionID);
    }
    byte[] rowKey = t.getKey();
    Put put = new Put(rowKey);
    put.add(fEntityValue, cEntityValue, t.getTransactionEntity());
    put.add(fEntityValue, cCommit, unCommited);
    if (!transcationTable.checkAndPut(t.getKey(), fEntityValue, cEntityValue,
        null, put)) {
      throw new AlreadyExsitsTransactionIdException("TransactionId:"
          + t.getTransactionID() + " already exsits.");
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("RedoLog Append Transaction:" + t.transactionID);
    }
    try {
      editQueue.put(new WALEdit(action, t));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    maxTransactionID = t.getTransactionID();
  }

  /**
   * 
   * @see org.apache.wasp.fserver.redo.Redo#commit(org.apache.wasp.fserver.redo.Transaction)
   */
  @Override
  public boolean commit(WALEdit edit) throws NotInitlizedRedoException,
      RedoLogNotServingException, AlreadyCommitTransactionException {
    checkInit();
    Transaction t = edit.getT();
    byte[] rowKey = t.getKey();
    Put put = new Put(rowKey);
    put.add(fEntityValue, cCommit, commited);
    try {
      boolean success = transcationTable.checkAndPut(t.getKey(), fEntityValue,
          cCommit, unCommited, put);
      if (!success) {
        throw new AlreadyCommitTransactionException(t);
      }
      lastCommitedTransaction = edit;
      t.commit(t.getTransactionID());
      if (LOG.isDebugEnabled()) {
        LOG.debug("RedoLog Commit Transaction:" + t.transactionID);
      }
      return true;
    } catch (IOException e) {
      LOG.error("Failed to commit transaction:" + t.getKeyStr(), e);
      return false;
    }
  }

  /**
   * 
   * @see org.apache.wasp.fserver.redo.Redo#lastCommitedTransaction()
   */
  @Override
  public WALEdit lastCommitedTransaction() throws IOException {
    checkInit();
    return lastCommitedTransaction;
  }

  /**
   * 
   * @see org.apache.wasp.fserver.redo.Redo#lastUnCommitedTransaction()
   */
  @Override
  public WALEdit lastUnCommitedTransaction() throws InterruptedException,
      NotInitlizedRedoException, RedoLogNotServingException {
    checkInit();
    return this.editQueue.take();
  }

  /**
   * @throws NotInitlizedRedoException
   * @see org.apache.wasp.fserver.redo.Redo#peekLastUnCommitedTransaction()
   */
  @Override
  public WALEdit peekLastUnCommitedTransaction() throws IOException {
    checkInit();
    return this.editQueue.peek();
  }

  /**
   * @see org.apache.wasp.fserver.redo.Redo#redo()
   */
  @Override
  public void redo() throws IOException {
    Scan scan = new Scan(emptyStringByte);
    ResultScanner scanner = transcationTable.getScanner(scan);
    Result result;
    try {
      while ((result = scanner.next()) != null) {
        byte[] commitBytes = result.getValue(fEntityValue, cCommit);
        int commit = Bytes.toInt(commitBytes);
        if (commit == Bytes.toInt(commited)) {
          this.lastCommitedTransaction = new WALEdit(null,
              Transaction.convert(result.getValue(fEntityValue, cEntityValue)));
          break;
        } else if (commit == Bytes.toInt(unCommited)) {
          Transaction t = Transaction.convert(result.getValue(fEntityValue,
              cEntityValue));
          try {
            this.editQueue.put(new WALEdit(null, t));
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        } else if (commitBytes == null) {
          // nothing
        } else {
          throw new IOException("Unkown commit state.");
        }
      }
    } finally {
      scanner.close();
    }
  }

  /**
   * 
   * @throws NotInitlizedRedoException
   */
  private void checkInit() throws NotInitlizedRedoException {
    if (!initlized) {
      throw new NotInitlizedRedoException();
    }
  }

  /**
   * Interrupt Transaction.
   * 
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    transcationTable.close();
  }

  /**
   * @see org.apache.wasp.fserver.redo.Redo#size()
   */
  @Override
  public int size() {
    return this.editQueue.size();
  }
}
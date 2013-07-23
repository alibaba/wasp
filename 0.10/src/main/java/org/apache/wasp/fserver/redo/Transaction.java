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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.protobuf.generated.MetaProtos.TransactionProto;
import org.apache.wasp.protobuf.generated.WaspProtos.Mutate;
import org.apache.wasp.util.MutateList;

import java.util.List;

public class Transaction {

  /** Id */
  long transactionID;

  private static final String rowSuffix = "_tid";

  private byte[] key;

  private String keyStr;

  private boolean commited = false;

  /** Put and Delete **/
  private MutateList edits = new MutateList();

  public Transaction(long transactionID) {
    this.transactionID = transactionID;
    initlize();
  }

  public Transaction() {
    this(System.currentTimeMillis());
  }

  private void initlize() {
    this.keyStr = (Long.MAX_VALUE - this.transactionID) + rowSuffix;
    this.key = Bytes.toBytes(keyStr);
  }

  /**
   * @param transactionID
   *          the transactionID to set
   */
  public void setTransactionID(long transactionID) {
    this.transactionID = transactionID;
    initlize();
  }

  /**
   * @return the transactionID
   */
  public long getTransactionID() {
    return transactionID;
  }

  /**
   * @return the commited
   */
  public boolean isCommited() {
    return commited;
  }

  /**
   * @param transactionID
   *          the commited to set
   */
  public void commit(long transactionID) {
    if (transactionID == this.transactionID) {
      this.commited = true;
    }
  }

  /**
   * @return the key
   */
  public byte[] getKey() {
    return key;
  }

  public String getKeyStr() {
    return this.keyStr;
  }

  public void addEntity(Mutate mutate) {
    edits.add(mutate);
  }

  public void setTransactionEdits(List<Mutate> edits) {
    this.edits.setMutates(edits);
  }

  /**
   * @return the edits
   */
  public List<Mutate> getEdits() {
    return edits.getMutates();
  }

  /**
   * return this byte[] of this object.
   * 
   * @return
   */
  public byte[] getTransactionEntity() {
    TransactionProto tProto = conver(this);
    return tProto.toByteArray();
  }

  /**
   * Parse data to Transaction.
   * 
   * @param data
   * @return
   */
  public static Transaction convert(byte[] data) {
    TransactionProto tProto;
    try {
      tProto = TransactionProto.parseFrom(data);
      Transaction t = new Transaction(tProto.getTid());
      t.setTransactionEdits(tProto.getMutatesList());
      return t;
    } catch (InvalidProtocolBufferException e) {
      RedoLog.LOG.error("Failed from TransactionProto.parseFrom.", e);
      return null;
    }
  }

  /**
   * convert Java Object to protobuf object.
   * 
   * @param t
   * @return
   */
  public static TransactionProto conver(Transaction t) {
    TransactionProto.Builder builder = TransactionProto.newBuilder();
    builder.setTid(t.getTransactionID());
    builder.addAllMutates(t.edits.getMutates());
    return builder.build();
  }
}

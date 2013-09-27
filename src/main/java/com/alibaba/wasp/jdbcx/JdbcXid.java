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
package com.alibaba.wasp.jdbcx;

import com.alibaba.wasp.SQLErrorCode;
import com.alibaba.wasp.jdbc.JdbcException;
import com.alibaba.wasp.util.StringUtils;

import javax.transaction.xa.Xid;
import java.util.StringTokenizer;

/**
 * An object of this class represents a transaction id.
 */
public class JdbcXid implements Xid {

  private static final String PREFIX = "XID";

  private final int formatId;
  private final byte[] branchQualifier;
  private final byte[] globalTransactionId;

  JdbcXid(String tid) {
    try {
      StringTokenizer tokenizer = new StringTokenizer(tid, "_");
      String prefix = tokenizer.nextToken();
      if (!PREFIX.equals(prefix)) {
        throw JdbcException.get(SQLErrorCode.WRONG_XID_FORMAT_1, tid);
      }
      formatId = Integer.parseInt(tokenizer.nextToken());
      branchQualifier = StringUtils.convertHexToBytes(tokenizer.nextToken());
      globalTransactionId = StringUtils
          .convertHexToBytes(tokenizer.nextToken());
    } catch (RuntimeException e) {
      throw JdbcException.get(SQLErrorCode.WRONG_XID_FORMAT_1, tid);
    }
  }

  /**
   * INTERNAL
   */
  public static String toString(Xid xid) {
    StringBuilder buff = new StringBuilder(PREFIX);
    buff.append('_').append(xid.getFormatId()).append('_')
        .append(StringUtils.convertBytesToHex(xid.getBranchQualifier()))
        .append('_')
        .append(StringUtils.convertBytesToHex(xid.getGlobalTransactionId()));
    return buff.toString();
  }

  /**
   * Get the format id.
   * 
   * @return the format id
   */
  public int getFormatId() {
    return formatId;
  }

  /**
   * The transaction branch identifier.
   * 
   * @return the identifier
   */
  public byte[] getBranchQualifier() {
    return branchQualifier;
  }

  /**
   * The global transaction identifier.
   * 
   * @return the transaction id
   */
  public byte[] getGlobalTransactionId() {
    return globalTransactionId;
  }

}

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
package com.alibaba.wasp.messagequeue;

/**
 * @author zhiyuan.daizy
 * 
 */
public class MessageID {

  private byte[] messageId;

  /**
   * @param messageId
   */
  public MessageID(byte[] messageId) {
    super();
    this.messageId = messageId;
  }
  
  /**
   * 
   */
  public MessageID() {
  }



  /**
   * @return the messageId
   */
  public byte[] getMessageId() {
    return messageId;
  }

  /**
   * @param messageId
   *          the messageId to set
   */
  public void setMessageId(byte[] messageId) {
    this.messageId = messageId;
  }
}

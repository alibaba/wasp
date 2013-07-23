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
package org.apache.wasp.fserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.messagequeue.Message;
import org.apache.wasp.messagequeue.MessageID;
import org.apache.wasp.messagequeue.Publisher;
import org.apache.wasp.plan.action.Action;
import org.apache.wasp.storage.StorageActionManager;

/**
 * TwoPhaseCommit is implemention of TwoPhaseCommitProtocol,it will be used by
 * Driver.
 * 
 */
public class TwoPhaseCommit implements TwoPhaseCommitProtocol {
  private StorageActionManager action;
  private Map<EntityGroupInfo, List<Action>> transcations;
  private Map<String, Publisher> pubs = new HashMap<String, Publisher>();
  private Map<String, List<MessageID>> preparedMessage = new HashMap<String, List<MessageID>>();
  private static TwoPhaseCommitProtocol instance;

  private TwoPhaseCommit(StorageActionManager action) {
    this.action = action;
  }

  /**
   * single mode.
   * 
   * @param action
   * @return
   */
  public static TwoPhaseCommitProtocol getInstance(StorageActionManager action) {
    if (instance == null) {
      instance = new TwoPhaseCommit(action);
    }
    return instance;
  }

  /**
   * 
   * @see org.apache.wasp.fserver.TwoPhaseCommitProtocol#rollback()
   */
  @Override
  public void rollback() {
    for (Map.Entry<String, List<MessageID>> entry : preparedMessage.entrySet()) {
      for (MessageID messageId : entry.getValue()) {
        while (!pubs.get(entry.getKey()).delete(messageId)) {
        }
      }
    }
    preparedMessage.clear();
    transcations = null;
  }

  /**
   * 
   * @see org.apache.wasp.fserver.TwoPhaseCommitProtocol#commit()
   */
  @Override
  public void commit() {
    for (Map.Entry<String, List<MessageID>> entry : preparedMessage.entrySet()) {
      for (MessageID messageId : entry.getValue()) {
        while (!pubs.get(entry.getKey()).commit(messageId)) {
        }
      }
    }
    preparedMessage.clear();
  }

  /**
   * 
   * @see org.apache.wasp.fserver.TwoPhaseCommitProtocol#preCommit()
   */
  @Override
  public boolean prepare() {
    for (Map.Entry<EntityGroupInfo, List<Action>> entry : transcations
        .entrySet()) {
      Publisher pub = new Publisher(entry.getKey(), action);
      String entityGroupName = entry.getKey().getEntityGroupNameAsString();
      pubs.put(entityGroupName, pub);
      for (Action action : entry.getValue()) {
        try {
          List<MessageID> messages = preparedMessage.get(entityGroupName);
          if (messages != null) {
            messages = new ArrayList<MessageID>();
          }
          messages.add(pub.doAsynchronous((Message) action));
          preparedMessage.put(entityGroupName, messages);
        } catch (IOException e) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * 
   * @see org.apache.wasp.fserver.TwoPhaseCommitProtocol#submit(java.util.Map)
   */
  @Override
  public synchronized boolean submit(
      Map<EntityGroupInfo, List<Action>> transcations) {
    this.transcations = transcations;
    if (prepare()) {
      this.commit();
      return true;
    } else {
      this.rollback();
      return false;
    }
  }
}

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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.wasp.EntityGroupInfo;import com.alibaba.wasp.FConstants;import com.alibaba.wasp.fserver.EntityGroup;import com.alibaba.wasp.fserver.LeaseException;import com.alibaba.wasp.fserver.LeaseListener;import com.alibaba.wasp.fserver.OnlineEntityGroups;import com.alibaba.wasp.storage.StorageTableNotFoundException;import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.fserver.EntityGroup;
import com.alibaba.wasp.fserver.LeaseException;
import com.alibaba.wasp.fserver.LeaseListener;
import com.alibaba.wasp.fserver.Leases;
import com.alibaba.wasp.fserver.Leases.LeaseStillHeldException;
import com.alibaba.wasp.fserver.OnlineEntityGroups;
import com.alibaba.wasp.storage.StorageTableNotFoundException;

/**
 * Message broker, it receives message from message queue and send message to
 * subscriber.
 * 
 */
public class MessageBroker extends Thread implements Closeable, Broker {

  public static final Log LOG = LogFactory.getLog(MessageBroker.class);

  /** entityGroup server **/
  private final OnlineEntityGroups service;

  /** for register **/
  protected final ConcurrentHashMap<String, Subscriber> subscribers = new ConcurrentHashMap<String, Subscriber>();

  private boolean closed = false;
  private Leases leases;
  private final int subscriberLeaseTimeoutPeriod;
  private final RenewRunnable renew;

  /**
   * @param service
   *          entityGroup server instance
   * @param conf
   *          configuration
   */
  public MessageBroker(OnlineEntityGroups service, Configuration conf) {
    this.service = service;
    this.leases = new Leases(conf.getInt(FConstants.THREAD_WAKE_FREQUENCY,
        10 * 1000));
    this.subscriberLeaseTimeoutPeriod = conf.getInt(
        FConstants.WASP_FSEVER_SUBSCRIBER_TIMEOUT_PERIOD,
        FConstants.DEFAULT_WASP_FSEVER_SUBSCRIBER_TIMEOUT_PERIOD);
    this.renew = new RenewRunnable();
    this.renew.start();
  }

  private class SubscriberListener implements LeaseListener {

    private Subscriber subscriber;

    /**
     * @param subscriber
     */
    public SubscriberListener(Subscriber subscriber) {
      super();
      this.subscriber = subscriber;
    }

    /**
     * @see com.alibaba.wasp.fserver.LeaseListener#leaseExpired()
     */
    @Override
    public void leaseExpired() {
      Subscriber s = subscribers.remove(this.subscriber.getEntityGroup()
          .getEntityGroupNameAsString());
      LOG.info("Subscriber " + s.getEntityGroup().getEntityGroupNameAsString()
          + " lease expired");
    }
  }

  private class RenewRunnable extends Thread implements Closeable {
    private boolean closed = false;

    @Override
    public void run() {

      while (!closed) {
        // renew lease,if some entityGroups hang then remove it by lease
        // manager.
        Collection<EntityGroup> entityGroups;
        try {
          entityGroups = service.getOnlineEntityGroups();
          for (EntityGroup entityGroup : entityGroups) {
            leases.renewLease(entityGroup.getEntityGroupNameAsString());
          }
          Thread.sleep(60 * 1000);
        } catch (Exception e) {
          LOG.error("RenewRunnable running.", e);
          try {
            close();
          } catch (IOException e1) {
            LOG.error("RenewRunnable doClosing.", e1);
          }
          return;
        }
      }
    }

    /**
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
      closed = true;
    }
  }

  /**
   * @see
   */
  @Override
  public void register(Subscriber subscriber) throws LeaseStillHeldException {
    String subscriberName = subscriber.getEntityGroup()
        .getEntityGroupNameAsString();
    Subscriber existing = subscribers.putIfAbsent(subscriberName, subscriber);
    if (existing == null) {
      this.leases
          .createLease(subscriberName, this.subscriberLeaseTimeoutPeriod,
              new SubscriberListener(subscriber));
    }
  }

  /**
   * @see
   */
  @Override
  public void remove(Subscriber subscriber) throws LeaseException {
    String sbuscriberName = subscriber.getEntityGroup()
        .getEntityGroupNameAsString();
    subscribers.remove(sbuscriberName);
    leases.removeLease(sbuscriberName);
  }

  /**
   * @see
   */
  @Override
  public void remove(EntityGroupInfo entityGroupInfo) throws LeaseException {
    String sbuscriberName = entityGroupInfo.getEntityGroupNameAsString();
    subscribers.remove(sbuscriberName);
    leases.removeLease(sbuscriberName);
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run() {
    while (!closed) {
      try {
        // fetch message from message queue then notify entityGroup.
        selectCurrentMessages();

        // wait for 500ms
        Thread.sleep(500);
      } catch (Exception e) {
        LOG.error("Messagebroker running.", e);
        try {
          close();
        } catch (IOException e1) {
          LOG.error("Messagebroker doClosing.", e1);
        }
        return;
      }
    }
  }

  /**
   * Fetch messages which were subscribed.
   * 
   * @throws HBaseTableNotFoundException
   * @throws IOException
   */
  private void selectCurrentMessages() throws StorageTableNotFoundException,
      IOException {
    Iterator<Subscriber> iter = subscribers.values().iterator();
    while (iter.hasNext()) {
      Subscriber subscriber = iter.next();

      List<Message> messages = subscriber.receive();
      for (Message message : messages) {
        subscriber.setCurrentMessageRow(message.getMessageID().getMessageId());
        subscriber.doAsynchronous(message);
      }
    }
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    this.renew.close();
    this.interrupt();
    this.renew.interrupt();
  }
}
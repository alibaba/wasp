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
package com.alibaba.wasp.fserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.concurrent.*;

/**
 * Compact entityGroup on request and then run split if appropriate
 */
public class SplitThread{
  static final Log LOG = LogFactory.getLog(SplitThread.class);

  private final FServer server;
  private final Configuration conf;

  private final ThreadPoolExecutor splits;

  /**
   * Splitting should not take place if the total number of entityGroups exceed
   * this. This is not a hard limit to the number of entityGroups but it is a
   * guideline to stop splitting after number of online entityGroups is greater
   * than this.
   */
  private int entityGroupSplitLimit;

  /** @param server */
  SplitThread(FServer server) {
    super();
    this.server = server;
    this.conf = server.getConfiguration();
    this.entityGroupSplitLimit = conf.getInt(
        "wasp.fserver.entityGroupSplitLimit", Integer.MAX_VALUE);

    int splitThreads = conf.getInt("wasp.fserver.thread.split", 1);

    final String n = Thread.currentThread().getName();

    this.splits = (ThreadPoolExecutor) Executors.newFixedThreadPool(
        splitThreads, new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(n + "-splits-" + System.currentTimeMillis());
            return t;
          }
        });
  }

  @Override
  public String toString() {
    return "split_queue=" + splits.getQueue().size();
  }

  public String dumpQueue() {
    StringBuffer queueLists = new StringBuffer();
    queueLists.append("Split Queue dump:\n");
    queueLists.append("  Split Queue:\n");
    BlockingQueue<Runnable> lq = splits.getQueue();
    Iterator<Runnable> it = lq.iterator();
    while (it.hasNext()) {
      queueLists.append("    " + it.next().toString());
      queueLists.append("\n");
    }

    return queueLists.toString();
  }

  public synchronized void requestSplit(final EntityGroup eg, byte[] midKey) {
    if (midKey == null) {
      LOG.debug("entityGroup " + eg.getEntityGroupNameAsString()
          + " not splittable because midkey=null");
      return;
    }
    try {
      this.splits.execute(new SplitRequest(eg, midKey, this.server));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Split requested for " + eg + ".  " + this);
      }
    } catch (RejectedExecutionException ree) {
      LOG.info("Could not execute split for " + eg, ree);
    }
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    splits.shutdown();
  }

  private void waitFor(ThreadPoolExecutor t, String name) {
    boolean done = false;
    while (!done) {
      try {
        done = t.awaitTermination(60, TimeUnit.SECONDS);
        LOG.debug("Waiting for " + name + " to finish...");
      } catch (InterruptedException ie) {
        LOG.debug("Interrupted waiting for " + name + " to finish...");
      }
    }
  }

  void join() {
    waitFor(splits, "Split Thread");
  }

  /**
   * @return the entityGroupSplitLimit
   */
  public int getEntityGroupSplitLimit() {
    return this.entityGroupSplitLimit;
  }
}

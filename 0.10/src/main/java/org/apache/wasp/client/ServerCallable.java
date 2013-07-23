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

package org.apache.wasp.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.wasp.EntityGroupLocation;
import org.apache.wasp.FConstants;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Abstract class that implements {@link java.util.concurrent.Callable}.
 * Implementation stipulates return type and method we actually invoke on remote
 * Server. Usually used inside a try/catch that fields usual connection failures
 * all wrapped up in a retry loop.
 * <p>
 * Call {@link #connect(boolean)} to connect to server hosting entityGroup that
 * contains the passed row in the passed table before invoking {@link #call()}.
 * 
 * @see FConnection#getFServerWithoutRetries(ServerCallable)
 * @param <T>
 *          the class that the ServerCallable handles
 */
public abstract class ServerCallable<T> implements Callable<T> {
  public final Log LOG = LogFactory.getLog(this.getClass());
  protected final FConnection connection;
  protected final byte[] tableName;
  protected final byte[] row;
  protected EntityGroupLocation location;
  protected ClientProtocol server;
  protected int callTimeout;
  protected long startTime, endTime;
  protected long pause;

  /**
   * @param connection
   *          Connection to use.
   * @param tableName
   *          Table name to which <code>row</code> belongs.
   * @param row
   *          The row we want in <code>tableName</code>.
   */
  public ServerCallable(FConnection connection, byte[] tableName, byte[] row) {
    this(connection, tableName, row,
        FConstants.DEFAULT_WASP_CLIENT_OPERATION_TIMEOUT);
  }

  public ServerCallable(FConnection connection, byte[] tableName, byte[] row,
      int callTimeout) {
    this.connection = connection;
    this.tableName = tableName;
    this.row = row;
    this.callTimeout = callTimeout;
    this.pause = connection.getConfiguration().getLong(FConstants.WASP_CLIENT_PAUSE,
          FConstants.DEFAULT_WASP_CLIENT_PAUSE);

  }

  /**
   * Connect to the server hosting entityGroup with row from table name.
   * 
   * @param reload
   *          Set this to true if connection should re-find the entityGroup
   * @throws java.io.IOException
   *           e
   */
  public void connect(final boolean reload) throws IOException {
    this.location = connection.getEntityGroupLocation(tableName, row, reload);
    this.server = connection.getClient(location.getHostname(),
        location.getPort());
  }

  public void beforeCall() {
    this.startTime = System.currentTimeMillis();
  }

  public void afterCall() {
    this.endTime = System.currentTimeMillis();
  }

  public void shouldRetry(Throwable throwable) throws IOException {
    if (this.callTimeout != FConstants.DEFAULT_WASP_CLIENT_OPERATION_TIMEOUT)
      if (throwable instanceof SocketTimeoutException
          || (this.endTime - this.startTime > this.callTimeout)) {
        throw (SocketTimeoutException) (SocketTimeoutException) new SocketTimeoutException(
            "Call to access row '" + Bytes.toString(row) + "' on table '"
                + Bytes.toString(tableName)
                + "' failed on socket timeout exception: " + throwable)
            .initCause(throwable);
      } else {
        this.callTimeout = ((int) (this.endTime - this.startTime));
      }
  }

  /**
   * @return {@link FConnection} instance used by this Callable.
   */
  FConnection getConnection() {
    return this.connection;
  }

  /**
   * Run this instance with retries, timed waits, and refinds of missing
   * entityGroups.
   * 
   * @return an object of type T
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   * @throws RuntimeException
   *           other unspecified error
   */
  public T withRetries() throws IOException, RuntimeException {
    Configuration c = getConnection().getConfiguration();
    final int numRetries = c.getInt(FConstants.WASP_CLIENT_RETRIES_NUMBER,
        FConstants.DEFAULT_WASP_CLIENT_RETRIES_NUMBER);
    List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions = new ArrayList<RetriesExhaustedException.ThrowableWithExtraContext>();
    for (int tries = 0; tries < numRetries; tries++) {
      try {
        beforeCall();
        connect(tries != 0);
        return call();
      } catch (Throwable t) {
        shouldRetry(t);
        t = translateException(t);
        if (t instanceof SocketTimeoutException
            || t instanceof ConnectException
            || t instanceof RetriesExhaustedException) {
          // if thrown these exceptions, we clear all the cache entries that
          // map to that slow/dead server; otherwise, let cache miss and ask
          // .FMETA. again to find the new location
          EntityGroupLocation egl = location;
          if (egl != null) {
            getConnection().clearCaches(egl.getHostnamePort());
          }
        }
        RetriesExhaustedException.ThrowableWithExtraContext qt = new RetriesExhaustedException.ThrowableWithExtraContext(
            t, System.currentTimeMillis(), toString());
        exceptions.add(qt);

        long pauseTime = ConnectionUtils.getPauseTime(this.pause, tries);
        LOG.info("withRetries attempt " + tries + " of " + numRetries
            + " failed; retrying after sleep of " + pauseTime);
        try {
          Thread.sleep(pauseTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(
              "Thread was interrupted while trying to connect to FServer.", e);
        }

        if (tries == numRetries - 1) {
          throw new RetriesExhaustedException(tries, exceptions);
        }
      } finally {
        afterCall();
      }
    }
    return null;
  }

  /**
   * Run this instance against the server once.
   * 
   * @return an object of type T
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   * @throws RuntimeException
   *           other unspecified error
   */
  public T withoutRetries() throws IOException, RuntimeException {
    try {
      beforeCall();
      connect(false);
      return call();
    } catch (Throwable t) {
      Throwable t2 = translateException(t);
      if (t2 instanceof IOException) {
        throw (IOException) t2;
      } else {
        throw new RuntimeException(t2);
      }
    } finally {
      afterCall();
    }
  }

  private static Throwable translateException(Throwable t) throws IOException {
    if (t instanceof UndeclaredThrowableException) {
      t = t.getCause();
    }
    if (t instanceof RemoteException) {
      t = ((RemoteException) t).unwrapRemoteException();
    }
    if (t instanceof DoNotRetryIOException) {
      throw (DoNotRetryIOException) t;
    }
    return t;
  }
}

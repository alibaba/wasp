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

import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.jdbc.JdbcException;
import com.alibaba.wasp.util.New;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * A simple standalone JDBC connection pool.
 * 
 */
public class JdbcConnectionPool implements DataSource, ConnectionEventListener {

  private final ConnectionPoolDataSource dataSource;
  private final ArrayList<PooledConnection> recycledConnections = New
      .arrayList();
  private PrintWriter logWriter;
  private int maxConnections;
  private int timeout;
  private AtomicInteger activeConnections = new AtomicInteger(0);
  private boolean isDisposed;
  private Configuration conf;

  protected JdbcConnectionPool(ConnectionPoolDataSource dataSource,
      Configuration conf) {
    this.dataSource = dataSource;
    this.conf = conf;
    this.maxConnections = conf.getInt(FConstants.JDBC_POOL_MAX_CONNECTIONS,
        FConstants.DEFAULT_JDBC_POOL_MAX_CONNECTIONS);
    this.timeout = conf.getInt(FConstants.JDBC_CONNECTION_TIMEOUT,
        FConstants.DEFAULT_JDBC_CONNECTION_TIMEOUT);
    if (dataSource != null) {
      try {
        logWriter = dataSource.getLogWriter();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  /**
   * Constructs a new connection pool.
   * 
   * @param dataSource
   *          the data source to create connections
   * @return the connection pool
   */
  public static JdbcConnectionPool create(ConnectionPoolDataSource dataSource,
      Configuration conf) {
    return new JdbcConnectionPool(dataSource, conf);
  }

  /**
   * Constructs a new connection pool for wasp.
   * 
   * @param url
   *          the database URL of the wasp connection
   * @param user
   *          the user name
   * @param password
   *          the password
   * @return the connection pool
   */
  public static JdbcConnectionPool create(String url, String user,
      String password, Configuration conf) {
    JdbcDataSource ds = new JdbcDataSource(conf);
    ds.setURL(url);
    ds.setUser(user);
    ds.setPassword(password);
    return new JdbcConnectionPool(ds, conf);
  }

  /**
   * Sets the maximum number of connections to use from now on. The default
   * value is 20 connections.
   * 
   * @param max
   *          the maximum number of connections
   */
  public synchronized void setMaxConnections(int max) {
    if (max < 1) {
      throw new IllegalArgumentException("Invalid maxConnections value: " + max);
    }
    this.maxConnections = max;
    // notify waiting threads if the value was increased
    notifyAll();
  }

  /**
   * Gets the maximum number of connections to use.
   * 
   * @return the max the maximum number of connections
   */
  public synchronized int getMaxConnections() {
    return maxConnections;
  }

  /**
   * Gets the maximum time in seconds to wait for a free connection.
   * 
   * @return the timeout in seconds
   */
  public synchronized int getLoginTimeout() {
    return timeout;
  }

  /**
   * Sets the maximum time in seconds to wait for a free connection. The default
   * timeout is 30 seconds. Calling this method with the value 0 will set the
   * timeout to the default value.
   * 
   * @param seconds
   *          the timeout, 0 meaning the default
   */
  public synchronized void setLoginTimeout(int seconds) {
    if (seconds == 0) {
      seconds = FConstants.DEFAULT_JDBC_CONNECTION_TIMEOUT;
    }
    this.timeout = seconds;
  }

  /**
   * Closes all unused pooled connections. Exceptions while closing are written
   * to the log stream (if set).
   */
  public synchronized void dispose() {
    if (isDisposed) {
      return;
    }
    isDisposed = true;
    ArrayList<PooledConnection> list = recycledConnections;
    for (int i = 0, size = list.size(); i < size; i++) {
      closeConnection(list.get(i));
    }
  }

  /**
   * Retrieves a connection from the connection pool. If
   * <code>maxConnections</code> connections are already in use, the method
   * waits until a connection becomes available or <code>timeout</code> seconds
   * elapsed. When the application is finished using the connection, it must
   * close it in order to return it to the pool. If no connection becomes
   * available within the given timeout, an exception with SQL state 08001 and
   * vendor code 8001 is thrown.
   * 
   * @return a new Connection object.
   * @throws java.sql.SQLException
   *           when a new connection could not be established, or a timeout
   *           occurred
   */
  public Connection getConnection() throws SQLException {
    long max = System.currentTimeMillis() + timeout * 1000;
    do {
      synchronized (this) {
        if (activeConnections.get() < maxConnections) {
          return getConnectionNow();
        }
        try {
          wait(1000);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    } while (System.currentTimeMillis() <= max);
    throw new SQLException("get connection timeout. activeConnections=" + activeConnections + ". maxConnections="
        + maxConnections, "08001", 8001);
  }

  /**
   * INTERNAL
   */
  public Connection getConnection(String user, String password) {
    throw new UnsupportedOperationException();
  }

  private Connection getConnectionNow() throws SQLException {
    if (isDisposed) {
      throw new IllegalStateException("Connection pool has been disposed.");
    }
    PooledConnection pc;
    if (!recycledConnections.isEmpty()) {
      pc = recycledConnections.remove(recycledConnections.size() - 1);
    } else {
      pc = dataSource.getPooledConnection();
    }
    Connection conn = pc.getConnection();
    activeConnections.incrementAndGet();
    pc.addConnectionEventListener(this);
    return conn;
  }

  /**
   * This method usually puts the connection back into the pool. There are some
   * exceptions: if the pool is disposed, the connection is disposed as well. If
   * the pool is full, the connection is closed.
   * 
   * @param pc
   *          the pooled connection
   */
  synchronized void recycleConnection(PooledConnection pc) {
    if (activeConnections.get() <= 0) {
      throw new AssertionError();
    }
    activeConnections.decrementAndGet();
    if (!isDisposed && activeConnections.get() < maxConnections) {
      recycledConnections.add(pc);
    } else {
      closeConnection(pc);
    }
    if (activeConnections.get() >= maxConnections - 1) {
      notifyAll();
    }
  }

  private void closeConnection(PooledConnection pc) {
    try {
      pc.close();
    } catch (SQLException e) {
      if (logWriter != null) {
        e.printStackTrace(logWriter);
      }
    }
  }

  /**
   * INTERNAL
   */
  public void connectionClosed(ConnectionEvent event) {
    PooledConnection pc = (PooledConnection) event.getSource();
    pc.removeConnectionEventListener(this);
    recycleConnection(pc);
  }

  /**
   * INTERNAL
   */
  public void connectionErrorOccurred(ConnectionEvent event) {
    // not used
  }

  /**
   * Returns the number of active (open) connections of this pool. This is the
   * number of <code>Connection</code> objects that have been issued by
   * getConnection() for which <code>Connection.close()</code> has not yet been
   * called.
   * 
   * @return the number of active connections.
   */
  public int getActiveConnections() {
    return activeConnections.get();
  }

  /**
   * INTERNAL
   */
  public PrintWriter getLogWriter() {
    return logWriter;
  }

  /**
   * INTERNAL
   */
  public void setLogWriter(PrintWriter logWriter) {
    this.logWriter = logWriter;
  }

  /**
   * [Not supported] Return an object of this class if possible.
   * 
   * @param iface
   *          the class
   */
  // ## Java 1.6 ##
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw JdbcException.getUnsupportedException("unwrap");
  }

  // */

  /**
   * [Not supported] Checks if unwrap can return an object of this class.
   * 
   * @param iface
   *          the class
   */
  // ## Java 1.6 ##
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw JdbcException.getUnsupportedException("isWrapperFor");
  }

  // */

  /**
   * [Not supported]
   */
  // ## Java 1.7 ##
  public Logger getParentLogger() {
    throw new NotImplementedException();
  }

}

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
import com.alibaba.wasp.jdbc.JdbcConnection;
import com.alibaba.wasp.jdbc.JdbcException;
import com.alibaba.wasp.util.JdbcUtils;
import com.alibaba.wasp.util.New;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * This class provides support for distributed transactions. An application
 * developer usually does not use this interface. It is used by the transaction
 * manager internally.
 */
public class JdbcXAConnection implements XAConnection, XAResource {

  // this connection is kept open as long as the XAConnection is alive
  private JdbcConnection physicalConn;

  // this connection is replaced whenever getConnection is called
  private volatile Connection handleConn;
  private final ArrayList<ConnectionEventListener> listeners = New.arrayList();
  private Xid currentTransaction;
  private boolean prepared;

  static {
    com.alibaba.wasp.jdbc.Driver.load();
  }

  JdbcXAConnection(JdbcConnection physicalConn) {
    this.physicalConn = physicalConn;
  }

  /**
   * Get the XAResource object.
   * 
   * @return itself
   */
  public XAResource getXAResource() {
    return this;
  }

  /**
   * Close the physical connection. This method is usually called by the
   * connection pool.
   * 
   * @throws java.sql.SQLException
   */
  public void close() throws SQLException {
    Connection lastHandle = handleConn;
    if (lastHandle != null) {
      listeners.clear();
      lastHandle.close();
    }
    if (physicalConn != null) {
      try {
        physicalConn.close();
      } finally {
        physicalConn = null;
      }
    }
  }

  /**
   * Get a connection that is a handle to the physical connection. This method
   * is usually called by the connection pool. This method closes the last
   * connection handle if one exists.
   * 
   * @return the connection
   */
  public Connection getConnection() throws SQLException {
    Connection lastHandle = handleConn;
    if (lastHandle != null) {
      lastHandle.close();
    }
    // this will ensure the rollback command is cached
    //physicalConn.rollback();
    handleConn = new PooledJdbcConnection(physicalConn);
    return handleConn;
  }

  /**
   * Register a new listener for the connection.
   * 
   * @param listener
   *          the event listener
   */
  public void addConnectionEventListener(ConnectionEventListener listener) {
    listeners.add(listener);
  }

  /**
   * Remove the event listener.
   * 
   * @param listener
   *          the event listener
   */
  public void removeConnectionEventListener(ConnectionEventListener listener) {
    listeners.remove(listener);
  }

  /**
   * INTERNAL
   */
  void closedHandle() {
    ConnectionEvent event = new ConnectionEvent(this);
    // go backward so that a listener can remove itself
    // (otherwise we need to clone the list)
    for (int i = listeners.size() - 1; i >= 0; i--) {
      ConnectionEventListener listener = listeners.get(i);
      listener.connectionClosed(event);
    }
    handleConn = null;
  }

  /**
   * Get the transaction timeout.
   * 
   * @return 0
   */
  public int getTransactionTimeout() {
    return 0;
  }

  /**
   * Set the transaction timeout.
   * 
   * @param seconds
   *          ignored
   * @return false
   */
  public boolean setTransactionTimeout(int seconds) {
    return false;
  }

  /**
   * Checks if this is the same XAResource.
   * 
   * @param xares
   *          the other object
   * @return true if this is the same object
   */
  public boolean isSameRM(XAResource xares) {
    return xares == this;
  }

  /**
   * Get the list of prepared transaction branches. This method is called by the
   * transaction manager during recovery.
   * 
   * @param flag
   *          TMSTARTRSCAN, TMENDRSCAN, or TMNOFLAGS. If no other flags are set,
   *          TMNOFLAGS must be used.
   * @return zero or more Xid objects
   * @throws javax.transaction.xa.XAException
   */
  public Xid[] recover(int flag) throws XAException {
    checkOpen();
    Statement stat = null;
    try {
      stat = physicalConn.createStatement();
      ResultSet rs = stat
          .executeQuery("SELECT * FROM INFORMATION_SCHEMA.IN_DOUBT ORDER BY TRANSACTION");
      ArrayList<Xid> list = New.arrayList();
      while (rs.next()) {
        String tid = rs.getString("TRANSACTION");
        Xid xid = new JdbcXid(tid);
        list.add(xid);
      }
      rs.close();
      Xid[] result = new Xid[list.size()];
      list.toArray(result);
      if (list.size() > 0) {
        prepared = true;
      }
      return result;
    } catch (SQLException e) {
      XAException xa = new XAException(XAException.XAER_RMERR);
      xa.initCause(e);
      throw xa;
    } finally {
      JdbcUtils.closeSilently(stat);
    }
  }

  /**
   * Prepare a transaction.
   * 
   * @param xid
   *          the transaction id
   * @return XA_OK
   * @throws javax.transaction.xa.XAException
   */
  public int prepare(Xid xid) throws XAException {
    checkOpen();
    if (!currentTransaction.equals(xid)) {
      throw new XAException(XAException.XAER_INVAL);
    }
    Statement stat = null;
    try {
      stat = physicalConn.createStatement();
      stat.execute("PREPARE COMMIT " + JdbcXid.toString(xid));
      prepared = true;
    } catch (SQLException e) {
      throw convertException(e);
    } finally {
      JdbcUtils.closeSilently(stat);
    }
    return XA_OK;
  }

  /**
   * Forget a transaction. This method does not have an effect for this
   * database.
   * 
   * @param xid
   *          the transaction id
   */
  public void forget(Xid xid) {
    prepared = false;
  }

  /**
   * Roll back a transaction.
   * 
   * @param xid
   *          the transaction id
   * @throws javax.transaction.xa.XAException
   */
  public void rollback(Xid xid) throws XAException {
    try {
      physicalConn.rollback();
      physicalConn.setAutoCommit(true);
      if (prepared) {
        Statement stat = null;
        try {
          stat = physicalConn.createStatement();
          stat.execute("ROLLBACK TRANSACTION " + JdbcXid.toString(xid));
        } finally {
          JdbcUtils.closeSilently(stat);
        }
        prepared = false;
      }
    } catch (SQLException e) {
      throw convertException(e);
    }
    currentTransaction = null;
  }

  /**
   * End a transaction.
   * 
   * @param xid
   *          the transaction id
   * @param flags
   *          TMSUCCESS, TMFAIL, or TMSUSPEND
   * @throws javax.transaction.xa.XAException
   */
  public void end(Xid xid, int flags) throws XAException {
    // TODO transaction end: implement this method
    if (flags == TMSUSPEND) {
      return;
    }
    if (!currentTransaction.equals(xid)) {
      throw new XAException(XAException.XAER_OUTSIDE);
    }
    prepared = false;
  }

  /**
   * Start or continue to work on a transaction.
   * 
   * @param xid
   *          the transaction id
   * @param flags
   *          TMNOFLAGS, TMJOIN, or TMRESUME
   * @throws javax.transaction.xa.XAException
   */
  public void start(Xid xid, int flags) throws XAException {
    if (flags == TMRESUME) {
      return;
    }
    if (flags == TMJOIN) {
      if (currentTransaction != null && !currentTransaction.equals(xid)) {
        throw new XAException(XAException.XAER_RMERR);
      }
    } else if (currentTransaction != null) {
      throw new XAException(XAException.XAER_NOTA);
    }
    try {
      physicalConn.setAutoCommit(false);
    } catch (SQLException e) {
      throw convertException(e);
    }
    currentTransaction = xid;
    prepared = false;
  }

  /**
   * Commit a transaction.
   * 
   * @param xid
   *          the transaction id
   * @param onePhase
   *          use a one-phase protocol if true
   * @throws javax.transaction.xa.XAException
   */
  public void commit(Xid xid, boolean onePhase) throws XAException {
    Statement stat = null;
    try {
      if (onePhase) {
        physicalConn.commit();
      } else {
        stat = physicalConn.createStatement();
        stat.execute("COMMIT TRANSACTION " + JdbcXid.toString(xid));
        prepared = false;
      }
      physicalConn.setAutoCommit(true);
    } catch (SQLException e) {
      throw convertException(e);
    } finally {
      JdbcUtils.closeSilently(stat);
    }
    currentTransaction = null;
  }

  /**
   * [Not supported] Add a statement event listener.
   * 
   * @param listener
   *          the new statement event listener
   */
  // ## Java 1.6 ##
  public void addStatementEventListener(StatementEventListener listener) {
    throw new UnsupportedOperationException();
  }


  /**
   * [Not supported] Remove a statement event listener.
   * 
   * @param listener
   *          the statement event listener
   */
  public void removeStatementEventListener(StatementEventListener listener) {
    throw new UnsupportedOperationException();
  }

  /**
   * INTERNAL
   */
  public String toString() {
    return "XAConnection : " + physicalConn;
  }

  private static XAException convertException(SQLException e) {
    XAException xa = new XAException(e.getMessage());
    xa.initCause(e);
    return xa;
  }

  private static String quoteFlags(int flags) {
    StringBuilder buff = new StringBuilder();
    if ((flags & XAResource.TMENDRSCAN) != 0) {
      buff.append("|XAResource.TMENDRSCAN");
    }
    if ((flags & XAResource.TMFAIL) != 0) {
      buff.append("|XAResource.TMFAIL");
    }
    if ((flags & XAResource.TMJOIN) != 0) {
      buff.append("|XAResource.TMJOIN");
    }
    if ((flags & XAResource.TMONEPHASE) != 0) {
      buff.append("|XAResource.TMONEPHASE");
    }
    if ((flags & XAResource.TMRESUME) != 0) {
      buff.append("|XAResource.TMRESUME");
    }
    if ((flags & XAResource.TMSTARTRSCAN) != 0) {
      buff.append("|XAResource.TMSTARTRSCAN");
    }
    if ((flags & XAResource.TMSUCCESS) != 0) {
      buff.append("|XAResource.TMSUCCESS");
    }
    if ((flags & XAResource.TMSUSPEND) != 0) {
      buff.append("|XAResource.TMSUSPEND");
    }
    if ((flags & XAResource.XA_RDONLY) != 0) {
      buff.append("|XAResource.XA_RDONLY");
    }
    if (buff.length() == 0) {
      buff.append("|XAResource.TMNOFLAGS");
    }
    return buff.toString().substring(1);
  }

  private void checkOpen() throws XAException {
    if (physicalConn == null) {
      throw new XAException(XAException.XAER_RMERR);
    }
  }

  /**
   * A pooled connection.
   */
  class PooledJdbcConnection extends JdbcConnection {

    private boolean isClosed;

    public PooledJdbcConnection(JdbcConnection conn) throws SQLException {
      super(conn);
    }

    public synchronized void close() throws SQLException {
      if (!isClosed) {
        try {
          //rollback();
          setAutoCommit(true);
        } catch (SQLException e) {
          // ignore
        }
        closedHandle();
        isClosed = true;
      }
    }

    public synchronized boolean isClosed() throws SQLException {
      return isClosed || super.isClosed();
    }

    protected synchronized void checkClosed(boolean write) {
      if (isClosed) {
        throw JdbcException.get(SQLErrorCode.OBJECT_CLOSED);
      }
      super.checkClosed(write);
    }

  }

}

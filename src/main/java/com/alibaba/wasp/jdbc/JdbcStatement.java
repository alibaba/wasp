/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.wasp.jdbc;

import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.SQLErrorCode;
import com.alibaba.wasp.jdbc.command.CommandInterface;
import com.alibaba.wasp.jdbc.result.JdbcResultSet;
import com.alibaba.wasp.jdbc.result.ResultInterface;
import com.alibaba.wasp.session.ExecuteSession;
import com.alibaba.wasp.session.SessionFactory;
import com.alibaba.wasp.util.New;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Represents a statement.
 */
public class JdbcStatement implements Statement {

  private Log log = LogFactory.getLog(JdbcStatement.class);
  protected JdbcConnection conn;
  protected ExecuteSession session;
  protected JdbcResultSet resultSet;
  protected int maxRows;
  protected int fetchSize;
  protected int updateCount;
  public final int resultSetType;
  protected final int resultSetConcurrency;
  protected final boolean closedByResultSet;
  private int lastExecutedCommandType;
  private ArrayList<String> batchCommands;
  private Configuration conf;
  private boolean autoCommit;

  JdbcStatement(JdbcConnection conn, int resultSetType,
      int resultSetConcurrency, boolean closeWithResultSet)  {
    this.conn = conn;
    this.session = SessionFactory.createExecuteSession();
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.closedByResultSet = closeWithResultSet;
    this.conf = conn.getConf();
    try {
      this.autoCommit = conn.getAutoCommit();
    } catch (SQLException e) {
      //TODO
    }
    this.fetchSize = conf.getInt(FConstants.WASP_JDBC_FETCHSIZE,
        FConstants.DEFAULT_WASP_JDBC_FETCHSIZE);
  }

  /**
   * Executes a query (select statement) and returns the result set. If another
   * result set exists for this statement, this will be closed (even if this
   * statement fails).
   * 
   * @param sql
   *          the SQL statement to execute
   * @return the result set
   */
  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    synchronized (session) {
      checkClosed();
      closeOldResultSet();
      CommandInterface command = conn.prepareCommand(sql, fetchSize, session);
      ResultInterface result = null;
      setExecutingStatement(command);
      try {
        result = command.executeQuery(maxRows);
        session.setSessionId(result.getSessionId());
      } finally {
        setExecutingStatement(null);
      }
      command.close();
      resultSet = new JdbcResultSet(conn, this, result, closedByResultSet);
    }
    return resultSet;
  }

  /**
   * Executes a statement (insert, update, delete, create, drop) and returns the
   * update count. If another result set exists for this statement, this will be
   * closed (even if this statement fails).
   * 
   * If auto commit is on, this statement will be committed. If the statement is
   * a DDL statement (create, drop, alter) and does not throw an exception, the
   * current transaction (if any) is committed after executing the statement.
   * 
   * @param sql
   *          the SQL statement
   * @return the update count (number of row affected by an insert, update or
   *         delete, or 0 if no rows or the statement was a create, drop, commit
   *         or rollback)
   * @throws java.sql.SQLException
   *           if a database error occurred or a select statement was executed
   */
  @Override
  public int executeUpdate(String sql) throws SQLException {
    return executeUpdateInternal(sql);
  }

  /**
   * Executes an arbitrary statement. If another result set exists for this
   * statement, this will be closed (even if this statement fails).
   * 
   * If the statement is a create or drop and does not throw an exception, the
   * current transaction (if any) is committed after executing the statement. If
   * auto commit is on, and the statement is not a select, this statement will
   * be committed.
   * 
   * @param sql
   *          the SQL statement to execute
   * @return true if a result set is available, false if not
   */
  @Override
  public boolean execute(String sql) throws SQLException {
    try {
      return executeInternal(sql);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the last result set produces by this statement.
   * 
   * @return the result set
   */
  @Override
  public ResultSet getResultSet() throws SQLException {
    checkClosed();
    return resultSet;
  }

  /**
   * Returns the last update count of this statement.
   * 
   * @return the update count (number of row affected by an insert, update or
   *         delete, or 0 if no rows or the statement was a create, drop, commit
   *         or rollback; -1 if the statement was a select).
   * @throws java.sql.SQLException
   *           if this object is closed or invalid
   */
  @Override
  public int getUpdateCount() throws SQLException {
    checkClosed();
    return updateCount;
  }

  /**
   * Closes this statement. All result sets that where created by this statement
   * become invalid after calling this method.
   */
  @Override
  public void close() throws SQLException {
    try {
      synchronized (session) {
        closeOldResultSet();
        if (conn != null) {
          conn = null;
        }
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the connection that created this object.
   * 
   * @return the connection
   */
  @Override
  public Connection getConnection() {
    return conn;
  }

  /**
   * Gets the first warning reported by calls on this object. This driver does
   * not support warnings, and will always return null.
   * 
   * @return null
   */
  @Override
  public SQLWarning getWarnings() throws SQLException {
    try {
      checkClosed();
      return null;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Clears all warnings. As this driver does not support warnings, this call is
   * ignored.
   */
  @Override
  public void clearWarnings() throws SQLException {
    try {
      checkClosed();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the name of the cursor. This call is ignored.
   * 
   * @param name
   *          ignored
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setCursorName(String name) throws SQLException {
    try {
      checkClosed();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the fetch direction. This call is ignored by this driver.
   * 
   * @param direction
   *          ignored
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setFetchDirection(int direction) throws SQLException {
    try {
      checkClosed();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the fetch direction.
   * 
   * @return FETCH_FORWARD
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public int getFetchDirection() throws SQLException {
    try {
      checkClosed();
      return ResultSet.FETCH_FORWARD;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the maximum number of rows for a ResultSet.
   * 
   * @return the number of rows where 0 means no limit
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public int getMaxRows() throws SQLException {
    try {
      checkClosed();
      return maxRows;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the maximum number of rows for a ResultSet.
   * 
   * @param maxRows
   *          the number of rows where 0 means no limit
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setMaxRows(int maxRows) throws SQLException {
    try {
      checkClosed();
      if (maxRows < 0) {
        throw JdbcException.getInvalidValueException("maxRows", maxRows);
      }
      this.maxRows = maxRows;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the number of rows suggested to read in one step. This value cannot be
   * higher than the maximum rows (setMaxRows) set by the statement or prepared
   * statement, otherwise an exception is throws. Setting the value to 0 will
   * set the default value. The default value can be changed using the system
   * property h2.serverResultSetFetchSize.
   * 
   * @param rows
   *          the number of rows
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setFetchSize(int rows) throws SQLException {
    try {
      checkClosed();
      if (rows < 0 || (rows > 0 && maxRows > 0 && rows > maxRows)) {
        throw JdbcException.getInvalidValueException("rows", rows);
      }
      if (rows == 0) {
        rows = conf.getInt(FConstants.WASP_JDBC_FETCHSIZE,
            FConstants.DEFAULT_WASP_JDBC_FETCHSIZE);
      }
      fetchSize = rows;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the number of rows suggested to read in one step.
   * 
   * @return the current fetch size
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public int getFetchSize() throws SQLException {
    try {
      checkClosed();
      return fetchSize;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the result set concurrency created by this object.
   * 
   * @return the concurrency
   */
  @Override
  public int getResultSetConcurrency() throws SQLException {
    try {
      checkClosed();
      return resultSetConcurrency;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the result set type.
   * 
   * @return the type
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public int getResultSetType() throws SQLException {
    try {
      checkClosed();
      return resultSetType;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the maximum number of bytes for a result set column.
   * 
   * @return always 0 for no limit
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public int getMaxFieldSize() throws SQLException {
    try {
      checkClosed();
      return 0;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the maximum number of bytes for a result set column. This method does
   * currently do nothing for this driver.
   * 
   * @param max
   *          the maximum size - ignored
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    try {
      checkClosed();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Enables or disables processing or JDBC escape syntax. See also
   * Connection.nativeSQL.
   * 
   * @param enable
   *          - true (default) or false (no conversion is attempted)
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    try {
      checkClosed();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Cancels a currently running statement. This method must be called from
   * within another thread than the execute method. Operations on large objects
   * are not interrupted, only operations that process many rows.
   * 
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void cancel() throws SQLException {
    throw JdbcException.getUnsupportedException("cancel");
  }

  /**
   * Gets the current query timeout in seconds. This method will return 0 if no
   * query timeout is set. The result is rounded to the next second. For
   * performance reasons, only the first call to this method will query the
   * database. If the query timeout was changed in another way than calling
   * setQueryTimeout, this method will always return the last value.
   * 
   * @return the timeout in seconds
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public int getQueryTimeout() throws SQLException {
    try {
      checkClosed();
      return conn.getQueryTimeout();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the current query timeout in seconds. Changing the value will affect
   * all statements of this connection. This method does not commit a
   * transaction, and rolling back a transaction does not affect this setting.
   * 
   * @param seconds
   *          the timeout in seconds - 0 means no timeout, values smaller 0 will
   *          throw an exception
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    try {
      checkClosed();
      if (seconds < 0) {
        throw JdbcException.getInvalidValueException("seconds", seconds);
      }
      conn.setQueryTimeout(seconds);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Adds a statement to the batch.
   * 
   * @param sql
   *          the SQL statement
   */
  @Override
  public void addBatch(String sql) throws SQLException {
    try {
      checkClosed();
      if (batchCommands == null) {
        batchCommands = New.arrayList();
      }
      batchCommands.add(sql);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Clears the batch.
   */
  @Override
  public void clearBatch() throws SQLException {
    checkClosed();
    batchCommands = null;
  }

  /**
   * Executes the batch. If one of the batched statements fails, this database
   * will continue.
   * 
   * @return the array of update counts
   */
  @Override
  public int[] executeBatch() throws SQLException {
    try {
      checkClosedForWrite();
      try {
        if (batchCommands == null) {
          batchCommands = New.arrayList();
        }
        int size = batchCommands.size();
        int[] result = new int[size];

        if(autoCommit) {
          throw new SQLException("batch only support without autoCommit mode");
        }

        closeOldResultSet();
        CommandInterface command = conn.prepareCommand(batchCommands, session);
        synchronized (session) {
          setExecutingStatement(command);
          try {
            updateCount = command.executeTransaction();
          } finally {
            setExecutingStatement(null);
          }
        }
        command.close();
        batchCommands = null;
        //wasp ensures all success or all failure
        for (int i = 0; i < result.length; i++) {
          result[i] = (updateCount == size) ? 1 : 0;
        }
        return result;
      } finally {
        afterWriting();
      }



    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Return a result set that contains the last generated auto-increment key for
   * this connection, if there was one. If no key was generated by the last
   * modification statement, then an empty result set is returned. The returned
   * result set only contains the data for the very last row.
   * 
   * @return the result set with one row and one column containing the key
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw JdbcException.getUnsupportedException("getGeneratedKeys");
  }

  /**
   * Moves to the next result set - however there is always only one result set.
   * This call also closes the current result set (if there is one). Returns
   * true if there is a next result set (that means - it always returns false).
   * 
   * @return false
   * @throws java.sql.SQLException
   *           if this object is closed.
   */
  @Override
  public boolean getMoreResults() throws SQLException {
    try {
      checkClosed();
      closeOldResultSet();
      return false;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Move to the next result set. This method always returns false.
   * 
   * @param current
   *          Statement.CLOSE_CURRENT_RESULT, Statement.KEEP_CURRENT_RESULT, or
   *          Statement.CLOSE_ALL_RESULTS
   * @return false
   */
  @Override
  public boolean getMoreResults(int current) throws SQLException {
    try {
      switch (current) {
      case Statement.CLOSE_CURRENT_RESULT:
      case Statement.CLOSE_ALL_RESULTS:
        checkClosed();
        closeOldResultSet();
        break;
      case Statement.KEEP_CURRENT_RESULT:
        // nothing to do
        break;
      default:
        throw JdbcException.getInvalidValueException("current", current);
      }
      return false;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Executes a statement and returns the update count. This method just calls
   * executeUpdate(String sql) internally. The method getGeneratedKeys supports
   * at most one columns and row.
   * 
   * @param sql
   *          the SQL statement
   * @param autoGeneratedKeys
   *          ignored
   * @return the update count (number of row affected by an insert, update or
   *         delete, or 0 if no rows or the statement was a create, drop, commit
   *         or rollback)
   * @throws java.sql.SQLException
   *           if a database error occurred or a select statement was executed
   */
  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys)
      throws SQLException {
    try {
      return executeUpdateInternal(sql);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Executes a statement and returns the update count. This method just calls
   * executeUpdate(String sql) internally. The method getGeneratedKeys supports
   * at most one columns and row.
   * 
   * @param sql
   *          the SQL statement
   * @param columnIndexes
   *          ignored
   * @return the update count (number of row affected by an insert, update or
   *         delete, or 0 if no rows or the statement was a create, drop, commit
   *         or rollback)
   * @throws java.sql.SQLException
   *           if a database error occurred or a select statement was executed
   */
  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    try {
      return executeUpdateInternal(sql);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Executes a statement and returns the update count. This method just calls
   * executeUpdate(String sql) internally. The method getGeneratedKeys supports
   * at most one columns and row.
   * 
   * @param sql
   *          the SQL statement
   * @param columnNames
   *          ignored
   * @return the update count (number of row affected by an insert, update or
   *         delete, or 0 if no rows or the statement was a create, drop, commit
   *         or rollback)
   * @throws java.sql.SQLException
   *           if a database error occurred or a select statement was executed
   */
  @Override
  public int executeUpdate(String sql, String[] columnNames)
      throws SQLException {
    try {
      return executeUpdateInternal(sql);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  private int executeUpdateInternal(String sql) throws SQLException {
    checkClosedForWrite();
    try {
      closeOldResultSet();
      CommandInterface command = conn.prepareCommand(sql, fetchSize, session);
      synchronized (session) {
        setExecutingStatement(command);
        try {
          updateCount = command.executeUpdate();
        } finally {
          setExecutingStatement(null);
        }
      }
      command.close();
      return updateCount;
    } finally {
      afterWriting();
    }
  }

  /**
   * Executes a statement and returns the update count. This method just calls
   * execute(String sql) internally. The method getGeneratedKeys supports at
   * most one columns and row.
   * 
   * @param sql
   *          the SQL statement
   * @param autoGeneratedKeys
   *          ignored
   * @return the update count (number of row affected by an insert, update or
   *         delete, or 0 if no rows or the statement was a create, drop, commit
   *         or rollback)
   * @throws java.sql.SQLException
   *           if a database error occurred or a select statement was executed
   */
  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    try {
      return executeInternal(sql);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  private boolean executeInternal(String sql) throws SQLException {
    checkClosedForWrite();
    try {
      closeOldResultSet();
      CommandInterface command = conn.prepareCommand(sql, fetchSize, session);
      boolean returnsResultSet;
      synchronized (session) {
        setExecutingStatement(command);
        try {
          if (command.isQuery()) {
            returnsResultSet = true;
            ResultInterface result = command.executeQuery(maxRows);
            resultSet = new JdbcResultSet(conn, this, result, closedByResultSet);
          } else {
            returnsResultSet = false;
            updateCount = command.executeUpdate();
          }
        } finally {
          setExecutingStatement(null);
        }
      }
      command.close();
      return returnsResultSet;
    } finally {
      afterWriting();
    }
  }

  /**
   * Executes a statement and returns the update count. This method just calls
   * execute(String sql) internally. The method getGeneratedKeys supports at
   * most one columns and row.
   * 
   * @param sql
   *          the SQL statement
   * @param columnIndexes
   *          ignored
   * @return the update count (number of row affected by an insert, update or
   *         delete, or 0 if no rows or the statement was a create, drop, commit
   *         or rollback)
   * @throws java.sql.SQLException
   *           if a database error occurred or a select statement was executed
   */
  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    try {
      return executeInternal(sql);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Executes a statement and returns the update count. This method just calls
   * execute(String sql) internally. The method getGeneratedKeys supports at
   * most one columns and row.
   * 
   * @param sql
   *          the SQL statement
   * @param columnNames
   *          ignored
   * @return the update count (number of row affected by an insert, update or
   *         delete, or 0 if no rows or the statement was a create, drop, commit
   *         or rollback)
   * @throws java.sql.SQLException
   *           if a database error occurred or a select statement was executed
   */
  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    try {
      return executeInternal(sql);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the result set holdability.
   * 
   * @return the holdability
   */
  @Override
  public int getResultSetHoldability() throws SQLException {
    try {
      checkClosed();
      return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Check if this connection is closed. The next operation is a read request.
   * 
   * @return true if the session was re-connected
   * @throws JdbcException
   *           if the connection or session is closed
   */
  public boolean checkClosed() {
    return checkClosed(false);
  }

  /**
   * Check if this connection is closed. The next operation may be a write
   * request.
   *
   * @return true if the session was re-connected
   * @throws JdbcException
   *           if the connection or session is closed
   */
  boolean checkClosedForWrite() {
    return checkClosed(true);
  }

  /**
   * INTERNAL. Check if the statement is closed.
   *
   * @param write
   *          if the next operation is possibly writing
   * @return true if a reconnect was required
   * @throws JdbcException
   *           if it is closed
   */
  protected boolean checkClosed(boolean write)  {
    if (conn == null) {
      throw JdbcException.get(SQLErrorCode.OBJECT_CLOSED);
    }
    session.checkClosed();
    conn.checkClosed(write);
    return session.isClosed();
  }

  /**
   * Called after each write operation.
   */
  void afterWriting() {
    if (conn != null) {
      conn.afterWriting();
    }
  }

  /**
   * INTERNAL. Close and old result set if there is still one open.
   */
  protected void closeOldResultSet() throws SQLException {
    try {
      if (!closedByResultSet) {
        if (resultSet != null) {
          resultSet.closeInternal();
        }
      }
    } finally {
      resultSet = null;
      updateCount = -1;
    }
  }

  /**
   * INTERNAL. Get the command type of the last executed command.
   */
  public int getLastExecutedCommandType() {
    return lastExecutedCommandType;
  }

  /**
   * Returns whether this statement is closed.
   * 
   * @return true if the statement is closed
   */
  @Override
  public boolean isClosed() throws SQLException {
    try {
      return conn == null;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * [Not supported] Return an object of this class if possible.
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw JdbcException.getUnsupportedException("unwrap");
  }

  /**
   * [Not supported] Checks if unwrap can return an object of this class.
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw JdbcException.getUnsupportedException("isWrapperFor");
  }

  /**
   * Returns whether this object is poolable.
   * 
   * @return false
   */
  @Override
  public boolean isPoolable() {
    return false;
  }

  /**
   * Requests that this object should be pooled or not. This call is ignored.
   * 
   * @param poolable
   *          the requested value
   */
  @Override
  public void setPoolable(boolean poolable) {

  }

  /**
   * INTERNAL. Set the statement that is currently running.
   * 
   * @param c
   *          the command
   */
  protected void setExecutingStatement(CommandInterface c) {
    if (c == null) {
      conn.setExecutingStatement(null);
    } else {
      conn.setExecutingStatement(this);
      lastExecutedCommandType = c.getCommandType();
    }
  }

  /**
   * INTERNAL
   */
  @Override
  public String toString() {
    return "";
  }

  public void closeOnCompletion() throws SQLException {
	  throw new NotImplementedException();
  }

  public boolean isCloseOnCompletion() throws SQLException {
	  throw new NotImplementedException();
  }

}

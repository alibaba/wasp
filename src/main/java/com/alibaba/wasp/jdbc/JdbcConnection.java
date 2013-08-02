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

import com.alibaba.wasp.jdbc.result.JdbcDatabaseMetaData;import com.alibaba.wasp.jdbc.value.Value;import com.alibaba.wasp.session.SessionFactory;import com.alibaba.wasp.session.SessionInterface;import com.alibaba.wasp.util.Utils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.SQLErrorCode;
import com.alibaba.wasp.client.FClient;
import com.alibaba.wasp.jdbc.command.CommandInterface;
import com.alibaba.wasp.jdbc.result.JdbcDatabaseMetaData;
import com.alibaba.wasp.jdbc.value.Value;
import com.alibaba.wasp.session.SessionFactory;
import com.alibaba.wasp.session.SessionInterface;
import com.alibaba.wasp.util.StringUtils;
import com.alibaba.wasp.util.Utils;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * <p>
 * Represents a connection (session) to wasp cluster.
 * </p>
 * <p>
 * Thread safety: the connection is thread-safe, because access is synchronized.
 * However, for compatibility with other wasp cluster, a connection should only
 * be used in one thread at any time.
 * </p>
 */
public class JdbcConnection implements Connection {

  private Log log = LogFactory.getLog(JdbcConnection.class);

  private static boolean keepOpenStackTrace = false;

  private final String url;
  private final String user;
  private int holdability = 1;
  private SessionInterface session;

  private final FClient fClient;
  private final Configuration conf;

  private Statement executingStatement;
  private final CloseWatcher watcher;
  private int queryTimeoutCache = -1;
  private Properties properties;

  /**
   * Constructor.
   * 
   * @param url
   * 
   * @param info
   * 
   * @throws SQLException
   */
  public JdbcConnection(String url, Properties info) throws SQLException {
    this(new ConnectionInfo(url, info), Utils
        .convertPropertiesToConfiguration(info));
    this.properties = info;
  }

  /**
   * Wasp's JDBC connection.
   * 
   * @param ci
   * @throws SQLException
   */
  private JdbcConnection(ConnectionInfo ci, Configuration conf)
      throws SQLException {
    try {
      // this will return an embedded or server connection
      session = SessionFactory.createSession(ci);
      this.user = ci.getUserName();
      log.info("Connection conn = DriverManager.getConnection("
          + StringUtils.quoteJavaString(ci.getOriginalURL()) + ", "
          + StringUtils.quoteJavaString(user) + ", \"\");");
      this.url = ci.getURL();
      closeOld();
      watcher = CloseWatcher.register(this, session, keepOpenStackTrace);
      this.conf = conf;
      this.fClient = new FClient(this.conf);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  private void closeOld() {
    while (true) {
      CloseWatcher w = CloseWatcher.pollUnclosed();
      if (w == null) {
        break;
      }
      try {
        w.getCloseable().close();
      } catch (Exception e) {
        log.error("closing session", e);
      }
    }
  }

  /**
   * Creates a new statement.
   * 
   * @return the new statement
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public Statement createStatement() throws SQLException {
    try {
      checkClosed();
      return new JdbcStatement(this, ResultSet.TYPE_FORWARD_ONLY,
          FConstants.DEFAULT_RESULT_SET_CONCURRENCY, false);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Creates a statement with the specified result set type and concurrency.
   * 
   * @param resultSetType
   *          the result set type (ResultSet.TYPE_*)
   * @param resultSetConcurrency
   *          the concurrency (ResultSet.CONCUR_*)
   * @return the statement
   * @throws java.sql.SQLException
   *           if the connection is closed or the result set type or concurrency
   *           are not supported
   */
  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    try {
      checkTypeConcurrency(resultSetType, resultSetConcurrency);
      checkClosed();
      return new JdbcStatement(this, resultSetType, resultSetConcurrency, false);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Creates a statement with the specified result set type, concurrency, and
   * holdability.
   * 
   * @param resultSetType
   *          the result set type (ResultSet.TYPE_*)
   * @param resultSetConcurrency
   *          the concurrency (ResultSet.CONCUR_*)
   * @param resultSetHoldability
   *          the holdability (ResultSet.HOLD* / CLOSE*)
   * @return the statement
   * @throws java.sql.SQLException
   *           if the connection is closed or the result set type, concurrency,
   *           or holdability are not supported
   */
  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    try {
      checkTypeConcurrency(resultSetType, resultSetConcurrency);
      checkHoldability(resultSetHoldability);
      checkClosed();
      return new JdbcStatement(this, resultSetType, resultSetConcurrency, false);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Creates a new prepared statement.
   * 
   * @param sql
   *          the SQL statement
   * @return the prepared statement
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    try {
      checkClosed();
      return new JdbcPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY,
          FConstants.DEFAULT_RESULT_SET_CONCURRENCY, false);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Prepare a statement that will automatically close when the result set is
   * closed. This method is used to retrieve database meta data.
   * 
   * @param sql
   *          the SQL statement
   * @return the prepared statement
   */
  PreparedStatement prepareAutoCloseStatement(String sql) throws SQLException {
    try {
      checkClosed();
      return new JdbcPreparedStatement(this, sql, ResultSet.TYPE_FORWARD_ONLY,
          FConstants.DEFAULT_RESULT_SET_CONCURRENCY, true);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the database meta data for this database.
   * 
   * @return the database meta data
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    try {
      checkClosed();
      return new JdbcDatabaseMetaData(this);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * INTERNAL
   */
  public SessionInterface getSession() {
    return session;
  }

  /**
   * Closes this connection. All open statements, prepared statements and result
   * sets that where created by this connection become invalid after calling
   * this method. If there is an uncommitted transaction, it will be rolled
   * back.
   */
  @Override
  public synchronized void close() throws SQLException {
    try {
      if (session == null) {
        return;
      }
      CloseWatcher.unregister(watcher);
      session.close();
      if (executingStatement != null) {
        try {
          executingStatement.cancel();
        } catch (NullPointerException e) {
          // ignore
        }
      }
      synchronized (session) {
        try {
          if (!session.isClosed()) {
            session.close();
          }
        } finally {
          session = null;
        }
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    // always true
  }

  /**
   * Gets the current setting for auto commit.
   * 
   * @return true for on, false for off
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public synchronized boolean getAutoCommit() throws SQLException {
    // always true
    return true;
  }

  /**
   * don't need commit. wasp autocommit
   * 
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public synchronized void commit() throws SQLException {
    // don't need commit. wasp autocommit
  }

  /**
   * unsupported rollback
   * 
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public synchronized void rollback() throws SQLException {
    throw JdbcException.getUnsupportedException("rollback unsupported");
  }

  /**
   * Returns true if this connection has been closed.
   * 
   * @return true if close was called
   */
  @Override
  public boolean isClosed() throws SQLException {
    return session == null || session.isClosed();
  }

  /**
   * Translates a SQL statement into the database grammar.
   * 
   * @param sql
   *          the SQL statement with or without JDBC escape sequences
   * @return the translated statement
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public String nativeSQL(String sql) throws SQLException {
    try {
      checkClosed();
      return sql;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * According to the JDBC specs, this setting is only a hint to the database to
   * enable optimizations - it does not cause writes to be prohibited.
   * 
   * @param readOnly
   *          ignored
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    checkClosed();
  }

  /**
   * Returns true if the database is read-only.
   * 
   * @return if the database is read-only
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public boolean isReadOnly() throws SQLException {
    checkClosed();
    return false;
  }

  /**
   * Set the default catalog name. This call is ignored.
   * 
   * @param catalog
   *          ignored
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public void setCatalog(String catalog) throws SQLException {
    throw JdbcException.getUnsupportedException("setCatalog unsupported");
  }

  /**
   * Gets the current catalog name.
   * 
   * @return the catalog name
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public String getCatalog() throws SQLException {
    throw JdbcException.getUnsupportedException("getCatalog unsupported");
  }

  /**
   * Gets the first warning reported by calls on this object.
   * 
   * @return null
   */
  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw JdbcException.getUnsupportedException("getWarnings unsupported");
  }

  /**
   * Clears all warnings.
   */
  @Override
  public void clearWarnings() throws SQLException {
    throw JdbcException.getUnsupportedException("clearWarnings unsupported");
  }

  /**
   * Creates a prepared statement with the specified result set type and
   * concurrency.
   * 
   * @param sql
   *          the SQL statement
   * @param resultSetType
   *          the result set type (ResultSet.TYPE_*)
   * @param resultSetConcurrency
   *          the concurrency (ResultSet.CONCUR_*)
   * @return the prepared statement
   * @throws java.sql.SQLException
   *           if the connection is closed or the result set type or concurrency
   *           are not supported
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    try {
      checkTypeConcurrency(resultSetType, resultSetConcurrency);
      checkClosed();
      return new JdbcPreparedStatement(this, sql, resultSetType,
          resultSetConcurrency, false);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    // default wasp level
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    queryTimeoutCache = seconds;
  }

  public int getQueryTimeout() throws SQLException {
    return queryTimeoutCache;
  }

  /**
   * Returns the current transaction isolation level.
   * 
   * @return the isolation level.
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public int getTransactionIsolation() throws SQLException {
    // default wasp level. the return value no needed.
    return 0;
  }

  /**
   * Changes the current result set holdability.
   * 
   * @param holdability
   *          ResultSet.HOLD_CURSORS_OVER_COMMIT or
   *          ResultSet.CLOSE_CURSORS_AT_COMMIT;
   * @throws java.sql.SQLException
   *           if the connection is closed or the holdability is not supported
   */
  @Override
  public void setHoldability(int holdability) throws SQLException {
    try {
      checkClosed();
      checkHoldability(holdability);
      this.holdability = holdability;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the current result set holdability.
   * 
   * @return the holdability
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public int getHoldability() throws SQLException {
    try {
      checkClosed();
      return holdability;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the type map.
   * 
   * @return null
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    try {
      checkClosed();
      return null;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * [Partially supported] Sets the type map. This is only supported if the map
   * is empty or null.
   */
  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    try {
      checkMap(map);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * unsupported
   * 
   * @param sql
   *          the SQL statement
   * @return the callable statement
   * @throws java.sql.SQLException
   *           if the connection is closed or the statement is not valid
   */
  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw JdbcException.getUnsupportedException("prepareCall unsupported");
  }

  /**
   * unsupported
   * 
   * @param sql
   *          the SQL statement
   * @param resultSetType
   *          the result set type (ResultSet.TYPE_*)
   * @param resultSetConcurrency
   *          the concurrency (ResultSet.CONCUR_*)
   * @return the callable statement
   * @throws java.sql.SQLException
   *           if the connection is closed or the result set type or concurrency
   *           are not supported
   */
  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    throw JdbcException.getUnsupportedException("prepareCall unsupported");
  }

  /**
   * unsupported
   * 
   * @param sql
   *          the SQL statement
   * @param resultSetType
   *          the result set type (ResultSet.TYPE_*)
   * @param resultSetConcurrency
   *          the concurrency (ResultSet.CONCUR_*)
   * @param resultSetHoldability
   *          the holdability (ResultSet.HOLD* / CLOSE*)
   * @return the callable statement
   * @throws java.sql.SQLException
   *           if the connection is closed or the result set type, concurrency,
   *           or holdability are not supported
   */
  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw JdbcException.getUnsupportedException("prepareCall unsupported");
  }

  /**
   * Creates a new unnamed savepoint.
   * 
   * @return the new savepoint
   */
  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw JdbcException.getUnsupportedException("setSavePoint unsupported");
  }

  /**
   * Creates a new named savepoint.
   * 
   * @param name
   *          the savepoint name
   * @return the new savepoint
   */
  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw JdbcException.getUnsupportedException("setSavePoint unsupported");
  }

  /**
   * Rolls back to a savepoint.
   * 
   * @param savepoint
   *          the savepoint
   */
  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw JdbcException.getUnsupportedException("rollback unsupported");
  }

  /**
   * Releases a savepoint.
   * 
   * @param savepoint
   *          the savepoint to release
   */
  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw JdbcException.getUnsupportedException("releaseSavePoint unsupported");
  }

  /**
   * Creates a prepared statement with the specified result set type,
   * concurrency, and holdability.
   * 
   * @param sql
   *          the SQL statement
   * @param resultSetType
   *          the result set type (ResultSet.TYPE_*)
   * @param resultSetConcurrency
   *          the concurrency (ResultSet.CONCUR_*)
   * @param resultSetHoldability
   *          the holdability (ResultSet.HOLD* / CLOSE*)
   * @return the prepared statement
   * @throws java.sql.SQLException
   *           if the connection is closed or the result set type, concurrency,
   *           or holdability are not supported
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    try {
      checkTypeConcurrency(resultSetType, resultSetConcurrency);
      checkHoldability(resultSetHoldability);
      checkClosed();
      return new JdbcPreparedStatement(this, sql, resultSetType,
          resultSetConcurrency, false);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Creates a new prepared statement. This method just calls
   * prepareStatement(String sql) internally. The method getGeneratedKeys only
   * supports one column.
   * 
   * @param sql
   *          the SQL statement
   * @param autoGeneratedKeys
   *          ignored
   * @return the prepared statement
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    return prepareStatement(sql);
  }

  /**
   * Creates a new prepared statement. This method just calls
   * prepareStatement(String sql) internally. The method getGeneratedKeys only
   * supports one column.
   * 
   * @param sql
   *          the SQL statement
   * @param columnIndexes
   *          ignored
   * @return the prepared statement
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    return prepareStatement(sql);
  }

  /**
   * Creates a new prepared statement. This method just calls
   * prepareStatement(String sql) internally. The method getGeneratedKeys only
   * supports one column.
   * 
   * @param sql
   *          the SQL statement
   * @param columnNames
   *          ignored
   * @return the prepared statement
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    return prepareStatement(sql);
  }

  @Override
  public Clob createClob() throws SQLException {
    throw JdbcException.getUnsupportedException("createClob unsupported");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw JdbcException.getUnsupportedException("createBlob unsupported");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw JdbcException.getUnsupportedException("createNClob unsupported");
  }

  private void checkTypeConcurrency(int resultSetType, int resultSetConcurrency) {
    switch (resultSetType) {
    case ResultSet.TYPE_FORWARD_ONLY:
    case ResultSet.TYPE_SCROLL_INSENSITIVE:
    case ResultSet.TYPE_SCROLL_SENSITIVE:
      break;
    default:
      throw JdbcException.getInvalidValueException("resultSetType",
          resultSetType);
    }
    switch (resultSetConcurrency) {
    case ResultSet.CONCUR_READ_ONLY:
    case ResultSet.CONCUR_UPDATABLE:
      break;
    default:
      throw JdbcException.getInvalidValueException("resultSetConcurrency",
          resultSetConcurrency);
    }
  }

  private void checkHoldability(int resultSetHoldability) {
    if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT
        && resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw JdbcException.getInvalidValueException("resultSetHoldability",
          resultSetHoldability);
    }
  }

  /**
   * Check if this connection is closed. The next operation is a read request.
   * 
   * @throws JdbcException
   *           if the connection or session is closed
   */
  public void checkClosed() {
    checkClosed(false);
  }

  /**
   * Check if this connection is closed.
   * 
   * @param write
   *          if the next operation is possibly writing
   * @throws JdbcException
   *           if the connection or session is closed
   */
  protected void checkClosed(boolean write) {
    if (session == null) {
      throw JdbcException.get(SQLErrorCode.OBJECT_CLOSED);
    }
    if (session.isClosed()) {
      throw JdbcException.get(SQLErrorCode.DATABASE_CALLED_AT_SHUTDOWN);
    }
  }

  public String getURL() {
    checkClosed();
    return url;
  }

  public String getUser() {
    checkClosed();
    return user;
  }

  /**
   * 
   */
  public void setExecutingStatement(Statement stat) {
    executingStatement = stat;
  }

  /**
   * [Not supported] Create a new empty SQLXML object.
   */
  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw JdbcException.getUnsupportedException("createSQLXML unsupported");
  }

  /**
   * [Not supported] Create a new empty Array object.
   */
  @Override
  public Array createArrayOf(String typeName, Object[] elements)
      throws SQLException {
    throw JdbcException.getUnsupportedException("createArrayOf unsupported");
  }

  /**
   * [Not supported] Create a new empty Struct object.
   */
  @Override
  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    throw JdbcException.getUnsupportedException("createStruct unsupported");
  }

  /**
   * Returns true if this connection is still valid.
   * 
   * @param timeout
   *          the number of seconds to wait for the database to respond
   *          (ignored)
   * @return true if the connection is valid.
   */
  @Override
  public synchronized boolean isValid(int timeout) {
    try {
      if (session == null || session.isClosed()) {
        return false;
      }
      // force a network round trip (if networked)
      getTransactionIsolation();
      return true;
    } catch (Exception e) {
      // this method doesn't throw an exception, but it logs it
      Logger.logAndConvert(log, e);
      return false;
    }
  }

  /**
   * [Not supported] Set a client property.
   */
  @Override
  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
    this.properties.setProperty(name, value);
    // throw new SQLClientInfoException();
  }

  /**
   * [Not supported] Set the client properties.
   */
  @Override
  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    this.properties.putAll(properties);
    // throw new SQLClientInfoException();
  }

  /**
   * [Not supported] Get the client properties.
   */
  @Override
  public Properties getClientInfo() throws SQLClientInfoException {
    return this.properties;
  }

  /**
   * [Not supported] Set a client property.
   */
  @Override
  public String getClientInfo(String name) throws SQLException {
    return this.properties.getProperty(name);
  }

  /**
   * [Not supported] Return an object of this class if possible.
   * 
   * @param iface
   *          the class
   */
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw JdbcException.getUnsupportedException("unwrap unsupported");
  }

  /**
   * [Not supported] Checks if unwrap can return an object of this class.
   * 
   * @param iface
   *          the class
   */
  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw JdbcException.getUnsupportedException("isWrapperFor unsupported");
  }

  private static void checkMap(Map<String, Class<?>> map) {
    if (map != null && map.size() > 0) {
      throw JdbcException.getUnsupportedException("map.size > 0");
    }
  }

  public String toString() {
    return "Conn : url=" + url + " user=" + user;
  }

  public CommandInterface prepareCommand(String sql, int fetchSize) {
    String readModel = properties.getProperty(FConstants.READ_MODEL);
    return session.prepareCommand(fClient, sql, fetchSize,
        Utils.getReadModel(readModel));
  }

  public Object convertToDefaultObject(Value v) {
    return v.getObject();
  }

  public Configuration getConf() {
    return this.conf;
  }

  /**
   * INTERNAL. Called after executing a command that could have written
   * something.
   */
  protected void afterWriting() {
    if (session != null) {
      session.afterWriting();
    }
  }

  public void setSchema(String schema) throws SQLException {
    throw new NotImplementedException();
  }

  public String getSchema() throws SQLException {
	  throw new NotImplementedException();
  }

  public void abort(Executor executor) throws SQLException {
	  throw new NotImplementedException();
  }

  public void setNetworkTimeout(Executor executor, int milliseconds)
		  throws SQLException {
	  throw new NotImplementedException();
  }

  public int getNetworkTimeout() throws SQLException {
	  throw new NotImplementedException();
  }

}

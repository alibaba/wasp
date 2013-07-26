package com.alibaba.wasp.jdbc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.alibaba.wasp.DataType;
import com.alibaba.wasp.SQLErrorCode;
import com.alibaba.wasp.jdbc.command.CommandInterface;
import com.alibaba.wasp.jdbc.expression.ParameterInterface;
import com.alibaba.wasp.jdbc.result.JdbcResultSet;
import com.alibaba.wasp.jdbc.result.JdbcResultSetMetaData;
import com.alibaba.wasp.jdbc.result.ResultInterface;
import com.alibaba.wasp.jdbc.value.Value;
import com.alibaba.wasp.jdbc.value.ValueBoolean;
import com.alibaba.wasp.jdbc.value.ValueByte;
import com.alibaba.wasp.jdbc.value.ValueBytes;
import com.alibaba.wasp.jdbc.value.ValueDate;
import com.alibaba.wasp.jdbc.value.ValueDecimal;
import com.alibaba.wasp.jdbc.value.ValueDouble;
import com.alibaba.wasp.jdbc.value.ValueFloat;
import com.alibaba.wasp.jdbc.value.ValueInt;
import com.alibaba.wasp.jdbc.value.ValueLong;
import com.alibaba.wasp.jdbc.value.ValueNull;
import com.alibaba.wasp.jdbc.value.ValueShort;
import com.alibaba.wasp.jdbc.value.ValueString;
import com.alibaba.wasp.jdbc.value.ValueTime;
import com.alibaba.wasp.jdbc.value.ValueTimestamp;
import com.alibaba.wasp.util.DateTimeUtils;
import com.alibaba.wasp.util.New;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;

/**
 * Represents a prepared statement.
 */
public class JdbcPreparedStatement extends JdbcStatement implements
    PreparedStatement {

  private Log log = LogFactory.getLog(JdbcPreparedStatement.class);

  protected CommandInterface command;
  private final String sqlStatement;
  private ArrayList<Value[]> batchParameters;

  JdbcPreparedStatement(JdbcConnection conn, String sql, int resultSetType,
      int resultSetConcurrency, boolean closeWithResultSet) {
    super(conn, resultSetType, resultSetConcurrency, closeWithResultSet);
    this.sqlStatement = sql;
    command = conn.prepareCommand(sql, fetchSize);
  }

  /**
   * Executes a query (select statement) and returns the result set. If another
   * result set exists for this statement, this will be closed (even if this
   * statement fails).
   * 
   * @return the result set
   * @throws java.sql.SQLException
   *           if this object is closed or invalid
   */
  @Override
  public ResultSet executeQuery() throws SQLException {
    try {
      synchronized (session) {
        checkClosed();
        closeOldResultSet();
        ResultInterface result;
        try {
          setExecutingStatement(command);
          result = command.executeQuery(maxRows);
        } finally {
          setExecutingStatement(null);
        }
        resultSet = new JdbcResultSet(conn, this, result, closedByResultSet);
      }
      return resultSet;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
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
   * @return the update count (number of row affected by an insert, update or
   *         delete, or 0 if no rows or the statement was a create, drop, commit
   *         or rollback)
   * @throws java.sql.SQLException
   *           if this object is closed or invalid
   */
  @Override
  public int executeUpdate() throws SQLException {
    try {
      checkClosedForWrite();
      try {
        return executeUpdateInternal();
      } finally {
        afterWriting();
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  private int executeUpdateInternal() throws SQLException {
    closeOldResultSet();
    synchronized (session) {
      try {
        setExecutingStatement(command);
        updateCount = command.executeUpdate();
      } finally {
        setExecutingStatement(null);
      }
    }
    return updateCount;
  }

  /**
   * Executes an arbitrary statement. If another result set exists for this
   * statement, this will be closed (even if this statement fails). If auto
   * commit is on, and the statement is not a select, this statement will be
   * committed.
   * 
   * @return true if a result set is available, false if not
   * @throws java.sql.SQLException
   *           if this object is closed or invalid
   */
  @Override
  public boolean execute() throws SQLException {
    try {
      checkClosedForWrite();
      try {
        boolean returnsResultSet;
        synchronized (conn.getSession()) {
          closeOldResultSet();
          try {
            setExecutingStatement(command);
            if (command.isQuery()) {
              returnsResultSet = true;
              ResultInterface result = command.executeQuery(maxRows);
              resultSet = new JdbcResultSet(conn, this, result,
                  closedByResultSet);
            } else {
              returnsResultSet = false;
              updateCount = command.executeUpdate();
            }
          } finally {
            setExecutingStatement(null);
          }
        }
        return returnsResultSet;
      } finally {
        afterWriting();
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Clears all parameters.
   * 
   * @throws java.sql.SQLException
   *           if this object is closed or invalid
   */
  @Override
  public void clearParameters() throws SQLException {
    try {
      checkClosed();
      ArrayList<? extends ParameterInterface> parameters = command
          .getParameters();
      for (int i = 0, size = parameters.size(); i < size; i++) {
        ParameterInterface param = parameters.get(i);
        // can only delete old temp files if they are not in the batch
        param.setValue(null, batchParameters == null);
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Calling this method is not legal on a PreparedStatement.
   * 
   * @param sql
   *          ignored
   * @throws java.sql.SQLException
   *           Unsupported Feature
   */
  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    try {
      throw JdbcException
          .get(SQLErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Calling this method is not legal on a PreparedStatement.
   * 
   * @param sql
   *          ignored
   * @throws java.sql.SQLException
   *           Unsupported Feature
   */
  @Override
  public void addBatch(String sql) throws SQLException {
    try {
      throw JdbcException
          .get(SQLErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Calling this method is not legal on a PreparedStatement.
   * 
   * @param sql
   *          ignored
   * @throws java.sql.SQLException
   *           Unsupported Feature
   */
  @Override
  public int executeUpdate(String sql) throws SQLException {
    try {
      throw JdbcException
          .get(SQLErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Calling this method is not legal on a PreparedStatement.
   * 
   * @param sql
   *          ignored
   * @throws java.sql.SQLException
   *           Unsupported Feature
   */
  @Override
  public boolean execute(String sql) throws SQLException {
    try {
      throw JdbcException
          .get(SQLErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  // =============================================================

  /**
   * Sets a parameter to null.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param sqlType
   *          the data type (Types.x)
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    try {
      setParameter(parameterIndex, ValueNull.INSTANCE);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    try {
      setParameter(parameterIndex, ValueInt.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    try {
      Value v = x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x);
      setParameter(parameterIndex, v);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x)
      throws SQLException {
    try {
      Value v = x == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(x);
      setParameter(parameterIndex, v);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setDate(int parameterIndex, java.sql.Date x) throws SQLException {
    try {
      Value v = x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x);
      setParameter(parameterIndex, v);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setTime(int parameterIndex, java.sql.Time x) throws SQLException {
    try {
      Value v = x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x);
      setParameter(parameterIndex, v);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setTimestamp(int parameterIndex, java.sql.Timestamp x)
      throws SQLException {
    try {
      Value v = x == null ? (Value) ValueNull.INSTANCE : ValueTimestamp.get(x);
      setParameter(parameterIndex, v);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter. Objects of unknown classes are serialized
   * (on the client side).
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    try {
      if (x == null) {
        // throw Errors.getInvalidValueException("null", "x");
        setParameter(parameterIndex, ValueNull.INSTANCE);
      } else {
        setParameter(parameterIndex,
            DataType.convertToValue(session, x, Value.UNKNOWN));
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter. The object is converted, if required, to the
   * specified data type before sending to the database. Objects of unknown
   * classes are serialized (on the client side).
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value, null is allowed
   * @param targetSqlType
   *          the type as defined in java.sql.Types
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType)
      throws SQLException {
    try {
      int type = DataType.convertSQLTypeToValueType(targetSqlType);
      if (x == null) {
        setParameter(parameterIndex, ValueNull.INSTANCE);
      } else {
        Value v = DataType.convertToValue(conn.getSession(), x, type);
        setParameter(parameterIndex, v.convertTo(type));
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter. The object is converted, if required, to the
   * specified data type before sending to the database. Objects of unknown
   * classes are serialized (on the client side).
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value, null is allowed
   * @param targetSqlType
   *          the type as defined in java.sql.Types
   * @param scale
   *          is ignored
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType,
      int scale) throws SQLException {
    try {
      setObject(parameterIndex, x, targetSqlType);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    try {
      setParameter(parameterIndex, ValueBoolean.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    try {
      setParameter(parameterIndex, ValueByte.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    try {
      setParameter(parameterIndex, ValueShort.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    try {
      setParameter(parameterIndex, ValueLong.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    try {
      setParameter(parameterIndex, ValueFloat.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    try {
      setParameter(parameterIndex, ValueDouble.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * [Not supported] Sets the value of a column as a reference.
   */
  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw JdbcException.getUnsupportedException("ref");
  }

  /**
   * Sets the date using a specified time zone. The value will be converted to
   * the local time zone.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param calendar
   *          the calendar
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setDate(int parameterIndex, java.sql.Date x, Calendar calendar)
      throws SQLException {
    try {
      if (x == null) {
        setParameter(parameterIndex, ValueNull.INSTANCE);
      } else {
        setParameter(parameterIndex, DateTimeUtils.convertDate(x, calendar));
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the time using a specified time zone. The value will be converted to
   * the local time zone.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param calendar
   *          the calendar
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setTime(int parameterIndex, java.sql.Time x, Calendar calendar)
      throws SQLException {
    try {
      if (x == null) {
        setParameter(parameterIndex, ValueNull.INSTANCE);
      } else {
        setParameter(parameterIndex, DateTimeUtils.convertTime(x, calendar));
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the timestamp using a specified time zone. The value will be converted
   * to the local time zone.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param calendar
   *          the calendar
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setTimestamp(int parameterIndex, java.sql.Timestamp x,
      Calendar calendar) throws SQLException {
    try {
      if (x == null) {
        setParameter(parameterIndex, ValueNull.INSTANCE);
      } else {
        setParameter(parameterIndex,
            DateTimeUtils.convertTimestamp(x, calendar));
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * [Not supported] This feature is deprecated and not supported.
   * 
   * @deprecated since JDBC 2.0, use setCharacterStream
   */
  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("unicodeStream");
  }

  /**
   * Sets a parameter to null.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param sqlType
   *          the data type (Types.x)
   * @param typeName
   *          this parameter is ignored
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName)
      throws SQLException {
    try {
      setNull(parameterIndex, sqlType);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter as a Blob.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw JdbcException.getUnsupportedException("setBlob");
  }

  /**
   * Sets the value of a parameter as a Blob. This method does not close the
   * stream. The stream may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setBlob(int parameterIndex, InputStream x) throws SQLException {
    throw JdbcException.getUnsupportedException("setBlob");
  }

  /**
   * Sets the value of a parameter as a Clob.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw JdbcException.getUnsupportedException("setClob");
  }

  /**
   * Sets the value of a parameter as a Clob. This method does not close the
   * reader. The reader may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setClob(int parameterIndex, Reader x) throws SQLException {
    throw JdbcException.getUnsupportedException("setClob");
  }

  /**
   * [Not supported] Sets the value of a parameter as a Array.
   */
  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw JdbcException.getUnsupportedException("setArray");
  }

  /**
   * Sets the value of a parameter as a byte array.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    try {
      Value v = x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x);
      setParameter(parameterIndex, v);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Sets the value of a parameter as an input stream. This method does not
   * close the stream. The stream may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param length
   *          the maximum number of bytes
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("setBinaryStream");
  }

  /**
   * Sets the value of a parameter as an input stream. This method does not
   * close the stream. The stream may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param length
   *          the maximum number of bytes
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    setBinaryStream(parameterIndex, x, (long) length);
  }

  /**
   * Sets the value of a parameter as an input stream. This method does not
   * close the stream. The stream may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setBinaryStream(int parameterIndex, InputStream x)
      throws SQLException {
    setBinaryStream(parameterIndex, x, -1);
  }

  /**
   * Sets the value of a parameter as an ASCII stream. This method does not
   * close the stream. The stream may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param length
   *          the maximum number of bytes
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    setAsciiStream(parameterIndex, x, (long) length);
  }

  /**
   * Sets the value of a parameter as an ASCII stream. This method does not
   * close the stream. The stream may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param length
   *          the maximum number of bytes
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("setAsciiStream");
  }

  /**
   * Sets the value of a parameter as an ASCII stream. This method does not
   * close the stream. The stream may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setAsciiStream(int parameterIndex, InputStream x)
      throws SQLException {
    setAsciiStream(parameterIndex, x, -1);
  }

  /**
   * Sets the value of a parameter as a character stream. This method does not
   * close the reader. The reader may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param length
   *          the maximum number of characters
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setCharacterStream(int parameterIndex, Reader x, int length)
      throws SQLException {
    setCharacterStream(parameterIndex, x, (long) length);
  }

  /**
   * Sets the value of a parameter as a character stream. This method does not
   * close the reader. The reader may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setCharacterStream(int parameterIndex, Reader x)
      throws SQLException {
    setCharacterStream(parameterIndex, x, -1);
  }

  /**
   * Sets the value of a parameter as a character stream. This method does not
   * close the reader. The reader may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param length
   *          the maximum number of characters
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setCharacterStream(int parameterIndex, Reader x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("setCharacterStream");
  }

  /**
   * [Not supported]
   */
  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw JdbcException.getUnsupportedException("url");
  }

  /**
   * Gets the result set metadata of the query returned when the statement is
   * executed. If this is not a query, this method returns null.
   * 
   * @return the meta data or null if this is not a query
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    try {
      checkClosed();
      ResultInterface result = command.getMetaData();
      if (result == null) {
        return null;
      }
      String catalog = conn.getCatalog();
      JdbcResultSetMetaData meta = new JdbcResultSetMetaData(null, result,
          catalog);
      return meta;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Clears the batch.
   */
  @Override
  public void clearBatch() throws SQLException {
    try {
      checkClosed();
      batchParameters = null;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Closes this statement. All result sets that where created by this statement
   * become invalid after calling this method.
   */
  @Override
  public void close() throws SQLException {
    try {
      super.close();
      batchParameters = null;
      if (command != null) {
        command.close();
        command = null;
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
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
      if (batchParameters == null) {
        batchParameters = New.arrayList();
      }
      int size = batchParameters.size();
      int[] result = new int[size];
      boolean error = false;
      SQLException next = null;
      checkClosedForWrite();
      try {
        for (int i = 0; i < size; i++) {
          Value[] set = batchParameters.get(i);
          ArrayList<? extends ParameterInterface> parameters = command
              .getParameters();
          for (int j = 0; j < set.length; j++) {
            Value value = set[j];
            ParameterInterface param = parameters.get(j);
            param.setValue(value, false);
          }
          try {
            result[i] = executeUpdateInternal();
          } catch (Exception re) {
            SQLException e = Logger.logAndConvert(log, re);
            if (next == null) {
              next = e;
            } else {
              e.setNextException(next);
              next = e;
            }
            result[i] = Statement.EXECUTE_FAILED;
            error = true;
          }
        }
        batchParameters = null;
        if (error) {
          JdbcBatchUpdateException e = new JdbcBatchUpdateException(next,
              result);
          throw e;
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
   * Adds the current settings to the batch.
   */
  @Override
  public void addBatch() throws SQLException {
    try {
      checkClosedForWrite();
      try {
        ArrayList<? extends ParameterInterface> parameters = command
            .getParameters();
        int size = parameters.size();
        Value[] set = new Value[size];
        for (int i = 0; i < size; i++) {
          ParameterInterface param = parameters.get(i);
          Value value = param.getParamValue();
          set[i] = value;
        }
        if (batchParameters == null) {
          batchParameters = New.arrayList();
        }
        batchParameters.add(set);
      } finally {
        afterWriting();
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Calling this method is not legal on a PreparedStatement.
   * 
   * @param sql
   *          ignored
   * @param autoGeneratedKeys
   *          ignored
   * @throws java.sql.SQLException
   *           Unsupported Feature
   */
  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys)
      throws SQLException {
    try {
      throw JdbcException
          .get(SQLErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Calling this method is not legal on a PreparedStatement.
   * 
   * @param sql
   *          ignored
   * @param columnIndexes
   *          ignored
   * @throws java.sql.SQLException
   *           Unsupported Feature
   */
  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    try {
      throw JdbcException
          .get(SQLErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Calling this method is not legal on a PreparedStatement.
   * 
   * @param sql
   *          ignored
   * @param columnNames
   *          ignored
   * @throws java.sql.SQLException
   *           Unsupported Feature
   */
  @Override
  public int executeUpdate(String sql, String[] columnNames)
      throws SQLException {
    try {
      throw JdbcException
          .get(SQLErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Calling this method is not legal on a PreparedStatement.
   * 
   * @param sql
   *          ignored
   * @param autoGeneratedKeys
   *          ignored
   * @throws java.sql.SQLException
   *           Unsupported Feature
   */
  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    try {
      throw JdbcException
          .get(SQLErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Calling this method is not legal on a PreparedStatement.
   * 
   * @param sql
   *          ignored
   * @param columnIndexes
   *          ignored
   * @throws java.sql.SQLException
   *           Unsupported Feature
   */
  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    try {
      throw JdbcException
          .get(SQLErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Calling this method is not legal on a PreparedStatement.
   * 
   * @param sql
   *          ignored
   * @param columnNames
   *          ignored
   * @throws java.sql.SQLException
   *           Unsupported Feature
   */
  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    try {
      throw JdbcException
          .get(SQLErrorCode.METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Get the parameter meta data of this prepared statement.
   * 
   * @return the meta data
   */
  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw JdbcException.getUnsupportedException("getParameterMetaData");
  }

  // =============================================================

  private void setParameter(int parameterIndex, Value value) {
    checkClosed();
    parameterIndex--;
    ArrayList<? extends ParameterInterface> parameters = command
        .getParameters();
    if (parameterIndex < 0 || parameterIndex >= parameters.size()) {
      throw JdbcException.getInvalidValueException("parameterIndex",
          parameterIndex + 1);
    }
    ParameterInterface param = parameters.get(parameterIndex);
    // can only delete old temp files if they are not in the batch
    param.setValue(value, batchParameters == null);
  }

  /**
   * [Not supported] Sets the value of a parameter as a row id.
   */
  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw JdbcException.getUnsupportedException("rowId");
  }

  // */@Override

  /**
   * Sets the value of a parameter.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setNString(int parameterIndex, String x) throws SQLException {
    throw JdbcException.getUnsupportedException("setNString");
  }

  /**
   * Sets the value of a parameter as a character stream. This method does not
   * close the reader. The reader may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param length
   *          the maximum number of characters
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setNCharacterStream(int parameterIndex, Reader x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("setNCharacterStream");
  }

  /**
   * Sets the value of a parameter as a character stream. This method does not
   * close the reader. The reader may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setNCharacterStream(int parameterIndex, Reader x)
      throws SQLException {
    setNCharacterStream(parameterIndex, x, -1);
  }

  /**
   * Sets the value of a parameter as a Clob.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setNClob(int parameterIndex, NClob x) throws SQLException {
    throw JdbcException.getUnsupportedException("setNClob");
  }

  // */@Override

  /**
   * Sets the value of a parameter as a Clob. This method does not close the
   * reader. The reader may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setNClob(int parameterIndex, Reader x) throws SQLException {
    throw JdbcException.getUnsupportedException("setNClob");
  }

  /**
   * Sets the value of a parameter as a Clob. This method does not close the
   * reader. The reader may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param length
   *          the maximum number of characters
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setClob(int parameterIndex, Reader x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("setClob");
  }

  /**
   * Sets the value of a parameter as a Blob. This method does not close the
   * stream. The stream may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param length
   *          the maximum number of bytes
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setBlob(int parameterIndex, InputStream x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("setBlob");
  }

  /**
   * Sets the value of a parameter as a Clob. This method does not close the
   * reader. The reader may be closed after executing the statement.
   * 
   * @param parameterIndex
   *          the parameter index (1, 2, ...)
   * @param x
   *          the value
   * @param length
   *          the maximum number of characters
   * @throws java.sql.SQLException
   *           if this object is closed
   */
  @Override
  public void setNClob(int parameterIndex, Reader x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("setNClob");
  }

  /**
   * [Not supported] Sets the value of a parameter as a SQLXML object.
   */
  @Override
  public void setSQLXML(int parameterIndex, SQLXML x) throws SQLException {
    throw JdbcException.getUnsupportedException("SQLXML");
  }

  /**
   * INTERNAL
   */
  @Override
  public String toString() {
    return "JdbcPreparedStatement : " + command;
  }

  protected boolean checkClosed(boolean write) {
    if (super.checkClosed(write)) {
      // if the session was re-connected, re-prepare the statement
      ArrayList<? extends ParameterInterface> oldParams = command
          .getParameters();
      command = conn.prepareCommand(sqlStatement, fetchSize);
      ArrayList<? extends ParameterInterface> newParams = command
          .getParameters();
      for (int i = 0, size = oldParams.size(); i < size; i++) {
        ParameterInterface old = oldParams.get(i);
        Value value = old.getParamValue();
        if (value != null) {
          ParameterInterface n = newParams.get(i);
          n.setValue(value, false);
        }
      }
      return true;
    }
    return false;
  }

}

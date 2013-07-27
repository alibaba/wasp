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
package com.alibaba.wasp.jdbc.result;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.alibaba.wasp.DataType;
import com.alibaba.wasp.SQLErrorCode;
import com.alibaba.wasp.jdbc.JdbcConnection;
import com.alibaba.wasp.jdbc.JdbcException;
import com.alibaba.wasp.jdbc.JdbcPreparedStatement;
import com.alibaba.wasp.jdbc.JdbcStatement;
import com.alibaba.wasp.jdbc.Logger;
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
import com.alibaba.wasp.util.MathUtils;
import com.alibaba.wasp.util.New;
import com.alibaba.wasp.util.StringUtils;

/**
 * <p>
 * Represents a result set.
 * </p>
 * <p>
 * Column labels are case-insensitive, quotes are not supported. The first
 * column has the column index 1.
 * </p>
 * <p>
 * Updatable result sets: Result sets are updatable when the result only
 * contains columns from one table, and if it contains all columns of a unique
 * index (primary key or other) of this table. Key columns may not contain NULL
 * (because multiple rows with NULL could exist). In updatable result sets, own
 * changes are visible, but not own inserts and deletes.
 * </p>
 */
public class JdbcResultSet implements ResultSet {

  private Log log = LogFactory.getLog(JdbcResultSet.class);

  private final boolean closeStatement;
  private ResultInterface result;
  private JdbcConnection conn;
  private JdbcStatement stat;
  private int columnCount;
  private boolean wasNull;
  private Value[] insertRow;
  private Value[] updateRow;
  private HashMap<String, Integer> columnLabelMap;

  public JdbcResultSet(JdbcConnection conn, JdbcStatement stat,
      ResultInterface result, boolean closeStatement) {
    this.conn = conn;
    this.stat = stat;
    this.result = result;
    columnCount = result.getVisibleColumnCount();
    this.closeStatement = closeStatement;
  }

  JdbcResultSet(JdbcConnection conn, JdbcPreparedStatement preparedStatement,
      ResultInterface result, boolean closeStatement,
      HashMap<String, Integer> columnLabelMap) {
    this(conn, preparedStatement, result, closeStatement);
    this.columnLabelMap = columnLabelMap;
  }

  /**
   * Moves the cursor to the next row of the result set.
   * 
   * @return true if successful, false if there are no more rows
   */
  @Override
  public boolean next() throws SQLException {
    try {
      checkClosed();
      return nextRow();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the meta data of this result set.
   * 
   * @return the meta data
   */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    try {
      checkClosed();
      JdbcResultSetMetaData meta = new JdbcResultSetMetaData(this, result, null);
      return meta;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns whether the last column accessed was null.
   * 
   * @return true if the last column accessed was null
   */
  @Override
  public boolean wasNull() throws SQLException {
    try {
      checkClosed();
      return wasNull;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Searches for a specific column in the result set. A case-insensitive search
   * is made.
   * 
   * @param columnLabel
   *          the column label
   * @return the column index (1,2,...)
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public int findColumn(String columnLabel) throws SQLException {
    try {
      return getColumnIndex(columnLabel);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Closes the result set.
   */
  @Override
  public void close() throws SQLException {
    try {
      closeInternal();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Close the result set. This method also closes the statement if required.
   */
  public void closeInternal() throws SQLException {
    if (result != null) {
      try {
        result.close();
        if (closeStatement && stat != null) {
          stat.close();
        }
      } finally {
        columnCount = 0;
        result = null;
        stat = null;
        conn = null;
        insertRow = null;
        updateRow = null;
      }
    }
  }

  /**
   * Returns the statement that created this object.
   * 
   * @return the statement or prepared statement, or null if created by a
   *         DatabaseMetaData call.
   */
  @Override
  public Statement getStatement() throws SQLException {
    try {
      checkClosed();
      if (closeStatement) {
        // if the result set was opened by a DatabaseMetaData call
        return null;
      }
      return stat;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the first warning reported by calls on this object.
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
   * Clears all warnings.
   */
  @Override
  public void clearWarnings() throws SQLException {
    try {
      checkClosed();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  // =============================================================

  /**
   * Returns the value of the specified column as a String.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public String getString(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getString();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a String.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public String getString(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getString();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as an int.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public int getInt(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getInt();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as an int.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public int getInt(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getInt();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a BigDecimal.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getBigDecimal();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a java.sql.Date.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Date getDate(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getDate();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a java.sql.Time.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Time getTime(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getTime();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a java.sql.Timestamp.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getTimestamp();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a BigDecimal.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getBigDecimal();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a java.sql.Date.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Date getDate(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getDate();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a java.sql.Time.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Time getTime(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getTime();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a java.sql.Timestamp.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getTimestamp();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns a column value as a Java object. The data is de-serialized into a
   * Java object (on the client side).
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value or null
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Object getObject(int columnIndex) throws SQLException {
    try {
      Value v = get(columnIndex);
      return conn.convertToDefaultObject(v);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns a column value as a Java object. The data is de-serialized into a
   * Java object (on the client side).
   * 
   * @param columnLabel
   *          the column label
   * @return the value or null
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Object getObject(String columnLabel) throws SQLException {
    try {
      Value v = get(columnLabel);
      return conn.convertToDefaultObject(v);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a boolean.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    try {
      Boolean v = get(columnIndex).getBoolean();
      return v == null ? false : v.booleanValue();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a boolean.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    try {
      Boolean v = get(columnLabel).getBoolean();
      return v == null ? false : v.booleanValue();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a byte.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public byte getByte(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getByte();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a byte.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public byte getByte(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getByte();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a short.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public short getShort(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getShort();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a short.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public short getShort(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getShort();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a long.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public long getLong(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getLong();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a long.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public long getLong(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getLong();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a float.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public float getFloat(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getFloat();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a float.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public float getFloat(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getFloat();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a double.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public double getDouble(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getDouble();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a double.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public double getDouble(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getDouble();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a BigDecimal.
   * 
   * @deprecated use {@link #getBigDecimal(String)}
   * 
   * @param columnLabel
   *          the column label
   * @param scale
   *          the scale of the returned value
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale)
      throws SQLException {
    try {
      if (scale < 0) {
        throw JdbcException.getInvalidValueException("scale", scale);
      }
      BigDecimal bd = get(columnLabel).getBigDecimal();
      return bd == null ? null : MathUtils.setScale(bd, scale);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a BigDecimal.
   * 
   * @deprecated use {@link #getBigDecimal(int)}
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param scale
   *          the scale of the returned value
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale)
      throws SQLException {
    try {
      if (scale < 0) {
        throw JdbcException.getInvalidValueException("scale", scale);
      }
      BigDecimal bd = get(columnIndex).getBigDecimal();
      return bd == null ? null : MathUtils.setScale(bd, scale);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * [Not supported]
   * 
   * @deprecated since JDBC 2.0, use getCharacterStream
   */
  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("unicodeStream");
  }

  /**
   * [Not supported]
   * 
   * @deprecated since JDBC 2.0, use setCharacterStream
   */
  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("unicodeStream");
  }

  /**
   * [Not supported] Gets a column as a object using the specified type mapping.
   */
  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map)
      throws SQLException {
    throw JdbcException.getUnsupportedException("map");
  }

  /**
   * [Not supported] Gets a column as a object using the specified type mapping.
   */
  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map)
      throws SQLException {
    throw JdbcException.getUnsupportedException("map");
  }

  /**
   * [Not supported] Gets a column as a reference.
   */
  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("ref");
  }

  /**
   * [Not supported] Gets a column as a reference.
   */
  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("ref");
  }

  /**
   * Returns the value of the specified column as a java.sql.Date using a
   * specified time zone.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param calendar
   *          the calendar
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
    try {
      return DateTimeUtils.convertDate(get(columnIndex), calendar);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a java.sql.Date using a
   * specified time zone.
   * 
   * @param columnLabel
   *          the column label
   * @param calendar
   *          the calendar
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Date getDate(String columnLabel, Calendar calendar)
      throws SQLException {
    try {
      return DateTimeUtils.convertDate(get(columnLabel), calendar);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a java.sql.Time using a
   * specified time zone.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param calendar
   *          the calendar
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
    try {
      return DateTimeUtils.convertTime(get(columnIndex), calendar);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a java.sql.Time using a
   * specified time zone.
   * 
   * @param columnLabel
   *          the column label
   * @param calendar
   *          the calendar
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Time getTime(String columnLabel, Calendar calendar)
      throws SQLException {
    try {
      return DateTimeUtils.convertTime(get(columnLabel), calendar);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a java.sql.Timestamp using a
   * specified time zone.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param calendar
   *          the calendar
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar calendar)
      throws SQLException {
    try {
      Value value = get(columnIndex);
      return DateTimeUtils.convertTimestamp(value, calendar);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a java.sql.Timestamp.
   * 
   * @param columnLabel
   *          the column label
   * @param calendar
   *          the calendar
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar calendar)
      throws SQLException {
    try {
      Value value = get(columnLabel);
      return DateTimeUtils.convertTimestamp(value, calendar);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a Blob.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("getBlob");
  }

  /**
   * Returns the value of the specified column as a Blob.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("getBlob");
  }

  /**
   * Returns the value of the specified column as a byte array.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getBytes();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a byte array.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   * 
   *           if the column is not found or if the result set is closed
   */
  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getBytes();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as an input stream.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getInputStream();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as an input stream.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getInputStream();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a Clob.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("getClob");
  }

  /**
   * Returns the value of the specified column as a Clob.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("getClob");
  }

  /**
   * Returns the value of the specified column as an Array.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Array getArray(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("getArray");
  }

  /**
   * Returns the value of the specified column as an Array.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Array getArray(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("getArray");
  }

  /**
   * Returns the value of the specified column as an input stream.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("getAsciiStream");
  }

  /**
   * Returns the value of the specified column as an input stream.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("getAsciiStream");
  }

  /**
   * Returns the value of the specified column as a reader.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("getCharacterStream");
  }

  /**
   * Returns the value of the specified column as a reader.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("getCharacterStream");
  }

  /**
   * [Not supported]
   */
  @Override
  public URL getURL(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("getURL");
  }

  /**
   * [Not supported]
   */
  @Override
  public URL getURL(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("getURL");
  }

  // =============================================================

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNull(int columnIndex) throws SQLException {
    try {
      update(columnIndex, ValueNull.INSTANCE);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNull(String columnLabel) throws SQLException {
    try {
      update(columnLabel, ValueNull.INSTANCE);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    try {
      update(columnIndex, ValueBoolean.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if result set is closed or not updatable
   */
  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    try {
      update(columnLabel, ValueBoolean.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    try {
      update(columnIndex, ValueByte.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    try {
      update(columnLabel, ValueByte.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    try {
      update(columnIndex,
          x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    try {
      update(columnLabel,
          x == null ? (Value) ValueNull.INSTANCE : ValueBytes.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    try {
      update(columnIndex, ValueShort.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    try {
      update(columnLabel, ValueShort.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    try {
      update(columnIndex, ValueInt.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    try {
      update(columnLabel, ValueInt.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    try {
      update(columnIndex, ValueLong.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    try {
      update(columnLabel, ValueLong.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    try {
      update(columnIndex, ValueFloat.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    try {
      update(columnLabel, ValueFloat.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    try {
      update(columnIndex, ValueDouble.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    try {
      update(columnLabel, ValueDouble.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x)
      throws SQLException {
    try {
      update(columnIndex,
          x == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x)
      throws SQLException {
    try {
      update(columnLabel,
          x == null ? (Value) ValueNull.INSTANCE : ValueDecimal.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    try {
      update(columnIndex,
          x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    try {
      update(columnLabel,
          x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    try {
      update(columnIndex,
          x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    try {
      update(columnLabel,
          x == null ? (Value) ValueNull.INSTANCE : ValueDate.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    try {
      update(columnIndex,
          x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    try {
      update(columnLabel,
          x == null ? (Value) ValueNull.INSTANCE : ValueTime.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    try {
      update(columnIndex, x == null ? (Value) ValueNull.INSTANCE
          : ValueTimestamp.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateTimestamp(String columnLabel, Timestamp x)
      throws SQLException {
    try {
      update(columnLabel, x == null ? (Value) ValueNull.INSTANCE
          : ValueTimestamp.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateAsciiStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateAsciiStream(int columnIndex, InputStream x)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateAsciiStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateAsciiStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateAsciiStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public void updateAsciiStream(String columnLabel, InputStream x)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateAsciiStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateAsciiStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateBinaryStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateBinaryStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateBinaryStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBinaryStream(String columnLabel, InputStream x)
      throws SQLException {
    updateBinaryStream(columnLabel, x, -1);
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    updateBinaryStream(columnLabel, x, (long) length);
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateBinaryStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateCharacterStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateCharacterStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateCharacterStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateCharacterStream(String columnLabel, Reader x, int length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateCharacterStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateCharacterStream(String columnLabel, Reader x)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateCharacterStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateCharacterStream(String columnLabel, Reader x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateCharacterStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @param scale
   *          is ignored
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateObject(int columnIndex, Object x, int scale)
      throws SQLException {
    try {
      update(columnIndex, convertToUnknownValue(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @param scale
   *          is ignored
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateObject(String columnLabel, Object x, int scale)
      throws SQLException {
    try {
      update(columnLabel, convertToUnknownValue(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    try {
      update(columnIndex, convertToUnknownValue(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    try {
      update(columnLabel, convertToUnknownValue(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * [Not supported]
   */
  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw JdbcException.getUnsupportedException("updateRef");
  }

  /**
   * [Not supported]
   */
  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw JdbcException.getUnsupportedException("updateRef");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBlob(int columnIndex, InputStream x) throws SQLException {
    updateBlob(columnIndex, x, -1);
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @param length
   *          the length
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBlob(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateBlob");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw JdbcException.getUnsupportedException("updateBlob");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw JdbcException.getUnsupportedException("updateBlob");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBlob(String columnLabel, InputStream x) throws SQLException {
    throw JdbcException.getUnsupportedException("updateBlob");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @param length
   *          the length
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateBlob(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateBlob");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw JdbcException.getUnsupportedException("updateClob");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateClob(int columnIndex, Reader x) throws SQLException {
    throw JdbcException.getUnsupportedException("updateClob");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @param length
   *          the length
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateClob(int columnIndex, Reader x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateClob");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw JdbcException.getUnsupportedException("updateClob");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateClob(String columnLabel, Reader x) throws SQLException {
    throw JdbcException.getUnsupportedException("updateClob");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @param length
   *          the length
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateClob(String columnLabel, Reader x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("updateClob");
  }

  /**
   * [Not supported]
   */
  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw JdbcException.getUnsupportedException("updateArray");
  }

  /**
   * [Not supported]
   */
  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw JdbcException.getUnsupportedException("updateArray");
  }

  /**
   * [Not supported] Gets the cursor name if it was defined. This feature is
   * superseded by updateX methods. This method throws a SQLException because
   * cursor names are not supported.
   */
  @Override
  public String getCursorName() throws SQLException {
    throw JdbcException.getUnsupportedException("cursorName");
  }

  /**
   * Gets the current row number. The first row is row 1, the second 2 and so
   * on. This method returns 0 before the first and after the last row.
   * 
   * @return the row number
   */
  @Override
  public int getRow() throws SQLException {
    try {
      checkClosed();
      int rowId = result.getRowId();
      if (rowId >= result.getRowCount()) {
        return 0;
      }
      return rowId + 1;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Gets the result set concurrency. Result sets are only updatable if the
   * statement was created with updatable concurrency, and if the result set
   * contains all columns of the primary key or of a unique index of a table.
   * 
   * @return ResultSet.CONCUR_UPDATABLE if the result set is updatable, or
   *         ResultSet.CONCUR_READ_ONLY otherwise
   */
  @Override
  public int getConcurrency() throws SQLException {
    throw JdbcException.getUnsupportedException("getConcurrency");
  }

  /**
   * Gets the fetch direction.
   * 
   * @return the direction: FETCH_FORWARD
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
   * Gets the number of rows suggested to read in one step.
   * 
   * @return the current fetch size
   */
  @Override
  public int getFetchSize() throws SQLException {
    try {
      checkClosed();
      return result.getFetchSize();
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
   */
  @Override
  public void setFetchSize(int rows) throws SQLException {
    try {
      checkClosed();
      if (rows <= 0) {
        throw JdbcException.getInvalidValueException("rows", rows);
      } else {
        if (stat != null) {
          int maxRows = stat.getMaxRows();
          if (maxRows > 0 && rows > maxRows) {
            throw JdbcException.getInvalidValueException("rows", rows);
          }
        }
      }
      result.setFetchSize(rows);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * [Not supported] Sets (changes) the fetch direction for this result set.
   * This method should only be called for scrollable result sets, otherwise it
   * will throw an exception (no matter what direction is used).
   * 
   * @param direction
   *          the new fetch direction
   * @throws SQLException
   *           Unsupported Feature if the method is called for a forward-only
   *           result set
   */
  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw JdbcException.getUnsupportedException("setFetchDirection");
  }

  /**
   * Get the result set type.
   * 
   * @return the result set type (TYPE_FORWARD_ONLY, TYPE_SCROLL_INSENSITIVE or
   *         TYPE_SCROLL_SENSITIVE)
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public int getType() throws SQLException {
    try {
      checkClosed();
      return stat == null ? ResultSet.TYPE_FORWARD_ONLY : stat.resultSetType;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Checks if the current position is before the first row, that means next()
   * was not called yet, and there is at least one row.
   * 
   * @return if there are results and the current position is before the first
   *         row
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public boolean isBeforeFirst() throws SQLException {
    try {
      checkClosed();
      int row = result.getRowId();
      int count = result.getRowCount();
      return count > 0 && row < 0;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Checks if the current position is after the last row, that means next() was
   * called and returned false, and there was at least one row.
   * 
   * @return if there are results and the current position is after the last row
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public boolean isAfterLast() throws SQLException {
    try {
      checkClosed();
      int row = result.getRowId();
      int count = result.getRowCount();
      return count > 0 && row >= count;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Checks if the current position is row 1, that means next() was called once
   * and returned true.
   * 
   * @return if the current position is the first row
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public boolean isFirst() throws SQLException {
    try {
      checkClosed();
      int row = result.getRowId();
      return row == 0 && row < result.getRowCount();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Checks if the current position is the last row, that means next() was
   * called and did not yet returned false, but will in the next call.
   * 
   * @return if the current position is the last row
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public boolean isLast() throws SQLException {
    try {
      checkClosed();
      int row = result.getRowId();
      return row >= 0 && row == result.getRowCount() - 1;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Moves the current position to before the first row, that means resets the
   * result set.
   * 
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public void beforeFirst() throws SQLException {
    try {
      checkClosed();
      if (result.getRowId() >= 0) {
        resetResult();
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Moves the current position to after the last row, that means after the end.
   * 
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public void afterLast() throws SQLException {
    try {
      checkClosed();
      while (nextRow()) {
        // nothing
      }
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Moves the current position to the first row. This is the same as calling
   * beforeFirst() followed by next().
   * 
   * @return true if there is a row available, false if not
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public boolean first() throws SQLException {
    try {
      checkClosed();
      if (result.getRowId() < 0) {
        return nextRow();
      }
      resetResult();
      return nextRow();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Moves the current position to the last row.
   * 
   * @return true if there is a row available, false if not
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public boolean last() throws SQLException {
    try {
      checkClosed();
      return absolute(-1);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Moves the current position to a specific row.
   * 
   * @param rowNumber
   *          the row number. 0 is not allowed, 1 means the first row, 2 the
   *          second. -1 means the last row, -2 the row before the last row. If
   *          the value is too large, the position is moved after the last row,
   *          if if the value is too small it is moved before the first row.
   * @return true if there is a row available, false if not
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public boolean absolute(int rowNumber) throws SQLException {
    try {
      checkClosed();
      if (rowNumber < 0) {
        rowNumber = result.getRowCount() + rowNumber + 1;
      } else if (rowNumber > result.getRowCount() + 1) {
        rowNumber = result.getRowCount() + 1;
      }
      if (rowNumber <= result.getRowId()) {
        resetResult();
      }
      while (result.getRowId() + 1 < rowNumber) {
        nextRow();
      }
      int row = result.getRowId();
      return row >= 0 && row < result.getRowCount();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Moves the current position to a specific row relative to the current row.
   * 
   * @param rowCount
   *          0 means don't do anything, 1 is the next row, -1 the previous. If
   *          the value is too large, the position is moved after the last row,
   *          if if the value is too small it is moved before the first row.
   * @return true if there is a row available, false if not
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public boolean relative(int rowCount) throws SQLException {
    try {
      checkClosed();
      int row = result.getRowId() + 1 + rowCount;
      if (row < 0) {
        row = 0;
      } else if (row > result.getRowCount()) {
        row = result.getRowCount() + 1;
      }
      return absolute(row);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Moves the cursor to the last row, or row before first row if the current
   * position is the first row.
   * 
   * @return true if there is a row available, false if not
   * @throws SQLException
   *           if the result set is closed
   */
  @Override
  public boolean previous() throws SQLException {
    try {
      checkClosed();
      return relative(-1);
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Moves the current position to the insert row. The current row is
   * remembered.
   * 
   * @throws SQLException
   *           if the result set is closed or is not updatable
   */
  @Override
  public void moveToInsertRow() throws SQLException {
    try {
      checkUpdatable();
      insertRow = new Value[columnCount];
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Moves the current position to the current row.
   * 
   * @throws SQLException
   *           if the result set is closed or is not updatable
   */
  @Override
  public void moveToCurrentRow() throws SQLException {
    try {
      checkUpdatable();
      insertRow = null;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Detects if the row was updated (by somebody else or the caller).
   * 
   * @return false because this driver does not detect this
   */
  @Override
  public boolean rowUpdated() throws SQLException {
    try {
      return false;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Detects if the row was inserted.
   * 
   * @return false because this driver does not detect this
   */
  @Override
  public boolean rowInserted() throws SQLException {
    try {
      return false;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Detects if the row was deleted (by somebody else or the caller).
   * 
   * @return false because this driver does not detect this
   */
  @Override
  public boolean rowDeleted() throws SQLException {
    try {
      return false;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Inserts the current row. The current position must be the insert row.
   * 
   * @throws SQLException
   *           if the result set is closed or if not on the insert row, or if
   *           the result set it not updatable
   */
  @Override
  public void insertRow() throws SQLException {
    throw JdbcException.getUnsupportedException("insertRow");
  }

  /**
   * Updates the current row.
   * 
   * @throws SQLException
   *           if the result set is closed, if the current row is the insert row
   *           or if not on a valid row, or if the result set it not updatable
   */
  @Override
  public void updateRow() throws SQLException {
    throw JdbcException.getUnsupportedException("updateRow");
  }

  /**
   * Deletes the current row.
   * 
   * @throws SQLException
   *           if the result set is closed, if the current row is the insert row
   *           or if not on a valid row, or if the result set it not updatable
   */
  @Override
  public void deleteRow() throws SQLException {
    throw JdbcException.getUnsupportedException("deleteRow");
  }

  /**
   * Re-reads the current row from the database.
   * 
   * @throws SQLException
   *           if the result set is closed or if the current row is the insert
   *           row or if the row has been deleted or if not on a valid row
   */
  @Override
  public void refreshRow() throws SQLException {
    throw JdbcException.getUnsupportedException("refreshRow");
  }

  /**
   * Cancels updating a row.
   * 
   * @throws SQLException
   *           if the result set is closed or if the current row is the insert
   *           row
   */
  @Override
  public void cancelRowUpdates() throws SQLException {
    throw JdbcException.getUnsupportedException("cancelRowUpdates");
  }

  // =============================================================

  private int getColumnIndex(String columnLabel) {
    checkClosed();
    if (columnLabel == null) {
      throw JdbcException.getInvalidValueException("columnLabel", null);
    }
    if (columnCount >= 3) {
      // use a hash table if more than 2 columns
      if (columnLabelMap == null) {
        HashMap<String, Integer> map = New.hashMap(columnCount);
        // column labels have higher priority
        for (int i = 0; i < columnCount; i++) {
          String c = StringUtils.toUpperEnglish(result.getAlias(i));
          mapColumn(map, c, i);
        }
        for (int i = 0; i < columnCount; i++) {
          String colName = result.getColumnName(i);
          if (colName != null) {
            colName = StringUtils.toUpperEnglish(colName);
            mapColumn(map, colName, i);
            String tabName = result.getTableName(i);
            if (tabName != null) {
              colName = StringUtils.toUpperEnglish(tabName) + "." + colName;
              mapColumn(map, colName, i);
            }
          }
        }
        // assign at the end so concurrent access is supported
        columnLabelMap = map;
      }
      Integer index = columnLabelMap.get(StringUtils
          .toUpperEnglish(columnLabel));
      if (index == null) {
        throw JdbcException.get(SQLErrorCode.COLUMN_NOT_FOUND_1, columnLabel);
      }
      return index.intValue() + 1;
    }
    for (int i = 0; i < columnCount; i++) {
      if (columnLabel.equalsIgnoreCase(result.getAlias(i))) {
        return i + 1;
      }
    }
    int idx = columnLabel.indexOf('.');
    if (idx > 0) {
      String table = columnLabel.substring(0, idx);
      String col = columnLabel.substring(idx + 1);
      for (int i = 0; i < columnCount; i++) {
        if (table.equalsIgnoreCase(result.getTableName(i))
            && col.equalsIgnoreCase(result.getColumnName(i))) {
          return i + 1;
        }
      }
    } else {
      for (int i = 0; i < columnCount; i++) {
        if (columnLabel.equalsIgnoreCase(result.getColumnName(i))) {
          return i + 1;
        }
      }
    }
    throw JdbcException.get(SQLErrorCode.COLUMN_NOT_FOUND_1, columnLabel);
  }

  private static void mapColumn(HashMap<String, Integer> map, String label,
      int index) {
    // put the index (usually that's the only operation)
    Integer old = map.put(label, index);
    if (old != null) {
      // if there was a clash (which is seldom),
      // put the old one back
      map.put(label, old);
    }
  }

  private void checkColumnIndex(int columnIndex) {
    checkClosed();
    if (columnIndex < 1 || columnIndex > columnCount) {
      throw JdbcException.getInvalidValueException("columnIndex", columnIndex);
    }
  }

  /**
   * Check if this result set is closed.
   * 
   * @throws JdbcException
   *           if it is closed
   */
  void checkClosed() {
    if (result == null) {
      throw JdbcException.get(SQLErrorCode.OBJECT_CLOSED);
    }
    if (stat != null) {
      stat.checkClosed();
    }
    if (conn != null) {
      conn.checkClosed();
    }
  }

  private void checkOnValidRow() {
    if (result.getRowId() < 0 || result.getRowId() >= result.getRowCount()) {
      throw JdbcException.get(SQLErrorCode.NO_DATA_AVAILABLE);
    }
  }

  private Value get(int columnIndex) {
    checkColumnIndex(columnIndex);
    checkOnValidRow();
    Value[] list = result.currentRow();
    Value value = list[columnIndex - 1];
    wasNull = value == ValueNull.INSTANCE;
    return value;
  }

  private Value get(String columnLabel) {
    int columnIndex = getColumnIndex(columnLabel);
    return get(columnIndex);
  }

  private void update(String columnLabel, Value v) {
    int columnIndex = getColumnIndex(columnLabel);
    update(columnIndex, v);
  }

  private void update(int columnIndex, Value v) {
    checkUpdatable();
    checkColumnIndex(columnIndex);
    if (insertRow != null) {
      insertRow[columnIndex - 1] = v;
    } else {
      if (updateRow == null) {
        updateRow = new Value[columnCount];
      }
      updateRow[columnIndex - 1] = v;
    }
  }

  private boolean nextRow() throws SQLException {
    boolean next = result.next();
    if (!next) {
      result.close();
    }
    return next;
  }

  /**
   * [Not supported] Returns the value of the specified column as a row id.
   * 
   * @param columnIndex
   *          (1,2,...)
   */
  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("rowId");
  }

  /**
   * [Not supported] Returns the value of the specified column as a row id.
   * 
   * @param columnLabel
   *          the column label
   */
  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("rowId");
  }

  /**
   * [Not supported] Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   */
  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw JdbcException.getUnsupportedException("rowId");
  }

  /**
   * [Not supported] Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   */
  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw JdbcException.getUnsupportedException("rowId");
  }

  /**
   * Returns the current result set holdability.
   * 
   * @return the holdability
   * @throws SQLException
   *           if the connection is closed
   */
  @Override
  public int getHoldability() throws SQLException {
    try {
      checkClosed();
      return conn.getHoldability();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns whether this result set is closed.
   * 
   * @return true if the result set is closed
   */
  @Override
  public boolean isClosed() throws SQLException {
    try {
      return result == null;
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNString(int columnIndex, String x) throws SQLException {
    try {
      update(columnIndex,
          x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNString(String columnLabel, String x) throws SQLException {
    try {
      update(columnLabel,
          x == null ? (Value) ValueNull.INSTANCE : ValueString.get(x));
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * [Not supported]
   */
  @Override
  public void updateNClob(int columnIndex, NClob x) throws SQLException {
    throw JdbcException.getUnsupportedException("NClob");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNClob(int columnIndex, Reader x) throws SQLException {
    updateClob(columnIndex, x, -1);
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @param length
   *          the length
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNClob(int columnIndex, Reader x, long length)
      throws SQLException {
    updateClob(columnIndex, x, length);
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNClob(String columnLabel, Reader x) throws SQLException {
    updateClob(columnLabel, x, -1);
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @param length
   *          the length
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNClob(String columnLabel, Reader x, long length)
      throws SQLException {
    updateClob(columnLabel, x, length);
  }

  /**
   * [Not supported]
   */
  @Override
  public void updateNClob(String columnLabel, NClob x) throws SQLException {
    throw JdbcException.getUnsupportedException("NClob");
  }

  /**
   * Returns the value of the specified column as a Clob.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("NClob");
  }

  // */@Override

  /**
   * Returns the value of the specified column as a Clob.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("NClob");
  }

  /**
   * [Not supported] Returns the value of the specified column as a SQLXML
   * object.
   */
  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("SQLXML");
  }

  /**
   * [Not supported] Returns the value of the specified column as a SQLXML
   * object.
   */
  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("SQLXML");
  }

  // */@Override

  /**
   * [Not supported] Updates a column in the current or insert row.
   */
  @Override
  // ## Java 1.6 ##
  public void updateSQLXML(int columnIndex, SQLXML xmlObject)
      throws SQLException {
    throw JdbcException.getUnsupportedException("SQLXML");
  }

  /**
   * [Not supported] Updates a column in the current or insert row.
   */
  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject)
      throws SQLException {
    throw JdbcException.getUnsupportedException("SQLXML");
  }

  // */@Override

  /**
   * Returns the value of the specified column as a String.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public String getNString(int columnIndex) throws SQLException {
    try {
      return get(columnIndex).getString();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a String.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public String getNString(String columnLabel) throws SQLException {
    try {
      return get(columnLabel).getString();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the value of the specified column as a reader.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw JdbcException.getUnsupportedException("getNCharacterStream");
  }

  /**
   * Returns the value of the specified column as a reader.
   * 
   * @param columnLabel
   *          the column label
   * @return the value
   * @throws SQLException
   *           if the column is not found or if the result set is closed
   */
  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw JdbcException.getUnsupportedException("getNCharacterStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    throw JdbcException.getUnsupportedException("getNCharacterStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnIndex
   *          (1,2,...)
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("getNCharacterStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNCharacterStream(String columnLabel, Reader x)
      throws SQLException {
    throw JdbcException.getUnsupportedException("getNCharacterStream");
  }

  /**
   * Updates a column in the current or insert row.
   * 
   * @param columnLabel
   *          the column label
   * @param x
   *          the value
   * @param length
   *          the number of characters
   * @throws SQLException
   *           if the result set is closed or not updatable
   */
  @Override
  public void updateNCharacterStream(String columnLabel, Reader x, long length)
      throws SQLException {
    throw JdbcException.getUnsupportedException("getNCharacterStream");
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

  private void resetResult() {
    result.reset();
  }

  /**
   * INTERNAL
   */
  @Override
  public String toString() {
    return "ResultSet : " + result;
  }

  private Value convertToUnknownValue(Object x) {
    checkClosed();
    return DataType.convertToValue(conn.getSession(), x, Value.UNKNOWN);
  }

  private void checkUpdatable() {
    checkClosed();
    throw JdbcException.get(SQLErrorCode.RESULT_SET_READONLY);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
	  throw new NotImplementedException();
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
	  throw new NotImplementedException();
  }
}

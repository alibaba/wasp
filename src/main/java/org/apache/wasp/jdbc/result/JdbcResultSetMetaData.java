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
package org.apache.wasp.jdbc.result;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.wasp.jdbc.JdbcException;
import org.apache.wasp.util.MathUtils;

/**
 * Represents the meta data for a ResultSet.
 */
public class JdbcResultSetMetaData implements ResultSetMetaData {

  private final String catalog;
  private final JdbcResultSet rs;
  private final ResultInterface result;
  private final int columnCount;

  public JdbcResultSetMetaData(JdbcResultSet rs, ResultInterface result,
      String catalog) {
    this.catalog = catalog;
    this.rs = rs;
    this.result = result;
    this.columnCount = result.getVisibleColumnCount();
  }

  /**
   * Returns the number of columns.
   * 
   * @return the number of columns
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public int getColumnCount() throws SQLException {
    try {
      checkClosed();
      return columnCount;
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false; // To change body of implemented methods use File | Settings |
                  // File Templates.
  }

  private SQLException logAndConvert(Exception e) {
    return null; // To change body of created methods use File | Settings | File
                 // Templates.
  }

  /**
   * Returns the column label.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return the column label
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public String getColumnLabel(int column) throws SQLException {
    return getColumnName(column);
    // throw JdbcException.getUnsupportedException("getColumnLabel");
  }

  /**
   * Returns the column name.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return the column name
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public String getColumnName(int column) throws SQLException {
    try {
      checkColumnIndex(column);
      return result.getColumnName(--column);
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  /**
   * Returns the data type of a column. See also java.sql.Type.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return the data type
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public int getColumnType(int column) throws SQLException {
    throw JdbcException.getUnsupportedException("getColumnType");
  }

  /**
   * Returns the data type name of a column.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return the data type name
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public String getColumnTypeName(int column) throws SQLException {
    throw JdbcException.getUnsupportedException("getColumnTypeName");
  }

  /**
   * Returns the schema name.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return the schema name, or "" (an empty string) if not applicable
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public String getSchemaName(int column) throws SQLException {
    throw JdbcException.getUnsupportedException("getSchemaName");
  }

  /**
   * Returns the table name.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return the table name
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public String getTableName(int column) throws SQLException {
    return result.getTableName(0);
    // throw JdbcException.getUnsupportedException("getTableName");
  }

  /**
   * Returns the catalog name.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return the catalog name
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public String getCatalogName(int column) throws SQLException {
    try {
      checkColumnIndex(column);
      return catalog == null ? "" : catalog;
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  /**
   * Checks if this column is case sensitive. It always returns true.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return true
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    try {
      checkColumnIndex(column);
      return true;
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  /**
   * Checks if this column is searchable. It always returns true.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return true
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public boolean isSearchable(int column) throws SQLException {
    try {
      checkColumnIndex(column);
      return true;
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  /**
   * Checks if this is a currency column. It always returns false.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return false
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public boolean isCurrency(int column) throws SQLException {
    try {
      checkColumnIndex(column);
      return false;
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  /**
   * Checks if this is nullable column. Returns
   * ResultSetMetaData.columnNullableUnknown if this is not a column of a table.
   * Otherwise, it returns ResultSetMetaData.columnNoNulls if the column is not
   * nullable, and ResultSetMetaData.columnNullable if it is nullable.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return ResultSetMetaData.column*
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public int isNullable(int column) throws SQLException {
    try {
      checkColumnIndex(column);
      return result.getNullable(--column);
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return false; // To change body of implemented methods use File | Settings |
                  // File Templates.
  }

  /**
   * Checks if this column is read only. It always returns false.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return false
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public boolean isReadOnly(int column) throws SQLException {
    try {
      checkColumnIndex(column);
      return false;
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  /**
   * Checks whether it is possible for a write on this column to succeed. It
   * always returns true.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return true
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public boolean isWritable(int column) throws SQLException {
    try {
      checkColumnIndex(column);
      return true;
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  /**
   * Checks whether a write on this column will definitely succeed. It always
   * returns false.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return false
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    try {
      checkColumnIndex(column);
      return false;
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return null; // To change body of implemented methods use File | Settings |
                 // File Templates.
  }

  /**
   * Gets the precision for this column. This method always returns 0.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return the precision
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public int getPrecision(int column) throws SQLException {
    try {
      checkColumnIndex(column);
      long prec = result.getColumnPrecision(--column);
      return MathUtils.convertLongToInt(prec);
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  /**
   * Gets the scale for this column. This method always returns 0.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return the scale
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public int getScale(int column) throws SQLException {
    try {
      checkColumnIndex(column);
      return result.getColumnScale(--column);
    } catch (Exception e) {
      throw logAndConvert(e);
    }
  }

  /**
   * Gets the maximum display size for this column.
   * 
   * @param column
   *          the column index (1,2,...)
   * @return the display size
   * @throws java.sql.SQLException
   *           if the result set is closed or invalid
   */
  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    throw JdbcException.getUnsupportedException("getColumnDisplaySize");
  }

  private void checkClosed() {
    if (rs != null) {
      rs.checkClosed();
    }
  }

  private void checkColumnIndex(int columnIndex) throws SQLException {
    checkClosed();
    if (columnIndex < 1 || columnIndex > columnCount) {
      throw JdbcException
          .getInvalidValueException("columnIndex", columnIndex);
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null; // To change body of implemented methods use File | Settings |
                 // File Templates.
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false; // To change body of implemented methods use File | Settings |
                  // File Templates.
  }
}

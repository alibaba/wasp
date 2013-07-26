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
package com.alibaba.wasp.jdbc.result;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.jdbc.JdbcConnection;
import com.alibaba.wasp.jdbc.JdbcException;
import com.alibaba.wasp.jdbc.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

/**
 * Represents the meta data for a database.
 */
public class JdbcDatabaseMetaData implements DatabaseMetaData {

  private static Log log = LogFactory.getLog(JdbcDatabaseMetaData.class);

  private final JdbcConnection conn;

  public JdbcDatabaseMetaData(JdbcConnection conn) {
    this.conn = conn;
  }

  /**
   * Returns the major version of this driver.
   * 
   * @return the major version number
   */
  public int getDriverMajorVersion() {
    return FConstants.VERSION_MAJOR;
  }

  /**
   * Returns the minor version of this driver.
   * 
   * @return the minor version number
   */
  public int getDriverMinorVersion() {
    return FConstants.VERSION_MINOR;
  }

  /**
   * Gets the database product name.
   * 
   * @return the product name ("wasp")
   */
  public String getDatabaseProductName() {
    return "WASP";
  }

  /**
   * Gets the product version of the database.
   * 
   * @return the product version
   */
  public String getDatabaseProductVersion() {
    return FConstants.getFullVersion();
  }

  /**
   * Gets the name of the JDBC driver.
   * 
   * @return the driver name ("WASP JDBC Driver")
   */
  public String getDriverName() {
    return "WASP JDBC Driver";
  }

  /**
   * Gets the version number of the driver. The format is
   * [MajorVersion].[MinorVersion].
   * 
   * @return the version number
   */
  public String getDriverVersion() {
    return FConstants.getFullVersion();
  }


  public ResultSet getTables(String catalogPattern, String schemaPattern,
      String tableNamePattern, String[] types) throws SQLException {
    throw JdbcException.getUnsupportedException("getTables");
  }


  public ResultSet getColumns(String catalogPattern, String schemaPattern,
      String tableNamePattern, String columnNamePattern) throws SQLException {
    throw JdbcException.getUnsupportedException("getColumns");
  }

  /**
   * Unsupported
   * @param catalogPattern
   *          null or the catalog name
   * @param schemaPattern
   *          null (to get all objects) or a schema name (uppercase for unquoted
   *          names)
   * @param tableName
   *          table name (must be specified)
   * @param unique
   *          only unique indexes
   * @param approximate
   *          is ignored
   * @return the list of indexes and columns
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getIndexInfo(String catalogPattern, String schemaPattern,
      String tableName, boolean unique, boolean approximate)
      throws SQLException {
    throw JdbcException.getUnsupportedException("getIndexInfo");
  }

  /**
   * Unsupported
   *
   * @param catalogPattern
   *          null or the catalog name
   * @param schemaPattern
   *          null (to get all objects) or a schema name (uppercase for unquoted
   *          names)
   * @param tableName
   *          table name (must be specified)
   * @return the list of primary key columns
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getPrimaryKeys(String catalogPattern, String schemaPattern,
      String tableName) throws SQLException {
    throw JdbcException.getUnsupportedException("getPrimaryKeys");
  }

  /**
   * Checks if all procedures callable.
   * 
   * @return true
   */
  public boolean allProceduresAreCallable() {
    return true;
  }

  /**
   * Checks if it possible to query all tables returned by getTables.
   * 
   * @return true
   */
  public boolean allTablesAreSelectable() {
    return true;
  }

  /**
   * Returns the database URL for this connection.
   * 
   * @return the url
   */
  public String getURL() throws SQLException {
    try {
      return conn.getURL();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Returns the user name as passed to DriverManager.getConnection(url, user,
   * password).
   * 
   * @return the user name
   */
  public String getUserName() throws SQLException {
    try {
      return conn.getUser();
    } catch (Exception e) {
      throw Logger.logAndConvert(log,e);
    }
  }

  /**
   * Returns the same as Connection.isReadOnly().
   * 
   * @return if read only optimization is switched on
   */
  public boolean isReadOnly() throws SQLException {
    try {
      return conn.isReadOnly();
    } catch (Exception e) {
      throw Logger.logAndConvert(log, e);
    }
  }

  /**
   * Checks if NULL is sorted high (bigger than anything that is not null).
   * 
   * @return false by default; true if the system property h2.sortNullsHigh is
   *         set to true
   */
  public boolean nullsAreSortedHigh() {
    return false;
  }

  /**
   * Checks if NULL is sorted low (smaller than anything that is not null).
   * 
   * @return true by default; false if the system property h2.sortNullsHigh is
   *         set to true
   */
  public boolean nullsAreSortedLow() {
    return false;
  }

  /**
   * Checks if NULL is sorted at the beginning (no matter if ASC or DESC is
   * used).
   * 
   * @return false
   */
  public boolean nullsAreSortedAtStart() {
    return false;
  }

  /**
   * Checks if NULL is sorted at the end (no matter if ASC or DESC is used).
   * 
   * @return false
   */
  public boolean nullsAreSortedAtEnd() {
    return false;
  }

  /**
   * Returns the connection that created this object.
   * 
   * @return the connection
   */
  public Connection getConnection() {
    return conn;
  }

  /**
   * Gets the list of procedures. The result set is sorted by PROCEDURE_SCHEM,
   * PROCEDURE_NAME, and NUM_INPUT_PARAMS. There are potentially multiple
   * procedures with the same name, each with a different number of input
   * parameters.
   * 
   * <ul>
   * <li>1 PROCEDURE_CAT (String) catalog</li>
   * <li>2 PROCEDURE_SCHEM (String) schema</li>
   * <li>3 PROCEDURE_NAME (String) name</li>
   * <li>4 NUM_INPUT_PARAMS (int) the number of arguments</li>
   * <li>5 NUM_OUTPUT_PARAMS (int) for future use, always 0</li>
   * <li>6 NUM_RESULT_SETS (int) for future use, always 0</li>
   * <li>7 REMARKS (String) description</li>
   * <li>8 PROCEDURE_TYPE (short) if this procedure returns a result
   * (procedureNoResult or procedureReturnsResult)</li>
   * <li>9 SPECIFIC_NAME (String) name</li>
   * </ul>
   * 
   * @param catalogPattern
   *          null or the catalog name
   * @param schemaPattern
   *          null (to get all objects) or a schema name (uppercase for unquoted
   *          names)
   * @param procedureNamePattern
   *          the procedure name pattern
   * @return the procedures
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getProcedures(String catalogPattern, String schemaPattern,
      String procedureNamePattern) throws SQLException {
    throw JdbcException.getUnsupportedException("getProcedures");
  }

  /**
   * Unsupported
   * @param catalogPattern
   *          null or the catalog name
   * @param schemaPattern
   *          null (to get all objects) or a schema name (uppercase for unquoted
   *          names)
   * @param procedureNamePattern
   *          the procedure name pattern
   * @param columnNamePattern
   *          the procedure name pattern
   * @return the procedure columns
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getProcedureColumns(String catalogPattern,
      String schemaPattern, String procedureNamePattern,
      String columnNamePattern) throws SQLException {
    throw JdbcException.getUnsupportedException("getProcedureColumns");
  }

  /**
   * Gets the list of schemas. The result set is sorted by TABLE_SCHEM.
   * 
   * <ul>
   * <li>1 TABLE_SCHEM (String) schema name</li>
   * <li>2 TABLE_CATALOG (String) catalog name</li>
   * <li>3 IS_DEFAULT (boolean) if this is the default schema</li>
   * </ul>
   * 
   * @return the schema list
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getSchemas() throws SQLException {
    throw JdbcException.getUnsupportedException("getSchemas");
  }

  /**
   * unsupported
   * 
   * <ul>
   * <li>1 TABLE_CAT (String) catalog name</li>
   * </ul>
   * 
   * @return the catalog list
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getCatalogs() throws SQLException {
    throw JdbcException.getUnsupportedException("getCatalogs");
  }

  /**
   * unsupported
   *
   * @return the table types
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getTableTypes() throws SQLException {
    throw JdbcException.getUnsupportedException("getTableTypes");
  }

  /**
   * unsupported
   *
   * @param catalogPattern
   *          null (to get all objects) or the catalog name
   * @param schemaPattern
   *          null (to get all objects) or a schema name (uppercase for unquoted
   *          names)
   * @param table
   *          a table name (uppercase for unquoted names)
   * @param columnNamePattern
   *          null (to get all objects) or a column name (uppercase for unquoted
   *          names)
   * @return the list of privileges
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getColumnPrivileges(String catalogPattern,
      String schemaPattern, String table, String columnNamePattern)
      throws SQLException {
    throw JdbcException.getUnsupportedException("getColumnPrivileges");
  }

  /**
   * unsupported
   * 
   * @param catalogPattern
   *          null (to get all objects) or the catalog name
   * @param schemaPattern
   *          null (to get all objects) or a schema name (uppercase for unquoted
   *          names)
   * @param tableNamePattern
   *          null (to get all objects) or a table name (uppercase for unquoted
   *          names)
   * @return the list of privileges
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getTablePrivileges(String catalogPattern,
      String schemaPattern, String tableNamePattern) throws SQLException {
    throw JdbcException.getUnsupportedException("getTablePrivileges");
  }

  /**
   * unsupported
   * 
   * @param catalogPattern
   *          null (to get all objects) or the catalog name
   * @param schemaPattern
   *          null (to get all objects) or a schema name (uppercase for unquoted
   *          names)
   * @param tableName
   *          table name (must be specified)
   * @param scope
   *          ignored
   * @param nullable
   *          ignored
   * @return the primary key index
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getBestRowIdentifier(String catalogPattern,
      String schemaPattern, String tableName, int scope, boolean nullable)
      throws SQLException {
    throw JdbcException.getUnsupportedException("getBestRowIdentifier");
  }

  /**
   *
   * 
   * @param catalog
   *          null (to get all objects) or the catalog name
   * @param schema
   *          null (to get all objects) or a schema name
   * @param tableName
   *          table name (must be specified)
   * @return an empty result set
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getVersionColumns(String catalog, String schema,
      String tableName) throws SQLException {
    throw JdbcException.getUnsupportedException("getVersionColumns");
  }

  /**
   *
   * 
   * @param catalogPattern
   *          null (to get all objects) or the catalog name
   * @param schemaPattern
   *          the schema name of the foreign table
   * @param tableName
   *          the name of the foreign table
   * @return the result set
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getImportedKeys(String catalogPattern, String schemaPattern,
      String tableName) throws SQLException {
    throw JdbcException.getUnsupportedException("getImportedKeys");
  }

  /**
   *
   * @param catalogPattern
   *          null or the catalog name
   * @param schemaPattern
   *          the schema name of the primary table
   * @param tableName
   *          the name of the primary table
   * @return the result set
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getExportedKeys(String catalogPattern, String schemaPattern,
      String tableName) throws SQLException {
    throw JdbcException.getUnsupportedException("getExportedKeys");
  }

  /**
   *
   * @param primaryCatalogPattern
   *          null or the catalog name
   * @param primarySchemaPattern
   *          the schema name of the primary table (optional)
   * @param primaryTable
   *          the name of the primary table (must be specified)
   * @param foreignCatalogPattern
   *          null or the catalog name
   * @param foreignSchemaPattern
   *          the schema name of the foreign table (optional)
   * @param foreignTable
   *          the name of the foreign table (must be specified)
   * @return the result set
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getCrossReference(String primaryCatalogPattern,
      String primarySchemaPattern, String primaryTable,
      String foreignCatalogPattern, String foreignSchemaPattern,
      String foreignTable) throws SQLException {
    throw JdbcException.getUnsupportedException("getCrossReference");
  }

  /**
   *
   * @param catalog
   *          ignored
   * @param schemaPattern
   *          ignored
   * @param typeNamePattern
   *          ignored
   * @param types
   *          ignored
   * @return an empty result set
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getUDTs(String catalog, String schemaPattern,
      String typeNamePattern, int[] types) throws SQLException {
    throw JdbcException.getUnsupportedException("getUDTs");
  }

  /**
   *
   * @return the list of data types
   * @throws java.sql.SQLException
   *           if the connection is closed
   */
  public ResultSet getTypeInfo() throws SQLException {
    throw JdbcException.getUnsupportedException("getTypeInfo");
  }

  /**
   * Checks if this database store data in local files.
   * 
   * @return true
   */
  public boolean usesLocalFiles() {
    return true;
  }

  /**
   * Checks if this database use one file per table.
   * 
   * @return false
   */
  public boolean usesLocalFilePerTable() {
    return false;
  }

  /**
   * Returns the string used to quote identifiers.
   * 
   * @return a double quote
   */
  public String getIdentifierQuoteString() {
    return "\"";
  }

  /**
   *
   * @return a list of additional the keywords
   */
  public String getSQLKeywords() {
    return "LIMIT,MINUS,ROWNUM,SYSDATE,SYSTIME,SYSTIMESTAMP,TODAY";
  }

  /**
   * Returns the list of numeric functions supported by this database.
   * 
   * @return the list
   */
  public String getNumericFunctions() throws SQLException {
    return getFunctions("Functions (Numeric)");
  }

  /**
   * Returns the list of string functions supported by this database.
   * 
   * @return the list
   */
  public String getStringFunctions() throws SQLException {
    return getFunctions("Functions (String)");
  }

  /**
   * Returns the list of system functions supported by this database.
   * 
   * @return the list
   */
  public String getSystemFunctions() throws SQLException {
    return getFunctions("Functions (System)");
  }

  /**
   * Returns the list of date and time functions supported by this database.
   * 
   * @return the list
   */
  public String getTimeDateFunctions() throws SQLException {
    return getFunctions("Functions (Time and Date)");
  }

  private String getFunctions(String section) throws SQLException {
    throw JdbcException.getUnsupportedException("getFunctions");
  }

  /**
   * Returns the default escape character for DatabaseMetaData search patterns.
   * 
   * @return the default escape character (always '\', independent on the mode)
   */
  public String getSearchStringEscape() {
    return "\\";
  }

  /**
   * Returns the characters that are allowed for identifiers in addiction to
   * A-Z, a-z, 0-9 and '_'.
   * 
   * @return an empty String ("")
   */
  public String getExtraNameCharacters() {
    return "";
  }

  /**
   * Returns whether alter table with add column is supported.
   * 
   * @return true
   */
  public boolean supportsAlterTableWithAddColumn() {
    return true;
  }

  /**
   * Returns whether alter table with drop column is supported.
   * 
   * @return true
   */
  public boolean supportsAlterTableWithDropColumn() {
    return true;
  }

  /**
   * Returns whether column aliasing is supported.
   * 
   * @return true
   */
  public boolean supportsColumnAliasing() {
    return true;
  }

  /**
   * Returns whether NULL+1 is NULL or not.
   * 
   * @return true
   */
  public boolean nullPlusNonNullIsNull() {
    return true;
  }

  /**
   * Returns whether CONVERT is supported.
   * 
   * @return true
   */
  public boolean supportsConvert() {
    return true;
  }

  /**
   * Returns whether CONVERT is supported for one datatype to another.
   * 
   * @param fromType
   *          the source SQL type
   * @param toType
   *          the target SQL type
   * @return true
   */
  public boolean supportsConvert(int fromType, int toType) {
    return true;
  }

  /**
   * Returns whether table correlation names (table alias) are supported.
   * 
   * @return true
   */
  public boolean supportsTableCorrelationNames() {
    return true;
  }

  /**
   * Returns whether table correlation names (table alias) are restricted to be
   * different than table names.
   * 
   * @return false
   */
  public boolean supportsDifferentTableCorrelationNames() {
    return false;
  }

  /**
   * Returns whether expression in ORDER BY are supported.
   * 
   * @return true
   */
  public boolean supportsExpressionsInOrderBy() {
    return true;
  }

  /**
   * Returns whether ORDER BY is supported if the column is not in the SELECT
   * list.
   * 
   * @return true
   */
  public boolean supportsOrderByUnrelated() {
    return true;
  }

  /**
   * Returns whether GROUP BY is supported.
   * 
   * @return true
   */
  public boolean supportsGroupBy() {
    return true;
  }

  /**
   * Returns whether GROUP BY is supported if the column is not in the SELECT
   * list.
   * 
   * @return true
   */
  public boolean supportsGroupByUnrelated() {
    return true;
  }

  /**
   * Checks whether a GROUP BY clause can use columns that are not in the SELECT
   * clause, provided that it specifies all the columns in the SELECT clause.
   * 
   * @return true
   */
  public boolean supportsGroupByBeyondSelect() {
    return true;
  }

  /**
   * Returns whether LIKE... ESCAPE is supported.
   * 
   * @return true
   */
  public boolean supportsLikeEscapeClause() {
    return true;
  }

  /**
   * Returns whether multiple result sets are supported.
   * 
   * @return false
   */
  public boolean supportsMultipleResultSets() {
    return false;
  }

  /**
   * Returns whether multiple transactions (on different connections) are
   * supported.
   * 
   * @return true
   */
  public boolean supportsMultipleTransactions() {
    return true;
  }

  /**
   * Returns whether columns with NOT NULL are supported.
   * 
   * @return true
   */
  public boolean supportsNonNullableColumns() {
    return true;
  }

  /**
   * Returns whether ODBC Minimum SQL grammar is supported.
   * 
   * @return true
   */
  public boolean supportsMinimumSQLGrammar() {
    return true;
  }

  /**
   * Returns whether ODBC Core SQL grammar is supported.
   * 
   * @return true
   */
  public boolean supportsCoreSQLGrammar() {
    return true;
  }

  /**
   * Returns whether ODBC Extended SQL grammar is supported.
   * 
   * @return false
   */
  public boolean supportsExtendedSQLGrammar() {
    return false;
  }

  /**
   * Returns whether SQL-92 entry level grammar is supported.
   * 
   * @return true
   */
  public boolean supportsANSI92EntryLevelSQL() {
    return true;
  }

  /**
   * Returns whether SQL-92 intermediate level grammar is supported.
   * 
   * @return false
   */
  public boolean supportsANSI92IntermediateSQL() {
    return false;
  }

  /**
   * Returns whether SQL-92 full level grammar is supported.
   * 
   * @return false
   */
  public boolean supportsANSI92FullSQL() {
    return false;
  }

  /**
   * Returns whether referential integrity is supported.
   * 
   * @return true
   */
  public boolean supportsIntegrityEnhancementFacility() {
    return true;
  }

  /**
   * Returns whether outer joins are supported.
   * 
   * @return true
   */
  public boolean supportsOuterJoins() {
    return true;
  }

  /**
   * Returns whether full outer joins are supported.
   * 
   * @return false
   */
  public boolean supportsFullOuterJoins() {
    return false;
  }

  /**
   * Returns whether limited outer joins are supported.
   * 
   * @return true
   */
  public boolean supportsLimitedOuterJoins() {
    return true;
  }

  /**
   * Returns the term for "schema".
   * 
   * @return "schema"
   */
  public String getSchemaTerm() {
    return "schema";
  }

  /**
   * Returns the term for "procedure".
   * 
   * @return "procedure"
   */
  public String getProcedureTerm() {
    return "procedure";
  }

  /**
   * Returns the term for "catalog".
   * 
   * @return "catalog"
   */
  public String getCatalogTerm() {
    return "catalog";
  }

  /**
   * Returns whether the catalog is at the beginning.
   * 
   * @return true
   */
  public boolean isCatalogAtStart() {
    return true;
  }

  /**
   * Returns the catalog separator.
   * 
   * @return "."
   */
  public String getCatalogSeparator() {
    return ".";
  }

  /**
   * Returns whether the schema name in INSERT, UPDATE, DELETE is supported.
   * 
   * @return true
   */
  public boolean supportsSchemasInDataManipulation() {
    return true;
  }

  /**
   * Returns whether the schema name in procedure calls is supported.
   * 
   * @return true
   */
  public boolean supportsSchemasInProcedureCalls() {
    return true;
  }

  /**
   * Returns whether the schema name in CREATE TABLE is supported.
   * 
   * @return true
   */
  public boolean supportsSchemasInTableDefinitions() {
    return true;
  }

  /**
   * Returns whether the schema name in CREATE INDEX is supported.
   * 
   * @return true
   */
  public boolean supportsSchemasInIndexDefinitions() {
    return true;
  }

  /**
   * Returns whether the schema name in GRANT is supported.
   * 
   * @return true
   */
  public boolean supportsSchemasInPrivilegeDefinitions() {
    return true;
  }

  /**
   * Returns whether the catalog name in INSERT, UPDATE, DELETE is supported.
   * 
   * @return true
   */
  public boolean supportsCatalogsInDataManipulation() {
    return true;
  }

  /**
   * Returns whether the catalog name in procedure calls is supported.
   * 
   * @return false
   */
  public boolean supportsCatalogsInProcedureCalls() {
    return false;
  }

  /**
   * Returns whether the catalog name in CREATE TABLE is supported.
   * 
   * @return true
   */
  public boolean supportsCatalogsInTableDefinitions() {
    return true;
  }

  /**
   * Returns whether the catalog name in CREATE INDEX is supported.
   * 
   * @return true
   */
  public boolean supportsCatalogsInIndexDefinitions() {
    return true;
  }

  /**
   * Returns whether the catalog name in GRANT is supported.
   * 
   * @return true
   */
  public boolean supportsCatalogsInPrivilegeDefinitions() {
    return true;
  }

  /**
   * Returns whether positioned deletes are supported.
   * 
   * @return true
   */
  public boolean supportsPositionedDelete() {
    return true;
  }

  /**
   * Returns whether positioned updates are supported.
   * 
   * @return true
   */
  public boolean supportsPositionedUpdate() {
    return true;
  }

  /**
   * Returns whether SELECT ... FOR UPDATE is supported.
   * 
   * @return true
   */
  public boolean supportsSelectForUpdate() {
    return true;
  }

  /**
   * Returns whether stored procedures are supported.
   * 
   * @return false
   */
  public boolean supportsStoredProcedures() {
    return false;
  }

  /**
   * Returns whether subqueries (SELECT) in comparisons are supported.
   * 
   * @return true
   */
  public boolean supportsSubqueriesInComparisons() {
    return true;
  }

  /**
   * Returns whether SELECT in EXISTS is supported.
   * 
   * @return true
   */
  public boolean supportsSubqueriesInExists() {
    return true;
  }

  /**
   * Returns whether IN(SELECT...) is supported.
   * 
   * @return true
   */
  public boolean supportsSubqueriesInIns() {
    return true;
  }

  /**
   * Returns whether subqueries in quantified expression are supported.
   * 
   * @return true
   */
  public boolean supportsSubqueriesInQuantifieds() {
    return true;
  }

  /**
   * Returns whether correlated subqueries are supported.
   * 
   * @return true
   */
  public boolean supportsCorrelatedSubqueries() {
    return true;
  }

  /**
   * Returns whether UNION SELECT is supported.
   * 
   * @return true
   */
  public boolean supportsUnion() {
    return true;
  }

  /**
   * Returns whether UNION ALL SELECT is supported.
   * 
   * @return true
   */
  public boolean supportsUnionAll() {
    return true;
  }

  /**
   * Returns whether open result sets across commits are supported.
   * 
   * @return false
   */
  public boolean supportsOpenCursorsAcrossCommit() {
    return false;
  }

  /**
   * Returns whether open result sets across rollback are supported.
   * 
   * @return false
   */
  public boolean supportsOpenCursorsAcrossRollback() {
    return false;
  }

  /**
   * Returns whether open statements across commit are supported.
   * 
   * @return true
   */
  public boolean supportsOpenStatementsAcrossCommit() {
    return true;
  }

  /**
   * Returns whether open statements across rollback are supported.
   * 
   * @return true
   */
  public boolean supportsOpenStatementsAcrossRollback() {
    return true;
  }

  /**
   * Returns whether transactions are supported.
   * 
   * @return true
   */
  public boolean supportsTransactions() {
    return true;
  }

  /**
   * Returns whether a specific transaction isolation level is supported.
   * 
   * @param level
   *          the transaction isolation level (Connection.TRANSACTION_*)
   * @return true
   */
  public boolean supportsTransactionIsolationLevel(int level) {
    return true;
  }

  /**
   * Returns whether data manipulation and CREATE/DROP is supported in
   * transactions.
   * 
   * @return false
   */
  public boolean supportsDataDefinitionAndDataManipulationTransactions() {
    return false;
  }

  /**
   * Returns whether only data manipulations are supported in transactions.
   * 
   * @return true
   */
  public boolean supportsDataManipulationTransactionsOnly() {
    return true;
  }

  /**
   * Returns whether CREATE/DROP commit an open transaction.
   * 
   * @return true
   */
  public boolean dataDefinitionCausesTransactionCommit() {
    return true;
  }

  /**
   * Returns whether CREATE/DROP do not affect transactions.
   * 
   * @return false
   */
  public boolean dataDefinitionIgnoredInTransactions() {
    return false;
  }

  /**
   * Returns whether a specific result set type is supported.
   * ResultSet.TYPE_SCROLL_SENSITIVE is not supported.
   * 
   * @param type
   *          the result set type
   * @return true for all types except ResultSet.TYPE_FORWARD_ONLY
   */
  public boolean supportsResultSetType(int type) {
    return type != ResultSet.TYPE_SCROLL_SENSITIVE;
  }

  /**
   * Returns whether a specific result set concurrency is supported.
   * ResultSet.TYPE_SCROLL_SENSITIVE is not supported.
   * 
   * @param type
   *          the result set type
   * @param concurrency
   *          the result set concurrency
   * @return true if the type is not ResultSet.TYPE_SCROLL_SENSITIVE
   */
  public boolean supportsResultSetConcurrency(int type, int concurrency) {
    return type != ResultSet.TYPE_SCROLL_SENSITIVE;
  }

  /**
   * Returns whether own updates are visible.
   * 
   * @param type
   *          the result set type
   * @return true
   */
  public boolean ownUpdatesAreVisible(int type) {
    return true;
  }

  /**
   * Returns whether own deletes are visible.
   * 
   * @param type
   *          the result set type
   * @return false
   */
  public boolean ownDeletesAreVisible(int type) {
    return false;
  }

  /**
   * Returns whether own inserts are visible.
   * 
   * @param type
   *          the result set type
   * @return false
   */
  public boolean ownInsertsAreVisible(int type) {
    return false;
  }

  /**
   * Returns whether other updates are visible.
   * 
   * @param type
   *          the result set type
   * @return false
   */
  public boolean othersUpdatesAreVisible(int type) {
    return false;
  }

  /**
   * Returns whether other deletes are visible.
   * 
   * @param type
   *          the result set type
   * @return false
   */
  public boolean othersDeletesAreVisible(int type) {
    return false;
  }

  /**
   * Returns whether other inserts are visible.
   * 
   * @param type
   *          the result set type
   * @return false
   */
  public boolean othersInsertsAreVisible(int type) {
    return false;
  }

  /**
   * Returns whether updates are detected.
   * 
   * @param type
   *          the result set type
   * @return false
   */
  public boolean updatesAreDetected(int type) {
    return false;
  }

  /**
   * Returns whether deletes are detected.
   * 
   * @param type
   *          the result set type
   * @return false
   */
  public boolean deletesAreDetected(int type) {
    return false;
  }

  /**
   * Returns whether inserts are detected.
   * 
   * @param type
   *          the result set type
   * @return false
   */
  public boolean insertsAreDetected(int type) {
    return false;
  }

  /**
   * Returns whether batch updates are supported.
   * 
   * @return true
   */
  public boolean supportsBatchUpdates() {
    return true;
  }

  /**
   * Returns whether the maximum row size includes blobs.
   * 
   * @return false
   */
  public boolean doesMaxRowSizeIncludeBlobs() {
    return false;
  }

  /**
   * Returns the default transaction isolation level.
   * 
   * @return Connection.TRANSACTION_READ_COMMITTED
   */
  public int getDefaultTransactionIsolation() {
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  /**
   * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
   * table name.
   * 
   * @return false
   */
  public boolean supportsMixedCaseIdentifiers() {
    return false;
  }

  /**
   * Checks if a table created with CREATE TABLE "Test"(ID INT) is a different
   * table than a table created with CREATE TABLE TEST(ID INT).
   * 
   * @return true
   */
  public boolean supportsMixedCaseQuotedIdentifiers() {
    return true;
  }

  /**
   * Checks if for CREATE TABLE Test(ID INT), getTables returns TEST as the
   * table name.
   * 
   * @return true
   */
  public boolean storesUpperCaseIdentifiers() {
    return true;
  }

  /**
   * Checks if for CREATE TABLE Test(ID INT), getTables returns test as the
   * table name.
   * 
   * @return false
   */
  public boolean storesLowerCaseIdentifiers() {
    return false;
  }

  /**
   * Checks if for CREATE TABLE Test(ID INT), getTables returns Test as the
   * table name.
   * 
   * @return false
   */
  public boolean storesMixedCaseIdentifiers() {
    return false;
  }

  /**
   * Checks if for CREATE TABLE "Test"(ID INT), getTables returns TEST as the
   * table name.
   * 
   * @return false
   */
  public boolean storesUpperCaseQuotedIdentifiers() {
    return false;
  }

  /**
   * Checks if for CREATE TABLE "Test"(ID INT), getTables returns test as the
   * table name.
   * 
   * @return false
   */
  public boolean storesLowerCaseQuotedIdentifiers() {
    return false;
  }

  /**
   * Checks if for CREATE TABLE "Test"(ID INT), getTables returns Test as the
   * table name.
   * 
   * @return true
   */
  public boolean storesMixedCaseQuotedIdentifiers() {
    return true;
  }

  /**
   * Returns the maximum length for hex values (characters).
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxBinaryLiteralLength() {
    return 0;
  }

  /**
   * Returns the maximum length for literals.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxCharLiteralLength() {
    return 0;
  }

  /**
   * Returns the maximum length for column names.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxColumnNameLength() {
    return 0;
  }

  /**
   * Returns the maximum number of columns in GROUP BY.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxColumnsInGroupBy() {
    return 0;
  }

  /**
   * Returns the maximum number of columns in CREATE INDEX.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxColumnsInIndex() {
    return 0;
  }

  /**
   * Returns the maximum number of columns in ORDER BY.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxColumnsInOrderBy() {
    return 0;
  }

  /**
   * Returns the maximum number of columns in SELECT.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxColumnsInSelect() {
    return 0;
  }

  /**
   * Returns the maximum number of columns in CREATE TABLE.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxColumnsInTable() {
    return 0;
  }

  /**
   * Returns the maximum number of open connection.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxConnections() {
    return 0;
  }

  /**
   * Returns the maximum length for a cursor name.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxCursorNameLength() {
    return 0;
  }

  /**
   * Returns the maximum length for an index (in bytes).
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxIndexLength() {
    return 0;
  }

  /**
   * Returns the maximum length for a schema name.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxSchemaNameLength() {
    return 0;
  }

  /**
   * Returns the maximum length for a procedure name.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxProcedureNameLength() {
    return 0;
  }

  /**
   * Returns the maximum length for a catalog name.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxCatalogNameLength() {
    return 0;
  }

  /**
   * Returns the maximum size of a row (in bytes).
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxRowSize() {
    return 0;
  }

  /**
   * Returns the maximum length of a statement.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxStatementLength() {
    return 0;
  }

  /**
   * Returns the maximum number of open statements.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxStatements() {
    return 0;
  }

  /**
   * Returns the maximum length for a table name.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxTableNameLength() {
    return 0;
  }

  /**
   * Returns the maximum number of tables in a SELECT.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxTablesInSelect() {
    return 0;
  }

  /**
   * Returns the maximum length for a user name.
   * 
   * @return 0 for limit is unknown
   */
  public int getMaxUserNameLength() {
    return 0;
  }

  /**
   * Does the database support savepoints.
   * 
   * @return true
   */
  public boolean supportsSavepoints() {
    return true;
  }

  /**
   * Does the database support named parameters.
   * 
   * @return false
   */
  public boolean supportsNamedParameters() {
    return false;
  }

  /**
   * Does the database support multiple open result sets.
   * 
   * @return true
   */
  public boolean supportsMultipleOpenResults() {
    return true;
  }

  /**
   * Does the database support getGeneratedKeys.
   * 
   * @return true
   */
  public boolean supportsGetGeneratedKeys() {
    return true;
  }

  /**
   * [Not supported]
   */
  public ResultSet getSuperTypes(String catalog, String schemaPattern,
      String typeNamePattern) throws SQLException {
    throw JdbcException.getUnsupportedException("getSuperTypes");
  }

  /**
   *
   * 
   * @param catalog
   *          null (to get all objects) or the catalog name
   * @param schemaPattern
   *          null (to get all objects) or a schema name (uppercase for unquoted
   *          names)
   * @param tableNamePattern
   *          null (to get all objects) or a table name pattern (uppercase for
   *          unquoted names)
   * @return an empty result set
   */
  public ResultSet getSuperTables(String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException {
    throw JdbcException.getUnsupportedException("getSuperTypes");
  }

  /**
   * [Not supported]
   */
  public ResultSet getAttributes(String catalog, String schemaPattern,
      String typeNamePattern, String attributeNamePattern) throws SQLException {
    throw JdbcException.getUnsupportedException("getAttributes");
  }

  /**
   * Does this database supports a result set holdability.
   * 
   * @param holdability
   *          ResultSet.HOLD_CURSORS_OVER_COMMIT or CLOSE_CURSORS_AT_COMMIT
   * @return true if the holdability is ResultSet.CLOSE_CURSORS_AT_COMMIT
   */
  public boolean supportsResultSetHoldability(int holdability) {
    return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  /**
   * Gets the result set holdability.
   * 
   * @return ResultSet.CLOSE_CURSORS_AT_COMMIT
   */
  public int getResultSetHoldability() {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  /**
   * Gets the major version of the database.
   * 
   * @return the major version
   */
  public int getDatabaseMajorVersion() {
    return FConstants.VERSION_MAJOR;
  }

  /**
   * Gets the minor version of the database.
   * 
   * @return the minor version
   */
  public int getDatabaseMinorVersion() {
    return FConstants.VERSION_MINOR;
  }

  /**
   * Gets the major version of the supported JDBC API. This method returns 4
   * when the driver is compiled using Java 5 or 6. It returns 3 otherwise.
   * 
   * @return the major version (4 or 3)
   */
  public int getJDBCMajorVersion() {
    int majorVersion = 3;
    majorVersion = 4;
    return majorVersion;
  }

  /**
   * Gets the minor version of the supported JDBC API.
   * 
   * @return the minor version (0)
   */
  public int getJDBCMinorVersion() {
    return 0;
  }

  /**
   * Gets the SQL State type.
   * 
   * @return DatabaseMetaData.sqlStateSQL99
   */
  public int getSQLStateType() {
    return DatabaseMetaData.sqlStateSQL99;
  }

  /**
   * Does the database make a copy before updating.
   * 
   * @return false
   */
  public boolean locatorsUpdateCopy() {
    return false;
  }

  /**
   * Does the database support statement pooling.
   * 
   * @return false
   */
  public boolean supportsStatementPooling() {
    return false;
  }

  /**
   * Get the lifetime of a rowid.
   * 
   * @return ROWID_UNSUPPORTED
   */
  public RowIdLifetime getRowIdLifetime() {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  // */

  /**
   * [Not supported] Gets the list of schemas.
   */
  public ResultSet getSchemas(String catalog, String schemaPattern)
      throws SQLException {
    throw JdbcException.getUnsupportedException("getSchemas(., .)");
  }

  // */

  /**
   * Returns whether the database supports calling functions using the call
   * syntax.
   * 
   * @return true
   */
  public boolean supportsStoredFunctionsUsingCallSyntax() {
    return true;
  }

  /**
   * Returns whether an exception while auto commit is on closes all result
   * sets.
   * 
   * @return false
   */
  public boolean autoCommitFailureClosesAllResultSets() {
    return false;
  }

  /**
   * [Not supported] Returns the client info properties.
   */
  public ResultSet getClientInfoProperties() throws SQLException {
    throw JdbcException.getUnsupportedException("clientInfoProperties");
  }

  /**
   * [Not supported] Return an object of this class if possible.
   */
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw JdbcException.getUnsupportedException("unwrap");
  }

  /**
   * [Not supported] Checks if unwrap can return an object of this class.
   */
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw JdbcException.getUnsupportedException("isWrapperFor");
  }

  /**
   * [Not supported] Gets the list of function columns.
   */
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
      String functionNamePattern, String columnNamePattern) throws SQLException {
    throw JdbcException.getUnsupportedException("getFunctionColumns");
  }

  /**
   * [Not supported] Gets the list of functions.
   */
  public ResultSet getFunctions(String catalog, String schemaPattern,
      String functionNamePattern) throws SQLException {
    throw JdbcException.getUnsupportedException("getFunctions");
  }


  /**
   * INTERNAL
   */
  public String toString() {
    return "conn : " + conn;
  }

}

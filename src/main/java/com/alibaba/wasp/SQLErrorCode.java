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
package com.alibaba.wasp;

/**
 * This class defines the error codes used for SQL exceptions. Error messages
 * are formatted as follows:
 * 
 * <pre>
 * { error message (possibly translated; may include quoted data) }
 * { error message in English if different }
 * { SQL statement if applicable }
 * { [ error code - build number ] }
 * </pre>
 * 
 * Example:
 * 
 * <pre>
 * Syntax error in SQL statement "SELECT * FORM[*] TEST ";
 * SQL statement: select * form test [42000-125]
 * </pre>
 * 
 * The [*] marks the position of the syntax error (FORM instead of FROM in this
 * case). The error code is 42000, and the build number is 125, meaning version
 * 1.2.125.
 */
public class SQLErrorCode {

  /**
   * The error with code <code>2000</code> is thrown when the result set is
   * positioned before the first or after the last row, or not on a valid row
   * for the given operation. Example of wrong usage:
   * 
   * <pre>
   * ResultSet rs = stat.executeQuery(&quot;SELECT * FROM DUAL&quot;);
   * rs.getString(1);
   * </pre>
   * 
   * Correct:
   * 
   * <pre>
   * ResultSet rs = stat.executeQuery(&quot;SELECT * FROM DUAL&quot;);
   * rs.next();
   * rs.getString(1);
   * </pre>
   */
  public static final int NO_DATA_AVAILABLE = 2000;

  /**
   * The error with code <code>22003</code> is thrown when a value is out of
   * range when converting to another data type. Example:
   * 
   * <pre>
   * CALL CAST(1000000 AS TINYINT);
   * SELECT CAST(124.34 AS DECIMAL(2, 2));
   * </pre>
   */
  public static final int NUMERIC_VALUE_OUT_OF_RANGE_1 = 22003;

  /**
   * The error with code <code>22007</code> is thrown when a text can not be
   * converted to a date, time, or timestamp constant. Examples:
   * 
   * <pre>
   * CALL DATE '2007-January-01';
   * CALL TIME '14:61:00';
   * CALL TIMESTAMP '2001-02-30 12:00:00';
   * </pre>
   */
  public static final int INVALID_DATETIME_CONSTANT_2 = 22007;

  /**
   * The error with code <code>22012</code> is thrown when trying to divide a
   * value by zero. Example:
   * 
   * <pre>
   * CALL 1/0;
   * </pre>
   */
  public static final int DIVISION_BY_ZERO_1 = 22012;

  /**
   * The error with code <code>22018</code> is thrown when trying to convert a
   * value to a data type where the conversion is undefined, or when an error
   * occurred trying to convert. Example:
   * 
   * <pre>
   * CALL CAST(DATE '2001-01-01' AS BOOLEAN);
   * CALL CAST('CHF 99.95' AS INT);
   * </pre>
   */
  public static final int DATA_CONVERSION_ERROR_1 = 22018;

  /**
   * The error with code <code>42000</code> is thrown when trying to execute an
   * invalid SQL statement. Example:
   * 
   * <pre>
   * CREATE ALIAS REMAINDER FOR "IEEEremainder";
   * </pre>
   */
  public static final int SYNTAX_ERROR_1 = 42000;

  /**
   * The error with code <code>42001</code> is thrown when trying to execute an
   * invalid SQL statement. Example:
   * 
   * <pre>
   * CREATE TABLE TEST(ID INT);
   * INSERT INTO TEST(1);
   * </pre>
   */
  public static final int SYNTAX_ERROR_2 = 42001;

  /**
   * The error with code <code>42122</code> is thrown when referencing an
   * non-existing column. Example:
   * 
   * <pre>
   * CREATE TABLE TEST(ID INT);
   * SELECT NAME FROM TEST;
   * </pre>
   */
  public static final int COLUMN_NOT_FOUND_1 = 42122;

  /**
   * The error with code <code>50000</code> is thrown when something unexpected
   * occurs, for example an internal stack overflow. For details about the
   * problem, see the cause of the exception in the stack trace.
   */
  public static final int GENERAL_ERROR_1 = 50000;

  /**
   * The error with code <code>50004</code> is thrown when creating a table with
   * an unsupported data type, or when the data type is unknown because
   * parameters are used. Example:
   * 
   * <pre>
   * CREATE TABLE TEST(ID VERYSMALLINT);
   * </pre>
   */
  public static final int UNKNOWN_DATA_TYPE_1 = 50004;

  /**
   * The error with code <code>50100</code> is thrown when calling an
   * unsupported JDBC method or database feature. See the stack trace for
   * details.
   */
  public static final int FEATURE_NOT_SUPPORTED_1 = 50100;

  /**
   * The error with code <code>90001</code> is thrown when
   * Statement.executeUpdate() was called for a SELECT statement. This is not
   * allowed according to the JDBC specs.
   */
  public static final int METHOD_NOT_ALLOWED_FOR_QUERY = 90001;

  /**
   * The error with code <code>90002</code> is thrown when
   * Statement.executeQuery() was called for a statement that does not return a
   * result set (for example, an UPDATE statement). This is not allowed
   * according to the JDBC specs.
   */
  public static final int METHOD_ONLY_ALLOWED_FOR_QUERY = 90002;

  /**
   * The error with code <code>90003</code> is thrown when trying to convert a
   * String to a binary value. Two hex digits per byte are required. Example of
   * wrong usage:
   * 
   * <pre>
   * CALL X'00023';
   * Hexadecimal string with odd number of characters: 00023
   * </pre>
   * 
   * Correct:
   * 
   * <pre>
   * CALL X'000023';
   * </pre>
   */
  public static final int HEX_STRING_ODD_1 = 90003;

  /**
   * The error with code <code>90004</code> is thrown when trying to convert a
   * text to binary, but the expression contains a non-hexadecimal character.
   * Example:
   * 
   * <pre>
   * CALL X'ABCDEFGH';
   * CALL CAST('ABCDEFGH' AS BINARY);
   * </pre>
   * 
   * Conversion from text to binary is supported, but the text must represent
   * the hexadecimal encoded bytes.
   */
  public static final int HEX_STRING_WRONG_1 = 90004;

  /**
   * The error with code <code>90007</code> is thrown when trying to call a JDBC
   * method on an object that has been closed.
   */
  public static final int OBJECT_CLOSED = 90007;

  /**
   * The error with code <code>90008</code> is thrown when trying to use a value
   * that is not valid for the given operation. Example:
   * 
   * <pre>
   * CREATE SEQUENCE TEST INCREMENT 0;
   * </pre>
   */
  public static final int INVALID_VALUE_2 = 90008;

  /**
   * The error with code <code>90014</code> is thrown when trying to parse a
   * date with an unsupported format string, or when the date can not be parsed.
   * Example:
   * 
   * <pre>
   * CALL PARSEDATETIME('2001 January', 'yyyy mm');
   * </pre>
   */
  public static final int PARSE_ERROR_1 = 90014;

  /**
   * The error with code <code>90015</code> is thrown when trying to DataType to
   * Value Type
   * 
   */
  public static final int PARSE_ERROR_2 = 90015;

  /**
   * The error with code <code>90105</code> is thrown when an exception occurred
   * in a user-defined method. Example:
   * 
   * <pre>
   * CREATE ALIAS SYS_PROP FOR "java.lang.System.getProperty";
   * CALL SYS_PROP(NULL);
   * </pre>
   */
  public static final int EXCEPTION_IN_FUNCTION_1 = 90105;

  /**
   * The error with code <code>90108</code> is thrown when not enough heap
   * memory was available. A possible solutions is to increase the memory size
   * using <code>java -Xmx128m ...</code>. Another solution is to reduce the
   * cache size.
   */
  public static final int OUT_OF_MEMORY = 90108;

  /**
   * The error with code <code>90028</code> is thrown when an input / output
   * error occurred. For more information, see the root cause of the exception.
   */
  public static final int IO_EXCEPTION_1 = 90028;

  /**
   * The error with code <code>90031</code> is thrown when an input / output
   * error occurred. For more information, see the root cause of the exception.
   */
  public static final int IO_EXCEPTION_2 = 90031;

  /**
   * The error with code <code>90029</code> is thrown when calling
   * ResultSet.deleteRow(), insertRow(), or updateRow() when the current row is
   * not updatable. Example:
   * 
   * <pre>
   * ResultSet rs = stat.executeQuery(&quot;SELECT * FROM TEST&quot;);
   * rs.next();
   * rs.insertRow();
   * </pre>
   */
  public static final int NOT_ON_UPDATABLE_ROW = 90029;

  /**
   * The error with code <code>90046</code> is thrown when trying to open a
   * connection to a database using an unsupported URL format. Please see the
   * documentation on the supported URL format and examples. Example:
   * 
   * 
   */
  public static final int URL_FORMAT_ERROR_2 = 90046;

  /**
   * The error with code <code>90066</code> is thrown when the same property
   * appears twice in the database URL or in the connection properties. Example:
   * 
   */
  public static final int DUPLICATE_PROPERTY_1 = 90066;

  /**
   * The error with code <code>90095</code> is thrown when calling the method
   * STRINGDECODE with an invalid escape sequence. Only Java style escape
   * sequences and Java properties file escape sequences are supported. Example:
   * 
   * <pre>
   * CALL STRINGDECODE('\i');
   * </pre>
   */
  public static final int STRING_FORMAT_ERROR_1 = 90095;

  /**
   * The error with code <code>90113</code> is thrown when the wasp URL contains
   * unsupported settings. Example:
   * 
   */
  public static final int UNSUPPORTED_SETTING_1 = 90113;

  /**
   * The error with code <code>90121</code> is thrown when a database operation
   * is started while the virtual machine exits (for example in a shutdown
   * hook), or when the session is closed.
   */
  public static final int DATABASE_CALLED_AT_SHUTDOWN = 90121;

  /**
   * The error with code <code>90125</code> is thrown when
   * PreparedStatement.setBigDecimal is called with object that extends the
   * class BigDecimal, and the system property h2.allowBigDecimalExtensions is
   * not set. Using extensions of BigDecimal is dangerous because the database
   * relies on the behavior of BigDecimal. Example of wrong usage:
   * 
   * <pre>
   * BigDecimal bd = new MyDecimal("$10.3");
   * prep.setBigDecimal(1, bd);
   * Invalid class, expected java.math.BigDecimal but got MyDecimal
   * </pre>
   * 
   * Correct:
   * 
   * <pre>
   * BigDecimal bd = new BigDecimal(&quot;10.3&quot;);
   * prep.setBigDecimal(1, bd);
   * </pre>
   */
  public static final int INVALID_CLASS_2 = 90125;

  /**
   * The error with code <code>90130</code> is thrown when an execute method of
   * PreparedStatement was called with a SQL statement. This is not allowed
   * according to the JDBC specification. Instead, use an execute method of
   * Statement. Example of wrong usage:
   * 
   * <pre>
   * PreparedStatement prep = conn.prepareStatement(&quot;SELECT * FROM TEST&quot;);
   * prep.execute(&quot;DELETE FROM TEST&quot;);
   * </pre>
   * 
   * Correct:
   * 
   * <pre>
   * Statement stat = conn.createStatement();
   * stat.execute(&quot;DELETE FROM TEST&quot;);
   * </pre>
   */
  public static final int METHOD_NOT_ALLOWED_FOR_PREPARED_STATEMENT = 90130;

  /**
   * The error with code <code>90140</code> is thrown when trying to update or
   * delete a row in a result set if the statement was not created with
   * updatable concurrency. Result sets are only updatable if the statement was
   * created with updatable concurrency, and if the result set contains all
   * columns of the primary key or of a unique index of a table.
   */
  public static final int RESULT_SET_READONLY = 90140;

  /**
   * The error with code <code>90150</code> is thrown when trying to insert a
   * row to table but we found the primary key is all ready exits in the table.
   */
  public static final int PRIMARY_KEY_EXIST = 90150;
  
  /**
   * The error with code <code>90151</code>not exist pk.
   */
  public static final int PRIMARY_KEY_NOT_EXIST = 90151;

  /**
   * The error with code <code>90160</code> is thrown when operating a row
   * without primary key.
   */
  public static final int PRIMARY_KEY_NOT_MATCH = 90160;
  
  /**
   * The error with code <code>90170</code> is thrown when operating a not supported sql.
   */
  public static final int NOT_SUPPORTED = 90170;

  /**
   * INTERNAL
   */
  public static String getState(int errorCode) {
    // To convert SQLState to error code, replace
    // 21S: 210, 42S: 421, HY: 50, C: 1, T: 2

    switch (errorCode) {

    // 02: no data
    case NO_DATA_AVAILABLE:
      return "02000";

    case COLUMN_NOT_FOUND_1:
      return "42S22";

      // 0A: feature not supported

      // HZ: remote database access

      // HY
    case GENERAL_ERROR_1:
      return "HY000";
    case UNKNOWN_DATA_TYPE_1:
      return "HY004";

    case FEATURE_NOT_SUPPORTED_1:
      return "HYC00";

    default:
      return "" + errorCode;
    }
  }
}

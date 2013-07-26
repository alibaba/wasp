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
package com.alibaba.wasp.jdbc;

import com.alibaba.wasp.util.Utils;import org.apache.hadoop.conf.Configuration;
import com.alibaba.wasp.util.Utils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Properties;


/**
 * The base class for all tests.
 */
public abstract class TestJdbcBase {

  /**
   * The last time something was printed.
   */
  private static long lastPrint;

  /**
   * The time when the test was started.
   */
  protected long start;

  private final LinkedList<byte[]> memory = new LinkedList<byte[]>();

  /**
   * Open a database connection in admin mode. The default user name and
   * password is used.
   * 
   * @param name
   *          the database name
   * @return the connection
   */
  public static Connection getConnection(String name, Configuration conf) throws SQLException {
    Properties properties = Utils.convertConfigurationToProperties(conf);
    properties.put("user", getUser());
    properties.put("password", getPassword());
    return getConnectionInternal(getURL(name, true), properties);
  }



  /**
   * user password
   * 
   * @param userPassword
   *          the password of this user
   * @return the login password
   */
  protected static String getPassword(String userPassword) {
    return userPassword;
  }

  /**
   * Get the login password. This is usually the user password. If file
   * encryption is used it is combined with the file password.
   * 
   * @return the login password
   */
  protected static String getPassword() {
    return getPassword("123");
  }

  /**
   * Get the database URL for the given database name using the current
   * configuration options.
   * 
   * @param name
   *          the database name
   * @param admin
   *          true if the current user is an admin
   * @return the database URL
   */
  protected static String getURL(String name, boolean admin) {
    String url;
    if (name.startsWith("jdbc:")) {
      return name;
    }
    url = name;
    return "jdbc:wasp:" + url;
  }

  private static String addOption(String url, String option, String value) {
    if (url.indexOf(";" + option + "=") < 0) {
      url += ";" + option + "=" + value;
    }
    return url;
  }

  private static Connection getConnectionInternal(String url, Properties properties) throws SQLException {
    com.alibaba.wasp.jdbc.Driver.load();
    Enumeration<java.sql.Driver> drivers =  DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      java.sql.Driver driver = (java.sql.Driver) drivers.nextElement();
      if(!(driver instanceof Driver)) {
        DriverManager.deregisterDriver(driver);
      }
    }
    return DriverManager.getConnection(url, properties);
  }

  protected static String getUser() {
    return "sa";
  }

  /**
   * Write a message to system out if trace is enabled.
   * 
   * @param x
   *          the value to write
   */
  protected void trace(int x) {
    trace("" + x);
  }

  /**
   * Write a message to system out if trace is enabled.
   * 
   * @param s
   *          the message to write
   */
  public void trace(String s) {
    lastPrint = 0;
    println(s);
  }

  /**
   * Print how much memory is currently used.
   */
  protected void traceMemory() {
    trace("mem=" + getMemoryUsed());
  }

  /**
   * Print the currently used memory, the message and the given time in
   * milliseconds.
   * 
   * @param s
   *          the message
   * @param time
   *          the time in millis
   */
  public void printTimeMemory(String s, long time) {
    println(getMemoryUsed() + " MB: " + s + " ms: " + time);
  }

  /**
   * Get the number of megabytes heap memory in use.
   * 
   * @return the used megabytes
   */
  public static int getMemoryUsed() {
    Runtime rt = Runtime.getRuntime();
    long memory = Long.MAX_VALUE;
    for (int i = 0; i < 8; i++) {
      rt.gc();
      long memNow = rt.totalMemory() - rt.freeMemory();
      if (memNow >= memory) {
        break;
      }
      memory = memNow;
    }
    int mb = (int) (memory / 1024 / 1024);
    return mb;
  }

  /**
   * Called if the test reached a point that was not expected.
   * 
   * @throws AssertionError
   *           always throws an AssertionError
   */
  public void fail() {
    fail("Failure");
  }

  /**
   * Called if the test reached a point that was not expected.
   * 
   * @param string
   *          the error message
   * @throws AssertionError
   *           always throws an AssertionError
   */
  protected void fail(String string) {
    lastPrint = 0;
    println(string);
    throw new AssertionError(string);
  }

  /**
   * Print a message to system out.
   * 
   * @param s
   *          the message
   */
  public void println(String s) {
    long now = System.currentTimeMillis();
    if (now > lastPrint + 1000) {
      lastPrint = now;
      long time = now - start;
      printlnWithTime(time, getClass().getName() + " " + s);
    }
  }

  /**
   * Print a message, prepended with the specified time in milliseconds.
   * 
   * @param millis
   *          the time in milliseconds
   * @param s
   *          the message
   */
  static void printlnWithTime(long millis, String s) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
    s = dateFormat.format(new java.util.Date()) + " " + formatTime(millis)
        + " " + s;
    System.out.println(s);
  }

  /**
   * Print the current time and a message to system out.
   * 
   * @param s
   *          the message
   */
  protected void printTime(String s) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
    println(dateFormat.format(new java.util.Date()) + " " + s);
  }

  /**
   * Format the time in the format hh:mm:ss.1234 where 1234 is milliseconds.
   * 
   * @param millis
   *          the time in milliseconds
   * @return the formatted time
   */
  static String formatTime(long millis) {
    String s = new java.sql.Time(java.sql.Time.valueOf("0:0:0").getTime()
        + millis).toString()
        + "." + ("" + (1000 + (millis % 1000))).substring(1);
    if (s.startsWith("00:")) {
      s = s.substring(3);
    }
    return s;
  }

  /**
   * Check if two values are equal, and if not throw an exception.
   * 
   * @param message
   *          the message to print in case of error
   * @param expected
   *          the expected value
   * @param actual
   *          the actual value
   * @throws AssertionError
   *           if the values are not equal
   */
  public void assertEquals(String message, int expected, int actual) {
    if (expected != actual) {
      fail("Expected: " + expected + " actual: " + actual + " message: "
          + message);
    }
  }

  /**
   * Check if two values are equal, and if not throw an exception.
   * 
   * @param expected
   *          the expected value
   * @param actual
   *          the actual value
   * @throws AssertionError
   *           if the values are not equal
   */
  public void assertEquals(int expected, int actual) {
    if (expected != actual) {
      fail("Expected: " + expected + " actual: " + actual);
    }
  }

  /**
   * Check if two values are equal, and if not throw an exception.
   * 
   * @param expected
   *          the expected value
   * @param actual
   *          the actual value
   * @throws AssertionError
   *           if the values are not equal
   */
  public void assertEquals(byte[] expected, byte[] actual) {
    if (expected == null || actual == null) {
      assertTrue(expected == actual);
      return;
    }
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] != actual[i]) {
        fail("[" + i + "]: expected: " + (int) expected[i] + " actual: "
            + (int) actual[i]);
      }
    }
  }

  /**
   * Check if two values are equal, and if not throw an exception.
   * 
   * @param expected
   *          the expected value
   * @param actual
   *          the actual value
   * @throws AssertionError
   *           if the values are not equal
   */
  public void assertEquals(Object[] expected, Object[] actual) {
    if (expected == null || actual == null) {
      assertTrue(expected == actual);
      return;
    }
    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null || actual[i] == null) {
        if (expected[i] != actual[i]) {
          fail("[" + i + "]: expected: " + expected[i] + " actual: "
              + actual[i]);
        }
      } else if (!expected[i].equals(actual[i])) {
        fail("[" + i + "]: expected: " + expected[i] + " actual: " + actual[i]);
      }
    }
  }

  /**
   * Check if two values are equal, and if not throw an exception.
   * 
   * @param message
   *          the message to use if the check fails
   * @param expected
   *          the expected value
   * @param actual
   *          the actual value
   * @throws AssertionError
   *           if the values are not equal
   */
  protected void assertEquals(String message, String expected, String actual) {
    if (expected == null && actual == null) {
      return;
    } else if (expected == null || actual == null) {
      fail("Expected: " + expected + " Actual: " + actual + " " + message);
    } else if (!expected.equals(actual)) {
      for (int i = 0; i < expected.length(); i++) {
        String s = expected.substring(0, i);
        if (!actual.startsWith(s)) {
          expected = expected.substring(0, i) + "<*>" + expected.substring(i);
          break;
        }
      }
      int al = expected.length();
      int bl = actual.length();
      if (al > 4000) {
        expected = expected.substring(0, 4000);
      }
      if (bl > 4000) {
        actual = actual.substring(0, 4000);
      }
      fail("Expected: " + expected + " (" + al + ") actual: " + actual + " ("
          + bl + ") " + message);
    }
  }

  /**
   * Check if two values are equal, and if not throw an exception.
   * 
   * @param expected
   *          the expected value
   * @param actual
   *          the actual value
   * @throws AssertionError
   *           if the values are not equal
   */
  protected void assertEquals(String expected, String actual) {
    assertEquals("", expected, actual);
  }

  /**
   * Check if two result sets are equal, and if not throw an exception.
   * 
   * @param message
   *          the message to use if the check fails
   * @param rs0
   *          the first result set
   * @param rs1
   *          the second result set
   * @throws AssertionError
   *           if the values are not equal
   */
  protected void assertEquals(String message, ResultSet rs0, ResultSet rs1)
      throws SQLException {
    ResultSetMetaData meta = rs0.getMetaData();
    int columns = meta.getColumnCount();
    assertEquals(columns, rs1.getMetaData().getColumnCount());
    while (rs0.next()) {
      assertTrue(message, rs1.next());
      for (int i = 0; i < columns; i++) {
        assertEquals(message, rs0.getString(i + 1), rs1.getString(i + 1));
      }
    }
    assertFalse(message, rs0.next());
    assertFalse(message, rs1.next());
  }

  /**
   * Check if the first value is larger or equal than the second value, and if
   * not throw an exception.
   * 
   * @param a
   *          the first value
   * @param b
   *          the second value (must be smaller than the first value)
   * @throws AssertionError
   *           if the first value is smaller
   */
  protected void assertSmaller(long a, long b) {
    if (a >= b) {
      fail("a: " + a + " is not smaller than b: " + b);
    }
  }

  /**
   * Check that a result contains the given substring.
   * 
   * @param result
   *          the result value
   * @param contains
   *          the term that should appear in the result
   * @throws AssertionError
   *           if the term was not found
   */
  protected void assertContains(String result, String contains) {
    if (result.indexOf(contains) < 0) {
      fail(result + " does not contain: " + contains);
    }
  }

  /**
   * Check that a text starts with the expected characters..
   * 
   * @param text
   *          the text
   * @param expectedStart
   *          the expected prefix
   * @throws AssertionError
   *           if the text does not start with the expected characters
   */
  protected void assertStartsWith(String text, String expectedStart) {
    if (!text.startsWith(expectedStart)) {
      fail("[" + text + "] does not start with: [" + expectedStart + "]");
    }
  }

  /**
   * Check if two values are equal, and if not throw an exception.
   * 
   * @param expected
   *          the expected value
   * @param actual
   *          the actual value
   * @throws AssertionError
   *           if the values are not equal
   */
  protected void assertEquals(long expected, long actual) {
    if (expected != actual) {
      fail("Expected: " + expected + " actual: " + actual);
    }
  }

  /**
   * Check if two values are equal, and if not throw an exception.
   * 
   * @param expected
   *          the expected value
   * @param actual
   *          the actual value
   * @throws AssertionError
   *           if the values are not equal
   */
  protected void assertEquals(double expected, double actual) {
    if (expected != actual) {
      if (Double.isNaN(expected) && Double.isNaN(actual)) {
        // if both a NaN, then there is no error
      } else {
        fail("Expected: " + expected + " actual: " + actual);
      }
    }
  }

  /**
   * Check if two values are equal, and if not throw an exception.
   * 
   * @param expected
   *          the expected value
   * @param actual
   *          the actual value
   * @throws AssertionError
   *           if the values are not equal
   */
  protected void assertEquals(float expected, float actual) {
    if (expected != actual) {
      if (Float.isNaN(expected) && Float.isNaN(actual)) {
        // if both a NaN, then there is no error
      } else {
        fail("Expected: " + expected + " actual: " + actual);
      }
    }
  }

  /**
   * Check if two values are equal, and if not throw an exception.
   * 
   * @param expected
   *          the expected value
   * @param actual
   *          the actual value
   * @throws AssertionError
   *           if the values are not equal
   */
  protected void assertEquals(boolean expected, boolean actual) {
    if (expected != actual) {
      fail("Boolean expected: " + expected + " actual: " + actual);
    }
  }

  /**
   * Check that the passed boolean is true.
   * 
   * @param condition
   *          the condition
   * @throws AssertionError
   *           if the condition is false
   */
  public void assertTrue(boolean condition) {
    assertTrue("Expected: true got: false", condition);
  }

  /**
   * Check that the passed object is null.
   * 
   * @param obj
   *          the object
   * @throws AssertionError
   *           if the condition is false
   */
  public void assertNull(Object obj) {
    if (obj != null) {
      fail("Expected: null got: " + obj);
    }
  }

  /**
   * Check that the passed boolean is true.
   * 
   * @param message
   *          the message to print if the condition is false
   * @param condition
   *          the condition
   * @throws AssertionError
   *           if the condition is false
   */
  protected void assertTrue(String message, boolean condition) {
    if (!condition) {
      fail(message);
    }
  }

  /**
   * Check that the passed boolean is false.
   * 
   * @param value
   *          the condition
   * @throws AssertionError
   *           if the condition is true
   */
  protected void assertFalse(boolean value) {
    assertFalse("Expected: false got: true", value);
  }

  /**
   * Check that the passed boolean is false.
   * 
   * @param message
   *          the message to print if the condition is false
   * @param value
   *          the condition
   * @throws AssertionError
   *           if the condition is true
   */
  protected void assertFalse(String message, boolean value) {
    if (value) {
      fail(message);
    }
  }

  /**
   * Check that the result set row count matches.
   * 
   * @param expected
   *          the number of expected rows
   * @param rs
   *          the result set
   * @throws AssertionError
   *           if a different number of rows have been found
   */
  protected void assertResultRowCount(int expected, ResultSet rs)
      throws SQLException {
    int i = 0;
    while (rs.next()) {
      i++;
    }
    assertEquals(expected, i);
  }

  /**
   * Check that the result set of a query is exactly this value.
   * 
   * @param stat
   *          the statement
   * @param sql
   *          the SQL statement to execute
   * @param expected
   *          the expected result value
   * @throws AssertionError
   *           if a different result value was returned
   */
  protected void assertSingleValue(Statement stat, String sql, int expected)
      throws SQLException {
    ResultSet rs = stat.executeQuery(sql);
    assertTrue(rs.next());
    assertEquals(expected, rs.getInt(1));
    assertFalse(rs.next());
  }

  /**
   * Check that the result set of a query is exactly this value.
   * 
   * @param expected
   *          the expected result value
   * @param stat
   *          the statement
   * @param sql
   *          the SQL statement to execute
   * @throws AssertionError
   *           if a different result value was returned
   */
  protected void assertResult(String expected, Statement stat, String sql)
      throws SQLException {
    ResultSet rs = stat.executeQuery(sql);
    if (rs.next()) {
      String actual = rs.getString(1);
      assertEquals(expected, actual);
    } else {
      assertEquals(expected, null);
    }
  }

  /**
   * Check if the result set meta data is correct.
   * 
   * @param rs
   *          the result set
   * @param columnCount
   *          the expected column count
   * @param labels
   *          the expected column labels
   * @param datatypes
   *          the expected data types
   * @param precision
   *          the expected precisions
   * @param scale
   *          the expected scales
   */
  protected void assertResultSetMeta(ResultSet rs, int columnCount,
      String[] labels, int[] datatypes, int[] precision, int[] scale)
      throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    int cc = meta.getColumnCount();
    if (cc != columnCount) {
      fail("result set contains " + cc + " columns not " + columnCount);
    }
    for (int i = 0; i < columnCount; i++) {
      if (labels != null) {
        String l = meta.getColumnLabel(i + 1);
        if (!labels[i].equals(l)) {
          fail("column label " + i + " is " + l + " not " + labels[i]);
        }
      }
      if (datatypes != null) {
        int t = meta.getColumnType(i + 1);
        if (datatypes[i] != t) {
          fail("column datatype " + i + " is " + t + " not " + datatypes[i]
              + " (prec=" + meta.getPrecision(i + 1) + " scale="
              + meta.getScale(i + 1) + ")");
        }
        String typeName = meta.getColumnTypeName(i + 1);
        String className = meta.getColumnClassName(i + 1);
        switch (t) {
        case Types.INTEGER:
          assertEquals("INTEGER", typeName);
          assertEquals("java.lang.Integer", className);
          break;
        case Types.VARCHAR:
          assertEquals("VARCHAR", typeName);
          assertEquals("java.lang.String", className);
          break;
        case Types.SMALLINT:
          assertEquals("SMALLINT", typeName);
          assertEquals("java.lang.Short", className);
          break;
        case Types.TIMESTAMP:
          assertEquals("TIMESTAMP", typeName);
          assertEquals("java.sql.Timestamp", className);
          break;
        case Types.DECIMAL:
          assertEquals("DECIMAL", typeName);
          assertEquals("java.math.BigDecimal", className);
          break;
        default:
        }
      }
      if (precision != null) {
        int p = meta.getPrecision(i + 1);
        if (precision[i] != p) {
          fail("column precision " + i + " is " + p + " not " + precision[i]);
        }
      }
      if (scale != null) {
        int s = meta.getScale(i + 1);
        if (scale[i] != s) {
          fail("column scale " + i + " is " + s + " not " + scale[i]);
        }
      }

    }
  }

  /**
   * Check if a result set contains the expected data. The sort order is
   * significant
   * 
   * @param rs
   *          the result set
   * @param data
   *          the expected data
   * @throws AssertionError
   *           if there is a mismatch
   */
  protected void assertResultSetOrdered(ResultSet rs, String[][] data)
      throws SQLException {
    assertResultSet(true, rs, data);
  }

  /**
   * Check if a result set contains the expected data.
   * 
   * @param ordered
   *          if the sort order is significant
   * @param rs
   *          the result set
   * @param data
   *          the expected data
   * @throws AssertionError
   *           if there is a mismatch
   */
  private void assertResultSet(boolean ordered, ResultSet rs, String[][] data)
      throws SQLException {
    int len = rs.getMetaData().getColumnCount();
    int rows = data.length;
    if (rows == 0) {
      // special case: no rows
      if (rs.next()) {
        fail("testResultSet expected rowCount:" + rows + " got:0");
      }
    }
    int len2 = data[0].length;
    if (len < len2) {
      fail("testResultSet expected columnCount:" + len2 + " got:" + len);
    }
    for (int i = 0; i < rows; i++) {
      if (!rs.next()) {
        fail("testResultSet expected rowCount:" + rows + " got:" + i);
      }
      String[] row = getData(rs, len);
      if (ordered) {
        String[] good = data[i];
        if (!testRow(good, row, good.length)) {
          fail("testResultSet row not equal, got:\n" + formatRow(row) + "\n"
              + formatRow(good));
        }
      } else {
        boolean found = false;
        for (int j = 0; j < rows; j++) {
          String[] good = data[i];
          if (testRow(good, row, good.length)) {
            found = true;
            break;
          }
        }
        if (!found) {
          fail("testResultSet no match for row:" + formatRow(row));
        }
      }
    }
    if (rs.next()) {
      String[] row = getData(rs, len);
      fail("testResultSet expected rowcount:" + rows + " got:>=" + (rows + 1)
          + " data:" + formatRow(row));
    }
  }

  private static boolean testRow(String[] a, String[] b, int len) {
    for (int i = 0; i < len; i++) {
      String sa = a[i];
      String sb = b[i];
      if (sa == null || sb == null) {
        if (sa != sb) {
          return false;
        }
      } else {
        if (!sa.equals(sb)) {
          return false;
        }
      }
    }
    return true;
  }

  private static String[] getData(ResultSet rs, int len) throws SQLException {
    String[] data = new String[len];
    for (int i = 0; i < len; i++) {
      data[i] = rs.getString(i + 1);
      // just check if it works
      rs.getObject(i + 1);
    }
    return data;
  }

  private static String formatRow(String[] row) {
    String sb = "";
    for (String r : row) {
      sb += "{" + r + "}";
    }
    return "{" + sb + "}";
  }

  /**
   * Check if two values are equal, and if not throw an exception.
   * 
   * @param expected
   *          the expected value
   * @param actual
   *          the actual value
   * @throws AssertionError
   *           if the values are not equal
   */
  protected void assertEquals(Integer expected, Integer actual) {
    if (expected == null || actual == null) {
      assertTrue(expected == null && actual == null);
    } else {
      assertEquals(expected.intValue(), actual.intValue());
    }
  }

  /**
   * Create a new object of the calling class.
   * 
   * @return the new test
   */
  public static TestJdbcBase createCaller() {
    return createCaller(new Exception().getStackTrace()[1].getClassName());
  }

  /**
   * Create a new object of the given class.
   * 
   * @param className
   *          the class name
   * @return the new test
   */
  public static TestJdbcBase createCaller(String className) {
    com.alibaba.wasp.jdbc.Driver.load();
    try {
      return (TestJdbcBase) Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Can not create object " + className, e);
    }
  }

  /**
   * Use up almost all memory.
   * 
   * @param remainingKB
   *          the number of kilobytes that are not referenced
   */
  protected void eatMemory(int remainingKB) {
    byte[] reserve = new byte[remainingKB * 1024];
    // first, eat memory in 16 KB blocks, then eat in 16 byte blocks
    for (int size = 16 * 1024; size > 0; size /= 1024) {
      while (true) {
        try {
          byte[] block = new byte[16 * 1024];
          memory.add(block);
        } catch (OutOfMemoryError e) {
          break;
        }
      }
    }
    // silly code - makes sure there are no warnings
    reserve[0] = reserve[1];
  }

  /**
   * Remove the hard reference to the memory.
   */
  protected void freeMemory() {
    memory.clear();
  }

  /**
   * Verify the next method call on the object will throw an exception.
   * 
   * @param <T>
   *          the class of the object
   * @param expectedExceptionClass
   *          the expected exception class to be thrown
   * @param obj
   *          the object to wrap
   * @return a proxy for the object
   */
  protected <T> T assertThrows(final Class<?> expectedExceptionClass,
      final T obj) {
    return assertThrows(new TestResultVerifier() {
      public boolean verify(Object returnValue, Throwable t, Method m,
          Object... args) {
        if (t == null) {
          throw new AssertionError("Expected an exception of type "
              + expectedExceptionClass.getSimpleName()
              + " to be thrown, but the method returned " + returnValue);
        }
        if (!expectedExceptionClass.isAssignableFrom(t.getClass())) {
          AssertionError ae = new AssertionError(
              "Expected an exception of type\n"
                  + expectedExceptionClass.getSimpleName()
                  + " to be thrown, but the method under test threw an exception of type\n"
                  + t.getClass().getSimpleName()
                  + " (see in the 'Caused by' for the exception that was thrown) ");
          ae.initCause(t);
          throw ae;
        }
        return false;
      }
    }, obj);
  }

  /**
   * Verify the next method call on the object will throw an exception.
   * 
   * @param <T>
   *          the class of the object
   * @param expectedErrorCode
   *          the expected error code
   * @param obj
   *          the object to wrap
   * @return a proxy for the object
   */
  protected <T> T assertThrows(final int expectedErrorCode, final T obj) {
    return assertThrows(new TestResultVerifier() {
      public boolean verify(Object returnValue, Throwable t, Method m,
          Object... args) {
        int errorCode;
        if (t instanceof JdbcException) {
          errorCode = ((JdbcException) t).getErrorCode();
        } else if (t instanceof SQLException) {
          errorCode = ((SQLException) t).getErrorCode();
        } else {
          errorCode = 0;
        }
        if (errorCode != expectedErrorCode) {
          AssertionError ae = new AssertionError(
              "Expected an SQLException or DbException with error code "
                  + expectedErrorCode);
          ae.initCause(t);
          throw ae;
        }
        return false;
      }
    }, obj);
  }

  /**
   * Verify the next method call on the object will throw an exception.
   * 
   * @param <T>
   *          the class of the object
   * @param verifier
   *          the result verifier to call
   * @param obj
   *          the object to wrap
   * @return a proxy for the object
   */
  @SuppressWarnings("unchecked")
  protected <T> T assertThrows(final TestResultVerifier verifier, final T obj) {
    Class<?> c = obj.getClass();
    InvocationHandler ih = new InvocationHandler() {
      private Exception called = new Exception("No method called");

      protected void finalize() {
        if (called != null) {
          called.printStackTrace(System.err);
        }
      }

      public Object invoke(Object proxy, Method method, Object[] args)
          throws Exception {
        try {
          called = null;
          Object ret = method.invoke(obj, args);
          verifier.verify(ret, null, method, args);
          return ret;
        } catch (InvocationTargetException e) {
          verifier.verify(null, e.getTargetException(), method, args);
          Class<?> retClass = method.getReturnType();
          if (!retClass.isPrimitive()) {
            return null;
          }
          if (retClass == boolean.class) {
            return false;
          } else if (retClass == byte.class) {
            return (byte) 0;
          } else if (retClass == char.class) {
            return (char) 0;
          } else if (retClass == short.class) {
            return (short) 0;
          } else if (retClass == int.class) {
            return 0;
          } else if (retClass == long.class) {
            return 0L;
          } else if (retClass == float.class) {
            return 0F;
          } else if (retClass == double.class) {
            return 0D;
          }
          return null;
        }
      }
    };
    Class<?>[] interfaces = c.getInterfaces();
    if (Modifier.isFinal(c.getModifiers())
        || (interfaces.length > 0 && getClass() != c)) {
      // interface class proxies
      if (interfaces.length == 0) {
        throw new RuntimeException("Can not create a proxy for the class "
            + c.getSimpleName()
            + " because it doesn't implement any interfaces and is final");
      }
      return (T) Proxy.newProxyInstance(c.getClassLoader(), interfaces, ih);
    }
    return null;
  }

}

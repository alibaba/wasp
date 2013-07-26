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

import com.alibaba.wasp.WaspTestingUtility;import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.SQLErrorCode;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.client.FClient;
import com.alibaba.wasp.util.ResultInHBasePrinter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;

public class TestJdbcResultSet extends TestJdbcBase {

  final Log LOG = LogFactory.getLog(getClass());

  private static Connection conn;
  private static Statement stat;

  private final static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  private static FClient client;
  public static final String TABLE_NAME = "test";
  public static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("wasp.client.retries.number", 3);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.createTable(TABLE);
    TEST_UTIL.getWaspAdmin().disableTable(TABLE);
    client = new FClient(TEST_UTIL.getConfiguration());
    client.execute("create index test_index on " + TABLE_NAME + "(column3);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    client.execute("create index test_index2 on " + TABLE_NAME
        + "(column2);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    client.execute("create index test_index3 on " + TABLE_NAME
        + "(column1,column3);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    TEST_UTIL.getWaspAdmin().enableTable(TABLE);

    Class.forName("com.alibaba.wasp.jdbc.Driver");
    conn = getConnection("test", TEST_UTIL.getConfiguration());
    conn.setClientInfo(FConstants.READ_MODEL, ReadModel.CURRENT.name());
    stat = conn.createStatement();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    stat.close();
    conn.close();
  }

  @Test
  public void testQuery() throws IOException, SQLException {
    String INSERT = "Insert into " + TABLE_NAME
        + "(column1,column2,column3) values (123,456,'binlijin');";
    stat.execute(INSERT);

    assertTrue(stat.getUpdateCount() == 1);
    ResultSet rs = stat.executeQuery("select column1,column2,column3 from "
        + TABLE_NAME + " where column1=123 and column2=456");
    assertTrue(rs.next());
    assertEquals(123, rs.getInt("column1"));
    assertEquals(456, rs.getLong("column2"));
    assertEquals("binlijin", rs.getString("column3"));
    stat.close();
  }

  //@Test
  public void testBeforeFirstAfterLast() throws SQLException {
    // stat.executeUpdate("create table test(id int)");
    stat = conn.createStatement();
    stat.execute("insert into test (column1,column2,column3) values(1,21,'binlijin2')");
    assertTrue(stat.getUpdateCount() == 1);
    // With a result
    ResultSet rs = stat
        .executeQuery("select column1,column2,column3 from " + TABLE_NAME + " where column1=1 and column3='binlijin2'");
    assertTrue(rs.isBeforeFirst());
    assertFalse(rs.isAfterLast());
    rs.next();
    assertFalse(rs.isBeforeFirst());
    assertFalse(rs.isAfterLast());
    rs.next();
    assertFalse(rs.isBeforeFirst());
    assertTrue(rs.isAfterLast());
    rs.close();
    rs = stat
        .executeQuery("select column1,column2,column3 from test where column2 = -222");
    assertFalse(rs.isBeforeFirst());
    assertFalse(rs.isAfterLast());
    rs.next();
    assertFalse(rs.isBeforeFirst());
    assertFalse(rs.isAfterLast());
    rs.close();
  }

  // @Test TODO not support now
  public void testSubstringDataType() throws SQLException {
    stat = conn.createStatement();
    ResultSet rs = stat
        .executeQuery("select substr(column3, 1, 1) from test where column1=1 and column3='binlijin'");
    rs.next();
    assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(1));
  }

  // @Test TODO not support now
  public void testColumnLabelColumnName() throws SQLException {
    stat = conn.createStatement();
    stat.executeUpdate("Insert into " + TABLE_NAME
        + "(column1,column2,column3) values (2,1,'binlijin');");
    ResultSet rs = stat
        .executeQuery("select column3 as y from test where column1=2 and column3='binlijin' ");
    rs.next();
    rs.getString("column3");
    rs.getString("y");
    rs.close();
    rs = conn.getMetaData().getColumns(null, null, null, null);
    ResultSetMetaData meta = rs.getMetaData();
    int columnCount = meta.getColumnCount();
    String[] columnName = new String[columnCount];
    for (int i = 1; i <= columnCount; i++) {
      columnName[i - 1] = meta.getColumnName(i);
    }
    while (rs.next()) {
      for (int i = 0; i < columnCount; i++) {
        rs.getObject(columnName[i]);
      }
    }
  }

  // @Test TODO not support now
  public void testAbsolute() throws SQLException {
    stat = conn.createStatement();
    stat.execute("CREATE TABLE test(ID INT PRIMARY KEY)");
    // there was a problem when more than MAX_MEMORY_ROWS where in the result
    // set
    stat.execute("INSERT INTO test SELECT X FROM SYSTEM_RANGE(1, 200)");
    Statement s2 = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY);
    ResultSet rs = s2.executeQuery("SELECT * FROM test ORDER BY ID");
    for (int i = 100; i > 0; i--) {
      rs.absolute(i);
      assertEquals(i, rs.getInt(1));
    }
    stat.execute("DROP TABLE test");
  }

  // @Test TODO not support now
  public void testFetchSize() throws SQLException {
    stat = conn.createStatement();
    ResultSet rs = stat.executeQuery("SELECT * FROM SYSTEM_RANGE(1, 100)");
    int a = stat.getFetchSize();
    int b = rs.getFetchSize();
    assertEquals(a, b);
    rs.setFetchSize(b + 1);
    b = rs.getFetchSize();
    assertEquals(a + 1, b);
  }

  private void checkPrecision(int expected, String sql) throws SQLException {
    ResultSetMetaData meta = stat.executeQuery(sql).getMetaData();
    assertEquals(expected, meta.getPrecision(1));
  }

  // @Test TODO not support now
  public void testSubstringPrecision() throws SQLException {
    trace("testSubstringPrecision");
    stat = conn.createStatement();
    stat.execute("CREATE TABLE test(ID INT, NAME VARCHAR(10))");
    stat.execute("INSERT INTO test VALUES(1, 'Hello'), (2, 'WorldPeace')");
    checkPrecision(0, "SELECT SUBSTR(NAME, 12, 4) FROM test");
    checkPrecision(9, "SELECT SUBSTR(NAME, 2) FROM test");
    checkPrecision(10, "SELECT SUBSTR(NAME, ID) FROM test");
    checkPrecision(4, "SELECT SUBSTR(NAME, 2, 4) FROM test");
    checkPrecision(3, "SELECT SUBSTR(NAME, 8, 4) FROM test");
    checkPrecision(4, "SELECT SUBSTR(NAME, 7, 4) FROM test");
    checkPrecision(8, "SELECT SUBSTR(NAME, 3, ID*0) FROM test");
    stat.execute("DROP TABLE test");
  }

 // @Test
  public void testFindColumn() throws SQLException {
    trace("testFindColumn");
    stat = conn.createStatement();
    stat.executeUpdate("Insert into " + TABLE_NAME
        + "(column1,column2,column3) values (3031,11,'binlijin3031');");
    ResultSet rs = stat
        .executeQuery("SELECT column1,column2,column3 FROM test where column1=3031 and column3='binlijin3031'");
//    assertEquals(1, rs.findColumn("COLUMN1"));
//    assertEquals(2, rs.findColumn("COLUMN2"));
    assertEquals(1, rs.findColumn("column1"));
    assertEquals(2, rs.findColumn("column2"));
//    assertEquals(1, rs.findColumn("Column1"));
//    assertEquals(2, rs.findColumn("Column2"));
  }

  @Test
  public void testInt() throws SQLException {
    trace("test INT");
    ResultSet rs;
    Object o;
    stat = conn.createStatement();
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(31,-1, 'testInt')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(32,0, 'testInt')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(33,1, 'testInt')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(34,"
        + Integer.MAX_VALUE + ", 'testInt')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(35,"
        + Integer.MIN_VALUE + ", 'testInt')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(36,0, 'testInt')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(37,0, 'testInt')");
    // this should not be read - maxrows=6

    // MySQL compatibility (is this required?)
    rs = stat
        .executeQuery("SELECT column1,column2,column3 FROM test where column3='testInt' ORDER BY column1");
    // MySQL compatibility
    assertEquals(1, rs.findColumn("column1"));
    assertEquals(2, rs.findColumn("column2"));

    ResultSetMetaData meta = rs.getMetaData();
    assertEquals(3, meta.getColumnCount());

    assertTrue(rs.getRow() == 0);

    rs.next();
    trace("default fetch size=" + rs.getFetchSize());
    // 0 should be an allowed value (but it's not defined what is actually
    // means)
    rs.setFetchSize(1);
    assertThrows(SQLErrorCode.INVALID_VALUE_2, rs).setFetchSize(-1);
    // fetch size 100 is bigger than maxrows - not allowed
    rs.setFetchSize(6);

    assertTrue(rs.getRow() == 1);
    assertEquals(2, rs.findColumn("COLUMN2"));
    assertEquals(2, rs.findColumn("column2"));
    assertEquals(2, rs.findColumn("Column2"));
    assertEquals(1, rs.findColumn("COLUMN1"));
    assertEquals(1, rs.findColumn("column1"));
    assertEquals(1, rs.findColumn("Column1"));
    assertEquals(1, rs.findColumn("colUMN1"));
    assertTrue(rs.getInt(2) == -1 && !rs.wasNull());
    assertTrue(rs.getInt("COLUMN2") == -1 && !rs.wasNull());
    assertTrue(rs.getInt("column2") == -1 && !rs.wasNull());
    assertTrue(rs.getInt("Column2") == -1 && !rs.wasNull());
    assertTrue(rs.getString("Column2").equals("-1") && !rs.wasNull());

    o = rs.getObject("column2");
    trace(o.getClass().getName());
    assertTrue(o instanceof Long);
    assertTrue(((Long) o).longValue() == -1);
    o = rs.getObject(2);
    trace(o.getClass().getName());
    assertTrue(o instanceof Long);
    assertTrue(((Long) o).longValue() == -1);
    assertTrue(rs.getBoolean("Column2"));
    assertTrue(rs.getByte("Column2") == (byte) -1);
    assertTrue(rs.getShort("Column2") == (short) -1);
    assertTrue(rs.getLong("Column2") == -1);
    assertTrue(rs.getFloat("Column2") == -1.0);
    assertTrue(rs.getDouble("Column2") == -1.0);

    assertTrue(rs.getString("Column2").equals("-1") && !rs.wasNull());
    assertTrue(rs.getInt("COLUMN1") == 31 && !rs.wasNull());
    assertTrue(rs.getInt("column1") == 31 && !rs.wasNull());
    assertTrue(rs.getInt("Column1") == 31 && !rs.wasNull());
    assertTrue(rs.getInt(1) == 31 && !rs.wasNull());
    rs.next();
    assertTrue(rs.getRow() == 2);
    assertTrue(rs.getInt(2) == 0 && !rs.wasNull());
    assertTrue(!rs.getBoolean(2));
    assertTrue(rs.getByte(2) == 0);
    assertTrue(rs.getShort(2) == 0);
    assertTrue(rs.getLong(2) == 0);
    assertTrue(rs.getFloat(2) == 0.0);
    assertTrue(rs.getDouble(2) == 0.0);
    assertTrue(rs.getString(2).equals("0") && !rs.wasNull());
    assertTrue(rs.getInt(1) == 32 && !rs.wasNull());
    rs.next();
    assertTrue(rs.getRow() == 3);
    assertTrue(rs.getInt("COLUMN1") == 33 && !rs.wasNull());
    assertTrue(rs.getInt("COLUMN2") == 1 && !rs.wasNull());
    rs.next();
    assertTrue(rs.getRow() == 4);
    assertTrue(rs.getInt("COLUMN1") == 34 && !rs.wasNull());
    assertTrue(rs.getInt("COLUMN2") == Integer.MAX_VALUE && !rs.wasNull());
    rs.next();
    assertTrue(rs.getRow() == 5);
    assertTrue(rs.getInt("column1") == 35 && !rs.wasNull());
    assertTrue(rs.getInt("column2") == Integer.MIN_VALUE && !rs.wasNull());
    assertTrue(rs.getString(1).equals("35") && !rs.wasNull());
    rs.next();
    assertTrue(rs.getRow() == 6);
    assertTrue(rs.getInt("column1") == 36 && !rs.wasNull());
    assertTrue(rs.getInt("column2") == 0 && !rs.wasNull());
    assertTrue(rs.getInt(2) == 0 && !rs.wasNull());
    assertTrue(rs.getInt(1) == 36 && !rs.wasNull());
    assertTrue(rs.getString(1).equals("36") && !rs.wasNull());
    assertTrue(rs.getString(2).equals("0") && !rs.wasNull());
    assertTrue(!rs.wasNull());
    // assertFalse(rs.next());
    // assertEquals(0, rs.getRow());
    // there is one more row, but because of setMaxRows we don't get it
  }

  @Test
  public void testVarchar() throws SQLException {
    trace("test VARCHAR");
    ResultSet rs;
    Object o;
    stat = conn.createStatement();
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(1,10,'')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(2,10,' ')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(3,10,'  ')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(4,10,'')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(5,10,'Hi')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(6,10,' Hi ')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(7,10,'Joe''s')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(8,10,'{escape}')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(9,10,'\\n')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(10,10,'\\'')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(11,10,'\\%')");
    stat.execute("INSERT INTO test (column1,column2,column3) VALUES(12,10,'\\%')");
    rs = stat
        .executeQuery("SELECT column1,column2,column3 FROM test where column2=10 ORDER BY column1");
    String value;
    rs.next();
    value = rs.getString(3);
    trace("Value: <" + value + "> (should be: <>)");
    assertTrue(value != null && value.equals("") && !rs.wasNull());
    assertTrue(rs.getInt(1) == 1 && !rs.wasNull());
    rs.next();
    value = rs.getString(3);
    trace("Value: <" + value + "> (should be: < >)");
    assertTrue(rs.getString(3).equals(" ") && !rs.wasNull());
    assertTrue(rs.getInt(1) == 2 && !rs.wasNull());
    rs.next();
    value = rs.getString(3);
    trace("Value: <" + value + "> (should be: <  >)");
    assertTrue(rs.getString(3).equals("  ") && !rs.wasNull());
    assertTrue(rs.getInt(1) == 3 && !rs.wasNull());
    rs.next();
    value = rs.getString(3);
    trace("Value: <" + value + "> (should be: <null>)");
    assertTrue(rs.getString(3).equals("") && !rs.wasNull());
    assertTrue(rs.getInt(1) == 4 && !rs.wasNull());
    rs.next();
    value = rs.getString(3);
    trace("Value: <" + value + "> (should be: <Hi>)");
    assertTrue(rs.getInt(1) == 5 && !rs.wasNull());
    assertTrue(rs.getString(3).equals("Hi") && !rs.wasNull());
    o = rs.getObject("column3");
    trace(o.getClass().getName());
    assertTrue(o instanceof String);
    assertTrue(o.toString().equals("Hi"));
    rs.next();
    value = rs.getString(3);
    trace("Value: <" + value + "> (should be: < Hi >)");
    assertTrue(rs.getInt(1) == 6 && !rs.wasNull());
    assertTrue(rs.getString(3).equals(" Hi ") && !rs.wasNull());
    rs.next();
    value = rs.getString(3);
    trace("Value: <" + value + "> (should be: <Joe's>)");
    assertTrue(rs.getInt(1) == 7 && !rs.wasNull());
    assertTrue(rs.getString(3).equals("Joe's") && !rs.wasNull());
    rs.next();
    value = rs.getString(3);
    trace("Value: <" + value + "> (should be: <{escape}>)");
    assertTrue(rs.getInt(1) == 8 && !rs.wasNull());
    assertTrue(rs.getString(3).equals("{escape}") && !rs.wasNull());
    rs.next();
    value = rs.getString(3);
    trace("Value: <" + value + "> (should be: <\\n>)");
    assertTrue(rs.getInt(1) == 9 && !rs.wasNull());
    assertTrue(rs.getString(3).equals("\n") && !rs.wasNull());
    rs.next();
    value = rs.getString(3);
    trace("Value: <" + value + "> (should be: <\\'>)");
    assertTrue(rs.getInt(1) == 10 && !rs.wasNull());
    assertTrue(rs.getString(3).equals("\'") && !rs.wasNull());
    rs.next();
    value = rs.getString(3);
    trace("Value: <" + value + "> (should be: <\\%>)");
    assertTrue(rs.getInt(1) == 11 && !rs.wasNull());
    assertTrue(rs.getString(3).equals("%") && !rs.wasNull());
    // assertTrue(!rs.next());
  }

  @Test
  public void testDecimal() throws SQLException {
    trace("test DECIMAL");
    ResultSet rs;
    Object o;

    stat = conn.createStatement();
    stat.execute("INSERT INTO test (column1,column5,column2,column3) VALUES(21,-1,9,'testDecimal')");
    stat.execute("INSERT INTO test (column1,column5,column2,column3) VALUES(22,.0,9,'testDecimal')");
    stat.execute("INSERT INTO test (column1,column5,column2,column3) VALUES(23,1.0,9,'testDecimal')");
    stat.execute("INSERT INTO test (column1,column5,column2,column3) VALUES(24,12345678.89,9,'testDecimal')");
    stat.execute("INSERT INTO test (column1,column5,column2,column3) VALUES(25,99999998.99,9,'testDecimal')");
    stat.execute("INSERT INTO test (column1,column5,column2,column3) VALUES(26,-99999998.99,9,'testDecimal')");
    stat.execute("INSERT INTO test (column1,column5,column2,column3) VALUES(27,-99999998.99,9,'testDecimal')");
    rs = stat
        .executeQuery("SELECT column1,column5 FROM test where column3='testDecimal' ORDER BY column1");
    BigDecimal bd;
    rs.next();
    assertTrue(rs.getInt(1) == 21);
    assertTrue(!rs.wasNull());
    assertTrue(rs.getInt(2) == -1);
    assertTrue(!rs.wasNull());
    bd = rs.getBigDecimal(2);
    assertTrue(bd.compareTo(new BigDecimal("-1.00")) == 0);
    assertTrue(!rs.wasNull());
    o = rs.getObject(2);
    trace(o.getClass().getName());
    assertTrue(o instanceof Double);
    assertTrue(new BigDecimal((Double) o).compareTo(new BigDecimal("-1.00")) == 0);
    rs.next();
    assertTrue(rs.getInt(1) == 22);
    assertTrue(!rs.wasNull());
    assertTrue(rs.getInt(2) == 0);
    assertTrue(!rs.wasNull());
    bd = rs.getBigDecimal(2);
    assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
    assertTrue(!rs.wasNull());
    rs.next();
    checkColumnBigDecimal(rs, 2, 1, "1.00");
    rs.next();
    checkColumnBigDecimal(rs, 2, 12345679, "12345678.89");
    rs.next();
    checkColumnBigDecimal(rs, 2, 99999999, "99999998.99");
    rs.next();
    checkColumnBigDecimal(rs, 2, -99999999, "-99999998.99");
    // assertTrue(!rs.next());
  }

  @Test
  public void testDoubleFloat() throws SQLException, IOException {
    trace("test DOUBLE - FLOAT");

    ResultInHBasePrinter.printFMETA(TEST_UTIL.getConfiguration(), LOG);
    ResultInHBasePrinter.printMETA(TEST_UTIL.getConfiguration(), LOG);
    ResultSet rs;
    Object o;

    stat = conn.createStatement();
    stat.execute("INSERT INTO test (column1,column5,column4,column2,column3) VALUES(11, -1, -1, 2, 'testDoubleFloat')");
    stat.execute("INSERT INTO test (column1,column5,column4,column2,column3) VALUES(12,.0, .0, 2, 'testDoubleFloat')");
    stat.execute("INSERT INTO test (column1,column5,column4,column2,column3) VALUES(13, 1., 1., 2, 'testDoubleFloat')");
    stat.execute("INSERT INTO test (column1,column5,column4,column2,column3) VALUES(14, 12345678.89, 12345678.89, 2, 'testDoubleFloat')");
    stat.execute("INSERT INTO test (column1,column5,column4,column2,column3) VALUES(15, 99999999.99, 99999999.99, 2, 'testDoubleFloat')");
    stat.execute("INSERT INTO test (column1,column5,column4,column2,column3) VALUES(16, -99999999.99, -99999999.99, 2, 'testDoubleFloat')");
    stat.execute("INSERT INTO test (column1,column5,column4,column2,column3) VALUES(17, -99999999.99, -99999999.99, 2, 'testDoubleFloat')");
    // stat.execute("INSERT INTO test (column1,column5,column4,column2,column3) VALUES(8, NULL, NULL, 2, 'testDoubleFloat')");

    rs = stat
        .executeQuery("SELECT column1,column5,column4 FROM test where column3='testDoubleFloat'  ORDER BY column1");
    // assertResultSetMeta(rs, 3, new String[] { "ID", "D", "R" }, new int[] {
    // Types.INTEGER, Types.DOUBLE, Types.REAL }, new int[] { 10, 17, 7 },
    // new int[] { 0, 0, 0 });
    BigDecimal bd;
    rs.next();
    assertTrue(rs.getInt(1) == 11);
    assertTrue(!rs.wasNull());
    assertTrue(rs.getInt(2) == -1);
    assertTrue(rs.getInt(3) == -1);
    assertTrue(!rs.wasNull());
    bd = rs.getBigDecimal(2);
    assertTrue(bd.compareTo(new BigDecimal("-1.00")) == 0);
    assertTrue(!rs.wasNull());
    o = rs.getObject(2);
    trace(o.getClass().getName());
    assertTrue(o instanceof Double);
    assertTrue(((Double) o).compareTo(new Double("-1.00")) == 0);
    o = rs.getObject(3);
    trace(o.getClass().getName());
    assertTrue(o instanceof Float);
    assertTrue(((Float) o).compareTo(new Float("-1.00")) == 0);
    rs.next();
    assertTrue(rs.getInt(1) == 12);
    assertTrue(!rs.wasNull());
    assertTrue(rs.getInt(2) == 0);
    assertTrue(!rs.wasNull());
    assertTrue(rs.getInt(3) == 0);
    assertTrue(!rs.wasNull());
    bd = rs.getBigDecimal(2);
    assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
    assertTrue(!rs.wasNull());
    bd = rs.getBigDecimal(3);
    assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
    assertTrue(!rs.wasNull());
    rs.next();
    assertEquals(1.0, rs.getDouble(2));
    assertEquals(1.0f, rs.getFloat(3));
    rs.next();
    assertEquals(12345678.89, rs.getDouble(2));
    assertEquals(12345678.89f, rs.getFloat(3));
    rs.next();
    assertEquals(99999999.99, rs.getDouble(2));
    assertEquals(99999999.99f, rs.getFloat(3));
    rs.next();
    assertEquals(-99999999.99, rs.getDouble(2));
    assertEquals(-99999999.99f, rs.getFloat(3));
    // rs.next();
    // checkColumnBigDecimal(rs, 2, 0, null);
    // checkColumnBigDecimal(rs, 3, 0, null);
    // assertTrue(!rs.next());
    // stat.execute("DROP TABLE test");
  }

  @Test
  public void testDatetime() throws SQLException {
    trace("test DATETIME");
    ResultSet rs;
    Object o;

    // rs = stat.executeQuery("call date '99999-12-23'");
    // rs.next();
    // assertEquals("99999-12-23", rs.getString(1));
    // rs = stat.executeQuery("call timestamp '99999-12-23 01:02:03.000'");
    // rs.next();
    // assertEquals("99999-12-23 01:02:03.0", rs.getString(1));
    // rs = stat.executeQuery("call date '-99999-12-23'");
    // rs.next();
    // assertEquals("-99999-12-23", rs.getString(1));
    // rs = stat.executeQuery("call timestamp '-99999-12-23 01:02:03.000'");
    // rs.next();
    // assertEquals("-99999-12-23 01:02:03.0", rs.getString(1));

    stat = conn.createStatement();
    // stat.execute("CREATE TABLE test(ID INT PRIMARY KEY,VALUE DATETIME)");
    stat.execute("INSERT INTO test (column1,column6,column2,column3) VALUES (1,'2011-11-11 0:0:0', 13, 'testDatetime')");
    stat.execute("INSERT INTO test (column1,column6,column2,column3) VALUES (2,'2002-02-02 02:02:02', 13, 'testDatetime')");
    stat.execute("INSERT INTO test (column1,column6,column2,column3) VALUES (3,'1800-01-01 0:0:0', 13, 'testDatetime')");
    stat.execute("INSERT INTO test (column1,column6,column2,column3) VALUES (4,'9999-12-31 23:59:59', 13, 'testDatetime')");
    stat.execute("INSERT INTO test (column1,column6,column2,column3) VALUES (5,'9999-12-31 23:59:59', 13, 'testDatetime')");
    // stat.execute("INSERT INTO test (column1,column6,column2,column3) VALUES(5,NULL)");
    rs = stat
        .executeQuery("SELECT column1,column6 FROM test where column3='testDatetime' ORDER BY column1");
    // assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] {
    // Types.INTEGER, Types.TIMESTAMP }, new int[] { 10, 23 }, new int[] { 0,
    // 10 });
    // rs = stat.executeQuery("SELECT * FROM test ORDER BY ID");
    // assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" }, new int[] {
    // Types.INTEGER, Types.TIMESTAMP }, new int[] { 10, 23 }, new int[] { 0,
    // 10 });
    rs.next();
    java.sql.Date date;
    java.sql.Time time;
    java.sql.Timestamp ts;
    date = rs.getDate(2);
    assertTrue(!rs.wasNull());
    time = rs.getTime(2);
    assertTrue(!rs.wasNull());
    ts = rs.getTimestamp(2);
    assertTrue(!rs.wasNull());
    trace("Date: " + date.toString() + " Time:" + time.toString()
        + " Timestamp:" + ts.toString());
    trace("Date ms: " + date.getTime() + " Time ms:" + time.getTime()
        + " Timestamp ms:" + ts.getTime());
    trace("1970 ms: "
        + java.sql.Timestamp.valueOf("1970-01-01 00:00:00.0").getTime());
    assertEquals(java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0").getTime(),
        date.getTime());
    assertEquals(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.0").getTime(),
        time.getTime());
    assertEquals(java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0").getTime(),
        ts.getTime());
    assertTrue(date.equals(java.sql.Date.valueOf("2011-11-11")));
    assertTrue(time.equals(java.sql.Time.valueOf("00:00:00")));
    assertTrue(ts.equals(java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
    assertFalse(rs.wasNull());
    o = rs.getObject(2);
    trace(o.getClass().getName());
    assertTrue(o instanceof Timestamp);
    assertTrue(((Timestamp) o).equals(Timestamp.valueOf("2011-11-11 00:00:00")));
    assertFalse(rs.wasNull());
    rs.next();
    date = rs.getDate("COLUMN6");
    assertTrue(!rs.wasNull());
    time = rs.getTime("COLUMN6");
    assertTrue(!rs.wasNull());
    ts = rs.getTimestamp("COLUMN6");
    assertTrue(!rs.wasNull());
    trace("Date: " + date.toString() + " Time:" + time.toString()
        + " Timestamp:" + ts.toString());
    assertEquals("2002-02-02", date.toString());
    assertEquals("02:02:02", time.toString());
    assertEquals("2002-02-02 02:02:02.0", ts.toString());
    rs.next();
    assertEquals("1800-01-01", rs.getDate("column6").toString());
    assertEquals("00:00:00", rs.getTime("column6").toString());
    assertEquals("1800-01-01 00:00:00.0", rs.getTimestamp("column6").toString());
    rs.next();
    assertEquals("9999-12-31", rs.getDate("Column6").toString());
    assertEquals("23:59:59", rs.getTime("Column6").toString());
    assertEquals("9999-12-31 23:59:59.0", rs.getTimestamp("Column6").toString());
    // assertTrue(!rs.next());

  }

  // @Test do not needed now
  public void testDatetimeWithCalendar() throws SQLException {
    trace("test DATETIME with Calendar");
    ResultSet rs;

    stat = conn.createStatement();
    stat.execute("CREATE TABLE test(ID INT PRIMARY KEY, D DATE, T TIME, TS TIMESTAMP)");
    PreparedStatement prep = conn
        .prepareStatement("INSERT INTO test VALUES(?, ?, ?, ?)");
    Calendar regular = Calendar.getInstance();
    Calendar other = null;
    // search a locale that has a _different_ raw offset
    long testTime = java.sql.Date.valueOf("2001-02-03").getTime();
    for (String s : TimeZone.getAvailableIDs()) {
      TimeZone zone = TimeZone.getTimeZone(s);
      long rawOffsetDiff = regular.getTimeZone().getRawOffset()
          - zone.getRawOffset();
      // must not be the same timezone (not 0 h and not 24 h difference
      // as for Pacific/Auckland and Etc/GMT+12)
      if (rawOffsetDiff != 0 && rawOffsetDiff != 1000 * 60 * 60 * 24) {
        if (regular.getTimeZone().getOffset(testTime) != zone
            .getOffset(testTime)) {
          other = Calendar.getInstance(zone);
          break;
        }
      }
    }

    trace("regular offset = " + regular.getTimeZone().getRawOffset()
        + " other = " + other.getTimeZone().getRawOffset());

    prep.setInt(1, 0);
    prep.setDate(2, null, regular);
    prep.setTime(3, null, regular);
    prep.setTimestamp(4, null, regular);
    prep.execute();

    prep.setInt(1, 1);
    prep.setDate(2, null, other);
    prep.setTime(3, null, other);
    prep.setTimestamp(4, null, other);
    prep.execute();

    prep.setInt(1, 2);
    prep.setDate(2, java.sql.Date.valueOf("2001-02-03"), regular);
    prep.setTime(3, java.sql.Time.valueOf("04:05:06"), regular);
    prep.setTimestamp(4,
        java.sql.Timestamp.valueOf("2007-08-09 10:11:12.131415"), regular);
    prep.execute();

    prep.setInt(1, 3);
    prep.setDate(2, java.sql.Date.valueOf("2101-02-03"), other);
    prep.setTime(3, java.sql.Time.valueOf("14:05:06"), other);
    prep.setTimestamp(4,
        java.sql.Timestamp.valueOf("2107-08-09 10:11:12.131415"), other);
    prep.execute();

    prep.setInt(1, 4);
    prep.setDate(2, java.sql.Date.valueOf("2101-02-03"));
    prep.setTime(3, java.sql.Time.valueOf("14:05:06"));
    prep.setTimestamp(4,
        java.sql.Timestamp.valueOf("2107-08-09 10:11:12.131415"));
    prep.execute();

    rs = stat.executeQuery("SELECT * FROM test ORDER BY ID");
    assertResultSetMeta(rs, 4, new String[] { "ID", "D", "T", "TS" },
        new int[] { Types.INTEGER, Types.DATE, Types.TIME, Types.TIMESTAMP },
        new int[] { 10, 8, 6, 23 }, new int[] { 0, 0, 0, 10 });

    rs.next();
    assertEquals(0, rs.getInt(1));
    assertTrue(rs.getDate(2, regular) == null && rs.wasNull());
    assertTrue(rs.getTime(3, regular) == null && rs.wasNull());
    assertTrue(rs.getTimestamp(3, regular) == null && rs.wasNull());

    rs.next();
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.getDate(2, other) == null && rs.wasNull());
    assertTrue(rs.getTime(3, other) == null && rs.wasNull());
    assertTrue(rs.getTimestamp(3, other) == null && rs.wasNull());

    rs.next();
    assertEquals(2, rs.getInt(1));
    assertEquals("2001-02-03", rs.getDate(2, regular).toString());
    assertEquals("04:05:06", rs.getTime(3, regular).toString());
    assertFalse(rs.getTime(3, other).toString().equals("04:05:06"));
    assertEquals("2007-08-09 10:11:12.131415", rs.getTimestamp(4, regular)
        .toString());
    assertFalse(rs.getTimestamp(4, other).toString()
        .equals("2007-08-09 10:11:12.131415"));

    rs.next();
    assertEquals(3, rs.getInt("ID"));
    assertFalse(rs.getTimestamp("TS", regular).toString()
        .equals("2107-08-09 10:11:12.131415"));
    assertEquals("2107-08-09 10:11:12.131415", rs.getTimestamp("TS", other)
        .toString());
    assertFalse(rs.getTime("T", regular).toString().equals("14:05:06"));
    assertEquals("14:05:06", rs.getTime("T", other).toString());
    // checkFalse(rs.getDate(2, regular).toString(), "2101-02-03");
    // check(rs.getDate("D", other).toString(), "2101-02-03");

    rs.next();
    assertEquals(4, rs.getInt("ID"));
    assertEquals("2107-08-09 10:11:12.131415", rs.getTimestamp("TS").toString());
    assertEquals("14:05:06", rs.getTime("T").toString());
    assertEquals("2101-02-03", rs.getDate("D").toString());

    assertFalse(rs.next());
    stat.execute("DROP TABLE test");
  }

  private void checkColumnBigDecimal(ResultSet rs, int column, int i, String bd)
      throws SQLException {
    BigDecimal bd1 = rs.getBigDecimal(column);
    int i1 = rs.getInt(column);
    if (bd == null) {
      trace("should be: null");
      assertTrue(rs.wasNull());
    } else {
      trace("BigDecimal i=" + i + " bd=" + bd + " ; i1=" + i1 + " bd1=" + bd1);
      assertTrue(!rs.wasNull());
      assertTrue(i1 == i);
      assertTrue(bd1.compareTo(new BigDecimal(bd)) == 0);
    }
  }
}
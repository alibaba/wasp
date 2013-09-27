/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.alibaba.wasp.jdbc;

import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.SQLErrorCode;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.client.FClient;
import com.alibaba.wasp.util.DateTimeUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Tests for the PreparedStatement implementation.
 */
public class TestPreparedStatement extends TestJdbcBase {

  private static final int LOB_SIZE = 4000, LOB_SIZE_BIG = 512 * 1024;

  private static Connection conn;
  private static Configuration conf;
  private final static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  public static final String TABLE_NAME = "TEST";
  public static final byte[] TABLE = Bytes.toBytes("TEST");
  final Log LOG = LogFactory.getLog(getClass());
  private static FClient client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("wasp.client.retries.number", 3);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.createTable(TABLE);
    TEST_UTIL.getWaspAdmin().disableTable(TABLE);
    client = new FClient(TEST_UTIL.getConfiguration());
//    client.execute("create index test_index on " + TABLE_NAME + "(column3);");
//    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
//    client.execute("create index test_index2 on " + TABLE_NAME + "(column2);");
//    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
//    client.execute("create index test_index3 on " + TABLE_NAME
//        + "(column1,column3);");
//    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
//    client.execute("create index test_index4 on " + TABLE_NAME + "(column1,column3,column4,column5);");
//    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    TEST_UTIL.getWaspAdmin().enableTable(TABLE);

    Class.forName("com.alibaba.wasp.jdbc.Driver");
    conn = getConnection("test", TEST_UTIL.getConfiguration());
    conn.setClientInfo(FConstants.READ_MODEL, ReadModel.CURRENT.name());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    conn.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testInsertQuery() throws SQLException {
    String INSERT = "Insert into " + TABLE_NAME
    + "(column1,column2,column3) values (?,?,?);";

    PreparedStatement prep = conn
        .prepareStatement(INSERT);

    prep.setInt(1, 2);
    prep.setLong(2, 33445);
    prep.setString(3, "testInsertQuery");
    int ret = prep.executeUpdate();

    assertTrue(ret == 1);

    prep = conn.prepareStatement("select column1,column2,column3 from "
        + TABLE_NAME + " where column1=? and column2=?");
    prep.setInt(1, 2);
    prep.setLong(2, 33445);

    ResultSet rs = prep.executeQuery();
    assertTrue(rs.next());
    assertEquals(2, rs.getInt("column1"));
    assertEquals(33445, rs.getLong("column2"));
    assertEquals("testInsertQuery", rs.getString("column3"));
    prep.close();
  }

//  @Test
  public void testChangeType() throws SQLException {
    PreparedStatement prep = conn
        .prepareStatement("select (? || ? || ?) from dual");
    prep.setString(1, "a");
    prep.setString(2, "b");
    prep.setString(3, "c");
    prep.executeQuery();
    prep.setInt(1, 1);
    prep.setString(2, "ab");
    prep.setInt(3, 45);
    prep.executeQuery();
  }

//  @Test
  public void testDateTimeTimestampWithCalendar()
      throws SQLException {
    Statement stat = conn.createStatement();
    stat.execute("create table ts(x timestamp primary key)");
    stat.execute("create table t(x time primary key)");
    stat.execute("create table d(x date)");
    Calendar utcCalendar = new GregorianCalendar(new SimpleTimeZone(0, "Z"));
    TimeZone old = TimeZone.getDefault();
    DateTimeUtils.resetCalendar();
    TimeZone.setDefault(TimeZone.getTimeZone("PST"));
    try {
      Timestamp ts1 = Timestamp.valueOf("2010-03-13 18:15:00");
      Time t1 = new Time(ts1.getTime());
      Date d1 = new Date(ts1.getTime());
      // when converted to UTC, this is 03:15, which doesn't actually exist
      // because of summer time change at that day
      Timestamp ts2 = Timestamp.valueOf("2010-03-13 19:15:00");
      Time t2 = new Time(ts2.getTime());
      Date d2 = new Date(ts2.getTime());
      PreparedStatement prep;
      ResultSet rs;
      prep = conn.prepareStatement("insert into ts values(?)");
      prep.setTimestamp(1, ts1, utcCalendar);
      prep.execute();
      prep.setTimestamp(1, ts2, utcCalendar);
      prep.execute();
      prep = conn.prepareStatement("insert into t values(?)");
      prep.setTime(1, t1, utcCalendar);
      prep.execute();
      prep.setTime(1, t2, utcCalendar);
      prep.execute();
      prep = conn.prepareStatement("insert into d values(?)");
      prep.setDate(1, d1, utcCalendar);
      prep.execute();
      prep.setDate(1, d2, utcCalendar);
      prep.execute();
      rs = stat.executeQuery("select * from ts order by x");
      rs.next();
      assertEquals("2010-03-14 02:15:00.0", rs.getString(1));
      assertEquals("2010-03-13 18:15:00.0", rs.getTimestamp(1, utcCalendar)
          .toString());
      assertEquals("2010-03-14 03:15:00.0", rs.getTimestamp(1).toString());
      assertEquals("2010-03-14 02:15:00.0", rs.getString("x"));
      assertEquals("2010-03-13 18:15:00.0", rs.getTimestamp("x", utcCalendar)
          .toString());
      assertEquals("2010-03-14 03:15:00.0", rs.getTimestamp("x").toString());
      rs.next();
      assertEquals("2010-03-14 03:15:00.0", rs.getString(1));
      assertEquals("2010-03-13 19:15:00.0", rs.getTimestamp(1, utcCalendar)
          .toString());
      assertEquals("2010-03-14 03:15:00.0", rs.getTimestamp(1).toString());
      assertEquals("2010-03-14 03:15:00.0", rs.getString("x"));
      assertEquals("2010-03-13 19:15:00.0", rs.getTimestamp("x", utcCalendar)
          .toString());
      assertEquals("2010-03-14 03:15:00.0", rs.getTimestamp("x").toString());
      rs = stat.executeQuery("select * from t order by x");
      rs.next();
      assertEquals("02:15:00", rs.getString(1));
      assertEquals("18:15:00", rs.getTime(1, utcCalendar).toString());
      assertEquals("02:15:00", rs.getTime(1).toString());
      assertEquals("02:15:00", rs.getString("x"));
      assertEquals("18:15:00", rs.getTime("x", utcCalendar).toString());
      assertEquals("02:15:00", rs.getTime("x").toString());
      rs.next();
      assertEquals("03:15:00", rs.getString(1));
      assertEquals("19:15:00", rs.getTime(1, utcCalendar).toString());
      assertEquals("03:15:00", rs.getTime(1).toString());
      assertEquals("03:15:00", rs.getString("x"));
      assertEquals("19:15:00", rs.getTime("x", utcCalendar).toString());
      assertEquals("03:15:00", rs.getTime("x").toString());
      rs = stat.executeQuery("select * from d order by x");
      rs.next();
      assertEquals("2010-03-14", rs.getString(1));
      assertEquals("2010-03-13", rs.getDate(1, utcCalendar).toString());
      assertEquals("2010-03-14", rs.getDate(1).toString());
      assertEquals("2010-03-14", rs.getString("x"));
      assertEquals("2010-03-13", rs.getDate("x", utcCalendar).toString());
      assertEquals("2010-03-14", rs.getDate("x").toString());
      rs.next();
      assertEquals("2010-03-14", rs.getString(1));
      assertEquals("2010-03-13", rs.getDate(1, utcCalendar).toString());
      assertEquals("2010-03-14", rs.getDate(1).toString());
      assertEquals("2010-03-14", rs.getString("x"));
      assertEquals("2010-03-13", rs.getDate("x", utcCalendar).toString());
      assertEquals("2010-03-14", rs.getDate("x").toString());
    } finally {
      TimeZone.setDefault(old);
      DateTimeUtils.resetCalendar();
    }
    stat.execute("drop table ts");
    stat.execute("drop table t");
    stat.execute("drop table d");
  }

//  @Test
  public void testCallTablePrepared()
      throws SQLException {
    PreparedStatement prep = conn.prepareStatement("call table(x int = (1))");
    prep.executeQuery();
    prep.executeQuery();
  }

//  @Test
  public void testValues() throws SQLException {
    PreparedStatement prep = conn.prepareStatement("values(?, ?)");
    prep.setInt(1, 1);
    prep.setString(2, "Hello");
    ResultSet rs = prep.executeQuery();
    rs.next();
    assertEquals(1, rs.getInt(1));
    assertEquals("Hello", rs.getString(2));

    prep = conn.prepareStatement("select * from values(?, ?), (2, 'World!')");
    prep.setInt(1, 1);
    prep.setString(2, "Hello");
    rs = prep.executeQuery();
    rs.next();
    assertEquals(1, rs.getInt(1));
    assertEquals("Hello", rs.getString(2));
    rs.next();
    assertEquals(2, rs.getInt(1));
    assertEquals("World!", rs.getString(2));

    prep = conn.prepareStatement("values 1, 2");
    rs = prep.executeQuery();
    rs.next();
    assertEquals(1, rs.getInt(1));
    rs.next();
    assertEquals(2, rs.getInt(1));
  }

//  @Test
  public void testToString() throws SQLException {
    PreparedStatement prep = conn.prepareStatement("call 1");
    assertTrue(prep.toString().endsWith(": call 1"));
    prep = conn.prepareStatement("call ?");
    assertTrue(prep.toString().endsWith(": call ?"));
    prep.setString(1, "Hello World");
    assertTrue(prep.toString().endsWith(": call ? {1: 'Hello World'}"));
  }

//  @Test
  public void testExecuteUpdateCall() throws SQLException {
    assertThrows(SQLErrorCode.DATA_CONVERSION_ERROR_1, conn.createStatement())
        .executeUpdate("CALL HASH('SHA256', STRINGTOUTF8('Password'), 1000)");
  }

//  @Test
  public void testPrepareExecute() throws SQLException {
    Statement stat = conn.createStatement();
    stat.execute("prepare test(int, int) as select ?1*?2");
    ResultSet rs = stat.executeQuery("execute test(3, 2)");
    rs.next();
    assertEquals(6, rs.getInt(1));
    stat.execute("deallocate test");
  }

//  @Test
  public void testLobTempFiles() throws SQLException {
    Statement stat = conn.createStatement();
    stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, DATA CLOB)");
    PreparedStatement prep = conn
        .prepareStatement("INSERT INTO TEST VALUES(?, ?)");
    for (int i = 0; i < 5; i++) {
      prep.setInt(1, i);
      if (i % 2 == 0) {
        prep.setCharacterStream(2, new StringReader(getString(i)), -1);
      }
      prep.execute();
    }
    ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
    int check = 0;
    for (int i = 0; i < 5; i++) {
      assertTrue(rs.next());
      if (i % 2 == 0) {
        check = i;
      }
      assertEquals(getString(check), rs.getString(2));
    }
    assertFalse(rs.next());
    stat.execute("DELETE FROM TEST");
    for (int i = 0; i < 3; i++) {
      prep.setInt(1, i);
      prep.setCharacterStream(2, new StringReader(getString(i)), -1);
      prep.addBatch();
    }
    prep.executeBatch();
    rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
    for (int i = 0; i < 3; i++) {
      assertTrue(rs.next());
      assertEquals(getString(i), rs.getString(2));
    }
    assertFalse(rs.next());
    stat.execute("DROP TABLE TEST");
  }

  private static String getString(int i) {
    return new String(new char[100000]).replace('\0', (char) ('0' + i));
  }

//  @Test
  public void testInsertFunction() throws SQLException {
    Statement stat = conn.createStatement();
    PreparedStatement prep;
    ResultSet rs;

    stat.execute("CREATE TABLE TEST(ID INT, H BINARY)");
    prep = conn
        .prepareStatement("INSERT INTO TEST VALUES(?, HASH('SHA256', STRINGTOUTF8(?), 5))");
    prep.setInt(1, 1);
    prep.setString(2, "One");
    prep.execute();
    prep.setInt(1, 2);
    prep.setString(2, "Two");
    prep.execute();
    rs = stat.executeQuery("SELECT COUNT(DISTINCT H) FROM TEST");
    rs.next();
    assertEquals(2, rs.getInt(1));

    stat.execute("DROP TABLE TEST");
  }

//  @Test
  public void testUnknownDataType() throws SQLException {
    assertThrows(SQLErrorCode.UNKNOWN_DATA_TYPE_1, conn).prepareStatement(
        "SELECT * FROM (SELECT ? FROM DUAL)");
    PreparedStatement prep = conn.prepareStatement("SELECT -?");
    prep.setInt(1, 1);
    prep.execute();
    prep = conn.prepareStatement("SELECT ?-?");
    prep.setInt(1, 1);
    prep.setInt(2, 2);
    prep.execute();
  }

  public static void testCoalesce() throws SQLException {
    Statement stat = conn.createStatement();
    stat.executeUpdate("create table test(tm timestamp)");
    stat.executeUpdate("insert into test values(current_timestamp)");
    PreparedStatement prep = conn
        .prepareStatement("update test set tm = coalesce(?,tm)");
    prep.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
    prep.executeUpdate();
    stat.executeUpdate("drop table test");
  }

  public void testPreparedStatementMetaData()
      throws SQLException {
    PreparedStatement prep = conn
        .prepareStatement("select * from table(x int = ?, name varchar = ?)");
    ResultSetMetaData meta = prep.getMetaData();
    assertEquals(2, meta.getColumnCount());
    assertEquals("INTEGER", meta.getColumnTypeName(1));
    assertEquals("VARCHAR", meta.getColumnTypeName(2));
    prep = conn.prepareStatement("call 1");
    meta = prep.getMetaData();
    assertEquals(1, meta.getColumnCount());
    assertEquals("INTEGER", meta.getColumnTypeName(1));
  }

  @Test
  public void testArray() throws SQLException {
    PreparedStatement prep = conn
        .prepareStatement("select * from table(x int = ?) order by x");
    prep.setObject(1, new Object[] { new BigDecimal("1"), "2" });
    ResultSet rs = prep.executeQuery();
    rs.next();
    assertEquals("1", rs.getString(1));
    rs.next();
    assertEquals("2", rs.getString(1));
    assertFalse(rs.next());
  }

//  @Test
  public void testUUID() throws SQLException {
    Statement stat = conn.createStatement();
    stat.execute("create table test_uuid(id uuid primary key)");
    UUID uuid = new UUID(-2, -1);
    PreparedStatement prep = conn
        .prepareStatement("insert into test_uuid values(?)");
    prep.setObject(1, uuid);
    prep.execute();
    ResultSet rs = stat.executeQuery("select * from test_uuid");
    rs.next();
    assertEquals("ffffffff-ffff-fffe-ffff-ffffffffffff", rs.getString(1));
    Object o = rs.getObject(1);
    assertEquals("java.util.UUID", o.getClass().getName());
    stat.execute("drop table test_uuid");
  }

//  @Test
  public void testUUIDGeneratedKeys() throws SQLException {
    Statement stat = conn.createStatement();
    stat.execute("CREATE TABLE TEST_UUID(id UUID DEFAULT random_UUID() PRIMARY KEY)");
    stat.execute("INSERT INTO TEST_UUID() VALUES()");
    ResultSet rs = stat.getGeneratedKeys();
    rs.next();
    byte[] data = rs.getBytes(1);
    assertEquals(16, data.length);
    stat.execute("INSERT INTO TEST_UUID VALUES(random_UUID())");
    rs = stat.getGeneratedKeys();
    assertFalse(rs.next());
    stat.execute("DROP TABLE TEST_UUID");
  }

//  @Test
  public void testSetObject() throws SQLException {
    Statement stat = conn.createStatement();
    stat.execute("CREATE TABLE TEST(C CHAR(1))");
    PreparedStatement prep = conn
        .prepareStatement("INSERT INTO TEST VALUES(?)");
    prep.setObject(1, 'x');
    prep.execute();
    stat.execute("DROP TABLE TEST");
    stat.execute("CREATE TABLE TEST(ID INT, DATA BINARY, JAVA OTHER)");
    prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?, ?)");
    prep.setInt(1, 1);
    prep.setObject(2, 11);
    prep.setObject(3, null);
    prep.execute();
    prep.setInt(1, 2);
    prep.setObject(2, 101, Types.OTHER);
    prep.setObject(3, 103, Types.OTHER);
    prep.execute();
    PreparedStatement p2 = conn
        .prepareStatement("SELECT * FROM TEST ORDER BY ID");
    ResultSet rs = p2.executeQuery();
    rs.next();
    Object o = rs.getObject(2);
    assertTrue(o instanceof byte[]);
    assertTrue(rs.getObject(3) == null);
    rs.next();
    o = rs.getObject(2);
    assertTrue(o instanceof byte[]);
    o = rs.getObject(3);
    assertTrue(o instanceof Integer);
    assertEquals(103, ((Integer) o).intValue());
    assertFalse(rs.next());
    stat.execute("DROP TABLE TEST");
  }

//  @Test
  public void testDate() throws SQLException {
    PreparedStatement prep = conn.prepareStatement("SELECT ?");
    Timestamp ts = Timestamp.valueOf("2001-02-03 04:05:06");
    prep.setObject(1, new java.util.Date(ts.getTime()));
    ResultSet rs = prep.executeQuery();
    rs.next();
    Timestamp ts2 = rs.getTimestamp(1);
    assertEquals(ts.toString(), ts2.toString());
  }

  public void testParameterMetaData() throws SQLException {
    PreparedStatement prep = conn.prepareStatement("SELECT ?, ?, ? FROM DUAL");
    ParameterMetaData pm = prep.getParameterMetaData();
    assertEquals("java.lang.String", pm.getParameterClassName(1));
    assertEquals("VARCHAR", pm.getParameterTypeName(1));
    assertEquals(3, pm.getParameterCount());
    assertEquals(ParameterMetaData.parameterModeIn, pm.getParameterMode(1));
    assertEquals(Types.VARCHAR, pm.getParameterType(1));
    assertEquals(0, pm.getPrecision(1));
    assertEquals(0, pm.getScale(1));
    assertEquals(ResultSetMetaData.columnNullableUnknown, pm.isNullable(1));
    assertEquals(pm.isSigned(1), true);
    assertThrows(SQLErrorCode.INVALID_VALUE_2, pm).getPrecision(0);
    assertThrows(SQLErrorCode.INVALID_VALUE_2, pm).getPrecision(4);
    prep.close();
    assertThrows(SQLErrorCode.OBJECT_CLOSED, pm).getPrecision(1);

    Statement stat = conn.createStatement();
    stat.execute("CREATE TABLE TEST3(ID INT, NAME VARCHAR(255), DATA DECIMAL(10,2))");
    PreparedStatement prep1 = conn
        .prepareStatement("UPDATE TEST3 SET ID=?, NAME=?, DATA=?");
    PreparedStatement prep2 = conn
        .prepareStatement("INSERT INTO TEST3 VALUES(?, ?, ?)");
    checkParameter(prep1, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
    checkParameter(prep1, 2, "java.lang.String", 12, "VARCHAR", 255, 0);
    checkParameter(prep1, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
    checkParameter(prep2, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
    checkParameter(prep2, 2, "java.lang.String", 12, "VARCHAR", 255, 0);
    checkParameter(prep2, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
    PreparedStatement prep3 = conn
        .prepareStatement("SELECT * FROM TEST3 WHERE ID=? AND NAME LIKE ? AND ?>DATA");
    checkParameter(prep3, 1, "java.lang.Integer", 4, "INTEGER", 10, 0);
    checkParameter(prep3, 2, "java.lang.String", 12, "VARCHAR", 0, 0);
    checkParameter(prep3, 3, "java.math.BigDecimal", 3, "DECIMAL", 10, 2);
    stat.execute("DROP TABLE TEST3");
  }

  private void checkParameter(PreparedStatement prep, int index,
      String className, int type, String typeName, int precision, int scale)
      throws SQLException {
    ParameterMetaData meta = prep.getParameterMetaData();
    assertEquals(className, meta.getParameterClassName(index));
    assertEquals(type, meta.getParameterType(index));
    assertEquals(typeName, meta.getParameterTypeName(index));
    assertEquals(precision, meta.getPrecision(index));
    assertEquals(scale, meta.getScale(index));
  }

//  @Test
  public void testDataTypes() throws SQLException {
    conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY);
    conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
        ResultSet.CONCUR_UPDATABLE);
    Statement stat = conn.createStatement();
    PreparedStatement prep;
    ResultSet rs;
    trace("Create tables");
    stat.execute("CREATE TABLE T_INT(ID INT PRIMARY KEY,VALUE INT)");
    stat.execute("CREATE TABLE T_VARCHAR(ID INT PRIMARY KEY,VALUE VARCHAR(255))");
    stat.execute("CREATE TABLE T_DECIMAL_0(ID INT PRIMARY KEY,VALUE DECIMAL(30,0))");
    stat.execute("CREATE TABLE T_DECIMAL_10(ID INT PRIMARY KEY,VALUE DECIMAL(20,10))");
    stat.execute("CREATE TABLE T_DATETIME(ID INT PRIMARY KEY,VALUE DATETIME)");
    prep = conn.prepareStatement("INSERT INTO T_INT VALUES(?,?)",
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    prep.setInt(1, 1);
    prep.setInt(2, 0);
    prep.executeUpdate();
    prep.setInt(1, 2);
    prep.setInt(2, -1);
    prep.executeUpdate();
    prep.setInt(1, 3);
    prep.setInt(2, 3);
    prep.executeUpdate();
    prep.setInt(1, 4);
    prep.setNull(2, Types.INTEGER);
    prep.executeUpdate();
    prep.setInt(1, 5);
    prep.setBigDecimal(2, new BigDecimal("0"));
    prep.executeUpdate();
    prep.setInt(1, 6);
    prep.setString(2, "-1");
    prep.executeUpdate();
    prep.setInt(1, 7);
    prep.setObject(2, new Integer(3));
    prep.executeUpdate();
    prep.setObject(1, "8");
    // should throw an exception
    prep.setObject(2, null);
    // some databases don't allow calling setObject with null (no data type)
    prep.executeUpdate();
    prep.setInt(1, 9);
    prep.setObject(2, -4, Types.VARCHAR);
    prep.executeUpdate();
    prep.setInt(1, 10);
    prep.setObject(2, "5", Types.INTEGER);
    prep.executeUpdate();
    prep.setInt(1, 11);
    prep.setObject(2, null, Types.INTEGER);
    prep.executeUpdate();
    prep.setInt(1, 12);
    prep.setBoolean(2, true);
    prep.executeUpdate();
    prep.setInt(1, 13);
    prep.setBoolean(2, false);
    prep.executeUpdate();
    prep.setInt(1, 14);
    prep.setByte(2, (byte) -20);
    prep.executeUpdate();
    prep.setInt(1, 15);
    prep.setByte(2, (byte) 100);
    prep.executeUpdate();
    prep.setInt(1, 16);
    prep.setShort(2, (short) 30000);
    prep.executeUpdate();
    prep.setInt(1, 17);
    prep.setShort(2, (short) (-30000));
    prep.executeUpdate();
    prep.setInt(1, 18);
    prep.setLong(2, Integer.MAX_VALUE);
    prep.executeUpdate();
    prep.setInt(1, 19);
    prep.setLong(2, Integer.MIN_VALUE);
    prep.executeUpdate();

    assertTrue(stat.execute("SELECT * FROM T_INT ORDER BY ID"));
    rs = stat.getResultSet();
    assertResultSetOrdered(rs, new String[][] { { "1", "0" }, { "2", "-1" },
        { "3", "3" }, { "4", null }, { "5", "0" }, { "6", "-1" }, { "7", "3" },
        { "8", null }, { "9", "-4" }, { "10", "5" }, { "11", null },
        { "12", "1" }, { "13", "0" }, { "14", "-20" }, { "15", "100" },
        { "16", "30000" }, { "17", "-30000" },
        { "18", "" + Integer.MAX_VALUE }, { "19", "" + Integer.MIN_VALUE }, });

    prep = conn.prepareStatement("INSERT INTO T_DECIMAL_0 VALUES(?,?)");
    prep.setInt(1, 1);
    prep.setLong(2, Long.MAX_VALUE);
    prep.executeUpdate();
    prep.setInt(1, 2);
    prep.setLong(2, Long.MIN_VALUE);
    prep.executeUpdate();
    prep.setInt(1, 3);
    prep.setFloat(2, 10);
    prep.executeUpdate();
    prep.setInt(1, 4);
    prep.setFloat(2, -20);
    prep.executeUpdate();
    prep.setInt(1, 5);
    prep.setFloat(2, 30);
    prep.executeUpdate();
    prep.setInt(1, 6);
    prep.setFloat(2, -40);
    prep.executeUpdate();

    rs = stat.executeQuery("SELECT VALUE FROM T_DECIMAL_0 ORDER BY ID");
    checkBigDecimal(rs, new String[] { "" + Long.MAX_VALUE,
        "" + Long.MIN_VALUE, "10", "-20", "30", "-40" });
  }

//  @Test
  public void testGetMoreResults() throws SQLException {
    Statement stat = conn.createStatement();
    PreparedStatement prep;
    ResultSet rs;
    stat.execute("CREATE TABLE TEST(ID INT)");
    stat.execute("INSERT INTO TEST VALUES(1)");

    prep = conn.prepareStatement("SELECT * FROM TEST");
    // just to check if it doesn't throw an exception - it may be null
    prep.getMetaData();
    assertTrue(prep.execute());
    rs = prep.getResultSet();
    assertFalse(prep.getMoreResults());
    assertEquals(-1, prep.getUpdateCount());
    // supposed to be closed now
    assertThrows(SQLErrorCode.OBJECT_CLOSED, rs).next();
    assertEquals(-1, prep.getUpdateCount());

    prep = conn.prepareStatement("UPDATE TEST SET ID = 2");
    assertFalse(prep.execute());
    assertEquals(1, prep.getUpdateCount());
    assertFalse(prep.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
    assertEquals(-1, prep.getUpdateCount());
    // supposed to be closed now
    assertThrows(SQLErrorCode.OBJECT_CLOSED, rs).next();
    assertEquals(-1, prep.getUpdateCount());

    prep = conn.prepareStatement("DELETE FROM TEST");
    prep.executeUpdate();
    assertFalse(prep.getMoreResults());
    assertEquals(-1, prep.getUpdateCount());
  }

//  @Test
  public void testObject() throws SQLException {
    Statement stat = conn.createStatement();
    ResultSet rs;
    stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
    stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
    PreparedStatement prep = conn
        .prepareStatement("SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? FROM TEST");
    prep.setObject(1, Boolean.TRUE);
    prep.setObject(2, "Abc");
    prep.setObject(3, new BigDecimal("10.2"));
    prep.setObject(4, new Byte((byte) 0xff));
    prep.setObject(5, new Short(Short.MAX_VALUE));
    prep.setObject(6, new Integer(Integer.MIN_VALUE));
    prep.setObject(7, new Long(Long.MAX_VALUE));
    prep.setObject(8, new Float(Float.MAX_VALUE));
    prep.setObject(9, new Double(Double.MAX_VALUE));
    prep.setObject(10, Date.valueOf("2001-02-03"));
    prep.setObject(11, Time.valueOf("04:05:06"));
    prep.setObject(12, Timestamp.valueOf("2001-02-03 04:05:06.123456789"));
    prep.setObject(13, new java.util.Date(Date.valueOf("2001-02-03").getTime()));
    prep.setObject(14, new byte[] { 10, 20, 30 });
    prep.setObject(15, new Character('a'), Types.OTHER);
    prep.setObject(16, "2001-01-02", Types.DATE);
    // converting to null seems strange...
    prep.setObject(17, "2001-01-02", Types.NULL);
    prep.setObject(18, "3.725", Types.DOUBLE);
    prep.setObject(19, "23:22:21", Types.TIME);
    prep.setObject(20, new java.math.BigInteger("12345"), Types.OTHER);
    rs = prep.executeQuery();
    rs.next();
    assertTrue(rs.getObject(1).equals(Boolean.TRUE));
    assertTrue(rs.getObject(2).equals("Abc"));
    assertTrue(rs.getObject(3).equals(new BigDecimal("10.2")));
    assertTrue(rs.getObject(4).equals((byte) 0xff));
    assertTrue(rs.getObject(5).equals(new Short(Short.MAX_VALUE)));
    assertTrue(rs.getObject(6).equals(new Integer(Integer.MIN_VALUE)));
    assertTrue(rs.getObject(7).equals(new Long(Long.MAX_VALUE)));
    assertTrue(rs.getObject(8).equals(new Float(Float.MAX_VALUE)));
    assertTrue(rs.getObject(9).equals(new Double(Double.MAX_VALUE)));
    assertTrue(rs.getObject(10).equals(Date.valueOf("2001-02-03")));
    assertEquals("04:05:06", rs.getObject(11).toString());
    assertTrue(rs.getObject(11).equals(Time.valueOf("04:05:06")));
    assertTrue(rs.getObject(12).equals(
        Timestamp.valueOf("2001-02-03 04:05:06.123456789")));
    assertTrue(rs.getObject(13)
        .equals(Timestamp.valueOf("2001-02-03 00:00:00")));
    assertEquals(new byte[] { 10, 20, 30 }, (byte[]) rs.getObject(14));
    assertTrue(rs.getObject(15).equals('a'));
    assertTrue(rs.getObject(16).equals(Date.valueOf("2001-01-02")));
    assertTrue(rs.getObject(17) == null && rs.wasNull());
    assertTrue(rs.getObject(18).equals(new Double(3.725)));
    assertTrue(rs.getObject(19).equals(Time.valueOf("23:22:21")));
    assertTrue(rs.getObject(20).equals(new java.math.BigInteger("12345")));

    // } else if(x instanceof java.io.Reader) {
    // return session.createLob(Value.CLOB,
    // TypeConverter.getInputStream((java.io.Reader)x), 0);
    // } else if(x instanceof java.io.InputStream) {
    // return session.createLob(Value.BLOB, (java.io.InputStream)x, 0);
    // } else {
    // return ValueBytes.get(TypeConverter.serialize(x));

    stat.execute("DROP TABLE TEST");

  }

  private void checkBigDecimal(ResultSet rs, String[] value)
      throws SQLException {
    for (String v : value) {
      assertTrue(rs.next());
      BigDecimal x = rs.getBigDecimal(1);
      trace("v=" + v + " x=" + x);
      if (v == null) {
        assertTrue(x == null);
      } else {
        assertTrue(x.compareTo(new BigDecimal(v)) == 0);
      }
    }
    assertTrue(!rs.next());
  }

}

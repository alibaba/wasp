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
package com.alibaba.wasp.jdbcx;

import com.alibaba.wasp.jdbc.Driver;
import com.alibaba.wasp.jdbc.JdbcConnection;
import com.alibaba.wasp.jdbc.JdbcException;
import com.alibaba.wasp.util.StringUtils;
import com.alibaba.wasp.util.Utils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;

import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;

/*## Java 1.7 ##
 import java.util.logging.Logger;
 //*/

/**
 * A data source for H2 database connections. It is a factory for XAConnection
 * and Connection objects. This class is usually registered in a JNDI naming
 * service. To create a data source object and register it with a JNDI service,
 * use the following code:
 * 
 * <pre>
 * import com.alibaba.wasp.jdbcx.JdbcDataSource;
 * import javax.naming.Context;
 * import javax.naming.InitialContext;
 * JdbcDataSource ds = new JdbcDataSource();
 * ds.setURL(&quot;jdbc:wasp:&tilde;/test&quot;);
 * ds.setUser(&quot;sa&quot;);
 * ds.setPassword(&quot;sa&quot;);
 * Context ctx = new InitialContext();
 * ctx.bind(&quot;jdbc/dsName&quot;, ds);
 * </pre>
 * 
 * To use a data source that is already registered, use the following code:
 * 
 * <pre>
 * import java.sql.Connection;
 * import javax.sql.DataSource;
 * import javax.naming.Context;
 * import javax.naming.InitialContext;
 * Context ctx = new InitialContext();
 * DataSource ds = (DataSource) ctx.lookup(&quot;jdbc/dsName&quot;);
 * Connection conn = ds.getConnection();
 * </pre>
 * 
 * In this example the user name and password are serialized as well; this may
 * be a security problem in some cases.
 */
public class JdbcDataSource implements XADataSource, DataSource,
    ConnectionPoolDataSource, Serializable, Referenceable {

  private static final long serialVersionUID = 1288136338451857771L;

  private transient JdbcDataSourceFactory factory;
  private transient PrintWriter logWriter;
  private int loginTimeout;
  private String userName = "";
  private char[] passwordChars = {};
  private String url = "";
  private String description;
  private Configuration conf;

  static {
    com.alibaba.wasp.jdbc.Driver.load();
  }

  /**
   * The public constructor.
   */
  public JdbcDataSource(Configuration conf) {
    this.conf = conf;
    initFactory();
  }

  /**
   * Called when de-serializing the object.
   * 
   * @param in
   *          the input stream
   */
  private void readObject(ObjectInputStream in) throws IOException,
      ClassNotFoundException {
    initFactory();
    in.defaultReadObject();
  }

  private void initFactory() {
    factory = new JdbcDataSourceFactory(conf);
  }

  /**
   * Get the login timeout in seconds, 0 meaning no timeout.
   * 
   * @return the timeout in seconds
   */
  public int getLoginTimeout() {
    return loginTimeout;
  }

  /**
   * Set the login timeout in seconds, 0 meaning no timeout. The default value
   * is 0. This value is ignored by this database.
   * 
   * @param timeout
   *          the timeout in seconds
   */
  public void setLoginTimeout(int timeout) {
    this.loginTimeout = timeout;
  }

  /**
   * Get the current log writer for this object.
   * 
   * @return the log writer
   */
  public PrintWriter getLogWriter() {
    return logWriter;
  }

  /**
   * Set the current log writer for this object. This value is ignored by this
   * database.
   * 
   * @param out
   *          the log writer
   */
  public void setLogWriter(PrintWriter out) {
    logWriter = out;
  }

  /**
   * Open a new connection using the current URL, user name and password.
   * 
   * @return the connection
   */
  public Connection getConnection() throws SQLException {
    return getJdbcConnection(userName,
        StringUtils.cloneCharArray(passwordChars));
  }

  /**
   * Open a new connection using the current URL and the specified user name and
   * password.
   * 
   * @param user
   *          the user name
   * @param password
   *          the password
   * @return the connection
   */
  public Connection getConnection(String user, String password)
      throws SQLException {
    return getJdbcConnection(user, convertToCharArray(password));
  }

  private JdbcConnection getJdbcConnection(String user, char[] password)
      throws SQLException {
    Properties info = Utils.convertConfigurationToProperties(conf);
    info.setProperty("user", user);
    info.put("password", password);
    Connection conn = getConnectionInternal(getURL(url, true), info);
    if (conn == null) {
      throw new SQLException("No suitable driver found for " + url, "08001",
          8001);
    } else if (!(conn instanceof JdbcConnection)) {
      throw new SQLException("Connecting with old version is not supported: "
          + url, "08001", 8001);
    }
    return (JdbcConnection) conn;
  }

  private static Connection getConnectionInternal(String url, Properties properties) throws SQLException {
    //com.alibaba.wasp.jdbc.Driver.load();
    Enumeration<java.sql.Driver> drivers =  DriverManager.getDrivers();
    while (drivers.hasMoreElements()) {
      java.sql.Driver driver = (java.sql.Driver) drivers.nextElement();
      if(!(driver instanceof Driver)) {
        DriverManager.deregisterDriver(driver);
      }
    }
    return DriverManager.getConnection(url, properties);
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

  /**
   * Get the current URL.
   * 
   * @return the URL
   */
  public String getURL() {
    return url;
  }

  /**
   * Set the current URL.
   * 
   * @param url
   *          the new URL
   */
  public void setURL(String url) {
    this.url = url;
  }

  /**
   * Set the current password.
   * 
   * @param password
   *          the new password.
   */
  public void setPassword(String password) {
    this.passwordChars = convertToCharArray(password);
  }

  /**
   * Set the current password in the form of a char array.
   * 
   * @param password
   *          the new password in the form of a char array.
   */
  public void setPasswordChars(char[] password) {
    this.passwordChars = password;
  }

  private static char[] convertToCharArray(String s) {
    return s == null ? null : s.toCharArray();
  }

  private static String convertToString(char[] a) {
    return a == null ? null : new String(a);
  }

  /**
   * Get the current password.
   * 
   * @return the password
   */
  public String getPassword() {
    return convertToString(passwordChars);
  }

  /**
   * Get the current user name.
   * 
   * @return the user name
   */
  public String getUser() {
    return userName;
  }

  /**
   * Set the current user name.
   * 
   * @param user
   *          the new user name
   */
  public void setUser(String user) {
    this.userName = user;
  }

  /**
   * Get the current description.
   * 
   * @return the description
   */
  public String getDescription() {
    return description;
  }

  /**
   * Set the description.
   * 
   * @param description
   *          the new description
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Get a new reference for this object, using the current settings.
   * 
   * @return the new reference
   */
  public Reference getReference() {
    String factoryClassName = JdbcDataSourceFactory.class.getName();
    Reference ref = new Reference(getClass().getName(), factoryClassName, null);
    ref.add(new StringRefAddr("url", url));
    ref.add(new StringRefAddr("user", userName));
    ref.add(new StringRefAddr("password", convertToString(passwordChars)));
    ref.add(new StringRefAddr("loginTimeout", String.valueOf(loginTimeout)));
    ref.add(new StringRefAddr("description", description));
    return ref;
  }

  /**
   * Open a new XA connection using the current URL, user name and password.
   * 
   * @return the connection
   */
  public XAConnection getXAConnection() throws SQLException {
    return new JdbcXAConnection(getJdbcConnection(userName,
        StringUtils.cloneCharArray(passwordChars)));
  }

  /**
   * Open a new XA connection using the current URL and the specified user name
   * and password.
   * 
   * @param user
   *          the user name
   * @param password
   *          the password
   * @return the connection
   */
  public XAConnection getXAConnection(String user, String password)
      throws SQLException {
    return new JdbcXAConnection(getJdbcConnection(user,
        convertToCharArray(password)));
  }

  /**
   * Open a new pooled connection using the current URL, user name and password.
   * 
   * @return the connection
   */
  public PooledConnection getPooledConnection() throws SQLException {
    return getXAConnection();
  }

  /**
   * Open a new pooled connection using the current URL and the specified user
   * name and password.
   * 
   * @param user
   *          the user name
   * @param password
   *          the password
   * @return the connection
   */
  public PooledConnection getPooledConnection(String user, String password)
      throws SQLException {
    return getXAConnection(user, password);
  }

  /**
   * [Not supported] Return an object of this class if possible.
   * 
   * @param iface
   *          the class
   */
  // ## Java 1.6 ##
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw JdbcException.getUnsupportedException("isWrapperFor unsupported");
  }

  // */

  /**
   * [Not supported] Checks if unwrap can return an object of this class.
   * 
   * @param iface
   *          the class
   */
  // ## Java 1.6 ##
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw JdbcException.getUnsupportedException("isWrapperFor unsupported");
  }

  // */

  /**
   * [Not supported]
   */
  public Logger getParentLogger() {
    throw new NotImplementedException();
  }

  /**
   * INTERNAL
   */
  public String toString() {
    return "JdbcDataSource : url=" + url + " user=" + userName;
  }

}

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

import com.alibaba.wasp.FConstants;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC<com.alibaba.wasp.jdbc.Driver>.Please use
 * code<Class.forName("com.alibaba.wasp.jdbc.Driver");>
 * 
 */
public class Driver implements java.sql.Driver {

  private static Log log = LogFactory.getLog(Driver.class);

  private static final Driver INSTANCE = new Driver();
  private static final String DEFAULT_URL = "jdbc:default:connection";
  private static final ThreadLocal<Connection> DEFAULT_CONNECTION = new ThreadLocal<Connection>();

  private static volatile boolean registered;

  static {
    load();
  }

  /**
   * Open a database connection. This method should not be called by an
   * application. Instead, the method DriverManager.getConnection should be
   * used.
   * 
   * @param url
   *          the database URL
   * @param info
   *          the connection properties
   * @return the new connection or null if the URL is not supported
   */
  public Connection connect(String url, Properties info) throws SQLException {
    try {
      if (info == null) {
        info = new Properties();
      }
      if (!acceptsURL(url)) {
        return null;
      }
      if (url.equals(DEFAULT_URL)) {
        return DEFAULT_CONNECTION.get();
      }
      return new JdbcConnection(url, info);
    } catch (Exception e) {
      throw JdbcException.toSQLException(e);
    }
  }

  /**
   * Check if the driver understands this URL. This method should not be called
   * by an application.
   * 
   * @param url
   *          the database URL
   * @return if the driver understands the URL
   */
  @Override
  public boolean acceptsURL(String url) {
    if (url != null) {
      if (url.startsWith(FConstants.START_URL)) {
        return true;
      } else if (url.equals(DEFAULT_URL)) {
        return DEFAULT_CONNECTION.get() != null;
      }
    }
    return false;
  }

  /**
   * Get the major version number of the driver. This method should not be
   * called by an application.
   * 
   * @return the major version number
   */
  @Override
  public int getMajorVersion() {
    return FConstants.VERSION_MAJOR;
  }

  /**
   * Get the minor version number of the driver. This method should not be
   * called by an application.
   * 
   * @return the minor version number
   */
  @Override
  public int getMinorVersion() {
    return FConstants.VERSION_MINOR;
  }

  /**
   * Get the list of supported properties. This method should not be called by
   * an application.
   * 
   * @param url
   *          the database URL
   * @param info
   *          the connection properties
   * @return a zero length array
   */
  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    return new DriverPropertyInfo[0];
  }

  /**
   * Check if this driver is compliant to the JDBC specification. This method
   * should not be called by an application.
   * 
   * @return true
   */
  @Override
  public boolean jdbcCompliant() {
    return true;
  }

  /**
   * INTERNAL
   */
  public static synchronized Driver load() {
    try {
      if (!registered) {
        registered = true;
        DriverManager.registerDriver(INSTANCE);
      }
    } catch (SQLException e) {
      if(log.isErrorEnabled()) {
        log.error(e);
      }
    }
    return INSTANCE;
  }

  /**
   * INTERNAL
   */
  public static synchronized void unload() {
    try {
      if (registered) {
        registered = false;
        DriverManager.deregisterDriver(INSTANCE);
      }
    } catch (SQLException e) {
      if(log.isErrorEnabled()) {
        log.error(e);
      }
    }
  }

  /**
   * INTERNAL
   */
  public static void setDefaultConnection(Connection c) {
    DEFAULT_CONNECTION.set(c);
  }

  /**
   * INTERNAL
   */
  public static void setThreadContextClassLoader(Thread thread) {
    // This is very likely to create a memory leak.
    try {
      thread.setContextClassLoader(Driver.class.getClassLoader());
    } catch (Throwable t) {
      // ignore
    }
  }

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
	  throw new NotImplementedException();
  }
}
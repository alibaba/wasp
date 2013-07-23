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
package org.apache.wasp.jdbc;

import org.apache.wasp.SQLErrorCode;
import org.apache.wasp.util.StringUtils;
import org.apache.wasp.util.Utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;

/**
 * Thrown when the execution jdbc
 */
public class JdbcException extends RuntimeException {
  private static final long serialVersionUID = 1L << 11 - 1L;

  private int errorCode;

  private static final Properties MESSAGES = new Properties();

  static {
    try {
      byte[] messages = Utils.getResource("/jdbc/_messages_en.prop");
      if (messages != null) {
        MESSAGES.load(new ByteArrayInputStream(messages));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  /**
   * Constructor
   * 
   * @param s
   *          message
   */
  public JdbcException(String s) {
    super(s);
  }

  private JdbcException(SQLException e) {
    super(e.getMessage(), e);
  }

  public static JdbcException getInvalidValueException(String param,
      Object value) {
    return get(SQLErrorCode.INVALID_VALUE_2,
        value == null ? "null" : value.toString(), param);
  }

  public static JdbcException getUnsupportedException(String message) {
    return get(SQLErrorCode.FEATURE_NOT_SUPPORTED_1, message);
  }

  public static JdbcException get(int errorCode, String p1) {
    return get(errorCode, new String[] { p1 });
  }

  /**
   * Create a database exception for a specific error code.
   * 
   * @param errorCode
   *          the error code
   * @param params
   *          the list of parameters of the message
   * @return the exception
   */
  public static JdbcException get(int errorCode, String... params) {
    return new JdbcException(getJdbcSQLException(errorCode, null, params));
  }

  /**
   * Gets the SQL exception object for a specific error code.
   * 
   * @param errorCode
   *          the error code
   * @param cause
   *          the cause of the exception
   * @param params
   *          the list of parameters of the message
   * @return the SQLException object
   */
  private static JdbcSQLException getJdbcSQLException(int errorCode,
      Throwable cause, String... params) {
    String sqlstate = SQLErrorCode.getState(errorCode);
    String message = translate(sqlstate, params);
    return new JdbcSQLException(message, null, sqlstate, errorCode, cause, null);
  }

  private static String translate(String key, String... params) {
    String message = null;
    if (MESSAGES != null) {
      // Tomcat sets final static fields to null sometimes
      message = MESSAGES.getProperty(key);
    }
    if (message == null) {
      message = "(Message " + key + " not found)";
    }
    if (params != null) {
      for (int i = 0; i < params.length; i++) {
        String s = params[i];
        if (s != null && s.length() > 0) {
          params[i] = StringUtils.quoteIdentifier(s);
        }
      }
      message = MessageFormat.format(message, (Object[]) params);
    }
    return message;
  }

  /**
   * Convert a throwable to an SQL exception using the default mapping. All
   * errors except the following are re-thrown: StackOverflowError,
   * LinkageError.
   * 
   * @param e
   *          the root cause
   * @return the exception object
   */
  public static JdbcException convert(Throwable e) {
    if (e instanceof JdbcException) {
      return (JdbcException) e;
    } else if (e instanceof SQLException) {
      return new JdbcException((SQLException) e);
    } else if (e instanceof InvocationTargetException) {
      return convertInvocation((InvocationTargetException) e, null);
    } else if (e instanceof IOException) {
      return get(SQLErrorCode.IO_EXCEPTION_1, e, e.toString());
    } else if (e instanceof OutOfMemoryError) {
      return get(SQLErrorCode.OUT_OF_MEMORY, e);
    } else if (e instanceof StackOverflowError || e instanceof LinkageError) {
      return get(SQLErrorCode.GENERAL_ERROR_1, e, e.toString());
    } else if (e instanceof Error) {
      throw (Error) e;
    }
    return get(SQLErrorCode.GENERAL_ERROR_1, e, e.toString());
  }

  /**
   * Create a database exception for a specific error code.
   * 
   * @param errorCode
   *          the error code
   * @param cause
   *          the cause of the exception
   * @param params
   *          the list of parameters of the message
   * @return the exception
   */
  public static JdbcException get(int errorCode, Throwable cause,
      String... params) {
    return new JdbcException(getJdbcSQLException(errorCode, cause, params));
  }

  /**
   * Convert an InvocationTarget exception to a database exception.
   * 
   * @param te
   *          the root cause
   * @param message
   *          the added message or null
   * @return the database exception object
   */
  public static JdbcException convertInvocation(InvocationTargetException te,
      String message) {
    Throwable t = te.getTargetException();
    if (t instanceof SQLException || t instanceof JdbcException) {
      return convert(t);
    }
    message = message == null ? t.getMessage() : message + ": "
        + t.getMessage();
    return get(SQLErrorCode.EXCEPTION_IN_FUNCTION_1, t, message);
  }

  /**
   * Throw an internal error. This method seems to return an exception object,
   * so that it can be used instead of 'return', but in fact it always throws
   * the exception.
   * 
   * @param s
   *          the message
   * @return the RuntimeException object
   * @throws RuntimeException
   *           the exception
   */
  public static JdbcException throwInternalError(String s) {
    RuntimeException e = new RuntimeException(s);
    throw e;
  }

  /**
   * Convert an exception to a SQL exception using the default mapping.
   * 
   * @param e
   *          the root cause
   * @return the SQL exception object
   */
  public static SQLException toSQLException(Exception e) {
    if (e instanceof SQLException) {
      return (SQLException) e;
    }
    return convert(e).getSQLException();
  }

  /**
   * Get the SQLException object.
   * 
   * @return the exception
   */
  public SQLException getSQLException() {
    return (SQLException) getCause();
  }

  public int getErrorCode() {
    return errorCode;
  }

  public static JdbcException getSyntaxError(String sql, int index) {
    sql = StringUtils.addAsterisk(sql, index);
    return get(SQLErrorCode.SYNTAX_ERROR_1, sql);
  }

  public static JdbcException getSyntaxError(String sql, int index,
      String expected) {
    sql = StringUtils.addAsterisk(sql, index);
    return get(SQLErrorCode.SYNTAX_ERROR_2, sql, expected);
  }

  /**
   * Create a database exception for a specific error code.
   * 
   * @param errorCode
   *          the error code
   * @return the exception
   */
  public static JdbcException get(int errorCode) {
    return get(errorCode, (String) null);
  }

  /**
   * Convert an IO exception to a database exception.
   * 
   * @param e
   *          the root cause
   * @param message
   *          the message or null
   * @return the database exception object
   */
  public static JdbcException convertIOException(IOException e, String message) {
    if (message == null) {
      Throwable t = e.getCause();
      if (t instanceof JdbcException) {
        return (JdbcException) t;
      }
      return get(SQLErrorCode.IO_EXCEPTION_1, e, e.toString());
    }
    return get(SQLErrorCode.IO_EXCEPTION_2, e, e.toString(), message);
  }

  /**
   * Convert an exception to an IO exception.
   * 
   * @param e
   *          the root cause
   * @return the IO exception
   */
  public static IOException convertToIOException(Throwable e) {
    if (e instanceof IOException) {
      return (IOException) e;
    }
    if (e instanceof JdbcSQLException) {
      JdbcSQLException e2 = (JdbcSQLException) e;
      if (e2.getOriginalCause() != null) {
        e = e2.getOriginalCause();
      }
    }
    IOException io = new IOException(e.toString());
    io.initCause(e);
    return io;
  }

}

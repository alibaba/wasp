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

import org.apache.hadoop.conf.Configuration;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.SQLErrorCode;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.util.New;
import com.alibaba.wasp.util.StringUtils;
import com.alibaba.wasp.util.Utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

/**
 * Encapsulates the connection settings, including user name and password.
 */
public class ConnectionInfo implements Cloneable {

  private static final HashSet<String> KNOWN_SETTINGS = New.hashSet();

  private Properties properties = new Properties();
  private String originalURL;
  private String url;
  private String user;
  private byte[] userPasswordHash;

  /**
   * The database name
   */
  private String name;
  private boolean remote;
  private boolean ssl;
  private boolean persistent;
  private boolean unnamed;

  private Configuration conf = WaspConfiguration.create();

  /**
   * Create a connection info object.
   * 
   * @param name
   *          the database name (including tags), but without the "jdbc:wasp:"
   *          prefix
   */
  public ConnectionInfo(String name) {
    this.name = name;
    this.url = FConstants.START_URL + name;
    parseName();
  }

  /**
   * Create a connection info object.
   * 
   * @param url
   *          the database URL (must start with jdbc:h2:)
   * @param info
   *          the connection properties
   */
  public ConnectionInfo(String url, Properties info) {
    this.originalURL = url;
    if (!url.startsWith(FConstants.START_URL)) {
      throw JdbcException.getInvalidValueException("url", url);
    }
    this.url = url;
    readProperties(info);
    readSettingsFromURL();
    setUserName(removeProperty("USER", ""));
    name = this.url.substring(FConstants.START_URL.length());
    parseName();
  }

  static {
    ArrayList<String> list = SetTypes.getTypes();
    HashSet<String> set = KNOWN_SETTINGS;
    set.addAll(list);
    String[] connectionTime = { "ACCESS_MODE_DATA", "AUTOCOMMIT", "CIPHER",
        "CREATE", "CACHE_TYPE", "FILE_LOCK", "IGNORE_UNKNOWN_SETTINGS",
        "IFEXISTS", "INIT", "PASSWORD", "RECOVER", "RECOVER_TEST", "USER",
        "AUTO_SERVER", "AUTO_SERVER_PORT", "NO_UPGRADE", "AUTO_RECONNECT",
        "OPEN_NEW", "PAGE_SIZE", "PASSWORD_HASH", "JMX" };
    for (String key : connectionTime) {
      if (set.contains(key)) {
        JdbcException.throwInternalError(key);
      }
      set.add(key);
    }
  }

  private static boolean isKnownSetting(String s) {
    return KNOWN_SETTINGS.contains(s);
  }

  public Object clone() throws CloneNotSupportedException {
    ConnectionInfo clone = (ConnectionInfo) super.clone();
    clone.properties = (Properties) properties.clone();
    clone.userPasswordHash = Utils.cloneByteArray(userPasswordHash);
    return clone;
  }

  private void parseName() {
    if (".".equals(name)) {
      name = "mem:";
    }
    if (name.startsWith("tcp:")) {
      remote = true;
      name = name.substring("tcp:".length());
    } else if (name.startsWith("ssl:")) {
      remote = true;
      ssl = true;
      name = name.substring("ssl:".length());
    } else if (name.startsWith("mem:")) {
      persistent = false;
      if ("mem:".equals(name)) {
        unnamed = true;
      }
    } else if (name.startsWith("file:")) {
      name = name.substring("file:".length());
      persistent = true;
    } else {
      persistent = true;
    }
    if (persistent && !remote) {
      name = name.replace('/', '\\');
    }
  }

  /**
   * Check if this is a remote connection.
   * 
   * @return true if it is
   */
  public boolean isRemote() {
    return remote;
  }

  /**
   * Check if the referenced database is persistent.
   * 
   * @return true if it is
   */
  public boolean isPersistent() {
    return persistent;
  }

  /**
   * Check if the referenced database is an unnamed in-memory database.
   * 
   * @return true if it is
   */
  boolean isUnnamedInMemory() {
    return unnamed;
  }

  private void readProperties(Properties info) {
    Object[] list = new Object[info.size()];
    info.keySet().toArray(list);
    Configuration conf = null;
    for (Object k : list) {
      String key = StringUtils.toUpperEnglish(k.toString());
      if (properties.containsKey(key)) {
        throw JdbcException.get(SQLErrorCode.DUPLICATE_PROPERTY_1, key);
      }
      Object value = info.get(k);
      if (isKnownSetting(key)) {
        properties.put(key, value);
      } else {
        if (conf == null) {
          conf = getConf();
        }
        if (conf.get(key) != null) {
          properties.put(key, value);
        }
      }
    }
  }

  private void readSettingsFromURL() {
    Configuration conf = getConf();
    int idx = url.indexOf(';');
    if (idx >= 0) {
      String settings = url.substring(idx + 1);
      url = url.substring(0, idx);
      String[] list = StringUtils.arraySplit(settings, ';', false);
      for (String setting : list) {
        if (setting.length() == 0) {
          continue;
        }
        int equal = setting.indexOf('=');
        if (equal < 0) {
          throw getFormatException();
        }
        String value = setting.substring(equal + 1);
        String key = setting.substring(0, equal);
        key = StringUtils.toUpperEnglish(key);
        if (!isKnownSetting(key) && conf.get(key) == null) {
          throw JdbcException.get(SQLErrorCode.UNSUPPORTED_SETTING_1, key);
        }
        String old = properties.getProperty(key);
        if (old != null && !old.equals(value)) {
          throw JdbcException.get(SQLErrorCode.DUPLICATE_PROPERTY_1, key);
        }
        properties.setProperty(key, value);
      }
    }
  }

  /**
   * Get a boolean property if it is set and return the value.
   * 
   * @param key
   *          the property name
   * @param defaultValue
   *          the default value
   * @return the value
   */
  boolean getProperty(String key, boolean defaultValue) {
    String x = getProperty(key, null);
    if (x == null) {
      return defaultValue;
    }
    // support 0 / 1 (like the parser)
    if (x.length() == 1 && Character.isDigit(x.charAt(0))) {
      return Integer.parseInt(x) != 0;
    }
    return Boolean.parseBoolean(x);
  }

  /**
   * Remove a boolean property if it is set and return the value.
   * 
   * @param key
   *          the property name
   * @param defaultValue
   *          the default value
   * @return the value
   */
  public boolean removeProperty(String key, boolean defaultValue) {
    String x = removeProperty(key, null);
    return x == null ? defaultValue : Boolean.parseBoolean(x);
  }

  /**
   * Remove a String property if it is set and return the value.
   * 
   * @param key
   *          the property name
   * @param defaultValue
   *          the default value
   * @return the value
   */
  private String removeProperty(String key, String defaultValue) {
    if (!isKnownSetting(key)) {
      JdbcException.throwInternalError(key);
    }
    Object x = properties.remove(key);
    return x == null ? defaultValue : x.toString();
  }

  /**
   * Get the name of the user.
   * 
   * @return the user name
   */
  public String getUserName() {
    return user;
  }

  /**
   * Get the user password hash.
   * 
   * @return the password hash
   */
  byte[] getUserPasswordHash() {
    return userPasswordHash;
  }

  /**
   * Get the property keys.
   * 
   * @return the property keys
   */
  String[] getKeys() {
    String[] keys = new String[properties.size()];
    properties.keySet().toArray(keys);
    return keys;
  }

  /**
   * Get the value of the given property.
   * 
   * @param key
   *          the property key
   * @return the value as a String
   */
  String getProperty(String key) {
    Object value = properties.get(key);
    if (value == null || !(value instanceof String)) {
      return null;
    }
    return value.toString();
  }

  /**
   * Get the value of the given property.
   * 
   * @param key
   *          the property key
   * @param defaultValue
   *          the default value
   * @return the value as a String
   */
  int getProperty(String key, int defaultValue) {
    if (!isKnownSetting(key)) {
      JdbcException.throwInternalError(key);
    }
    String s = getProperty(key);
    return s == null ? defaultValue : Integer.parseInt(s);
  }

  /**
   * Get the value of the given property.
   * 
   * @param key
   *          the property key
   * @param defaultValue
   *          the default value
   * @return the value as a String
   */
  public String getProperty(String key, String defaultValue) {
    if (!isKnownSetting(key)) {
      JdbcException.throwInternalError(key);
    }
    String s = getProperty(key);
    return s == null ? defaultValue : s;
  }

  /**
   * Get the value of the given property.
   * 
   * @param setting
   *          the setting id
   * @param defaultValue
   *          the default value
   * @return the value as a String
   */
  String getProperty(int setting, String defaultValue) {
    String key = SetTypes.getTypeName(setting);
    String s = getProperty(key);
    return s == null ? defaultValue : s;
  }

  /**
   * Get the value of the given property.
   * 
   * @param setting
   *          the setting id
   * @param defaultValue
   *          the default value
   * @return the value as an integer
   */
  int getIntProperty(int setting, int defaultValue) {
    String key = SetTypes.getTypeName(setting);
    String s = getProperty(key, null);
    try {
      return s == null ? defaultValue : Integer.decode(s);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Check if this is a remote connection with SSL enabled.
   * 
   * @return true if it is
   */
  boolean isSSL() {
    return ssl;
  }

  /**
   * Overwrite the user name. The user name is case-insensitive and stored in
   * uppercase. English conversion is used.
   * 
   * @param name
   *          the user name
   */
  public void setUserName(String name) {
    this.user = StringUtils.toUpperEnglish(name);
  }

  /**
   * Set the user password hash.
   * 
   * @param hash
   *          the new hash value
   */
  public void setUserPasswordHash(byte[] hash) {
    this.userPasswordHash = hash;
  }

  /**
   * Overwrite a property.
   * 
   * @param key
   *          the property name
   * @param value
   *          the value
   */
  public void setProperty(String key, String value) {
    // value is null if the value is an object
    if (value != null) {
      properties.setProperty(key, value);
    }
  }

  /**
   * Get the database URL.
   * 
   * @return the URL
   */
  public String getURL() {
    return url;
  }

  /**
   * Get the complete original database URL.
   * 
   * @return the database URL
   */
  public String getOriginalURL() {
    return originalURL;
  }

  /**
   * Set the original database URL.
   * 
   * @param url
   *          the database url
   */
  public void setOriginalURL(String url) {
    originalURL = url;
  }

  /**
   * Generate an URL format exception.
   * 
   * @return the exception
   */
  JdbcException getFormatException() {
    String format = FConstants.URL_FORMAT;
    return JdbcException.get(SQLErrorCode.URL_FORMAT_ERROR_2, format, url);
  }

  /**
   * Switch to server mode, and set the server name and database key.
   * 
   * @param serverKey
   *          the server name, '/', and the security key
   */
  public void setServerKey(String serverKey) {
    remote = true;
    persistent = false;
    this.name = serverKey;
  }

  public Configuration getConf() {
    return conf;
  }

  public String getName() {
    return name;
  }
}

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
package com.alibaba.wasp.util;

import org.apache.hadoop.conf.Configuration;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.conf.WaspConfiguration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Utils {

  /**
   * An 0-size byte array.
   */
  public static final byte[] EMPTY_BYTES = {};

  private static final HashMap<String, byte[]> RESOURCES = New.hashMap();

  private Utils() {
    // utility class
  }

  /**
   * Create a new byte array and copy all the data. If the size of the byte
   * array is zero, the same array is returned.
   * 
   * @param b
   *          the byte array (may not be null)
   * @return a new byte array
   */
  public static byte[] cloneByteArray(byte[] b) {
    if (b == null) {
      return null;
    }
    int len = b.length;
    if (len == 0) {
      return EMPTY_BYTES;
    }
    byte[] copy = new byte[len];
    System.arraycopy(b, 0, copy, 0, len);
    return copy;
  }

  /**
   * Get the system property. If the system property is not set, or if a
   * security exception occurs, the default value is returned.
   * 
   * @param key
   *          the key
   * @param defaultValue
   *          the default value
   * @return the value
   */
  public static String getProperty(Configuration conf, String key,
      String defaultValue) {
    try {
      return conf.get(key, defaultValue);
    } catch (SecurityException se) {
      return defaultValue;
    }
  }

  /**
   * Get the system property. If the system property is not set, or if a
   * security exception occurs, the default value is returned.
   * 
   * @param key
   *          the key
   * @param defaultValue
   *          the default value
   * @return the value
   */
  public static int getProperty(Configuration conf, String key, int defaultValue) {
    String s = getProperty(conf, key, null);
    if (s != null) {
      try {
        return Integer.decode(s).intValue();
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return defaultValue;
  }

  /**
   * Get the system property. If the system property is not set, or if a
   * security exception occurs, the default value is returned.
   * 
   * @param key
   *          the key
   * @param defaultValue
   *          the default value
   * @return the value
   */
  public static boolean getProperty(Configuration conf, String key,
      boolean defaultValue) {
    String s = getProperty(conf, key, null);
    if (s != null) {
      try {
        return Boolean.parseBoolean(s);
      } catch (NumberFormatException e) {
        // ignore
      }
    }
    return defaultValue;
  }

  /**
   * Read a long value from the byte array at the given position. The most
   * significant byte is read first.
   * 
   * @param buff
   *          the byte array
   * @param pos
   *          the position
   * @return the value
   */
  public static long readLong(byte[] buff, int pos) {
    return (((long) readInt(buff, pos)) << 32)
        + (readInt(buff, pos + 4) & 0xffffffffL);
  }

  public static int readInt(byte[] buff, int pos) {
    return (buff[pos++] << 24) + ((buff[pos++] & 0xff) << 16)
        + ((buff[pos++] & 0xff) << 8) + (buff[pos] & 0xff);
  }

  public static double readDouble(byte[] b) {
    long l;
    l = b[0];
    l &= 0xff;
    l |= ((long) b[1] << 8);
    l &= 0xffff;
    l |= ((long) b[2] << 16);
    l &= 0xffffff;
    l |= ((long) b[3] << 24);
    l &= 0xffffffffl;
    l |= ((long) b[4] << 32);
    l &= 0xffffffffffl;
    l |= ((long) b[5] << 40);
    l &= 0xffffffffffffl;
    l |= ((long) b[6] << 48);
    l |= ((long) b[7] << 56);
    return Double.longBitsToDouble(l);
  }

  /**
   * Compare the contents of two byte arrays. If the content or length of the
   * first array is smaller than the second array, -1 is returned. If the
   * content or length of the second array is smaller than the first array, 1 is
   * returned. If the contents and lengths are the same, 0 is returned.
   * 
   * @param data1
   *          the first byte array (must not be null)
   * @param data2
   *          the second byte array (must not be null)
   * @return the result of the comparison (-1, 1 or 0)
   */
  public static int compareNotNull(byte[] data1, byte[] data2) {
    if (data1 == data2) {
      return 0;
    }
    int len = Math.min(data1.length, data2.length);
    for (int i = 0; i < len; i++) {
      byte b = data1[i];
      byte b2 = data2[i];
      if (b != b2) {
        return b > b2 ? 1 : -1;
      }
    }
    return Integer.signum(data1.length - data2.length);
  }

  /**
   * Calculate the hash code of the given byte array.
   * 
   * @param value
   *          the byte array
   * @return the hash code
   */
  public static int getByteArrayHash(byte[] value) {
    int len = value.length;
    int h = len;
    if (len < 50) {
      for (int i = 0; i < len; i++) {
        h = 31 * h + value[i];
      }
    } else {
      int step = len / 16;
      for (int i = 0; i < 4; i++) {
        h = 31 * h + value[i];
        h = 31 * h + value[--len];
      }
      for (int i = 4 + step; i < len; i += step) {
        h = 31 * h + value[i];
      }
    }
    return h;
  }

  public static float readFloat(byte[] values) {
    int accum = 0;
    for (int shiftBy = 0; shiftBy < values.length; shiftBy++) {
      accum |= (values[shiftBy] & 0xff) << shiftBy * 8;
    }
    return Float.intBitsToFloat(accum);
  }

  /**
   * Get a resource from the resource map.
   * 
   * @param name
   *          the name of the resource
   * @return the resource data
   */
  public static byte[] getResource(String name) throws IOException {
    byte[] data = RESOURCES.get(name);
    if (data == null) {
      data = loadResource(name);
      RESOURCES.put(name, data);
    }
    return data == null ? EMPTY_BYTES : data;
  }

  private static byte[] loadResource(String name) throws IOException {
    InputStream in = Utils.class.getResourceAsStream("data.zip");
    if (in == null) {
      in = Utils.class.getResourceAsStream(name);
      if (in == null) {
        return null;
      }
      return IOUtils.readBytesAndClose(in, 0);
    }
    ZipInputStream zipIn = new ZipInputStream(in);
    try {
      while (true) {
        ZipEntry entry = zipIn.getNextEntry();
        if (entry == null) {
          break;
        }
        String entryName = entry.getName();
        if (!entryName.startsWith("/")) {
          entryName = "/" + entryName;
        }
        if (entryName.equals(name)) {
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          IOUtils.copy(zipIn, out);
          zipIn.closeEntry();
          return out.toByteArray();
        }
        zipIn.closeEntry();
      }
    } catch (IOException e) {
      // if this happens we have a real problem
      e.printStackTrace();
    } finally {
      zipIn.close();
    }
    return null;
  }

  public static Properties convertConfigurationToProperties(Configuration conf) {
    Properties properties = new Properties();
    for (Map.Entry<String, String> configurationEntry : conf) {
      properties
          .put(configurationEntry.getKey(), configurationEntry.getValue());
    }
    return properties;
  }

  public static Configuration convertPropertiesToConfiguration(
      Properties properties) {
    Configuration conf = WaspConfiguration.create();
    for (Map.Entry entry : properties.entrySet()) {
      if (entry.getKey() instanceof String) {
        Object value = properties.get(entry.getKey());
        if (value instanceof String) {
          conf.set((String) entry.getKey(), (String) value);
        }
      }
    }
    return conf;
  }

  public static ReadModel getReadModel(String readModel) {
    if(StringUtils.isNullOrEmpty(readModel)) {
      return ReadModel.SNAPSHOT;
    }

    for (ReadModel model : ReadModel.values()) {
      if (readModel.equals(model)) {
        return model;
      }
    }
    return ReadModel.SNAPSHOT;
  }
}

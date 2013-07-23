/**
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
package org.apache.wasp.util;

import org.apache.wasp.FConstants;
import org.apache.wasp.jdbc.JdbcException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * I/O utils
 */
public class IOUtils {

  /**
   * Read a number of bytes from an input stream and close the stream.
   * 
   * @param in
   *          the input stream
   * @param length
   *          the maximum number of bytes to read, or -1 to read until the end
   *          of file
   * @return the bytes read
   */
  public static byte[] readBytesAndClose(InputStream in, int length)
      throws IOException {
    try {
      if (length <= 0) {
        length = Integer.MAX_VALUE;
      }
      int block = Math.min(FConstants.IO_BUFFER_SIZE, length);
      ByteArrayOutputStream out = new ByteArrayOutputStream(block);
      copy(in, out, length);
      return out.toByteArray();
    } catch (Exception e) {
      throw JdbcException.convertToIOException(e);
    } finally {
      in.close();
    }
  }

  /**
   * Copy all data from the input stream to the output stream. Both streams are
   * kept open.
   * 
   * @param in
   *          the input stream
   * @param out
   *          the output stream (null if writing is not required)
   * @return the number of bytes copied
   */
  public static long copy(InputStream in, OutputStream out) throws IOException {
    return copy(in, out, Long.MAX_VALUE);
  }

  /**
   * Copy all data from the input stream to the output stream. Both streams are
   * kept open.
   * 
   * @param in
   *          the input stream
   * @param out
   *          the output stream (null if writing is not required)
   * @param length
   *          the maximum number of bytes to copy
   * @return the number of bytes copied
   */
  public static long copy(InputStream in, OutputStream out, long length)
      throws IOException {
    try {
      long copied = 0;
      int len = (int) Math.min(length, FConstants.IO_BUFFER_SIZE);
      byte[] buffer = new byte[len];
      while (length > 0) {
        len = in.read(buffer, 0, len);
        if (len < 0) {
          break;
        }
        if (out != null) {
          out.write(buffer, 0, len);
        }
        copied += len;
        length -= len;
        len = (int) Math.min(length, FConstants.IO_BUFFER_SIZE);
      }
      return copied;
    } catch (Exception e) {
      throw JdbcException.convertToIOException(e);
    }
  }
}

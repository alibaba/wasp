/**
 * Copyright 2007 The Apache Software Foundation
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
package com.alibaba.wasp;

import java.io.IOException;

import org.apache.hadoop.ipc.RemoteException;

/**
 * An immutable class which contains a static method for handling
 * org.apache.hadoop.ipc.RemoteException exceptions.
 */
public class RemoteExceptionHandler {
  /* Not instantiable */
  private RemoteExceptionHandler() {
    super();
  }

  /**
   * Examine passed Throwable. See if its carrying a RemoteException. If so, run
   * {@link #decodeRemoteException(RemoteException)} on it. Otherwise, pass back
   * <code>t</code> unaltered.
   * 
   * @param t
   *          Throwable to examine.
   * @return Decoded RemoteException carried by <code>t</code> or <code>t</code>
   *         unaltered.
   */
  public static Throwable checkThrowable(final Throwable t) {
    Throwable result = t;
    if (t instanceof RemoteException) {
      try {
        result = ((RemoteException) t).unwrapRemoteException();
      } catch (Throwable tt) {
        result = tt;
      }
    }
    return result;
  }

  /**
   * Examine passed IOException. See if its carrying a RemoteException. If so,
   * run {@link #decodeRemoteException(RemoteException)} on it. Otherwise, pass
   * back <code>e</code> unaltered.
   * 
   * @param e
   *          Exception to examine.
   * @return Decoded RemoteException carried by <code>e</code> or <code>e</code>
   *         unaltered.
   */
  public static IOException checkIOException(final IOException e) {
    Throwable t = checkThrowable(e);
    return t instanceof IOException ? (IOException) t : new IOException(t);
  }
}

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
package com.alibaba.wasp.client;

import com.alibaba.wasp.FConstants;

/**
 * Utility used by client connections such as
 * {@link com.alibaba.wasp.client.FConnection} and
 * {@link ServerCallable}
 */
public class ConnectionUtils {
  /**
   * Calculate pause time. Built on {@link com.alibaba.wasp.FConstants#RETRY_BACKOFF}.
   * 
   * @param pause
   * @param tries
   * @return How long to wait after <code>tries</code> retries
   */
  public static long getPauseTime(final long pause, final int tries) {
    int ntries = tries;
    if (ntries >= FConstants.RETRY_BACKOFF.length) {
      ntries = FConstants.RETRY_BACKOFF.length - 1;
    }
    return pause * FConstants.RETRY_BACKOFF[ntries];
  }
}
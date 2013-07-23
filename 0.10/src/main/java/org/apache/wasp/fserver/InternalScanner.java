/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.wasp.fserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.protobuf.generated.ClientProtos.QueryResultProto;

public interface InternalScanner extends Closeable {

  /**
   * @return The EntityGroupInfo for this scanner.
   */
  public EntityGroupInfo getEntityGroupInfo();

  /**
   * Grab the next row's worth of values.
   * 
   * @param results
   *          return output array
   * @return true if more rows exist after this one, false if scanner is done
   * @throws IOException
   *           e
   */
  public boolean next(List<QueryResultProto> results) throws IOException;

  /**
   * Grab the next row's worth of values.
   * 
   * @param results
   *          return output array
   * @param metric
   *          the metric name
   * @return true if more rows exist after this one, false if scanner is done
   * @throws IOException
   *           e
   */
  public boolean next(List<QueryResultProto> results, String metric) throws IOException;


  /**
   * Closes the scanner and releases any resources it has allocated
   * 
   * @throws IOException
   */
  public void close() throws IOException;
}

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
package com.alibaba.wasp.master;

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.ServerName;


/**
 * Protocol that FServer/FClient communicate with the FMETA server(FMaster)
 */
public interface FMetaServerProtocol {
  public static final long VERSION = 1L;

  /**
   * Adds a FMETA row for the specified new entityGroup.
   */
  public void addEntityGroupToMeta(EntityGroupInfo entityGroupInfo);

  /**
   * Updates the location of the specified entityGroup in FMETA to be the
   * specified server hostname and startcode.
   */
  public void updateEntityGroupLocation(ServerName serverName);

  /**
   * Deletes the specified entityGroup from FMETA.
   */
  public void deleteEntityGroup(EntityGroupInfo entityGroupInfo);

  /**
   * Offline parent and two daughters qualifier in FMETA. Used when splitting.
   */
  public void offlineParentInMeta(EntityGroupInfo parentEntityGroup);

  /**
   * Adds two daughters entityGroup with location in FMETA.
   */
  public void addDaughter(EntityGroupInfo daugherA, EntityGroupInfo daugherB);

}

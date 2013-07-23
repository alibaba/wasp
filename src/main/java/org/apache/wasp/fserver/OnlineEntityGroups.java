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
package org.apache.wasp.fserver;

import org.apache.wasp.Server;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Interface to Map of online entityGroups. In the Map, the key is the
 * entityGroup's encoded name and the value is an {@link EntityGroup} instance.
 * It is used by the same JVM.
 */
public interface OnlineEntityGroups extends Server {
  /**
   * Add to online entityGroups.
   * 
   * @param e
   */
  public void addToOnlineEntityGroups(final EntityGroup e);

  /**
   * This method removes EntityGroup corresponding to egi from the Map of
   * onlineEntityGroups.
   * 
   * @param encodedEntityGroupName
   * @return True if we removed a entityGroup from online list.
   */
  public boolean removeFromOnlineEntityGroups(String encodedEntityGroupName);

  /**
   * Return {@link EntityGroup} instance. Only works if caller is in same
   * context. EntityGroup is not serializable.
   * 
   * @param encodedEntityGroupName
   * @return entityGroup for the passed encoded
   *         <code>encodedEntityGroupName</code> or null if named entityGroup is
   *         not member of the online entityGroups.
   */
  public EntityGroup getFromOnlineEntityGroups(String encodedEntityGroupName);

  /**
   * Get all online entityGroups of a table in this FServer.
   * 
   * @param tableName
   * @return List of EntityGroup
   * @throws java.io.IOException
   */
  public List<EntityGroup> getOnlineEntityGroups(byte[] tableName)
      throws IOException;

  /**
   * Get all online entityGroups in this FServer.
   * 
   * @return
   * @throws IOException
   */
  public Collection<EntityGroup> getOnlineEntityGroups() throws IOException;
}
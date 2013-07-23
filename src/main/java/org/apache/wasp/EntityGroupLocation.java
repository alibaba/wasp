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
package org.apache.wasp;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.wasp.protobuf.generated.WaspProtos.EntityGroupLocationProtos;

/**
 * Data structure to hold EntityGroupInfo and the address for the hosting
 * FServer. Immutable. Comparable, but we compare the 'location' only: i.e. the
 * hostname and port, and *not* the entitygroupinfo. This means two instances
 * are the same if they refer to the same 'location' (the same hostname and
 * port), though they may be carrying different entityGroups.
 */

public class EntityGroupLocation implements Comparable<EntityGroupLocation> {
  private EntityGroupInfo entityGroupInfo;
  private final String hostname;
  private final int port;
  // Cache of the 'toString' result.
  private String cachedString = null;
  // Cache of the hostname + port
  private String cachedHostnamePort;

  /**
   * Constructor
   * 
   * @param entityGroupInfo
   *          the EntityGroupInfo for the entityGroup
   * @param hostname
   *          Hostname
   * @param port
   *          port
   */
  public EntityGroupLocation(EntityGroupInfo entityGroupInfo,
      final String hostname, final int port) {
    this.entityGroupInfo = entityGroupInfo;
    this.hostname = hostname;
    this.port = port;
  }

  public EntityGroupLocation(final String hostname, final int port) {
    this.hostname = hostname;
    this.port = port;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public synchronized String toString() {
    if (this.cachedString == null) {
      this.cachedString = "entityGroup="
          + this.entityGroupInfo.getEntityGroupNameAsString() + ", hostname="
          + this.hostname + ", port=" + this.port;
    }
    return this.cachedString;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof EntityGroupLocation)) {
      return false;
    }
    return this.compareTo((EntityGroupLocation) o) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int result = this.hostname.hashCode();
    result ^= this.port;
    return result;
  }

  /** @return EntityGroupInfo */
  public EntityGroupInfo getEntityGroupInfo() {
    return entityGroupInfo;
  }

  public String getHostname() {
    return this.hostname;
  }

  public int getPort() {
    return this.port;
  }

  /**
   * @return String made of hostname and port formatted as per
   *         {@link Addressing#createHostAndPortStr(String, int)}
   */
  public synchronized String getHostnamePort() {
    if (this.cachedHostnamePort == null) {
      this.cachedHostnamePort = Addressing.createHostAndPortStr(this.hostname,
          this.port);
    }
    return this.cachedHostnamePort;
  }

  //
  // Comparable
  //

  public int compareTo(EntityGroupLocation o) {
    int result = this.hostname.compareTo(o.getHostname());
    if (result != 0)
      return result;
    return this.port - o.getPort();
  }

  public EntityGroupLocationProtos convert() {
    EntityGroupLocationProtos.Builder builder = EntityGroupLocationProtos
        .newBuilder();
    builder.setHostname(this.getHostname());
    builder.setPort(this.getPort());
    return builder.build();
  }

  public byte[] toByte() {
    return convert().toByteArray();
  }

  public static EntityGroupLocation convert(byte[] value) {
    if (value == null || value.length <= 0)
      return null;
    try {
      EntityGroupLocationProtos eglp = EntityGroupLocationProtos.newBuilder()
          .mergeFrom(value, 0, value.length).build();
      return convert(eglp);
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      return null;
    }
  }

  public static EntityGroupLocation convert(
      final EntityGroupLocationProtos proto) {
    if (proto == null)
      return null;
    String hostname = proto.getHostname();
    int port = proto.getPort();
    EntityGroupLocation egl = new EntityGroupLocation(hostname, port);
    return egl;
  }

  public static EntityGroupLocation getEntityGroupLocation(final Result r) {
    byte[] locationValue = r.getValue(FConstants.CATALOG_FAMILY,
        FConstants.EGLOCATION);
    return convert(locationValue);
  }
}
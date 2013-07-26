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
package com.alibaba.wasp;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JenkinsHash;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.io.DataInputBuffer;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.generated.WaspProtos.EntityGroupInfoProtos;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * EntityGroup information. Contains EntityGroup id, start and end keys, a
 * reference to this EntityGroup' table descriptor, etc.
 */
public class EntityGroupInfo implements Comparable<EntityGroupInfo> {
  private static final Log LOG = LogFactory.getLog(EntityGroupInfo.class);

  /**
   * Separator used to demarcate the encodedName in a entityGroup name in the
   * new format. See description on new format above.
   */
  private static final int ENC_SEPARATOR = '.';
  public static final int MD5_HEX_LENGTH = 32;

  /**
   * Does entityGroup name contain its encoded name?
   * 
   * @param entityGroupName
   *          entityGroup name
   * @return boolean indicating if this a new format entityGroup name which
   *         contains its encoded name.
   */
  private static boolean hasEncodedName(final byte[] entityGroupName) {
    // check if entityGroup name ends in ENC_SEPARATOR
    if ((entityGroupName.length >= 1)
        && (entityGroupName[entityGroupName.length - 1] == ENC_SEPARATOR)) {
      // entityGroup name is new format. it contains the encoded name.
      return true;
    }
    return false;
  }

  /**
   * @param entityGroupName
   * @return the encodedName
   */
  public static String encodeEntityGroupName(final byte[] entityGroupName) {
    String encodedName;
    if (hasEncodedName(entityGroupName)) {
      // entityGroup is in new format:
      // <tableName>,<startKey>,<entityGroupIdTimeStamp>/encodedName/
      encodedName = Bytes.toString(entityGroupName, entityGroupName.length
          - MD5_HEX_LENGTH - 1, MD5_HEX_LENGTH);
    } else {
      // old format entityGroup name. first META entityGroup also
      // use this format.EncodedName is the JenkinsHash value.
      int hashVal = Math.abs(JenkinsHash.getInstance().hash(entityGroupName,
          entityGroupName.length, 0));
      encodedName = String.valueOf(hashVal);
    }
    return encodedName;
  }

  private boolean offLine = false;
  private byte[] endKey = FConstants.EMPTY_BYTE_ARRAY;

  private long entityGroupId = -1;
  private transient byte[] entityGroupName = FConstants.EMPTY_BYTE_ARRAY;
  private String entityGroupNameStr = "";
  private boolean split = false;
  private byte[] startKey = FConstants.EMPTY_BYTE_ARRAY;
  private int hashCode = -1;
  public static final String NO_HASH = null;
  private volatile String encodedName = NO_HASH;
  private byte[] encodedNameAsBytes = null;

  // Current TableName
  private byte[] tableName = null;

  private void setHashCode() {
    int result = Arrays.hashCode(this.entityGroupName);
    result ^= this.entityGroupId;
    result ^= Arrays.hashCode(this.startKey);
    result ^= Arrays.hashCode(this.endKey);
    result ^= Boolean.valueOf(this.offLine).hashCode();
    result ^= Arrays.hashCode(this.tableName);
    this.hashCode = result;
  }

  /**
   * 
   * @param tableName
   */
  public EntityGroupInfo(String tableName) {
    this(Bytes.toBytes(tableName), null, null);
  }

  /**
   * 
   * @param tableName
   */
  public EntityGroupInfo(final byte[] tableName) {
    this(tableName, null, null);
  }

  /**
   * 
   * @param tableName
   * @param startKey
   * @param endKey
   * @throws IllegalArgumentException
   */
  public EntityGroupInfo(String tableName, final byte[] startKey,
      final byte[] endKey) throws IllegalArgumentException {
    this(Bytes.toBytes(tableName), startKey, endKey, false);
  }

  /**
   * Construct EntityGroupInfo with explicit parameters
   * 
   * @param tableName
   *          the table name
   * @param startKey
   *          first key in entityGroup
   * @param endKey
   *          end of key range
   * @throws IllegalArgumentException
   */
  public EntityGroupInfo(final byte[] tableName, final byte[] startKey,
      final byte[] endKey) throws IllegalArgumentException {
    this(tableName, startKey, endKey, false);
  }

  /**
   * Construct EntityGroupInfo with explicit parameters
   * 
   * @param tableName
   *          the table descriptor
   * @param startKey
   *          first key in entityGroup
   * @param endKey
   *          end of key range
   * @param split
   *          true if this entityGroup has split and we have daughter
   *          entityGroups entityGroups that may or may not hold references to
   *          this entityGroup.
   * @throws IllegalArgumentException
   */
  public EntityGroupInfo(final byte[] tableName, final byte[] startKey,
      final byte[] endKey, final boolean split) throws IllegalArgumentException {
    this(tableName, startKey, endKey, split, System.currentTimeMillis());
  }

  /**
   * Construct EntityGroupInfo with explicit parameters
   * 
   * @param tableName
   *          the table descriptor
   * @param startKey
   *          first key in entityGroup
   * @param endKey
   *          end of key range
   * @param split
   *          true if this entityGroup has split and we have daughter
   *          entityGroups entityGroups that may or may not hold references to
   *          this entityGroup.
   * @param entityGroupId
   *          EntityGroup id to use.
   * @throws IllegalArgumentException
   */
  public EntityGroupInfo(final byte[] tableName, final byte[] startKey,
      final byte[] endKey, final boolean split, final long entityGroupId)
      throws IllegalArgumentException {
    super();
    if (tableName == null) {
      throw new IllegalArgumentException("tableName cannot be null");
    }
    this.tableName = tableName.clone();
    this.offLine = false;
    this.entityGroupId = entityGroupId;

    this.entityGroupName = createEntityGroupName(this.tableName, startKey,
        entityGroupId, true);

    this.entityGroupNameStr = Bytes.toStringBinary(this.entityGroupName);
    this.split = split;
    this.endKey = endKey == null ? FConstants.EMPTY_END_ROW : endKey.clone();
    this.startKey = startKey == null ? FConstants.EMPTY_START_ROW : startKey
        .clone();
    this.tableName = tableName.clone();
    setHashCode();
  }

  /**
   * Construct a copy of another EntityGroupInfo
   * 
   * @param other
   */
  public EntityGroupInfo(EntityGroupInfo other) {
    super();
    this.endKey = other.getEndKey();
    this.offLine = other.isOffline();
    this.entityGroupId = other.getEntityGroupId();
    this.entityGroupName = other.getEntityGroupName();
    this.entityGroupNameStr = Bytes.toStringBinary(this.entityGroupName);
    this.split = other.isSplit();
    this.startKey = other.getStartKey();
    this.hashCode = other.hashCode();
    this.encodedName = other.getEncodedName();
    this.tableName = other.tableName;
  }

  /**
   * Make a entityGroup name of passed parameters.
   * 
   * @param tableName
   * @param startKey
   *          Can be null
   * @param entityGroupId
   *          EntityGroup id (Usually timestamp from when EntityGroup was
   *          created).
   * @param newFormat
   *          should we create the EntityGroup name in the new format (such that
   *          it contains its encoded name?).
   * @return EntityGroup name made of passed tableName, startKey and id
   */
  public static byte[] createEntityGroupName(final byte[] tableName,
      final byte[] startKey, final long entityGroupId, boolean newFormat) {
    return createEntityGroupName(tableName, startKey,
        Long.toString(entityGroupId), newFormat);
  }

  /**
   * Make a entityGroup name of passed parameters.
   * 
   * @param tableName
   * @param startKey
   *          Can be null
   * @param id
   *          EntityGroup id (Usually timestamp from when entityGroup was
   *          created).
   * @param newFormat
   *          should we create the entityGroup name in the new format (such that
   *          it contains its encoded name?).
   * @return EntityGroup name made of passed tableName, startKey and id
   */
  public static byte[] createEntityGroupName(final byte[] tableName,
      final byte[] startKey, final String id, boolean newFormat) {
    return createEntityGroupName(tableName, startKey, Bytes.toBytes(id),
        newFormat);
  }

  /**
   * Make a entityGroup name of passed parameters.
   * 
   * @param tableName
   * @param startKey
   *          Can be null
   * @param id
   *          RntityGroup id (Usually timestamp from when entityGroup was
   *          created).
   * @param newFormat
   *          should we create the entityGroup name in the new format (such that
   *          it contains its encoded name?).
   * @return EntityGroup name made of passed tableName, startKey and id
   */
  public static byte[] createEntityGroupName(final byte[] tableName,
      final byte[] startKey, final byte[] id, boolean newFormat) {
    byte[] b = new byte[tableName.length + 2 + id.length
        + (startKey == null ? 0 : startKey.length)
        + (newFormat ? (MD5_HEX_LENGTH + 2) : 0)];

    int offset = tableName.length;
    System.arraycopy(tableName, 0, b, 0, offset);
    b[offset++] = FConstants.DELIMITER;
    if (startKey != null && startKey.length > 0) {
      System.arraycopy(startKey, 0, b, offset, startKey.length);
      offset += startKey.length;
    }
    b[offset++] = FConstants.DELIMITER;
    System.arraycopy(id, 0, b, offset, id.length);
    offset += id.length;

    if (newFormat) {
      //
      // Encoded name should be built into the entityGroup name.
      //
      // Use the entityGroup name thus far (namely, <tablename>,<startKey>,<id>)
      // to compute a MD5 hash to be used as the encoded name, and append
      // it to the byte buffer.
      //
      String md5Hash = MD5Hash.getMD5AsHex(b, 0, offset);
      byte[] md5HashBytes = Bytes.toBytes(md5Hash);

      if (md5HashBytes.length != MD5_HEX_LENGTH) {
        LOG.error("MD5-hash length mismatch: Expected=" + MD5_HEX_LENGTH
            + "; Got=" + md5HashBytes.length);
      }

      // now append the bytes '.<encodedName>.' to the end
      b[offset++] = ENC_SEPARATOR;
      System.arraycopy(md5HashBytes, 0, b, offset, MD5_HEX_LENGTH);
      offset += MD5_HEX_LENGTH;
      b[offset++] = ENC_SEPARATOR;
    }

    return b;
  }

  /**
   * Gets the table name from the specified entityGroup name.
   * 
   * @param entityGroupName
   * @return Table name.
   */
  public static byte[] getTableName(byte[] entityGroupName) {
    int offset = -1;
    for (int i = 0; i < entityGroupName.length; i++) {
      if (entityGroupName[i] == FConstants.DELIMITER) {
        offset = i;
        break;
      }
    }
    byte[] tableName = new byte[offset];
    System.arraycopy(entityGroupName, 0, tableName, 0, offset);
    return tableName;
  }

  /**
   * Separate elements of a entityGroupName.
   * 
   * @param entityGroupName
   * @return Array of byte[] containing tableName, startKey and id
   * @throws IOException
   */
  public static byte[][] parseEntityGroupName(final byte[] entityGroupName)
      throws IOException {
    int offset = -1;
    for (int i = 0; i < entityGroupName.length; i++) {
      if (entityGroupName[i] == FConstants.DELIMITER) {
        offset = i;
        break;
      }
    }
    if (offset == -1)
      throw new IOException("Invalid entityGroupName format");
    byte[] tableName = new byte[offset];
    System.arraycopy(entityGroupName, 0, tableName, 0, offset);
    offset = -1;
    for (int i = entityGroupName.length - 1; i > 0; i--) {
      if (entityGroupName[i] == FConstants.DELIMITER) {
        offset = i;
        break;
      }
    }
    if (offset == -1)
      throw new IOException("Invalid entityGroupName format");
    byte[] startKey = FConstants.EMPTY_BYTE_ARRAY;
    if (offset != tableName.length + 1) {
      startKey = new byte[offset - tableName.length - 1];
      System.arraycopy(entityGroupName, tableName.length + 1, startKey, 0,
          offset - tableName.length - 1);
    }
    byte[] id = new byte[entityGroupName.length - offset - 1];
    System.arraycopy(entityGroupName, offset + 1, id, 0, entityGroupName.length
        - offset - 1);
    byte[][] elements = new byte[3][];
    elements[0] = tableName;
    elements[1] = startKey;
    elements[2] = id;
    return elements;
  }

  /** @return the entityGroupId */
  public long getEntityGroupId() {
    return entityGroupId;
  }

  /**
   * @return the entityGroupName as an array of bytes.
   * @see #getEntityGroupNameAsString()
   */
  public byte[] getEntityGroupName() {
    return entityGroupName;
  }

  /**
   * @return EntityGroup name as a String for use in logging, etc.
   */
  public String getEntityGroupNameAsString() {
    if (hasEncodedName(this.entityGroupName)) {
      // new format entityGroup names already have their encoded name.
      return this.entityGroupNameStr;
    }

    // old format. entityGroupNameStr doesn't have the entityGroup name.
    //
    //
    return this.entityGroupNameStr + "." + this.getEncodedName();
  }

  /** @return the encoded entityGroup name */
  public synchronized String getEncodedName() {
    if (this.encodedName == NO_HASH) {
      this.encodedName = encodeEntityGroupName(this.entityGroupName);
    }
    return this.encodedName;
  }

  public synchronized byte[] getEncodedNameAsBytes() {
    if (this.encodedNameAsBytes == null) {
      this.encodedNameAsBytes = Bytes.toBytes(getEncodedName());
    }
    return this.encodedNameAsBytes;
  }

  /** @return the startKey */
  public byte[] getStartKey() {
    return startKey;
  }

  /** @return the endKey */
  public byte[] getEndKey() {
    return endKey;
  }

  /**
   * Get current table name of the entityGroup
   * 
   * @return byte array of table name
   */
  public byte[] getTableName() {
    if (tableName == null || tableName.length == 0) {
      tableName = getTableName(getEntityGroupName());
    }
    return tableName;
  }

  /**
   * Get current table name as string
   * 
   * @return string representation of current table
   */
  public String getTableNameAsString() {
    return Bytes.toString(tableName);
  }

  /**
   * Returns true if the given inclusive range of rows is fully contained by
   * this entityGroup. For example, if the entityGroup is foo,a,g and this is
   * passed ["b","c"] or ["a","c"] it will return true, but if this is passed
   * ["b","z"] it will return false.
   * 
   * @throws IllegalArgumentException
   *           if the range passed is invalid (ie end < start)
   */
  public boolean containsRange(byte[] rangeStartKey, byte[] rangeEndKey) {
    if (Bytes.compareTo(rangeStartKey, rangeEndKey) > 0) {
      throw new IllegalArgumentException("Invalid range: "
          + Bytes.toStringBinary(rangeStartKey) + " > "
          + Bytes.toStringBinary(rangeEndKey));
    }

    boolean firstKeyInRange = Bytes.compareTo(rangeStartKey, startKey) >= 0;
    boolean lastKeyInRange = Bytes.compareTo(rangeEndKey, endKey) < 0
        || Bytes.equals(endKey, FConstants.EMPTY_BYTE_ARRAY);
    return firstKeyInRange && lastKeyInRange;
  }

  /**
   * Return true if the given row falls in this entityGroup.
   */
  public boolean containsRow(byte[] row) {
    return Bytes.compareTo(row, startKey) >= 0
        && (Bytes.compareTo(row, endKey) < 0 || Bytes.equals(endKey,
            FConstants.EMPTY_BYTE_ARRAY));
  }

  /**
   * @return True if has been split and has daughters.
   */
  public boolean isSplit() {
    return this.split;
  }

  /**
   * @param split
   *          set split status
   */
  public void setSplit(boolean split) {
    this.split = split;
  }

  /**
   * @return True if this entityGroup is offLine.
   */
  public boolean isOffline() {
    return this.offLine;
  }

  /**
   * The parent of a entityGroup split is offLine while split daughters hold
   * references to the parent. OffLined entityGroups are closed.
   * 
   * @param offLine
   *          Set online/offLine status.
   */
  public void setOffline(boolean offLine) {
    this.offLine = offLine;
  }

  /**
   * @return True if this is a split parent entityGroup.
   */
  public boolean isSplitParent() {
    if (!isSplit())
      return false;
    if (!isOffline()) {
      LOG.warn("EntityGroup is split but NOT offline: "
          + getEntityGroupNameAsString());
    }
    return true;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "{" + FConstants.NAME + " => '" + this.entityGroupNameStr
        + "', STARTKEY => '" + Bytes.toStringBinary(this.startKey)
        + "', ENDKEY => '" + Bytes.toStringBinary(this.endKey)
        + "', ENCODED => " + getEncodedName() + ","
        + (isOffline() ? " OFFLINE => true," : "")
        + (isSplit() ? " SPLIT => true," : "") + "}";
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
    if (!(o instanceof EntityGroupInfo)) {
      return false;
    }
    return this.compareTo((EntityGroupInfo) o) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return this.hashCode;
  }

  //
  // Comparable
  //

  public int compareTo(EntityGroupInfo o) {
    if (o == null) {
      return 1;
    }

    // Are entityGroups of same table?
    int result = Bytes.compareTo(this.tableName, o.tableName);
    if (result != 0) {
      return result;
    }

    // Compare start keys.
    result = Bytes.compareTo(this.startKey, o.startKey);
    if (result != 0) {
      return result;
    }

    // Compare end keys.
    result = Bytes.compareTo(this.endKey, o.endKey);

    if (result != 0) {
      if (this.getStartKey().length != 0 && this.getEndKey().length == 0) {
        return 1; // this is last entityGroup
      }
      if (o.getStartKey().length != 0 && o.getEndKey().length == 0) {
        return -1; // o is the last entityGroup
      }
      return result;
    }

    // entityGroupId is usually milli timestamp -- this defines older stamps
    // to be "smaller" than newer stamps in sort order.
    if (this.entityGroupId > o.entityGroupId) {
      return 1;
    } else if (this.entityGroupId < o.entityGroupId) {
      return -1;
    }

    if (this.offLine == o.offLine)
      return 0;
    if (this.offLine == true)
      return -1;

    return 1;
  }

  /**
   * Convert a EntityGroupInfo to a EntityGroupInfoProtos
   * 
   * @return the converted EntityGroupInfoProtos
   */
  public EntityGroupInfoProtos convert() {
    return convert(this);
  }

  /**
   * Convert a EntityGroupInfo to a EntityGroupInfoProtos
   * 
   * @param info
   *          the EntityGroupInfo to convert
   * @return the converted EntityGroupInfoProtos
   */
  public static EntityGroupInfoProtos convert(final EntityGroupInfo info) {
    if (info == null)
      return null;
    EntityGroupInfoProtos.Builder builder = EntityGroupInfoProtos.newBuilder();
    builder.setTableName(ByteString.copyFrom(info.getTableName()));
    builder.setEntityGroupId(info.getEntityGroupId());
    if (info.getStartKey() != null) {
      builder.setStartKey(ByteString.copyFrom(info.getStartKey()));
    }
    if (info.getEndKey() != null) {
      builder.setEndKey(ByteString.copyFrom(info.getEndKey()));
    }
    builder.setOffline(info.isOffline());
    builder.setSplit(info.isSplit());
    return builder.build();
  }

  /**
   * Convert a EntityGroupInfoProtos to a EntityGroupInfo
   * 
   * @param proto
   *          the EntityGroupInfoProtos to convert
   * @return the converted EntityGroupInfo
   */
  public static EntityGroupInfo convert(final EntityGroupInfoProtos proto) {
    if (proto == null)
      return null;
    byte[] tableName = proto.getTableName().toByteArray();
    long entityGroupId = proto.getEntityGroupId();
    byte[] startKey = null;
    byte[] endKey = null;
    if (proto.hasStartKey()) {
      startKey = proto.getStartKey().toByteArray();
    }
    if (proto.hasEndKey()) {
      endKey = proto.getEndKey().toByteArray();
    }
    boolean split = false;
    if (proto.hasSplit()) {
      split = proto.getSplit();
    }
    EntityGroupInfo egi = new EntityGroupInfo(tableName, startKey, endKey,
        split, entityGroupId);
    if (proto.hasOffline()) {
      egi.setOffline(proto.getOffline());
    }
    return egi;
  }

  /**
   * @return This instance serialized as protobuf w/ a magic pb prefix.
   * @see #parseFrom(byte[]);
   */
  public byte[] toByte() {
    return convert().toByteArray();
  }

  /**
   * @param bytes
   * @return A deserialized {@link EntityGroupInfo} or null if we failed
   *         deserialize or passed bytes null
   * @see {@link #toByteArray()}
   */
  public static EntityGroupInfo parseFromOrNull(final byte[] bytes) {
    if (bytes == null || bytes.length <= 0)
      return null;
    try {
      return parseFrom(bytes);
    } catch (DeserializationException e) {
      return null;
    }
  }

  /**
   * @param bytes
   *          A pb EntityGroupInfo serialized with a pb magic prefix.
   * @return A deserialized {@link EntityGroupInfo}
   * @throws DeserializationException
   * @see {@link #toByteArray()}
   */
  public static EntityGroupInfo parseFrom(final byte[] bytes)
      throws DeserializationException {
    try {
      EntityGroupInfoProtos egi = EntityGroupInfoProtos.newBuilder()
          .mergeFrom(bytes, 0, bytes.length).build();
      return convert(egi);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
  }

  /**
   * Use this instead of {@link #toByteArray()} when writing to a stream and you
   * want to use the pb mergeDelimitedFrom (w/o the delimiter, pb reads to EOF
   * which may not be what you want).
   * 
   * @return This instance serialized as a delimited protobuf w/ a magic pb
   *         prefix.
   * @throws IOException
   * @see {@link #toByteArray()}
   */
  public byte[] toDelimitedByteArray() throws IOException {
    return ProtobufUtil.toDelimitedByteArray(convert());
  }

  /**
   * Extract a EntityGroupInfo and ServerName from catalog table {@link Result}.
   * 
   * @param r
   *          Result to pull from
   * @return A pair of the {@link EntityGroupInfo} and the {@link ServerName}
   *         (or null for server address if no address set in .FMETA.).
   * @throws IOException
   */
  public static Pair<EntityGroupInfo, ServerName> getEntityGroupInfoAndServerName(
      final Result r) {
    EntityGroupInfo info = getEntityGroupInfo(r, FConstants.EGINFO);
    ServerName sn = ServerName.getServerName(r);
    return new Pair<EntityGroupInfo, ServerName>(info, sn);
  }

  /**
   * Returns EntityGroupInfo object from the column
   * {@link FConstants#CATALOG_FAMILY};
   * {@link FConstants#ENTITYGROUPINFO_QUALIFIER} of the catalog table Result.
   * 
   * @param data
   *          a Result object from the catalog table scan
   * @return EntityGroupInfo or null
   */
  public static EntityGroupInfo getEntityGroupInfo(Result data) {
    byte[] bytes = data.getValue(FConstants.CATALOG_FAMILY, FConstants.EGINFO);
    if (bytes == null)
      return null;
    EntityGroupInfo info = parseFromOrNull(bytes);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Current INFO from scan results = " + info);
    }
    return info;
  }

  /**
   * Returns the daughter entityGroups by reading the corresponding columns of
   * the catalog table Result.
   * 
   * @param data
   *          a Result object from the catalog table scan
   * @return a pair of EntityGroupInfo or PairOfSameType(null, null) if the
   *         entityGroup is not a split parent
   */
  public static PairOfSameType<EntityGroupInfo> getDaughterEntityGroups(
      Result data) throws IOException {
    EntityGroupInfo splitA = getEntityGroupInfo(data,
        FConstants.SPLITA_QUALIFIER);
    EntityGroupInfo splitB = getEntityGroupInfo(data,
        FConstants.SPLITB_QUALIFIER);

    return new PairOfSameType<EntityGroupInfo>(splitA, splitB);
  }

  /**
   * Returns the EntityGroupInfo object from the column
   * {@link FConstants#CATALOG_FAMILY} and <code>qualifier</code> of the catalog
   * table result.
   * 
   * @param r
   *          a Result object from the catalog table scan
   * @param qualifier
   *          Column family qualifier -- either
   *          {@link FConstants#SPLITA_QUALIFIER},
   *          {@link FConstants#SPLITB_QUALIFIER} or
   *          {@link FConstants#ENTITYGROUPINFO_QUALIFIER}.
   * @return An EntityGroupInfo instance or null.
   * @throws IOException
   */
  public static EntityGroupInfo getEntityGroupInfo(final Result r,
      byte[] qualifier) {
    byte[] bytes = r.getValue(FConstants.CATALOG_FAMILY, qualifier);
    if (bytes == null || bytes.length <= 0)
      return null;
    return parseFromOrNull(bytes);
  }

  /**
   * Returns a {@link ServerName} from catalog table {@link Result}.
   * 
   * @param r
   *          Result to pull from
   * @return A ServerName instance or null if necessary fields not found or
   *         empty.
   */
  public static ServerName getServerName(final Result r) {
    return ServerName.getServerName(r);
  }

  /**
   * Parses an EntityGroupInfo instance from the passed in stream. Presumes the
   * EntityGroupInfo was serialized to the stream with
   * {@link #toDelimitedByteArray()}
   * 
   * @param in
   * @return An instance of EntityGroupInfo.
   * @throws IOException
   */
  public static EntityGroupInfo parseFrom(final DataInputStream in)
      throws IOException {
    return convert(EntityGroupInfoProtos.parseDelimitedFrom(in));
  }

  /**
   * Serializes given EntityGroupInfo's as a byte array. Use this instead of
   * {@link #toByteArray()} when writing to a stream and you want to use the pb
   * mergeDelimitedFrom (w/o the delimiter, pb reads to EOF which may not be
   * what you want). {@link #parseDelimitedFrom(byte[], int, int)} can be used
   * to read back the instances.
   * 
   * @param infos
   *          EntityGroupInfo objects to serialize
   * @return This instance serialized as a delimited protobuf w/ a magic pb
   *         prefix.
   * @throws IOException
   * @see {@link #toByteArray()}
   */
  public static byte[] toDelimitedByteArray(EntityGroupInfo... infos)
      throws IOException {
    byte[][] bytes = new byte[infos.length][];
    int size = 0;
    for (int i = 0; i < infos.length; i++) {
      bytes[i] = infos[i].toDelimitedByteArray();
      size += bytes[i].length;
    }

    byte[] result = new byte[size];
    int offset = 0;
    for (byte[] b : bytes) {
      System.arraycopy(b, 0, result, offset, b.length);
      offset += b.length;
    }
    return result;
  }

  /**
   * Parses all the EntityGroupInfo instances from the passed in stream until
   * EOF. Presumes the EntityGroupInfo's were serialized to the stream with
   * {@link #toDelimitedByteArray()}
   * 
   * @param bytes
   *          serialized bytes
   * @param offset
   *          the start offset into the byte[] buffer
   * @param length
   *          how far we should read into the byte[] buffer
   * @return All the entityGroupInfos that are in the byte array. Keeps reading
   *         till we hit the end.
   */
  public static List<EntityGroupInfo> parseDelimitedFrom(final byte[] bytes,
      final int offset, final int length) throws IOException {
    if (bytes == null) {
      throw new IllegalArgumentException(
          "Can't build an object with empty bytes array");
    }
    DataInputBuffer in = new DataInputBuffer();
    List<EntityGroupInfo> egis = new ArrayList<EntityGroupInfo>();
    try {
      in.reset(bytes, offset, length);
      while (in.available() > 0) {
        EntityGroupInfo egi = parseFrom(in);
        egis.add(egi);
      }
    } finally {
      in.close();
    }
    return egis;
  }
}
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
package com.alibaba.wasp.client;

import com.alibaba.wasp.*;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.fserver.AdminProtocol;
import com.alibaba.wasp.master.FMasterAdminProtocol;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Field;
import com.alibaba.wasp.meta.Index;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.RequestConverter;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEncodedEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEncodedEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.StopServerRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetClusterStatusRequest;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusRequest;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusResponse;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.KeeperException;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

public class WaspAdmin implements Abortable, Closeable {
  private static final Log LOG = LogFactory.getLog(WaspAdmin.class);

  // We use the implementation class rather then the interface because we
  // need the package protected functions to get the connection to master
  private FConnection connection;

  private volatile Configuration conf;
  private final long pause;
  private final int numRetries;
  // Some operations can take a long time such as disable of big table.
  // numRetries is for 'normal' stuff... Multiply by this factor when
  // want to wait a long time.
  private final int retryLongerMultiplier;
  private boolean aborted;

  /**
   * Constructor. See {@link #WaspAdmin(FConnection connection)}
   *
   * @param c
   *          Configuration object. Copied internally.
   */
  public WaspAdmin(Configuration c) throws MasterNotRunningException,
      ZooKeeperConnectionException {
    // Will not leak connections, as the new implementation of the constructor
    // does not throw exceptions anymore.
    this(FConnectionManager.getConnection(new Configuration(c)));
  }

  /**
   * Constructor for externally managed FConnections. The connection to master
   * will be created when required by admin functions.
   *
   * @param connection
   *          The FConnection instance to use
   * @throws com.alibaba.wasp.MasterNotRunningException
   *           , ZooKeeperConnectionException are not thrown anymore but kept
   *           into the interface for backward api compatibility
   */
  public WaspAdmin(FConnection connection) throws MasterNotRunningException,
      ZooKeeperConnectionException {
    this.conf = connection.getConfiguration();
    this.connection = connection;
    this.pause = this.conf.getLong("wasp.client.pause", 1000);
    this.numRetries = this.conf.getInt("wasp.client.retries.number", 10);
    this.retryLongerMultiplier = this.conf.getInt(
        "wasp.client.retries.longer.multiplier", 10);
  }

  @Override
  public void abort(String why, Throwable e) {
    // Currently does nothing but throw the passed message and exception
    this.aborted = true;
    throw new RuntimeException(why, e);
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }

  /** @return FConnection used by this object. */
  public FConnection getConnection() {
    return connection;
  }

  /**
   * @return - true if the master server is running. Throws an exception
   *         otherwise.
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   * @throws com.alibaba.wasp.MasterNotRunningException
   */
  public boolean isMasterRunning() throws MasterNotRunningException,
      ZooKeeperConnectionException, MasterNotRunningException {
    return connection.isMasterRunning();
  }

  /**
   * @param tableName
   *          Table to check.
   * @return True if table exists already.
   * @throws java.io.IOException
   */
  public boolean tableExists(final String tableName) throws IOException {
    return tableExists(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName
   *          Table to check.
   * @return True if table exists already.
   * @throws java.io.IOException
   */
  public boolean tableExists(final byte[] tableName) throws IOException {
    FTable ftable = getTableDescriptor(tableName);
    return ftable == null ? false : true;
  }

  /**
   * List all the userspace tables. In other words, scan the FMETA table.
   *
   * @return - returns an array of Table
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public FTable[] listTables() throws IOException {
    return this.connection.listTables();
  }

  /**
   * List all the userspace tables. In other words, scan the FMETA table.
   *
   * @return - returns an array of Table
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public Index[] listIndexes(final String tableName) throws IOException {
    FTable ftable = getTableDescriptor(Bytes.toBytes(tableName));
    LinkedHashMap<String, Index> indexMap = ftable.getIndex();
    Collection<Index> indexes = indexMap.values();
    return indexes.toArray(new Index[0]);
  }

  public String describeIndex(String tableName, String indexName)
      throws IOException {
    FTable table = getTableDescriptor(Bytes.toBytes(tableName));
    LinkedHashMap<String, Index> indexMap = table.getIndex();
    Index index = indexMap.get(indexName);
    if (index == null) {
      return "";
    }

    StringBuilder builder = new StringBuilder();
    builder.append("+----------------------+----------+-------+\n");
    builder.append("|              INDEX_KEYS                 |\n");
    builder.append("+----------------------+----------+-------+\n");
    builder.append("| Field                | Type     | ORDER |\n");
    builder.append("+----------------------+----------+-------+\n");
    String line = "| {0} | {1} | {2} |";
    LinkedHashMap<String, Field> indexKeys = index.getIndexKeys();
    Map<String, Field> storings = index.getStoring();
    Set<String> desc = index.getDesc();

    for (Field field : indexKeys.values()) {
      String fieldname = field.getName();
      String s0 = fieldname
          + (fieldname.length() < 20 ? getGivenBlanks(20 - fieldname.length())
              : "");
      String type = field.getType().toString();
      String s1 = type
          + (type.length() < 8 ? getGivenBlanks(8 - type.length()) : "");
      String s2 = desc.contains(fieldname) ? "desc " : "asc  ";
      builder.append(MessageFormat.format(line, s0, s1, s2));
      builder.append("\n");
    }
    builder.append("+----------------------+----------+-------+\n");
    builder.append("|               STORINGS                  |\n");
    builder.append("+----------------------+----------+-------+\n");
    builder.append("| Field                | Type     | ORDER |\n");
    builder.append("+----------------------+----------+-------+\n");
    for (Field field : storings.values()) {
      String fieldname = field.getName();
      String s0 = fieldname
          + (fieldname.length() < 15 ? getGivenBlanks(15 - fieldname.length())
              : "");
      String type = field.getType().toString();
      String s1 = type
          + (type.length() < 8 ? getGivenBlanks(8 - type.length()) : "");
      String s2 = desc.contains(fieldname) ? "desc " : "asc  ";
      builder.append(MessageFormat.format(line, s0, s1, s2));
      builder.append("\n");
    }
    builder.append("+----------------------+----------+-------+\n");
    return builder.toString();
  }

  public String describeTable(String tableName) throws IOException {
    FTable table = getTableDescriptor(Bytes.toBytes(tableName));
    String parentTableName = table.getParentName() == null ? "ROOT" : table.getParentName();
    StringBuilder builder = new StringBuilder();
    builder.append("+-------------------------------------------------------------+\n");
    builder.append("|                       Parent Table                          |\n");
    builder.append("+-------------------------------------------------------------+\n");
    builder.append("| ").append(parentTableName).append(getGivenBlanks(60 - parentTableName.length())).append("|\n");
    builder.append("+---------------------------+----------+----------+-----+-----+\n");
    builder.append("| Field                     | Type     | REQUIRED | Key | EGK |\n");
    builder.append("+---------------------------+----------+----------+-----+-----+\n");
    String line = "| {0} | {1} | {2} | {3} | {4} |";
    LinkedHashMap<String, Field> priKeys = table.getPrimaryKeys();
    Field egKey = table.getEntityGroupKey();
    for (Field field : table.getColumns().values()) {
      String fieldname = field.getName();
      String s0 = fieldname
          + (fieldname.length() < 25 ? getGivenBlanks(25 - fieldname.length())
              : "");
      String type = field.getType().toString();
      String s1 = type
          + (type.length() < 8 ? getGivenBlanks(8 - type.length()) : "");
      String nullAble = field.getKeyWord().toString();
      String s2 = nullAble
          + (nullAble.length() < 8 ? getGivenBlanks(8 - nullAble.length()) : "");
      String s3 = priKeys.get(fieldname) != null ? "PRI" : "   ";
      String s4 = fieldname.equals(egKey.getName()) ? "EGK" : "   ";
      builder.append(MessageFormat.format(line, s0, s1, s2, s3, s4));
      builder.append("\n");
    }
    builder.append("+---------------------------+----------+----------+-----+-----+\n");
    return builder.toString();
  }

  private String getGivenBlanks(int size) {
    StringBuilder builder = new StringBuilder(size);

    for (int i = 0; i < size; i++) {
      builder.append(" ");
    }

    return builder.toString();
  }

  /**
   * List all the userspace tables matching the given pattern.
   *
   * @param pattern
   *          The compiled regular expression to match against
   * @return - returns an array of Table
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   * @see #listTables()
   */
  public FTable[] listTables(Pattern pattern) throws IOException {
    List<FTable> matched = new LinkedList<FTable>();
    FTable[] tables = listTables();
    for (FTable table : tables) {
      if (pattern.matcher(table.getTableName()).matches()) {
        matched.add(table);
      }
    }
    return matched.toArray(new FTable[matched.size()]);
  }

  /**
   * List all the userspace tables matching the given regular expression.
   *
   * @param regex
   *          The regular expression to match against
   * @return - returns an array of Table
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   * @see #listTables(java.util.regex.Pattern)
   */
  public FTable[] listTables(String regex) throws IOException {
    return listTables(Pattern.compile(regex));
  }

  /**
   * Method for getting the tableDescriptor
   *
   * @param tableName
   *          as a byte []
   * @return the tableDescriptor
   * @throws com.alibaba.wasp.TableNotFoundException
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public FTable getTableDescriptor(final byte[] tableName) throws IOException {
    return this.connection.getFTableDescriptor(tableName);
  }

  private long getPauseTime(int tries) {
    int triesCount = tries;
    if (triesCount >= FConstants.RETRY_BACKOFF.length) {
      triesCount = FConstants.RETRY_BACKOFF.length - 1;
    }
    return this.pause * FConstants.RETRY_BACKOFF[triesCount];
  }

  /**
   * Creates a new table. Synchronous operation.
   *
   * @param desc
   *          table descriptor for table
   *
   * @throws IllegalArgumentException
   *           if the table name is reserved
   * @throws com.alibaba.wasp.MasterNotRunningException
   *           if master is not running
   * @throws com.alibaba.wasp.TableExistsException
   *           if table already exists (If concurrent threads, the table may
   *           have been created between test-for-existence and
   *           attempt-at-creation).
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void createTable(FTable desc) throws IOException {
    createTable(desc, null);
  }

  /**
   * Creates a new table with the specified number of entityGroups. The start
   * key specified will become the end key of the first entityGroup of the
   * table, and the end key specified will become the start key of the last
   * entityGroup of the table (the first entityGroup has a null start key and
   * the last entityGroup has a null end key).
   *
   * BigInteger math will be used to divide the key range specified into enough
   * segments to make the required number of total entityGroups.
   *
   * Synchronous operation.
   *
   * @param desc
   *          table descriptor for table
   * @param startKey
   *          beginning of key range
   * @param endKey
   *          end of key range
   * @param numEntityGroups
   *          the total number of entityGroups to create
   *
   * @throws IllegalArgumentException
   *           if the table name is reserved
   * @throws com.alibaba.wasp.MasterNotRunningException
   *           if master is not running
   * @throws com.alibaba.wasp.TableExistsException
   *           if table already exists (If concurrent threads, the table may
   *           have been created between test-for-existence and
   *           attempt-at-creation).
   * @throws java.io.IOException
   */
  public void createTable(FTable desc, byte[] startKey, byte[] endKey,
      int numEntityGroups) throws IOException {
    FTable.isLegalTableName(desc.getTableName());
    if (numEntityGroups < 3) {
      throw new IllegalArgumentException(
          "Must create at least three entityGroups");
    } else if (Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException(
          "Start key must be smaller than end key");
    }
    byte[][] splitKeys = Bytes.split(startKey, endKey, numEntityGroups - 3);
    if (splitKeys == null || splitKeys.length != numEntityGroups - 1) {
      throw new IllegalArgumentException(
          "Unable to split key range into enough entityGroups");
    }
    createTable(desc, splitKeys);
  }

  /**
   * Creates a new table with an initial set of empty entityGroups defined by
   * the specified split keys. The total number of entityGroups created will be
   * the number of split keys plus one. Synchronous operation. Note : Avoid
   * passing empty split key.
   *
   * @param desc
   *          table descriptor for table
   * @param splitKeys
   *          array of split keys for the initial entityGroups of the table
   *
   * @throws IllegalArgumentException
   *           if the table name is reserved, if the split keys are repeated and
   *           if the split key has empty byte array.
   * @throws com.alibaba.wasp.MasterNotRunningException
   *           if master is not running
   * @throws com.alibaba.wasp.TableExistsException
   *           if table already exists (If concurrent threads, the table may
   *           have been created between test-for-existence and
   *           attempt-at-creation).
   * @throws java.io.IOException
   */
  public void createTable(final FTable desc, byte[][] splitKeys)
      throws IOException {
    FTable.isLegalTableName(desc.getTableName());
    try {
      createTableAsync(desc, splitKeys);
    } catch (SocketTimeoutException ste) {
      LOG.warn("Creating " + desc.getTableName() + " took too long", ste);
    }
    final byte[] tableNameBytes = Bytes.toBytes(desc.getTableName());
    int numRegs = splitKeys == null ? 1 : splitKeys.length + 1;
    int prevRegCount = 0;
    boolean doneWithMetaScan = false;
    for (int tries = 0; tries < this.numRetries * this.retryLongerMultiplier; ++tries) {
      if (!doneWithMetaScan) {
        // Wait for new table to come on-line
        MasterAdminProtos.FetchEntityGroupSizeResponse res = execute(new MasterAdminCallable<MasterAdminProtos.FetchEntityGroupSizeResponse>() {
          @Override
          public MasterAdminProtos.FetchEntityGroupSizeResponse call() throws ServiceException {
            MasterAdminProtos.FetchEntityGroupSizeRequest request = RequestConverter
                .buildFetchEntityGroupSizeRequest(tableNameBytes);
            return masterAdmin.fetchEntityGroupSize(null, request);
          }
        });
        int actualEgCount = res.getEgSize();
        if (actualEgCount != numRegs && desc.isRootTable()) {
          if (tries == this.numRetries * this.retryLongerMultiplier - 1) {
            throw new EntityGroupOfflineException("Only " + actualEgCount
                + " of " + numRegs
                + " entityGroups are online; retries exhausted.");
          }
          try { // Sleep
            Thread.sleep(getPauseTime(tries));
          } catch (InterruptedException e) {
            throw new InterruptedIOException("Interrupted when opening"
                + " entityGroups; " + actualEgCount + " of " + numRegs
                + " entityGroups processed so far");
          }
          if (actualEgCount > prevRegCount) { // Making progress
            prevRegCount = actualEgCount;
            tries = -1;
          }
        } else {
          doneWithMetaScan = true;
          tries = -1;
        }
      } else if (isTableEnabled(desc.getTableName())) {
        return;
      } else {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException("Interrupted when waiting"
              + " for table to be enabled; meta scan was done");
        }
      }
    }
    throw new TableNotEnabledException(
        "Retries exhausted while still waiting for table: "
            + desc.getTableName() + " to be enabled");
  }

  /**
   * Creates a new table but does not block and wait for it to come online.
   * Asynchronous operation. To check if the table exists, use {@link:
   * #isTableAvailable()} -- it is not safe to create an HTable instance to this
   * table before it is available. Note : Avoid passing empty split key.
   *
   * @param desc
   *          table descriptor for table
   *
   * @throws IllegalArgumentException
   *           Bad table name, if the split keys are repeated and if the split
   *           key has empty byte array.
   * @throws com.alibaba.wasp.MasterNotRunningException
   *           if master is not running
   * @throws com.alibaba.wasp.TableExistsException
   *           if table already exists (If concurrent threads, the table may
   *           have been created between test-for-existence and
   *           attempt-at-creation).
   * @throws java.io.IOException
   */
  public void createTableAsync(final FTable desc, final byte[][] splitKeys)
      throws IOException {
    FTable.isLegalTableName(desc.getTableName());
    if (splitKeys != null && splitKeys.length > 0) {
      Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
      // Verify there are no duplicate split keys
      byte[] lastKey = null;
      for (byte[] splitKey : splitKeys) {
        if (Bytes.compareTo(splitKey, FConstants.EMPTY_BYTE_ARRAY) == 0) {
          throw new IllegalArgumentException(
              "Empty split key must not be passed in the split keys.");
        }
        if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
          throw new IllegalArgumentException("All split keys must be unique, "
              + "found duplicate: " + Bytes.toStringBinary(splitKey) + ", "
              + Bytes.toStringBinary(lastKey));
        }
        lastKey = splitKey;
      }
    }

    execute(new MasterAdminCallable<MasterAdminProtos.CreateTableResponse>() {
      @Override
      public MasterAdminProtos.CreateTableResponse call() throws ServiceException {
        MasterAdminProtos.CreateTableRequest request = RequestConverter.buildCreateTableRequest(
            desc, splitKeys);
        return masterAdmin.createTable(null, request);
      }
    });
  }

  /**
   * Create a index for given table.
   *
   * @param index
   *          index descriptor for index
   * @throws java.io.IOException
   */
  public void createIndex(final Index index) throws IOException {
    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        MasterAdminProtos.CreateIndexRequest request = RequestConverter
            .buildCreateIndexRequest(index);
        masterAdmin.createIndex(null, request);
        return null;
      }
    });
  }

  /**
   * Drop a index for given indexName.
   *
   * @param indexName
   *          index descriptor for index
   * @throws java.io.IOException
   */
  public void deleteIndex(final String tableName, final String indexName)
      throws IOException {
    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        MasterAdminProtos.DropIndexRequest request = RequestConverter.buildDropIndexRequest(
            tableName, indexName);
        masterAdmin.deleteIndex(null, request);
        return null;
      }
    });
  }

  /**
   * Deletes a table. Synchronous operation.
   *
   * @param tableName
   *          name of table to delete
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void deleteTable(final String tableName) throws IOException {
    deleteTable(Bytes.toBytes(tableName));
  }

  /**
   * Deletes a table. Synchronous operation.
   *
   * @param tableName
   *          name of table to delete
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void deleteTable(final byte[] tableName) throws IOException {
    FTable.isLegalTableName(Bytes.toString(tableName));
    boolean tableExists = true;

    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        MasterAdminProtos.DeleteTableRequest req = RequestConverter
            .buildDeleteTableRequest(tableName);
        masterAdmin.deleteTable(null, req);
        return null;
      }
    });

    // Wait until all entityGroups deleted
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      try {
        FTable fTable = this.getTableDescriptor(tableName);
        // let us wait until .FMETA. table is updated and
        tableExists = fTable != null;
        if (!tableExists) {
          break;
        }
      } catch (IOException ex) {
        if (tries == numRetries - 1) { // no more tries left
          if (ex instanceof RemoteException) {
            throw ((RemoteException) ex).unwrapRemoteException();
          }
          throw ex;
        }
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    if (tableExists) {
      throw new IOException("Retries exhausted, it took too long to wait"
          + " for the table " + Bytes.toString(tableName) + " to be deleted.");
    }
    // Delete cached information to prevent clients from using old locations
    this.connection.clearEntityGroupCache(tableName);
    LOG.info("Deleted " + Bytes.toString(tableName));
  }

  /**
   * Deletes tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(String)} and
   * {@link #deleteTable(byte[])}
   *
   * @param regex
   *          The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws java.io.IOException
   * @see #deleteTables(java.util.regex.Pattern)
   * @see #deleteTable(String)
   */
  public FTable[] deleteTables(String regex) throws IOException {
    return deleteTables(Pattern.compile(regex));
  }

  /**
   * Delete tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.util.regex.Pattern) } and
   * {@link #deleteTable(byte[])}
   *
   * @param pattern
   *          The pattern to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws java.io.IOException
   */
  public FTable[] deleteTables(Pattern pattern) throws IOException {
    List<FTable> failed = new LinkedList<FTable>();
    for (FTable table : listTables(pattern)) {
      try {
        deleteTable(table.getTableName());
      } catch (IOException ex) {
        LOG.info("Failed to delete table " + table.getTableName(), ex);
        failed.add(table);
      }
    }
    return failed.toArray(new FTable[failed.size()]);
  }

  public void enableTable(final String tableName) throws IOException {
    enableTable(Bytes.toBytes(tableName));
  }

  /**
   * Enable a table. May timeout. Use {@link #enableTableAsync(byte[])} and
   * {@link #isTableEnabled(byte[])} instead. The table has to be in disabled
   * state for it to be enabled.
   *
   * @param tableName
   *          name of the table
   * @throws java.io.IOException
   *           if a remote or network exception occurs There could be couple
   *           types of IOException TableNotFoundException means the table
   *           doesn't exist. TableNotDisabledException means the table isn't in
   *           disabled state.
   * @see #isTableEnabled(byte[])
   * @see #disableTable(byte[])
   * @see #enableTableAsync(byte[])
   */
  public void enableTable(final byte[] tableName) throws IOException {
    enableTableAsync(tableName);

    // Wait until all entityGroups are enabled
    boolean enabled = false;
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      enabled = isTableEnabled(tableName);
      if (enabled) {
        break;
      }
      long sleep = getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleeping= " + sleep
            + "ms, waiting for all entityGroups to be " + "enabled in "
            + Bytes.toString(tableName));
      }
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Do this conversion rather than let it out because do not want to
        // change the method signature.
        throw new IOException("Interrupted", e);
      }
    }
    if (!enabled) {
      throw new IOException("Unable to enable table "
          + Bytes.toString(tableName));
    }
    LOG.info("Enabled table " + Bytes.toString(tableName));
  }

  public void enableTableAsync(final String tableName) throws IOException {
    enableTableAsync(Bytes.toBytes(tableName));
  }

  /**
   * Brings a table on-line (enables it). Method returns immediately though
   * enable of table may take some time to complete, especially if the table is
   * large (All entityGroups are opened as part of enabling process). Check
   * {@link #isTableEnabled(byte[])} to learn when table is fully online. If
   * table is taking too long to online, check server logs.
   *
   * @param tableName
   * @throws java.io.IOException
   * @since 0.90.0
   */
  public void enableTableAsync(final byte[] tableName) throws IOException {
    FTable.isLegalTableName(Bytes.toString(tableName));
    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        LOG.info("Started enable of " + Bytes.toString(tableName));
        MasterAdminProtos.EnableTableRequest req = RequestConverter
            .buildEnableTableRequest(tableName);
        masterAdmin.enableTable(null, req);
        return null;
      }
    });
  }

  /**
   * Enable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(String)} and
   * {@link #enableTable(byte[])}
   *
   * @param regex
   *          The regular expression to match table names against
   * @throws java.io.IOException
   * @see #enableTables(java.util.regex.Pattern)
   * @see #enableTable(String)
   */
  public FTable[] enableTables(String regex) throws IOException {
    return enableTables(Pattern.compile(regex));
  }

  /**
   * Enable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.util.regex.Pattern) } and
   * {@link #enableTable(byte[])}
   *
   * @param pattern
   *          The pattern to match table names against
   * @throws java.io.IOException
   */
  public FTable[] enableTables(Pattern pattern) throws IOException {
    List<FTable> failed = new LinkedList<FTable>();
    for (FTable table : listTables(pattern)) {
      if (isTableDisabled(table.getTableName())) {
        try {
          enableTable(table.getTableName());
        } catch (IOException ex) {
          LOG.info("Failed to enable table " + table.getTableName(), ex);
          failed.add(table);
        }
      }
    }
    return failed.toArray(new FTable[failed.size()]);
  }

  public void disableTableAsync(final String tableName) throws IOException {
    disableTableAsync(Bytes.toBytes(tableName));
  }

  /**
   * Starts the disable of a table. If it is being served, the master will tell
   * the servers to stop serving it. This method returns immediately. The
   * disable of a table can take some time if the table is large (all
   * entityGroups are closed as part of table disable operation). Call
   * {@link #isTableDisabled(byte[])} to check for when disable completes. If
   * table is taking too long to online, check server logs.
   *
   * @param tableName
   *          name of table
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   * @see #isTableDisabled(byte[])
   * @see #isTableEnabled(byte[])
   * @since 0.90.0
   */
  public void disableTableAsync(final byte[] tableName) throws IOException {
    FTable.isLegalTableName(Bytes.toString(tableName));
    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        LOG.info("Started disable of " + Bytes.toString(tableName));
        MasterAdminProtos.DisableTableRequest req = RequestConverter
            .buildMasterDisableTableRequest(tableName);
        masterAdmin.disableTable(null, req);
        return null;
      }
    });
  }

  public void disableTable(final String tableName) throws IOException {
    disableTable(Bytes.toBytes(tableName));
  }

  /**
   * Disable table and wait on completion. May timeout eventually. Use
   * {@link #disableTableAsync(byte[])} and {@link #isTableDisabled(String)}
   * instead. The table has to be in enabled state for it to be disabled.
   *
   * @param tableName
   * @throws java.io.IOException
   *           There could be couple types of IOException TableNotFoundException
   *           means the table doesn't exist. TableNotEnabledException means the
   *           table isn't in enabled state.
   */
  public void disableTable(final byte[] tableName) throws IOException {
    disableTableAsync(tableName);
    // Wait until table is disabled
    boolean disabled = false;
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      disabled = isTableDisabled(tableName);
      if (disabled) {
        break;
      }
      long sleep = getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Sleeping= " + sleep
            + "ms, waiting for all entityGroups to be " + "disabled in "
            + Bytes.toString(tableName));
      }
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        // Do this conversion rather than let it out because do not want to
        // change the method signature.
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted", e);
      }
    }
    if (!disabled) {
      throw new EntityGroupException(
          "Retries exhausted, it took too long to wait" + " for the table "
              + Bytes.toString(tableName) + " to be disabled.");
    }
    LOG.info("Disabled " + Bytes.toString(tableName));
  }

  /**
   * Disable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(String)} and
   * {@link #disableTable(byte[])}
   *
   * @param regex
   *          The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be disabled
   * @throws java.io.IOException
   * @see #disableTables(java.util.regex.Pattern)
   * @see #disableTable(String)
   */
  public FTable[] disableTables(String regex) throws IOException {
    return disableTables(Pattern.compile(regex));
  }

  /**
   * Disable tables matching the passed in pattern and wait on completion.
   *
   * Warning: Use this method carefully, there is no prompting and the effect is
   * immediate. Consider using {@link #listTables(java.util.regex.Pattern) } and
   * {@link #disableTable(byte[])}
   *
   * @param pattern
   *          The pattern to match table names against
   * @return Table descriptors for tables that couldn't be disabled
   * @throws java.io.IOException
   */
  public FTable[] disableTables(Pattern pattern) throws IOException {
    List<FTable> failed = new LinkedList<FTable>();
    for (FTable table : listTables(pattern)) {
      if (isTableEnabled(table.getTableName())) {
        try {
          disableTable(table.getTableName());
        } catch (IOException ex) {
          LOG.info("Failed to disable table " + table.getTableName(), ex);
          failed.add(table);
        }
      }
    }
    return failed.toArray(new FTable[failed.size()]);
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if table is on-line
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if table is on-line
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return connection.isTableEnabled(tableName);
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if table is off-line
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableDisabled(final String tableName) throws IOException {
    return isTableDisabled(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if table is off-line
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return connection.isTableDisabled(tableName);
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if all entityGroups of the table are available
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return connection.isTableAvailable(tableName);
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if all entityGroups of the table are available
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableAvailable(String tableName) throws IOException {
    return connection.isTableAvailable(Bytes.toBytes(tableName));
  }

  /**
   * @param tableName
   *          name of table to check
   * @return true if the table is locked
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean isTableLocked(byte[] tableName) throws IOException {
    return connection.isTableLocked(tableName);
  }

  public boolean isTableLocked(String tableName) throws IOException {
    return isTableLocked(Bytes.toBytes(tableName));
  }

  public void unlockTable(final String tableName) throws IOException {
    unlockTable(Bytes.toBytes(tableName));
  }

  /**
   * Unlock the table, use it carefully. For expert-admins.
   * @param tableName name of table to unlock
   * @throws java.io.IOException if a remote or network exception occurs
   */
  public void unlockTable(final byte[] tableName) throws IOException {
    FTable.isLegalTableName(Bytes.toString(tableName));
    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        LOG.info("Started unlock of " + Bytes.toString(tableName));
        MasterAdminProtos.UnlockTableRequest req = RequestConverter.buildUnlockTableRequest(tableName);
        masterAdmin.unlockTable(null, req);
        return null;
      }
    });
  }

  public void setTableState(final String tableName, final String state) throws IOException {
    setTableState(Bytes.toBytes(tableName), Bytes.toBytes(state));
  }

  /**
   * Set table state, use it carefully. For expert-admins.
   * @param tableName name of table to set state
   * @param state state that will set to
   * @throws java.io.IOException if a remote or network exception occurs
   */
  public void setTableState(final byte[] tableName, final byte[] state) throws IOException {
    FTable.isLegalTableName(Bytes.toString(tableName));
    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        LOG.info("Started set " + Bytes.toString(tableName) + " 'state to " + Bytes.toString(state));
        MasterAdminProtos.SetTableStateRequest req = RequestConverter.buildSetTableStateRequest(tableName, state);
        masterAdmin.setTableState(null, req);
        return null;
      }
    });
  }

	public void waitTableAvailable(final String table, long timeoutMillis)
			throws IOException, InterruptedException {
		waitTableAvailable(Bytes.toBytes(table), timeoutMillis);
	}

  /**
   * @param table
   * @param timeoutMillis
   * @throws InterruptedException
   * @throws java.io.IOException
   */
  public void waitTableAvailable(byte[] table, long timeoutMillis)
      throws IOException, InterruptedException {
    long startWait = System.currentTimeMillis();
    while (!isTableAvailable(table)) {
      if (System.currentTimeMillis() - startWait >= timeoutMillis) {
        throw new IOException(
            "Timed out waiting for table to become available "
                + Bytes.toStringBinary(table));
      }
      Thread.sleep(200);
    }
  }

	public void waitTableAvailable(final String table) throws IOException,
			InterruptedException {
		waitTableAvailable(Bytes.toBytes(table));
	}

  public void waitTableAvailable(byte[] table) throws IOException,
      InterruptedException {
    long startWait = System.currentTimeMillis();
    while (!isTableAvailable(table)) {
      LOG.info("Wait " + (System.currentTimeMillis() - startWait) / 1000
          + " s, for " + Bytes.toString(table) + " to be Available.");
      Thread.sleep(1000);
    }
  }

	public void waitTableNotAvailable(final String table) throws IOException,
			InterruptedException {
		waitTableNotAvailable(Bytes.toBytes(table));
	}

  public void waitTableNotAvailable(byte[] table) throws IOException,
      InterruptedException {
    long startWait = System.currentTimeMillis();
    while (tableExists(table)) {
      LOG.info("Wait " + (System.currentTimeMillis() - startWait) / 1000
          + " s, for " + Bytes.toString(table) + " to be Available.");
      Thread.sleep(1000);
    }
  }

	public void waitTableEnabled(final String table, long timeoutMillis)
			throws InterruptedException, IOException {
		waitTableEnabled(Bytes.toBytes(table), timeoutMillis);
	}

	public void waitTableEnabled(byte[] table, long timeoutMillis)
			throws InterruptedException, IOException {
		long startWait = System.currentTimeMillis();
		while (!isTableEnabled(table)) {
			if (System.currentTimeMillis() - startWait >= timeoutMillis) {
				throw new IOException("Timed out waiting for table "
						+ Bytes.toStringBinary(table));
			}
			Thread.sleep(200);
		}
	}

	public void waitTableDisabled(final String table, long timeoutMillis)
			throws InterruptedException, IOException {
		waitTableDisabled(Bytes.toBytes(table), timeoutMillis);
	}

	public void waitTableDisabled(byte[] table, long timeoutMillis)
			throws InterruptedException, IOException {
		long startWait = System.currentTimeMillis();
		while (!isTableDisabled(table)) {
			if (System.currentTimeMillis() - startWait >= timeoutMillis) {
				throw new IOException("Timed out waiting for table "
						+ Bytes.toStringBinary(table));
			}
			Thread.sleep(200);
		}
	}

	public void waitTableNotLocked(final String table) throws IOException,
			InterruptedException {
		waitTableNotLocked(Bytes.toBytes(table));
	}

	public void waitTableNotLocked(byte[] table) throws IOException,
			InterruptedException {
		long startWait = System.currentTimeMillis();
		while (isTableLocked(table)) {
			LOG.info("Wait " + (System.currentTimeMillis() - startWait) / 1000
					+ " s, for " + Bytes.toString(table) + " to be not locked.");
			Thread.sleep(1000);
		}
	}

  /**
   * Get the status of alter command - indicates how many entityGroups have
   * received the updated schema Asynchronous operation.
   *
   * @param tableName
   *          name of the table to get the status of
   * @return Pair indicating the number of entityGroups updated Pair.getFirst()
   *         is the entityGroups that are yet to be updated Pair.getSecond() is
   *         the total number of entityGroups of the table
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public Pair<Integer, Integer> getAlterStatus(final byte[] tableName)
      throws IOException {
    FTable.isLegalTableName(Bytes.toString(tableName));
    return execute(new MasterMonitorCallable<Pair<Integer, Integer>>() {
      @Override
      public Pair<Integer, Integer> call() throws ServiceException {
        GetSchemaAlterStatusRequest req = RequestConverter
            .buildGetSchemaAlterStatusRequest(tableName);
        GetSchemaAlterStatusResponse ret = masterMonitor.getSchemaAlterStatus(
            null, req);
        Pair<Integer, Integer> pair = new Pair<Integer, Integer>(
            Integer.valueOf(ret.getYetToUpdateEntityGroups()),
            Integer.valueOf(ret.getTotalEntityGroups()));
        return pair;
      }
    });
  }

  /**
   * Close a entityGroup. For expert-admins. Runs close on the
   * entityGroupserver. The master will not be informed of the close.
   *
   * @param entityGroupname
   *          entityGroup name to close
   * @param serverName
   *          If supplied, we'll use this location rather than the one currently
   *          in <code>.META.</code>
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void closeEntityGroup(final String entityGroupname,
      final String serverName) throws IOException {
    closeEntityGroup(Bytes.toBytes(entityGroupname), serverName);
  }

  /**
   * Close a entityGroup. For expert-admins Runs close on the entityGroupserver.
   * The master will not be informed of the close.
   *
   * @param entityGroupname
   *          entityGroup name to close
   * @param serverName
   *          The servername of the entityGroupserver. If passed null we will
   *          use servername found in the .META. table. A server name is made of
   *          host, port and startcode. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void closeEntityGroup(final byte[] entityGroupname,
      final String serverName) throws IOException {
    if (serverName != null) {
      if (("").equals(serverName.trim())) {
        throw new IllegalArgumentException(
            "The servername cannot be null or empty.");
      }
      MasterAdminProtos.GetEntityGroupResponse res = execute(new MasterAdminCallable<MasterAdminProtos.GetEntityGroupResponse>() {
        @Override
        public MasterAdminProtos.GetEntityGroupResponse call() throws ServiceException {
          return this.masterAdmin.getEntityGroup(null,
              RequestConverter.buildGetEntityGroupRequest(entityGroupname));
        }
      });
      if (!res.hasEgInfo()) {
        throw new UnknownEntityGroupException(
            Bytes.toStringBinary(entityGroupname));
      } else {
        closeEntityGroup(new ServerName(serverName),
            EntityGroupInfo.convert(res.getEgInfo()));
      }
    } else {
      MasterAdminProtos.GetEntityGroupResponse res = execute(new MasterAdminCallable<MasterAdminProtos.GetEntityGroupResponse>() {
        @Override
        public MasterAdminProtos.GetEntityGroupResponse call() throws ServiceException {
          return this.masterAdmin.getEntityGroup(null,
              RequestConverter.buildGetEntityGroupRequest(entityGroupname));
        }
      });
      if (!res.hasEgInfo()) {
        throw new UnknownEntityGroupException(
            Bytes.toStringBinary(entityGroupname));
      } else if (!res.hasServerName()) {
        throw new NoServerForEntityGroupException(
            Bytes.toStringBinary(entityGroupname));
      } else {
        closeEntityGroup(ServerName.convert(res.getServerName()),
            EntityGroupInfo.convert(res.getEgInfo()));
      }
    }
  }

  /**
   * Close a entityGroup. For expert-admins Runs close on the entityGroupserver.
   * The master will not be informed of the close.
   *
   * @param sn
   * @param egi
   * @throws java.io.IOException
   */
  public void closeEntityGroup(final ServerName sn, final EntityGroupInfo egi)
      throws IOException {
    AdminProtocol admin = this.connection.getAdmin(sn.getHostname(),
        sn.getPort());
    // Close the entityGroup without updating zk state.
    ProtobufUtil.closeEntityGroup(admin, egi, false);
  }

  /**
   * For expert-admins. Runs close on the fserver. Closes a entityGroup based on
   * the encoded entityGroup name. The fserver name is mandatory. If the
   * servername is provided then based on the online entityGroups in the specified
   * fserver the specified entityGroup will be closed. The master will not be
   * informed of the close. Note that the entityGroupName is the encoded entityGroupName.
   *
   * @param encodedEntityGroupName
   *          The encoded entityGroup name; i.e. the hash that makes up the
   *          entityGroup name suffix: e.g. if entityGroupName is
   *          <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>
   *          , then the encoded entityGroup name is:
   *          <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param serverName
   *          The servername of the fserver. A server name is made of host, port
   *          and startcode. This is mandatory. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @return true if the entityGroup was closed, false if not.
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public boolean closeEntityGroupWithEncodedEntityGroupName(
      final String encodedEntityGroupName, final String serverName)
      throws IOException {
    if (null == serverName || ("").equals(serverName.trim())) {
      throw new IllegalArgumentException(
          "The servername cannot be null or empty.");
    }
    ServerName sn = new ServerName(serverName);
    AdminProtocol admin = this.connection.getAdmin(sn.getHostname(),
        sn.getPort());
    CloseEncodedEntityGroupRequest request = RequestConverter
        .buildCloseEncodedEntityGroupRequest(encodedEntityGroupName, false);
    try {
      CloseEncodedEntityGroupResponse response = admin.closeEncodedEntityGroup(
          null, request);
      boolean success = response.getSuccess();
      if (!success) {
        LOG.error("Not able to close the entityGroup " + encodedEntityGroupName
            + ".");
      }
      return success;
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Get all the online entityGroups on a entityGroup server.
   */
  public List<EntityGroupInfo> getOnlineEntityGroups(final ServerName sn)
      throws IOException {
    AdminProtocol admin = this.connection.getAdmin(sn.getHostname(),
        sn.getPort());
    return ProtobufUtil.getOnlineEntityGroups(admin);
  }

  /**
   * Move the entityGroup <code>r</code> to <code>dest</code>.
   *
   * @param encodedEntityGroupName
   *          The encoded entityGroup name; i.e. the hash that makes up the
   *          entityGroup name suffix: e.g. if entityGroupname is
   *          <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>
   *          , then the encoded entityGroup name is:
   *          <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param destServerName
   *          The servername of the destination entityGroupserver. If passed the
   *          empty byte array we'll assign to a random server. A server name is
   *          made of host, port and startcode. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @throws com.alibaba.wasp.UnknownEntityGroupException
   *           Thrown if we can't find a entityGroup named
   *           <code>encodedEntityGroupName</code>
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   * @throws com.alibaba.wasp.MasterNotRunningException
   */
  public void move(final byte[] encodedEntityGroupName,
      final byte[] destServerName) throws UnknownEntityGroupException,
      MasterNotRunningException, ZooKeeperConnectionException {
    MasterAdminKeepAliveConnection master = connection
        .getKeepAliveMasterAdmin();
    try {
      MasterAdminProtos.MoveEntityGroupRequest request = RequestConverter
          .buildMoveEntityGroupRequest(encodedEntityGroupName, destServerName);
      master.moveEntityGroup(null, request);
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof UnknownEntityGroupException) {
        throw (UnknownEntityGroupException) ioe;
      }
      LOG.error("Unexpected exception: " + se
          + " from calling HMaster.moveEntityGroup");
    } finally {
      master.close();
    }
  }

  /**
   * @param entityGroupName
   *          EntityGroup name to assign.
   * @throws com.alibaba.wasp.MasterNotRunningException
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   * @throws java.io.IOException
   */
  public void assign(final byte[] entityGroupName)
      throws MasterNotRunningException, ZooKeeperConnectionException,
      IOException {
    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        MasterAdminProtos.AssignEntityGroupRequest request = RequestConverter
            .buildAssignEntityGroupRequest(entityGroupName);
        masterAdmin.assignEntityGroup(null, request);
        return null;
      }
    });
  }

  /**
   * Unassign a entityGroup from current hosting entityGroupserver. EntityGroup
   * will then be assigned to a entityGroupserver chosen at random. EntityGroup
   * could be reassigned back to the same server. Use
   * {@link #move(byte[], byte[])} if you want to control the entityGroup
   * movement.
   *
   * @param entityGroupName
   *          EntityGroup to unassign. Will clear any existing EntityGroupPlan
   *          if one found.
   * @param force
   *          If true, force unassign (Will remove entityGroup from
   *          entityGroups-in-transition too if present. If results in double
   *          assignment use hbck -fix to resolve. To be used by experts).
   * @throws com.alibaba.wasp.MasterNotRunningException
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   * @throws java.io.IOException
   */
  public void unassign(final byte[] entityGroupName, final boolean force)
      throws MasterNotRunningException, ZooKeeperConnectionException,
      IOException {
    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        MasterAdminProtos.UnassignEntityGroupRequest request = RequestConverter
            .buildUnassignEntityGroupRequest(entityGroupName, force);
        masterAdmin.unassignEntityGroup(null, request);
        return null;
      }
    });
  }

  /**
   * Special method, only used by hbck.
   */
  public void offline(final byte[] entityGroupName) throws IOException {
    MasterAdminKeepAliveConnection master = connection
        .getKeepAliveMasterAdmin();
    try {
      master.offlineEntityGroup(null,
          RequestConverter.buildOfflineEntityGroupRequest(entityGroupName));
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } finally {
      master.close();
    }
  }

  /**
   * Turn the load balancer on or off.
   *
   * @param on
   *          If true, enable balancer. If false, disable balancer.
   * @param synchronous
   *          If true, it waits until current balance() call, if outstanding, to
   *          return.
   * @return Previous balancer value
   */
  public boolean setBalancerRunning(final boolean on, final boolean synchronous)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    MasterAdminKeepAliveConnection master = connection
        .getKeepAliveMasterAdmin();
    try {
      MasterAdminProtos.SetBalancerRunningRequest req = RequestConverter
          .buildSetBalancerRunningRequest(on, synchronous);
      return master.setBalancerRunning(null, req).getPrevBalanceValue();
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof MasterNotRunningException) {
        throw (MasterNotRunningException) ioe;
      }
      if (ioe instanceof ZooKeeperConnectionException) {
        throw (ZooKeeperConnectionException) ioe;
      }

      // Throwing MasterNotRunningException even though not really valid in
      // order to not
      // break interface by adding additional exception type.
      throw new MasterNotRunningException(
          "Unexpected exception when calling balanceSwitch", se);
    } finally {
      master.close();
    }
  }

  /**
   * Turn the load balancer on or off.
   *
   * @param on
   *          If true, enable balancer. If false, disable balancer.
   * @return Previous balancer value
   */
  public boolean setBalancerRunning(final boolean on)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    return setBalancerRunning(on, false);
  }

  /**
   * Invoke the balancer. Will run the balancer and if entityGroups to move, it
   * will go ahead and do the reassignments. Can NOT run for various reasons.
   * Check logs.
   *
   * @return True if balancer ran, false otherwise.
   */
  public boolean balancer() throws MasterNotRunningException,
      ZooKeeperConnectionException, ServiceException {
    MasterAdminKeepAliveConnection master = connection
        .getKeepAliveMasterAdmin();
    try {
      return master.balance(null, RequestConverter.buildBalanceRequest())
          .getBalancerRan();
    } finally {
      master.close();
    }
  }

  /**
   * Split a table or an individual entityGroup. Asynchronous operation.
   *
   * @param tableNameOrEntityGroupName
   *          table to entityGroup to split
   * @param splitPoint
   *          the explicit position to split on
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  public void split(final String tableNameOrEntityGroupName,
      final String splitPoint) throws IOException, InterruptedException {
    split(Bytes.toBytes(tableNameOrEntityGroupName), splitPoint == null ? null
        : Bytes.toBytes(splitPoint));
  }

  /**
   * Split a table or an individual entityGroup. Asynchronous operation.
   *
   * @param tableNameOrEntityGroupName
   *          table to entityGroup to split
   * @param splitPoint
   *          the explicit position to split on
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   * @throws InterruptedException
   *           interrupt exception occurred
   */
  public void split(final byte[] tableNameOrEntityGroupName,
      final byte[] splitPoint) throws IOException, InterruptedException {
    if (splitPoint == null || splitPoint.length == 0) {
      throw new IncorrectParameterException("SplitPoint can't be null.");
    }
    if (isEntityGroupName(tableNameOrEntityGroupName)) {
      Pair<EntityGroupInfo, ServerName> entityGroupServerPair = getEntityGroup(tableNameOrEntityGroupName);
      if (entityGroupServerPair != null) {
        if (entityGroupServerPair.getSecond() == null) {
          throw new NoServerForEntityGroupException(
              Bytes.toStringBinary(tableNameOrEntityGroupName));
        } else {
          split(entityGroupServerPair.getSecond(),
              entityGroupServerPair.getFirst(), splitPoint);
        }
      }
    } else {
      String tableNameString = tableNameString(tableNameOrEntityGroupName);
      MasterAdminProtos.GetTableEntityGroupsResponse res = execute(new MasterAdminCallable<MasterAdminProtos.GetTableEntityGroupsResponse>() {
        @Override
        public MasterAdminProtos.GetTableEntityGroupsResponse call() throws ServiceException {
          MasterAdminProtos.GetTableEntityGroupsRequest req = RequestConverter
              .buildGetTableEntityGroupsRequest(tableNameOrEntityGroupName);
          return this.masterAdmin.getTableEntityGroups(null, req);
        }
      });
      List<Pair<EntityGroupInfo, ServerName>> pairs = new ArrayList<Pair<EntityGroupInfo, ServerName>>(
          res.getEntityGroupList().size());
      for (MasterAdminProtos.GetEntityGroupResponse response : res.getEntityGroupList()) {
        pairs.add(new Pair<EntityGroupInfo, ServerName>(EntityGroupInfo
            .convert(response.getEgInfo()), ServerName.convert(response
            .getServerName())));
      }
      if (pairs == null || pairs.size() == 0) {
        throw new TableNotFoundException(tableNameString);
      }

      for (Pair<EntityGroupInfo, ServerName> pair : pairs) {
        // May not be a server for a particular row
        if (pair.getSecond() == null)
          continue;
        EntityGroupInfo eg = pair.getFirst();
        // check for parents
        if (eg.isSplitParent())
          continue;
        // if a split point given, only split that particular entityGroup
        if (splitPoint != null && !eg.containsRow(splitPoint))
          continue;
        // call out to entityGroup server to do split now
        split(pair.getSecond(), pair.getFirst(), splitPoint);
      }
    }
  }

  private void split(final ServerName sn, final EntityGroupInfo egi,
      byte[] splitPoint) throws IOException {
    AdminProtocol admin = this.connection.getAdmin(sn.getHostname(),
        sn.getPort());
    ProtobufUtil.split(admin, egi, splitPoint);
  }

  /**
   * Add a column to an existing table. Asynchronous operation.
   *
   * @param tableName
   *          name of the table to add column to
   * @param column
   *          the field to be added
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void addColumn(final String tableName, Field column)
      throws IOException {
    addColumn(Bytes.toBytes(tableName), column);
  }

  /**
   * Add a column to an existing table. Asynchronous operation.
   *
   * @param tableName
   *          name of the table to add column to
   * @param column
   *          column descriptor of column to be added
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void addColumn(final byte[] tableName, final Field column)
      throws IOException {
    FTable newTable = this.getTableDescriptor(tableName);
    newTable.getColumns().put(column.getName(), column);
    modifyTable(tableName, newTable);
  }

  /**
   * Delete a column from a table. Asynchronous operation.
   *
   * @param tableName
   *          name of table
   * @param columnName
   *          name of column to be deleted
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void deleteColumn(final String tableName, final String columnName)
      throws IOException {
    deleteColumn(Bytes.toBytes(tableName), Bytes.toBytes(columnName));
  }

  /**
   * Delete a column from a table. Asynchronous operation.
   *
   * @param tableName
   *          name of table
   * @param columnName
   *          name of column to be deleted
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void deleteColumn(final byte[] tableName, final byte[] columnName)
      throws IOException {
    FTable newTable = this.getTableDescriptor(tableName);
    newTable.getColumns().remove(columnName);
    modifyTable(tableName, newTable);
  }

  /**
   * Modify an existing column family on a table. Asynchronous operation.
   *
   * @param tableName
   *          name of table
   * @param field
   *          new field to use
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void modifyColumn(final String tableName, Field field)
      throws IOException {
    modifyColumn(Bytes.toBytes(tableName), field);
  }

  /**
   * Modify an existing column family on a table. Asynchronous operation.
   *
   * @param tableName
   *          name of table
   * @param field
   *          new field to use
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void modifyColumn(final byte[] tableName, final Field field)
      throws IOException {
    FTable newTable = this.getTableDescriptor(tableName);
    Field oldField = newTable.getColumns().get(field.getName());
    if (oldField != null) {
      newTable.getColumns().put(field.getName(), field);
    }
    modifyTable(tableName, newTable);
  }

  /**
   * Get a connection to the currently set master.
   *
   * @return proxy connection to master server for this instance
   * @throws org.apache.hadoop.hbase.MasterNotRunningException
   *           if the master is not running
   * @throws org.apache.hadoop.hbase.ZooKeeperConnectionException
   *           if unable to connect to zookeeper
   */
  public FMasterAdminProtocol getMaster() throws IOException {
    return this.connection.getMasterAdmin();
  }

  /**
   * Modify an existing table, more IRB friendly version. Asynchronous
   * operation. This means that it may be a while before your schema change is
   * updated across all of the table.
   *
   * @param tableName
   *          name of table.
   * @param tableDescriptor
   *          modified description of the table
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public void modifyTable(final byte[] tableName, final FTable tableDescriptor)
      throws IOException {
    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        MasterAdminProtos.ModifyTableRequest request = RequestConverter.buildModifyTableRequest(
            tableName, tableDescriptor);
        masterAdmin.modifyTable(null, request);
        return null;
      }
    });
  }

  /**
   * @param tableNameOrEntityGroupName
   *          Name of a table or name of a entityGroup.
   * @return a pair of EntityGroupInfo and ServerName if
   *         <code>tableNameOrEntityGroupName</code> is a verified entityGroup
   *         name (we call
   *         {@link com.alibaba.wasp.meta.FMetaReader#getEntityGroupLocation(org.apache.hadoop.conf.Configuration, com.alibaba.wasp.EntityGroupInfo)}
   *         (com.alibaba.wasp.EntityGroupInfo)} else null. Throw an exception if
   *         <code>tableNameOrEntityGroupName</code> is null.
   * @throws java.io.IOException
   */
  Pair<EntityGroupInfo, ServerName> getEntityGroup(
      final byte[] tableNameOrEntityGroupName) throws IOException {
    if (tableNameOrEntityGroupName == null) {
      throw new IllegalArgumentException(
          "Pass a table name or entityGroup name");
    }
    MasterAdminProtos.GetEntityGroupWithScanResponse res = execute(new MasterAdminCallable<MasterAdminProtos.GetEntityGroupWithScanResponse>() {
      @Override
      public MasterAdminProtos.GetEntityGroupWithScanResponse call() throws ServiceException {
        return masterAdmin.getEntityGroupWithScan(null, RequestConverter
            .buildGetEntityGroupWithScanRequest(tableNameOrEntityGroupName));
      }
    });

    Pair<EntityGroupInfo, ServerName> pair = new Pair<EntityGroupInfo, ServerName>(
        EntityGroupInfo.convert(res.getEgInfo()), ServerName.convert(res
        .getServerName()));
    return pair;
  }

  /**
   * @param tableNameOrEntityGroupName
   *          Name of a table or name of a entityGroup.
   * @return True if <code>tableNameOrEntityGroupName</code> is a verified
   *         entityGroup name else false. Throw an exception if
   *         <code>tableNameOrEntityGroupName</code> is null.
   * @throws java.io.IOException
   */
  private boolean isEntityGroupName(final byte[] tableNameOrEntityGroupName)
      throws IOException {
    if (tableNameOrEntityGroupName == null) {
      throw new IllegalArgumentException(
          "Pass a table name or entityGroup name");
    }

    MasterAdminProtos.GetEntityGroupResponse res = execute(new MasterAdminCallable<MasterAdminProtos.GetEntityGroupResponse>() {
      @Override
      public MasterAdminProtos.GetEntityGroupResponse call() throws ServiceException {
        return masterAdmin.getEntityGroup(null, RequestConverter
            .buildGetEntityGroupRequest(tableNameOrEntityGroupName));
      }
    });

    return res.hasServerName();
  }

  /**
   * Convert the table name byte array into a table name string and check if
   * table exists or not.
   *
   * @param tableNameBytes
   *          Name of a table.
   * @return tableName in string form.
   * @throws java.io.IOException
   *           if a remote or network exception occurs.
   * @throws com.alibaba.wasp.TableNotFoundException
   *           if table does not exist.
   */
  private String tableNameString(final byte[] tableNameBytes)
      throws IOException {
    MasterAdminProtos.TableExistsResponse res = execute(new MasterAdminCallable<MasterAdminProtos.TableExistsResponse>() {
      @Override
      public MasterAdminProtos.TableExistsResponse call() throws ServiceException {
        return masterAdmin.tableExists(null,
            RequestConverter.buildTableExistsRequest(tableNameBytes));
      }
    });
    String tableNameString = Bytes.toString(tableNameBytes);
    if (!res.getExist()) {
      throw new TableNotFoundException(tableNameString);
    }
    return tableNameString;
  }

  /**
   * Shuts down the Wasp cluster
   *
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public synchronized void shutdown() throws IOException {
    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        masterAdmin.shutdown(null, MasterAdminProtos.ShutdownRequest.newBuilder().build());
        return null;
      }
    });
  }

  /**
   * Shuts down the current Wasp master only. Does not shutdown the cluster.
   *
   * @see #shutdown()
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public synchronized void stopMaster() throws IOException {
    execute(new MasterAdminCallable<Void>() {
      @Override
      public Void call() throws ServiceException {
        masterAdmin.stopMaster(null, MasterAdminProtos.StopMasterRequest.newBuilder().build());
        return null;
      }
    });
  }

  /**
   * Stop the designated entityGroupserver
   *
   * @param hostnamePort
   *          Hostname and port delimited by a <code>:</code> as in
   *          <code>example.org:1234</code>
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public synchronized void stopEntityGroupServer(final String hostnamePort)
      throws IOException {
    String hostname = Addressing.parseHostname(hostnamePort);
    int port = Addressing.parsePort(hostnamePort);
    AdminProtocol admin = this.connection.getAdmin(hostname, port);
    StopServerRequest request = RequestConverter
        .buildStopServerRequest("Called by admin client "
            + this.connection.toString());
    try {
      admin.stopServer(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * @return cluster status
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public ClusterStatus getClusterStatus() throws IOException {
    return execute(new MasterMonitorCallable<ClusterStatus>() {
      @Override
      public ClusterStatus call() throws ServiceException {
        GetClusterStatusRequest req = RequestConverter
            .buildGetClusterStatusRequest();
        return ClusterStatus.convert(masterMonitor.getClusterStatus(null, req)
            .getClusterStatus());
      }
    });
  }

  /**
   * @return Configuration used by the instance.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Check to see if Wasp is running. Throw an exception if not. We consider
   * that Wasp is running if ZooKeeper and Master are running.
   *
   * @param conf
   *          system configuration
   * @throws com.alibaba.wasp.MasterNotRunningException
   *           if the master is not running
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   *           if unable to connect to zookeeper
   */
  public static void checkWaspAvailable(Configuration conf)
      throws MasterNotRunningException, ZooKeeperConnectionException,
      ServiceException {
    Configuration copyOfConf = WaspConfiguration.create(conf);

    // We set it to make it fail as soon as possible if Wasp is not available
    copyOfConf.setInt("wasp.client.retries.number", 1);
    copyOfConf.setInt("zookeeper.recovery.retry", 0);

    FConnectionManager.FConnectionImplementation connection = (FConnectionManager.FConnectionImplementation) FConnectionManager
        .getConnection(copyOfConf);

    try {
      // Check ZK first.
      // If the connection exists, we may have a connection to ZK that does
      // not work anymore
      ZooKeeperWatcher zkw = null;
      try {
        zkw = connection.getZooKeeperWatcher();
        zkw.getRecoverableZooKeeper().getZooKeeper()
            .exists(zkw.baseZNode, false);

      } catch (IOException e) {
        throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
      } catch (KeeperException e) {
        throw new ZooKeeperConnectionException("Can't connect to ZooKeeper", e);
      } finally {
        if (zkw != null) {
          zkw.close();
        }
      }

      // Check Master
      connection.isMasterRunning();

    } finally {
      connection.close();
    }
  }

  /**
   * get the entityGroups of a given table.
   *
   * @param tableName
   *          the name of the table
   * @return Ordered list of {@link com.alibaba.wasp.EntityGroupInfo}.
   * @throws java.io.IOException
   */
  public List<EntityGroupInfo> getTableEntityGroups(final byte[] tableName)
      throws IOException {
    MasterAdminProtos.GetTableEntityGroupsResponse res = execute(new MasterAdminCallable<MasterAdminProtos.GetTableEntityGroupsResponse>() {
      @Override
      public MasterAdminProtos.GetTableEntityGroupsResponse call() throws ServiceException {
        MasterAdminProtos.GetTableEntityGroupsRequest req = RequestConverter
            .buildGetTableEntityGroupsRequest(tableName);
        return this.masterAdmin.getTableEntityGroups(null, req);
      }
    });
    List<EntityGroupInfo> egis = new ArrayList<EntityGroupInfo>();
    for (MasterAdminProtos.GetEntityGroupResponse response : res.getEntityGroupList()) {
      egis.add(EntityGroupInfo.convert(response.getEgInfo()));
    }
    return egis;
  }

  /**
   * Gets all the entityGroups and their address for this table.
   * <p>
   * This is mainly useful for the MapReduce integration.
   *
   * @return A map of EntityGroupInfo with it's server address
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public NavigableMap<EntityGroupInfo, ServerName> getEntityGroupLocations(
      final byte[] tableName) throws IOException {
    MasterAdminProtos.GetEntityGroupsResponse res = execute(new MasterAdminCallable<MasterAdminProtos.GetEntityGroupsResponse>() {
      @Override
      public MasterAdminProtos.GetEntityGroupsResponse call() throws ServiceException {
        MasterAdminProtos.GetEntityGroupsRequest req = RequestConverter
            .buildGetEntityGroupsRequest(tableName);
        return this.masterAdmin.getEntityGroups(null, req);
      }
    });
    NavigableMap<EntityGroupInfo, ServerName> entityGroups = null;
    if (!res.getEntityGroupList().isEmpty()) {
      entityGroups = new TreeMap<EntityGroupInfo, ServerName>();
      for (MasterAdminProtos.GetEntityGroupResponse response : res.getEntityGroupList()) {
        entityGroups.put(EntityGroupInfo.convert(response.getEgInfo()),
            ServerName.convert(response.getServerName()));
      }
    }
    return entityGroups;
  }

  public void truncate(String tableName) throws TableNotDisabledException,
      IOException {
    truncate(Bytes.toBytes(tableName));
  }

  public void truncate(final byte[] tableName)
      throws TableNotDisabledException, IOException {
    // Disable table
    disableTable(tableName);
    // Truncate table
    List<MasterAdminProtos.TruncateTableResponse> responses = new ArrayList<MasterAdminProtos.TruncateTableResponse>();
    if (this.connection.isTableDisabled(tableName)) {
      MasterAdminProtos.TruncateTableResponse res = execute(new MasterAdminCallable<MasterAdminProtos.TruncateTableResponse>() {
        @Override
        public MasterAdminProtos.TruncateTableResponse call() throws ServiceException {
          MasterAdminProtos.TruncateTableRequest request = RequestConverter
              .buildTruncateTableRequest(tableName);
          return this.masterAdmin.truncateTable(null, request);
        }
      });
      responses.add(res);
    } else {
      throw new TableNotDisabledException(tableName);
    }
    // Wait truncate finished
    try {
      waitTableNotLocked(tableName);
    } catch (InterruptedException e) {
    }
    // Enable table
    enableTable(tableName);
  }

  @Override
  public void close() throws IOException {
    if (this.connection != null) {
      this.connection.close();
    }
  }

  /**
   * Get tableDescriptors
   *
   * @param tableNames
   *          List of table names
   * @return FTD[] the tableDescriptor
   * @throws java.io.IOException
   *           if a remote or network exception occurs
   */
  public FTable[] getTableDescriptors(List<String> tableNames)
      throws IOException {
    return this.connection.getFTableDescriptors(tableNames);
  }

  /**
   * @see {@link #execute(MasterAdminCallable<V>)}
   */
  private abstract static class MasterAdminCallable<V> implements Callable<V> {
    protected MasterAdminKeepAliveConnection masterAdmin;
  }

  /**
   * @see {@link #execute(MasterMonitorCallable<V>)}
   */
  private abstract static class MasterMonitorCallable<V> implements Callable<V> {
    protected MasterMonitorKeepAliveConnection masterMonitor;
  }

  /**
   * This method allows to execute a function requiring a connection to master
   * without having to manage the connection creation/close. Create a
   * {@link MasterAdminCallable} to use it.
   */
  private <V> V execute(MasterAdminCallable<V> function) throws IOException {
    function.masterAdmin = connection.getKeepAliveMasterAdmin();
    try {
      return executeCallable(function);
    } finally {
      function.masterAdmin.close();
    }
  }

  /**
   * This method allows to execute a function requiring a connection to master
   * without having to manage the connection creation/close. Create a
   * {@link MasterAdminCallable} to use it.
   */
  private <V> V execute(MasterMonitorCallable<V> function) throws IOException {
    function.masterMonitor = connection.getKeepAliveMasterMonitor();
    try {
      return executeCallable(function);
    } finally {
      function.masterMonitor.close();
    }
  }

  /**
   * Helper function called by other execute functions.
   */
  private <V> V executeCallable(Callable<V> function) throws IOException {
    try {
      return function.call();
    } catch (RemoteException re) {
      throw re.unwrapRemoteException();
    } catch (IOException e) {
      throw e;
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } catch (Exception e) {
      // This should not happen...
      throw new IOException("Unexpected exception when calling master", e);
    }
  }
}

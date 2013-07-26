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
package com.alibaba.wasp.fserver;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.Pair;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.NotServingEntityGroupException;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.TableNotFoundException;
import com.alibaba.wasp.fserver.metrics.MetricsEntityGroup;
import com.alibaba.wasp.fserver.redo.AlreadyCommitTransactionException;
import com.alibaba.wasp.fserver.redo.AlreadyExsitsTransactionIdException;
import com.alibaba.wasp.fserver.redo.NotInitlizedRedoException;
import com.alibaba.wasp.fserver.redo.Redo;
import com.alibaba.wasp.fserver.redo.RedoLog;
import com.alibaba.wasp.fserver.redo.RedoLogNotServingException;
import com.alibaba.wasp.fserver.redo.Transaction;
import com.alibaba.wasp.fserver.redo.WALEdit;
import com.alibaba.wasp.messagequeue.Message;
import com.alibaba.wasp.messagequeue.MessageQueue;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Field;
import com.alibaba.wasp.meta.Index;
import com.alibaba.wasp.meta.RowBuilder;
import com.alibaba.wasp.meta.StorageTableNameBuilder;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.action.ColumnStruct;
import com.alibaba.wasp.plan.action.DeleteAction;
import com.alibaba.wasp.plan.action.GetAction;
import com.alibaba.wasp.plan.action.InsertAction;
import com.alibaba.wasp.plan.action.Primary;
import com.alibaba.wasp.plan.action.ScanAction;
import com.alibaba.wasp.plan.action.UpdateAction;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.generated.WaspProtos.Mutate;
import com.alibaba.wasp.protobuf.generated.WaspProtos.Mutate.MutateType;
import com.alibaba.wasp.storage.StorageServices;
import com.alibaba.wasp.storage.StorageServicesImpl;
import com.alibaba.wasp.storage.StorageTableNotFoundException;
import org.cliffc.high_scale_lib.Counter;

/**
 * 
 * The kernel of Wasp. EntityGroup stores data for a certain eg of a table,it
 * stores all columns for each row which includes index and entity. A given
 * table consists of one or more EntityGroups.
 * 
 */
public class EntityGroup extends Thread implements EntityGroupServices,
    MessageQueue<OperationStatus> {

  public static final Log LOG = LogFactory.getLog(EntityGroup.class);

  final EntityGroupInfo entityGroupInfo;

  final Configuration conf;

  final Redo redo;

  private long transactionRetryPause;

  final AtomicBoolean closed = new AtomicBoolean(false);

  protected final FServerServices services;

  final Counter readRequestsCount = new Counter();
  final Counter writeRequestsCount = new Counter();
  private final ConcurrentHashMap<HashedBytes, CountDownLatch> lockedRows = new ConcurrentHashMap<HashedBytes, CountDownLatch>();
  final int rowLockWaitDuration;
  static final int DEFAULT_ROWLOCK_WAIT_DURATION = 30000;

  private EntityGroupSplitPolicy splitPolicy;

  private final MetricsEntityGroup metricsEntityGroup;
  /*
   * Closing can take some time; use the closing flag if there is stuff we don't
   * want to do while in closing state; e.g. like offer this entityGroup up to
   * the master as a entityGroup to close if the carrying FServer is overloaded.
   * Once set, it is never cleared.
   */
  final AtomicBoolean closing = new AtomicBoolean(false);

  /*
   * Data structure of write state flags used coordinating closes.
   */
  static class WriteState {

    // Gets set in close.
    volatile boolean writesEnabled = true;

    // Set if entityGroup is read-only
    volatile boolean readOnly = false;

    /**
     * Set flags that make this entityGroup read-only.
     * 
     * @param onOff
     *          flip value for entityGroup r/o setting
     */
    synchronized void setReadOnly(final boolean onOff) {
      this.writesEnabled = !onOff;
      this.readOnly = onOff;
    }

    boolean isReadOnly() {
      return this.readOnly;
    }

    static final long HEAP_SIZE = ClassSize.align(ClassSize.OBJECT + 5
        * Bytes.SIZEOF_BOOLEAN);
  }

  final WriteState writestate = new WriteState();

  final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private byte[] explicitSplitPoint = null;

  private FTable ftableDescriptor = null;

  private Thread thread;

  private StorageServices storageServices = null;

  /**
   * default
   * 
   * @throws IOException
   **/
  @SuppressWarnings("unchecked")
  public EntityGroup(Configuration conf, EntityGroupInfo egi, FTable table,
      FServerServices service) throws IOException {
    this.conf = conf;
    this.ftableDescriptor = table;
    this.entityGroupInfo = egi;

    try {
      Class<? extends Redo> redoClass = (Class<? extends Redo>) conf.getClass(
          FConstants.REDO_IMPL, RedoLog.class);

      Constructor<? extends Redo> c = redoClass.getConstructor(
          EntityGroupInfo.class, Configuration.class);

      this.redo = c.newInstance(entityGroupInfo, conf);
    } catch (Throwable e) {
      throw new IllegalStateException(
          "Could not instantiate a entityGroup instance.", e);
    }

    this.transactionRetryPause = conf.getInt(
        FConstants.WASP_TRANSACTION_RETRY_PAUSE,
        FConstants.DEFAULT_WASP_TRANSACTION_RETRY_PAUSE);
    this.rowLockWaitDuration = conf.getInt("wasp.rowlock.wait.duration",
        DEFAULT_ROWLOCK_WAIT_DURATION);
    this.services = service;
    if (this.services != null) {
      this.metricsEntityGroup = new MetricsEntityGroup(
          new MetricsEntityGroupWrapperImpl(this));
    } else {
      this.metricsEntityGroup = null;
    }
    if (LOG.isDebugEnabled()) {
      // Write out EntityGroup name as string and its encoded name.
      LOG.debug("Instantiated " + this);
    }
  }

  /**
   * Initialize this entityGroup.
   * 
   * @return What the next sequence (edit) id should be.
   * @throws IOException
   *           e
   */
  public void initialize() throws IOException {
    redo.initlize();
    this.closed.set(false);
    this.closing.set(false);
    this.writestate.readOnly = false;
    this.writestate.writesEnabled = true;
    thread = new Thread(this);
    thread.start();
    // Initialize split policy
    this.splitPolicy = EntityGroupSplitPolicy.create(this, conf);
    if (services != null) {
      this.storageServices = new StorageServicesImpl(
          this.services.getActionManager());
    }
  }

  public boolean isAvailable() {
    return !isClosed() && !isClosing();
  }

  /** @return true if entityGroup is closed */
  public boolean isClosed() {
    return this.closed.get();
  }

  /**
   * @return True if closing process has started.
   */
  public boolean isClosing() {
    return this.closing.get();
  }

  /** @return start key for entityGroup */
  public byte[] getStartKey() {
    return this.entityGroupInfo.getStartKey();
  }

  /** @return end key for entityGroup */
  public byte[] getEndKey() {
    return this.entityGroupInfo.getEndKey();
  }

  /** @return entityGroup id */
  public long getEntityGroupId() {
    return this.entityGroupInfo.getEntityGroupId();
  }

  /** @return entityGroup name */
  public byte[] getEntityGroupName() {
    return this.entityGroupInfo.getEntityGroupName();
  }

  /** @return entityGroup name as string for logging */
  public String getEntityGroupNameAsString() {
    return this.entityGroupInfo.getEntityGroupNameAsString();
  }

  /** @return FTableDescriptor for this entityGroup */
  public FTable getTableDesc() {
    return this.ftableDescriptor;
  }

  /** @return Configuration object */
  public Configuration getConf() {
    return this.conf;
  }

  /** @return StorageServices object */
  public StorageServices getStorageServices() {
    return storageServices;
  }

  /**
   * @param StorageServices
   *          the StorageServices to set
   */
  public void setStorageServices(StorageServices storageServices) {
    this.storageServices = storageServices;
  }

  /**
   * No-index query using primary key.
   * 
   * @param action
   * @return
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  public Result get(GetAction action) throws IOException,
      StorageTableNotFoundException {
    Get get = new Get(RowBuilder.build().buildEntityRowKey(this.conf,
        action.getFTableName(), action.getCombinedPrimaryKey()));
    get.setMaxVersions(1);
    // as a result of the maximum timestamp value is exclusive, but we need the
    // inclusive one . so plus one to timestamp
    if (action.getReaderMode() == ReadModel.CURRENT) {
      this.internalObtainRowLock(action.getCombinedPrimaryKey());
      this.releaseRowLock(action.getCombinedPrimaryKey());
    }
    readRequestsCount.increment();
    get.setTimeRange(0, timestamp(action.getReaderMode()) + 1);
    return storageServices.getRow(
        StorageTableNameBuilder.buildEntityTableName(action.getFTableName()),
        get);
  }

  /**
   * Return an iterator that scans over the EntityGroup, returning the indicated
   * columns and rows specified by the
   * 
   * <p>
   * This Iterator must be closed by the caller.
   * 
   * @param action
   * @return
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  public EntityGroupScanner getScanner(ScanAction action) throws IOException,
      StorageTableNotFoundException {
    startEntityGroupOperation();
    readRequestsCount.increment();
    try {
      return instantiateEntityGroupScanner(action);
    } finally {
      closeEntityGroupOperation();
    }
  }

  /**
   * return EntityGroupScanner.
   * 
   * @param action
   * @param scanId
   * @return
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  protected EntityGroupScanner instantiateEntityGroupScanner(ScanAction action)
      throws StorageTableNotFoundException, IOException {
    long timestamp = timestamp(action.getReaderMode());
    return new EntityGroupScanner(this, action, timestamp);
  }

  /**
   * 
   * @param readerMode
   *          current,snapshot,inconsistent
   * @return
   * @throws IOException
   */
  private long timestamp(ReadModel readerMode) throws IOException {
    long timestamp;
    if (readerMode == ReadModel.CURRENT) {
      timestamp = currentReadTimeStamp();
    } else if (readerMode == ReadModel.SNAPSHOT) {
      timestamp = snapshotTimeStamp();
    } else if (readerMode == ReadModel.INCONSISTENT) {
      timestamp = System.currentTimeMillis();
    } else {
      throw new IOException("CODE INCONSISTENT.");
    }
    return timestamp;
  }

  /**
   * This method needs to be called before any public call that reads or
   * modifies data. It has to be called just before a try. Acquires checks if
   * the entityGroup is closing or closed.
   * 
   * @throws com.alibaba.wasp.NotServingEntityGroupException
   *           when the entityGroup is closing or closed
   */
  private void startEntityGroupOperation()
      throws NotServingEntityGroupException {
    if (this.closing.get()) {
      throw new NotServingEntityGroupException(
          entityGroupInfo.getEntityGroupNameAsString() + " is closing");
    }
    lock.readLock().lock();
    if (this.closed.get()) {
      lock.readLock().unlock();
      throw new NotServingEntityGroupException(
          entityGroupInfo.getEntityGroupNameAsString() + " is closed");
    }
  }

  private void closeEntityGroupOperation() {
    lock.readLock().unlock();
  }

  /**
   * Log insert transaction to redoLog.
   * 
   * @param action
   * @return
   */
  public OperationStatus insert(InsertAction action) throws IOException {
    startEntityGroupOperation();
    writeRequestsCount.increment();
    try {
      internalObtainRowLock(action.getCombinedPrimaryKey());
      Transaction transaction = new Transaction(currentReadTimeStamp());
      try {
        prepareInsertEntity(action, transaction);
      } catch (StorageTableNotFoundException e) {
        this.releaseRowLock(action.getCombinedPrimaryKey());
        throw new TableNotFoundException(action.getFTableName());
      } catch (Throwable e) {
        this.releaseRowLock(action.getCombinedPrimaryKey());
        LOG.error("PrepareInsertEntity failed.", e);
        return new OperationStatus(OperationStatusCode.FAILURE, e.getMessage(),
            e.getClass().getName());
      }
      if (this.metricsEntityGroup != null) {
        this.metricsEntityGroup.updateInsert();
      }
      return submitTransaction(action, transaction);
    } finally {
      closeEntityGroupOperation();
    }
  }

  /**
   * Make insert action into transaction;
   * 
   * @param action
   *          insert action
   * @param transaction
   *          transaction
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  private void prepareInsertEntity(InsertAction action, Transaction transaction)
      throws IOException, StorageTableNotFoundException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    RowBuilder builder = RowBuilder.build();
    TableSchemaCacheReader metaReader = TableSchemaCacheReader
        .getInstance(this.conf);
    LinkedHashMap<String, Index> indexs = metaReader.getSchema(
        action.getFTableName()).getIndex();
    if (LOG.isDebugEnabled()) {
      LOG.debug("prepareInsertEntity indexs:" + indexs.values());
    }

    NavigableMap<byte[], NavigableMap<byte[], byte[]>> set = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(
        Bytes.BYTES_COMPARATOR);
    for (ColumnStruct col : action.getColumns()) {
      byte[] family = Bytes.toBytes(col.getFamilyName());
      NavigableMap<byte[], byte[]> cols = set.get(family);
      if (cols == null) {
        cols = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
      }
      set.put(family, cols);
      cols.put(Bytes.toBytes(col.getColumnName()), col.getValue());
    }

    String entityTableName = StorageTableNameBuilder
        .buildEntityTableName(action.getFTableName());
    // entity put
    Put entityPut = builder.buildPut(action);
    transaction.addEntity(ProtobufUtil.toMutate(MutateType.PUT, entityPut,
        entityTableName));
    storageServices.checkRowExistsBeforeInsert(action, entityTableName,
        entityPut);

    // index put
    if (indexs != null) {
      for (Index index : indexs.values()) {
        Pair<byte[], String> indexPut = builder.buildIndexKey(index, set,
            entityPut.getRow());
        if (indexPut != null) {
          Put put = new Put(indexPut.getFirst());
          put.add(FConstants.INDEX_STORING_FAMILY_BYTES,
              FConstants.INDEX_STORE_ROW_QUALIFIER, entityPut.getRow());
          for (Entry<String, Field> entry : index.getStoring().entrySet()) {
            ColumnStruct storing = action.getName2Column().get(entry.getKey());
            if (storing != null) {
              put.add(FConstants.INDEX_STORING_FAMILY_BYTES,
                  Bytes.toBytes(entry.getKey()), storing.getValue());
            }
          }
          transaction.addEntity(ProtobufUtil.toMutate(MutateType.PUT, put,
              indexPut.getSecond()));
        }
      }
    }
    if (this.metricsEntityGroup != null) {
      this.metricsEntityGroup.updatePrepareInsertEntity(EnvironmentEdgeManager
          .currentTimeMillis() - before);
    }
  }

  /**
   * Log update transaction to redoLog.
   * 
   * @param action
   *          , basic action of recording update information.
   * @return
   * @throws IOException
   */
  public OperationStatus update(UpdateAction action)
      throws NotServingEntityGroupException, IOException {
    startEntityGroupOperation();
    writeRequestsCount.increment();
    try {
      internalObtainRowLock(action.getCombinedPrimaryKey());
      currentReadTimeStamp();
      Transaction transaction = new Transaction();
      try {
        if (!prepareUpdateEntity(action, transaction)) {
          this.releaseRowLock(action.getCombinedPrimaryKey());
          return OperationStatus.FAILURE;
        }
      } catch (StorageTableNotFoundException e) {
        this.releaseRowLock(action.getCombinedPrimaryKey());
        throw new TableNotFoundException(action.getFTableName());
      } catch (Throwable e) {
        this.releaseRowLock(action.getCombinedPrimaryKey());
        LOG.error("PrepareUpdateEntity failed.", e);
        return new OperationStatus(OperationStatusCode.FAILURE, e.getMessage(),
            e.getClass().getName());
      }

      return submitTransaction(action, transaction);
    } finally {
      closeEntityGroupOperation();
    }
  }

  private void relationIndexAndField(Map<String, Field> referField,
      Set<Index> indexs) {
    for (Index index : indexs) {
      for (Field field : index.getIndexKeys().values()) {
        referField.put(field.getName(), field);
      }
    }
  }

  /**
   * First delete old records,then insert or update new record.
   * 
   * @param action
   * @param transaction
   * @throws IOException
   * @throws StorageTableNotFoundException
   */
  private boolean prepareUpdateEntity(UpdateAction action,
      Transaction transaction) throws IOException,
      StorageTableNotFoundException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    TableSchemaCacheReader tableSchemaReader = TableSchemaCacheReader
        .getInstance(this.conf);

    // use meta reader fetch relation index and relation field
    Map<String, Field> referField = new HashMap<String, Field>();
    Set<Index> indexs = new HashSet<Index>(0);
    for (ColumnStruct column : action.getColumns()) {
      indexs.addAll(tableSchemaReader.getIndexsByField(action.getFTableName(),
          column.getColumnName()));
      if (indexs.size() == 0) {
        continue;
      }
      relationIndexAndField(referField, indexs);
      column.setIndex(true);
    }

    RowBuilder builder = RowBuilder.build();

    // fetch entity data
    Get get = new Get(builder.buildEntityRowKey(this.conf,
        action.getFTableName(), action.getCombinedPrimaryKey()));
    for (Map.Entry<String, Field> fieldEntry : referField.entrySet()) {
      get.addColumn(Bytes.toBytes(fieldEntry.getValue().getFamily()),
          Bytes.toBytes(fieldEntry.getValue().getName()));
    }

    Result result = storageServices.getRowBeforeUpdate(action,
        StorageTableNameBuilder.buildEntityTableName(action.getFTableName()),
        get);

    if (result == null || result.size() == 0) {
      return false;
    }

    // entity put
    Put entityPut = builder.buildPut(action);
    transaction.addEntity(ProtobufUtil.toMutate(MutateType.PUT, entityPut,
        StorageTableNameBuilder.buildEntityTableName(action.getFTableName())));

    // index delete
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> oldValues = result
        .getNoVersionMap();
    for (Index index : indexs) {
      Pair<byte[], String> delete = builder.buildIndexKey(index, oldValues,
          result.getRow());
      if(delete != null) {
        transaction.addEntity(ProtobufUtil.toMutate(MutateType.DELETE,
          new Delete(delete.getFirst()), delete.getSecond()));
      }
    }

    // index put
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> newValues = prepareUpdateValues(
        oldValues, action);
    for (Index index : indexs) {
      Pair<byte[], String> indexPut = builder.buildIndexKey(index, newValues,
          entityPut.getRow());
      if(indexPut != null) {
        Put put = new Put(indexPut.getFirst());
        put.add(FConstants.INDEX_STORING_FAMILY_BYTES,
            FConstants.INDEX_STORE_ROW_QUALIFIER, entityPut.getRow());
        for (Entry<String, Field> entry : index.getStoring().entrySet()) {
          byte[] columnNameByte = Bytes.toBytes(entry.getKey());
          ColumnStruct storing = action.getName2Column().get(entry.getKey());
          if (storing != null) {
            put.add(FConstants.INDEX_STORING_FAMILY_BYTES, columnNameByte,
                storing.getValue());
          }
        }

        transaction.addEntity(ProtobufUtil.toMutate(MutateType.PUT, put,
            indexPut.getSecond()));
      }
    }
    if (this.metricsEntityGroup != null) {
      this.metricsEntityGroup.updatePrepareUpdateEntity(EnvironmentEdgeManager
          .currentTimeMillis() - before);
    }
    return true;
  }

  /**
   * Use new value replace old value.
   * 
   * @param oldValues
   * @param action
   * @return
   */
  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> prepareUpdateValues(
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> oldValues,
      UpdateAction action) {
    Iterator<ColumnStruct> iterator = action.getColumns().iterator();
    while (iterator.hasNext()) {
      ColumnStruct column = iterator.next();
      if (column.isIndex()) {
        NavigableMap<byte[], byte[]> family = oldValues.get(Bytes
            .toBytes(column.getFamilyName()));
        byte[] columnName = Bytes.toBytes(column.getColumnName());
        family.put(columnName, column.getValue());
      }
    }
    return oldValues;
  }

  /**
   * Log delete transaction to redoLog.
   * 
   * @param action
   *          ,basic action of recording delete information
   * @return
   * @throws IOException
   */
  public OperationStatus delete(DeleteAction action)
      throws NotServingEntityGroupException, IOException {
    startEntityGroupOperation();
    writeRequestsCount.increment();
    try {
      internalObtainRowLock(action.getCombinedPrimaryKey());
      currentReadTimeStamp();
      Transaction transaction = new Transaction();
      try {
        if (!prepareDeleteEntity(action, transaction)) {
          this.releaseRowLock(action.getCombinedPrimaryKey());
          return OperationStatus.FAILURE;
        }
      } catch (StorageTableNotFoundException e) {
        this.releaseRowLock(action.getCombinedPrimaryKey());
        throw new TableNotFoundException(action.getFTableName());
      } catch (Throwable e) {
        this.releaseRowLock(action.getCombinedPrimaryKey());
        LOG.error("PrepareDeleteEntity failed.", e);
        return new OperationStatus(OperationStatusCode.FAILURE, e.getMessage(),
            e.getClass().getName());
      }

      return submitTransaction(action, transaction);
    } finally {
      closeEntityGroupOperation();
    }
  }

  private boolean prepareDeleteEntity(DeleteAction action,
      Transaction transaction) throws IOException,
      StorageTableNotFoundException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    RowBuilder factory = RowBuilder.build();
    String entityTableName = StorageTableNameBuilder
        .buildEntityTableName(action.getFTableName());

    // fetch entity data
    Get get = new Get(factory.buildEntityRowKey(this.conf,
        action.getFTableName(), action.getCombinedPrimaryKey()));
    Result result = storageServices.getRowBeforeDelete(action, entityTableName,
        get);

    if (result == null || result.size() == 0) {
      return false;
    }

    // entity delete
    Delete entityDelete = new Delete(get.getRow());
    transaction.addEntity(ProtobufUtil.toMutate(MutateType.DELETE,
        entityDelete, entityTableName));

    // index delete
    NavigableMap<byte[], NavigableMap<byte[], byte[]>> oldValues = result
        .getNoVersionMap();

    TableSchemaCacheReader metaReader = TableSchemaCacheReader
        .getInstance(this.conf);
    LinkedHashMap<String, Index> indexs = metaReader.getSchema(
        action.getFTableName()).getIndex();
    if (indexs != null) {
      for (Index index : indexs.values()) {
        Pair<byte[], String> delete = factory.buildIndexKey(index, oldValues,
            get.getRow());
        if (delete != null) {
          transaction.addEntity(ProtobufUtil.toMutate(MutateType.DELETE,
              new Delete(delete.getFirst()), delete.getSecond()));
        }
      }
    }
    if (this.metricsEntityGroup != null) {
      this.metricsEntityGroup.updatePrepareDeleteEntity(EnvironmentEdgeManager
          .currentTimeMillis() - before);
    }
    return true;
  }

  /**
   * Append transaction to redoLog.
   * 
   * @param transaction
   * @return
   */
  private OperationStatus submitTransaction(Primary action,
      Transaction transaction) {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    try {
      redo.append(action, transaction);
    } catch (AlreadyExsitsTransactionIdException e) {
      transaction.setTransactionID(System.currentTimeMillis());
      submitTransaction(action, transaction);
    } catch (Throwable e) {
      this.releaseRowLock(action.getCombinedPrimaryKey());
      LOG.error("Submit transaction failed.", e);
      return new OperationStatus(OperationStatusCode.FAILURE, e.getMessage(), e
          .getClass().getName());
    }
    if (this.metricsEntityGroup != null) {
      this.metricsEntityGroup.updateRedoLogAppend(EnvironmentEdgeManager
          .currentTimeMillis() - before);
    }
    return OperationStatus.SUCCESS;
  }

  /**
   * Obtains or tries to obtain the given row lock.
   * 
   * @param waitForLock
   *          if true, will block until the lock is available. Otherwise, just
   *          tries to obtain the lock and returns null if unavailable.
   */
  private void internalObtainRowLock(final byte[] row) throws IOException {
    checkRow(row, "row lock");
    long before = EnvironmentEdgeManager.currentTimeMillis();
    HashedBytes rowKey = new HashedBytes(row);
    CountDownLatch rowLatch = new CountDownLatch(1);

    // loop until we acquire the row lock (unless !waitForLock)
    while (true) {
      CountDownLatch existingLatch = lockedRows.putIfAbsent(rowKey, rowLatch);
      if (existingLatch == null) {
        break;
      } else {
        try {
          if (!existingLatch.await(this.rowLockWaitDuration,
              TimeUnit.MILLISECONDS)) {
            throw new IOException("Timed out on getting lock for row="
                + Bytes.toStringBinary(row));
          }
        } catch (InterruptedException ie) {
          // Empty
        }
      }
    }
    if (this.metricsEntityGroup != null) {
      this.metricsEntityGroup.updateObtainRowLock(EnvironmentEdgeManager
          .currentTimeMillis() - before);
    }
  }

  /**
   * Release the row lock!
   * 
   * @param lockId
   *          The lock ID to release.
   */
  public void releaseRowLock(final byte[] row) {
    HashedBytes rowKey = new HashedBytes(row);
    CountDownLatch rowLatch = lockedRows.remove(rowKey);
    if (rowLatch == null) {
      LOG.error("Releases row not locked,row: " + row);
      return;
    }
    rowLatch.countDown();
  }

  /**
   * 
   * @param read
   *          read or write
   * @return
   * @throws NotServingEntityGroupException
   * @throws IOException
   */
  private long currentReadTimeStamp() throws IOException {
    Transaction currentTransaction = null;
    WALEdit edit = redo.peekLastUnCommitedTransaction();
    if (edit != null) {
      currentTransaction = edit.getT();
    }
    if (currentTransaction != null) {
      return currentTransaction.getTransactionID();
    } else {
      return snapshotTimeStamp();
    }
  }

  private long snapshotTimeStamp() throws IOException {
    WALEdit edit = redo.lastCommitedTransaction();
    Transaction currentTransaction = null;
    if (edit != null) {
      currentTransaction = edit.getT();
    }
    if (currentTransaction != null) {
      return currentTransaction.getTransactionID();
    } else {
      return System.currentTimeMillis();
    }
  }

  /**
   * @throws IOException
   * @throws NotServingEntityGroupException
   * @see com.alibaba.wasp.messagequeue.MessageQueue#doAsynchronous()
   */
  @Override
  public OperationStatus doAsynchronous(Message message) {
    try {
      if (message instanceof DeleteAction) {
        return this.delete((DeleteAction) message);
      } else if (message instanceof UpdateAction) {
        return this.update((UpdateAction) message);
      } else if (message instanceof InsertAction) {
        return this.insert((InsertAction) message);
      } else {
        throw new IOException("Unkown action.");
      }
    } catch (Exception e) {
      return new OperationStatus(OperationStatusCode.FAILURE, e.getMessage(), e
          .getClass().getName());
    }
  }

  /**
   * Close down this EntityGroup. Finish the transaction. don't service any more
   * calls.
   * 
   * <p>
   * This method could take some time to execute, so don't call it from a
   * time-sensitive thread.
   * 
   * @return Vector of all transactions that the EntityGroup has not commited.
   *         Returns empty vector if already closed and null if judged that it
   *         should not close.
   * 
   * @throws IOException
   *           e
   */
  public void close() throws IOException {
    close(false);
  }

  private final Object closeLock = new Object();

  /**
   * Close down this EntityGroup. Finish the transaction unless abort parameter
   * is true, don't service any more calls.
   * 
   * This method could take some time to execute, so don't call it from a
   * time-sensitive thread.
   * 
   * @param abort
   *          true if server is aborting (only during testing)
   * @return Vector of all transactions that the EntityGroup has not commited.
   *         Returns empty vector if already closed and null if judged that it
   *         should not close.
   * 
   * @throws IOException
   *           e
   */
  public boolean close(final boolean abort) throws IOException {
    // Only allow one thread to close at a time. Serialize them so dual
    // threads attempting to close will run up against each other.
    MonitoredTask status = TaskMonitor.get().createStatus(
        "Closing entityGroup " + this + (abort ? " due to abort" : ""));

    status.setStatus("Waiting for close lock");
    try {
      synchronized (closeLock) {
        return doClose(abort, status);
      }
    } finally {
      status.cleanup();
    }
  }

  private boolean doClose(final boolean abort, MonitoredTask status)
      throws IOException {
    if (isClosed()) {
      LOG.warn("EntityGroup " + this + " already closed");
      return true;
    }

    synchronized (writestate) {
      // Disable transaction by background threads for this
      // entityGroup.
      writestate.writesEnabled = false;
      LOG.debug("Closing " + this);
    }
    this.closing.set(true);

    status.setStatus("Disabling writes for close");
    lock.writeLock().lock();
    try {
      if (this.isClosed()) {
        status.abort("Already got closed by another process");
        // SplitTransaction handles the null
        return false;
      }

      status.setStatus("Disabling transaction for entityGroup");
      LOG.debug("Updates disabled for entityGroup " + this);

      thread.interrupt();
      try {
        thread.join();
      } catch (InterruptedException e) {
        LOG.warn("interrupt the transaction thread by close entityGroup", e);
      }

      // Don't commit the transaction if we are aborting
      if (!abort) {
        LOG.info("Running commit transaction of "
            + this.getEntityGroupNameAsString() + " before close");
        internalCommitTransaction(status);
      }
      // close redo
      redo.close();

      this.closed.set(true);

      status.markComplete("Closed");
      LOG.info("Closed " + this);
      return true;
    } finally {
      lock.writeLock().unlock();
    }
  }

  protected void internalCommitTransaction() throws IOException {
    MonitoredTask status = TaskMonitor.get().createStatus(
        "Commiting " + this.getEntityGroupNameAsString());
    status.setStatus("Acquiring write lock on entityGroup");
    internalCommitTransaction(this.redo, status);
  }

  protected void internalCommitTransaction(MonitoredTask status)
      throws IOException {
    internalCommitTransaction(this.redo, status);
  }

  /**
   * commit the transaction and close redoLog
   * 
   * @param redoLog
   * @param status
   * @return
   * @throws IOException
   */
  protected void internalCommitTransaction(Redo redoLog, MonitoredTask status)
      throws IOException {
    while (true) {
      WALEdit edit = null;
      edit = redo.peekLastUnCommitedTransaction();
      if (edit == null) {
        return;
      }
      try {
        edit = redo.lastUnCommitedTransaction();
      } catch (InterruptedException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("Getting last uncommited transaction of redo log failed", e);
        }
      } catch (NotInitlizedRedoException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("the redo log has not been initialized", e);
        }
      } catch (RedoLogNotServingException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("the redo log didn't serving any more", e);
        }
      }
      try {
        commitTransaction(edit);
      } catch (IOException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("the redo log commit error.", e);
        }
        return;
      }
    }
  }

  /** @return RedoLog in use for this entityGroup */
  public Redo getLog() {
    return this.redo;
  }

  private synchronized void commitTransaction(WALEdit edit) throws IOException {
    Transaction t = edit.getT();
    if (LOG.isDebugEnabled()) {
      LOG.debug("EntityGroup commitTransaction:" + t.getTransactionID());
    }
    List<Mutate> mutates = t.getEdits();
    CompletionService<InsureStatus> completionService = new ExecutorCompletionService<InsureStatus>(
        this.services.getThreadPool());

    for (Mutate mutate : mutates) {
      String tableName = mutate.getTableName();
      try {
        if (mutate.getMutateType() == Mutate.MutateType.PUT) {
          Put put = ProtobufUtil.toPut(mutate, t.getTransactionID());
          completionService.submit(new InsurePut(tableName, put));
        } else if (mutate.getMutateType() == Mutate.MutateType.DELETE) {
          Delete delete = ProtobufUtil.toDelete(mutate);
          completionService.submit(new InsureDelete(tableName, delete));
        }
      } catch (DoNotRetryIOException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("convert mutate to Put or Delete error.", e);
        }
      }
    }

    int errors = 0;
    for (int i = 0; i < mutates.size(); i++) {
      try {
        Future<InsureStatus> result = completionService.take();
        if (InsureStatus.SUCCESS == result.get()) {
          // nothing,this operator is successful.
        } else if (InsureStatus.FAILED == result.get()) {
          errors++;
        } else {
          LOG.warn("What happened?");
          errors++;
        }
      } catch (InterruptedException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("transaction execute error", e);
        }
      } catch (ExecutionException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("transaction execute error", e);
        }
      }
    }
    if (errors != 0) {
      String message = "transaction id=" + t.getTransactionID()
          + " process occur " + errors + " errors";
      LOG.warn(message);
      throw new IOException(message);
    }

    try {
      redo.commit(edit);
    } catch (AlreadyCommitTransactionException e) {
      if (LOG.isErrorEnabled()) {
        LOG.error("the transaction id=" + t.getTransactionID()
            + " has all ready commited", e);
      }
    } catch (NotInitlizedRedoException e) {
      if (LOG.isErrorEnabled()) {
        LOG.error("the transaction id=" + t.getTransactionID()
            + " commited failed as a result of the redo log has a error ", e);
      }
    } catch (RedoLogNotServingException e) {
      if (LOG.isErrorEnabled()) {
        LOG.error("the transaction id=" + t.getTransactionID()
            + " commited failed as a result of the redo log has been closed ",
            e);
      }
    }
    Primary primary = edit.getAction();
    if (primary != null) {
      this.releaseRowLock(primary.getCombinedPrimaryKey());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Thread#run()
   */
  @Override
  public void run() {
    while (!closing.get()) {
      WALEdit edit = null;
      Transaction t = null;
      try {
        edit = redo.lastUnCommitedTransaction();
        t = edit.getT();
      } catch (InterruptedException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("Getting last uncommited transaction of redo log failed", e);
        }
        return;
      } catch (NotInitlizedRedoException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("the redo log has not been initialized", e);
        }
        return;
      } catch (RedoLogNotServingException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("the redo log didn't serving any more", e);
        }
        return;
      }
      List<Mutate> mutates = t.getEdits();
      if (!checkMutates(mutates)) {
        LOG.info("the mutates of transaction check failed. tid="
            + t.getTransactionID());
        return;
      }
      try {
        long before = EnvironmentEdgeManager.currentTimeMillis();
        commitTransaction(edit);
        if (this.metricsEntityGroup != null) {
          this.metricsEntityGroup
              .updateBackgroundRedoLog(EnvironmentEdgeManager
              .currentTimeMillis() - before);
        }
      } catch (IOException e) {
        return;
      }
    }
  }

  private boolean checkMutates(List<Mutate> mutates) {
    return true;
  }

  /**
   * The status of insure operator.
   * 
   */
  private enum InsureStatus {
    SUCCESS,
      FAILED
  }

  private class InsurePut implements Callable<InsureStatus> {
    private String tableName;
    private Put put;

    InsurePut(String tableName, Put put) {
      this.tableName = tableName;
      this.put = put;
    }

    @Override
    public InsureStatus call() throws Exception {
      while (true) {
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("InsurePut tableName:" + tableName + " Put " + put);
          }
          EntityGroup.this.storageServices.putRow(tableName, put);
          return InsureStatus.SUCCESS;
        } catch (StorageTableNotFoundException e) {
          if (LOG.isErrorEnabled()) {
            LOG.error("table '" + tableName + "' has not found", e);
          }
        } catch (IOException e) {
          if (LOG.isErrorEnabled()) {
            LOG.error(
                "table " + tableName + " put error, row:"
                    + Bytes.toString(put.getRow()), e);
          }
          try {
            Thread.sleep(transactionRetryPause);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
        return InsureStatus.FAILED;
      }
    }
  }

  private class InsureDelete implements Callable<InsureStatus> {
    private String tableName;
    private Delete delete;

    InsureDelete(String tableName, Delete delete) {
      this.tableName = tableName;
      this.delete = delete;
    }

    @Override
    public InsureStatus call() throws Exception {
      while (true) {
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("InsureDelete tableName:" + tableName + " Delete "
                + delete);
          }
          EntityGroup.this.storageServices.deleteRow(tableName, delete);
          return InsureStatus.SUCCESS;
        } catch (StorageTableNotFoundException e) {
          if (LOG.isErrorEnabled()) {
            LOG.error("table '" + tableName + "' has not found", e);
          }
        } catch (IOException e) {
          if (LOG.isErrorEnabled()) {
            LOG.error(
                "table " + tableName + " put error, row:"
                    + Bytes.toString(delete.getRow()), e);
          }
          try {
            Thread.sleep(transactionRetryPause);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
        return InsureStatus.FAILED;
      }
    }
  }

  // Utility methods
  /**
   * A utility method to create new instances of EntityGroup based on the
   * configuration property.
   * 
   * @param log
   *          The RedoLog is the outbound log for any updates to the EntityGroup
   *          (There's a single RedoLog for a EntityGroup.)
   * @param conf
   *          is global configuration settings.
   * @param entityGroupInfo
   *          - EntityGroupInfo that describes the entityGroup is new), then
   *          read them from the supplied path.
   * @param ftd
   * @param fsServices
   * @return the new instance
   */
  public static EntityGroup newEntityGroup(Configuration conf,
      EntityGroupInfo entityGroupInfo, final FTable ftd,
      FServerServices fsServices) {
    try {
      @SuppressWarnings("unchecked")
      Class<? extends EntityGroup> entityGroupClass = (Class<? extends EntityGroup>) conf
          .getClass(FConstants.ENTITYGROUP_IMPL, EntityGroup.class);

      Constructor<? extends EntityGroup> c = entityGroupClass.getConstructor(
          Configuration.class, EntityGroupInfo.class, FTable.class,
          FServerServices.class);

      return c.newInstance(conf, entityGroupInfo, ftd, fsServices);
    } catch (Throwable e) {
      throw new IllegalStateException(
          "Could not instantiate a entityGroup instance.", e);
    }
  }

  /**
   * Open a EntityGroup.
   * 
   * @param info
   *          Info for entityGroup to be opened.
   * @param log
   *          RedoLog for entityGroup to use.
   * @param conf
   * @return new EntityGroup
   * 
   * @throws IOException
   */
  public static EntityGroup openEntityGroup(final EntityGroupInfo info,
      final FTable ftd, final Configuration conf) throws IOException {
    return openEntityGroup(info, ftd, conf, null, null);
  }

  /**
   * Open a EntityGroup.
   * 
   * @param info
   *          Info for entityGroup to be opened.
   * @param log
   *          RedoLog for entityGroup to use.
   * @param conf
   * @param reporter
   *          An interface we can report progress against.
   * @return new EntityGroup
   * 
   * @throws IOException
   */
  public static EntityGroup openEntityGroup(final EntityGroupInfo info,
      final FTable table, final Configuration conf,
      final FServerServices fsServices, final CancelableProgressable reporter)
      throws IOException {
    if (info == null) {
      throw new NullPointerException("Passed EntityGroup info is null");
    }
    LOG.info("EntityGroup.openEntityGroup EntityGroup name =="
        + info.getEntityGroupNameAsString());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening EntityGroup: " + info);
    }
    EntityGroup eg = EntityGroup.newEntityGroup(conf, info, table, fsServices);
    eg.initialize();
    return eg;
  }

  /**
   * Open EntityGroup. Calls initialize and sets sequenceid.
   * 
   * @param reporter
   * @return Returns <code>this</code>
   * @throws IOException
   */
  protected EntityGroup openEntityGroup(final CancelableProgressable reporter)
      throws IOException {
    this.initialize();
    return this;
  }

  /**
   * This will do the necessary cleanup a call to
   * {@link #createEntityGroup(com.alibaba.wasp.EntityGroupInfo, org.apache.hadoop.conf.Configuration, com.alibaba.wasp.meta.FTable)}
   * associated {@link RedoLog} file. You use it if you call the other
   * createEntityGroup, the one that takes an {@link RedoLog} instance but don't
   * be surprised by the call to the
   * {@link com.alibaba.wasp.fserver.redo.RedoLog#close()} ()} on the
   * {@link RedoLog} the EntityGroup was carrying.
   * 
   * @param entityGroup
   * @throws IOException
   */
  public static void closeEntityGroup(final EntityGroup entityGroup)
      throws IOException {
    if (entityGroup == null) {
      return;
    }
    entityGroup.close();
    if (entityGroup.getLog() != null) {
      entityGroup.getLog().close();
    }
  }

  /**
   * Determines if the specified row is within the row range specified by the
   * specified EntityGroupInfo
   * 
   * @param info
   *          EntityGroupInfo that specifies the row range
   * @param row
   *          row to be checked
   * @return true if the row is within the range specified by the
   *         EntityGroupInfo
   */
  public static boolean rowIsInRange(EntityGroupInfo info, final byte[] row) {
    return ((info.getStartKey().length == 0) || (Bytes.compareTo(
        info.getStartKey(), row) <= 0))
        && ((info.getEndKey().length == 0) || (Bytes.compareTo(
            info.getEndKey(), row) > 0));
  }

  /**
   * Merge two EntityGroups. The entityGroups must be adjacent and must not
   * overlap.
   * 
   * @param srcA
   * @param srcB
   * @return new merged EntityGroup
   * @throws IOException
   */
  public static EntityGroup mergeAdjacent(final EntityGroup srcA,
      final EntityGroup srcB) throws IOException {
    EntityGroup a = srcA;
    EntityGroup b = srcB;

    // Make sure that srcA comes first; important for key-ordering during
    // write of the merged file.
    if (srcA.getStartKey() == null) {
      if (srcB.getStartKey() == null) {
        throw new IOException(
            "Cannot merge two entityGroups with null start key");
      }
      // A's start key is null but B's isn't. Assume A comes before B
    } else if ((srcB.getStartKey() == null)
        || (Bytes.compareTo(srcA.getStartKey(), srcB.getStartKey()) > 0)) {
      a = srcB;
      b = srcA;
    }

    if (!(Bytes.compareTo(a.getEndKey(), b.getStartKey()) == 0)) {
      throw new IOException("Cannot merge non-adjacent entityGroups");
    }
    return merge(a, b);
  }

  /**
   * Merge two entityGroups whether they are adjacent or not.
   * 
   * @param a
   *          entityGroup a
   * @param b
   *          entityGroup b
   * @return new merged entityGroup
   * @throws IOException
   */
  public static EntityGroup merge(EntityGroup a, EntityGroup b)
      throws IOException {
    if (!a.getEntityGroupInfo().getTableNameAsString()
        .equals(b.getEntityGroupInfo().getTableNameAsString())) {
      throw new IOException("EntityGroups do not belong to the same table");
    }

    // Compact each entityGroup so we only have one store file per family
    Configuration conf = a.getConf();
    FTable tabledesc = a.getTableDesc();
    // Presume both are of same entityGroup type
    final byte[] startKey = Bytes.compareTo(a.getStartKey(), b.getStartKey()) < 0 ? a
        .getStartKey() : b.getStartKey();
    final byte[] endKey = Bytes.compareTo(a.getEndKey(), b.getEndKey()) > 0 ? a
        .getEndKey() : b.getEndKey();

    EntityGroupInfo newEntityGroupInfo = new EntityGroupInfo(
        Bytes.toBytes(tabledesc.getTableName()), startKey, endKey);
    LOG.info("Creating new entityGroup " + newEntityGroupInfo.toString());

    LOG.info("starting merge of entityGroups: " + a + " and " + b
        + " into new entityGroup " + newEntityGroupInfo.toString()
        + " with start key <" + Bytes.toStringBinary(startKey)
        + "> and end key <" + Bytes.toStringBinary(endKey) + ">");
    // close a & b entityGroups
    a.close();
    b.close();

    EntityGroup dstEntityGroup = EntityGroup.newEntityGroup(conf,
        newEntityGroupInfo, null, null);
    dstEntityGroup.readRequestsCount.set(a.readRequestsCount.get()
        + b.readRequestsCount.get());
    dstEntityGroup.writeRequestsCount.set(a.writeRequestsCount.get()
        + b.writeRequestsCount.get());
    dstEntityGroup.initialize();

    LOG.info("merge completed. New EntityGroup is " + dstEntityGroup);

    return dstEntityGroup;
  }

  @Override
  public String toString() {
    return this.entityGroupInfo.getEntityGroupNameAsString();
  }

  byte[] getExplicitSplitPoint() {
    return this.explicitSplitPoint;
  }

  /** @return a EntityGroupInfo object for this EntityGroup */
  public EntityGroupInfo getEntityGroupInfo() {
    return this.entityGroupInfo;
  }

  void forceSplit(byte[] sp) {
    // NOTE : this EntityGroup will go away after the forced split is
    // successfull
    // therefore, no reason to clear this value
    if (sp != null) {
      this.explicitSplitPoint = sp;
    }
  }

  public byte[] checkSplit() {
    byte[] ret = splitPolicy.getSplitPoint(this.explicitSplitPoint);

    if (ret != null) {
      try {
        checkRow(ret, "calculated split");
      } catch (IOException e) {
        LOG.error("Ignoring invalid split", e);
        return null;
      }
    }
    return ret;
  }

  /**
   * Convenience method creating new EntityGroups. Used by createTable and by
   * the bootstrap code in the FMaster constructor. Note, this method creates an
   * {@link RedoLog} for the created entityGroup. It needs to be closed
   * explicitly. Use {@link EntityGroup#getLog()} to get access. <b>When done
   * with a entityGroup created using this method, you will need to explicitly
   * close the {@link RedoLog} it created too; it will not be done for you. Not
   * closing the log will leave at least a daemon thread running.</b> Call
   * {@link #closeEntityGroupOperation()} and it will do necessary cleanup for
   * you.
   * 
   * @param info
   *          Info for entityGroup to create.
   * @param conf
   * @param table
   * @return new EntityGroup
   * 
   * @throws IOException
   */
  public static EntityGroup createEntityGroup(final EntityGroupInfo info,
      final Configuration conf, final FTable table,
      final FServerServices services) throws IOException {
    return createEntityGroup(info, conf, table, null, services);
  }

  /**
   * Convenience method creating new EntityGroups. Used by createTable. The
   * {@link RedoLog} for the created entityGroup needs to be closed explicitly,
   * if it is not null. Use {@link EntityGroup#getLog()} to get access.
   * 
   * @param info
   *          Info for entityGroup to create.
   * @param conf
   * @param table
   * @param redoLog
   *          shared RedoLog
   * @return new EntityGroup
   * 
   * @throws IOException
   */
  public static EntityGroup createEntityGroup(final EntityGroupInfo info,
      final Configuration conf, final FTable table, final RedoLog redoLog,
      FServerServices services) throws IOException {
    return createEntityGroup(info, conf, table, true, services);
  }

  /**
   * Convenience method creating new EntityGroups. Used by createTable. The
   * {@link RedoLog} for the created entityGroup needs to be closed explicitly,
   * if it is not null. Use {@link EntityGroup#getLog()} to get access.
   * 
   * @param info
   *          Info for entityGroup to create.
   * @param conf
   * @param table
   * @param redoLog
   *          shared RedoLog
   * @param initialize
   *          - true to initialize the entityGroup
   * @return new EntityGroup
   * 
   * @throws IOException
   */
  public static EntityGroup createEntityGroup(final EntityGroupInfo info,
      final Configuration conf, final FTable table, final RedoLog redoLog,
      final boolean initialize, FServerServices services) throws IOException {
    return createEntityGroup(info, conf, table, initialize, services);
  }

  /**
   * Convenience method creating new EntityGroups. Used by createTable. The
   * {@link RedoLog} for the created entityGroup needs to be closed explicitly,
   * if it is not null. Use {@link EntityGroup#getLog()} to get access.
   * 
   * @param info
   *          Info for entityGroup to create.
   * @param conf
   * @param table
   * @param redoLog
   *          shared RedoLog
   * @param initialize
   *          - true to initialize the entityGroup
   * @param ignoreRedoLog
   *          - true to skip generate new redoLog if it is null, mostly for
   *          createTable
   * @return new EntityGroup
   * 
   * @throws IOException
   */
  public static EntityGroup createEntityGroup(final EntityGroupInfo info,
      final Configuration conf, final FTable table, final boolean initialize,
      final FServerServices services) throws IOException {
    LOG.info("creating EntityGroup " + info.getTableNameAsString()
        + " Table == " + table + " Table name == "
        + info.getTableNameAsString());

    EntityGroup entityGroup = EntityGroup.newEntityGroup(conf, info, table,
        services);
    if (initialize) {
      entityGroup.initialize();
    }
    return entityGroup;
  }

  public long getWriteRequestsCount() {
    return writeRequestsCount.get();
  }

  public long getReadRequestsCount() {
    return readRequestsCount.get();
  }

  public int getTransactionLogSize() {
    if (redo != null) {
      return redo.size();
    } else {
      return 0;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof EntityGroup)) {
      return false;
    }
    return Bytes.equals(this.getEntityGroupName(),
        ((EntityGroup) o).getEntityGroupName());
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(this.getEntityGroupName());
  }

  /** Make sure this is a valid row for the EntityGroup */
  void checkRow(final byte[] row, String op) throws IOException {
    if (!rowIsInRange(this.entityGroupInfo, row)) {
      throw new WrongEntityGroupException("Requested row out of range for "
          + op + " on EntityGroup " + this + ", startKey='"
          + Bytes.toStringBinary(this.entityGroupInfo.getStartKey())
          + "', getEndKey()='"
          + Bytes.toStringBinary(this.entityGroupInfo.getEndKey()) + "', row='"
          + Bytes.toStringBinary(row) + "'");
    }
  }

  /**
   * @see com.alibaba.wasp.fserver.EntityGroupServices#getFServerServices()
   */
  @Override
  public FServerServices getFServerServices() {
    return this.services;
  }
}

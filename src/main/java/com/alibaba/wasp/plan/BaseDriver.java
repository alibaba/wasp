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
package com.alibaba.wasp.plan;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.wasp.protobuf.generated.ClientProtos;import com.alibaba.wasp.protobuf.generated.MasterAdminProtos;import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.ReflectionUtils;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.TableNotDisabledException;
import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.client.FConnection;
import com.alibaba.wasp.client.FConnectionManager;
import com.alibaba.wasp.client.MasterAdminKeepAliveConnection;
import com.alibaba.wasp.fserver.FServer;
import com.alibaba.wasp.fserver.FServerServices;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Index;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.execute.ExecutionEngine;
import com.alibaba.wasp.plan.parser.ParseContext;
import com.alibaba.wasp.plan.parser.Parser;
import com.alibaba.wasp.plan.parser.UnsupportedException;
import com.alibaba.wasp.plan.parser.WaspParser;
import com.alibaba.wasp.protobuf.RequestConverter;
import com.alibaba.wasp.protobuf.ResponseConverter;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteResponse;
import com.alibaba.wasp.protobuf.generated.ClientProtos.QueryResultProto;
import com.alibaba.wasp.protobuf.generated.ClientProtos.StringDataTypePair;
import com.alibaba.wasp.protobuf.generated.ClientProtos.WriteResultProto;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DeleteTableResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.TruncateTableResponse;
import org.cliffc.high_scale_lib.Counter;

import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLParseException;
import com.google.protobuf.ServiceException;

/**
 * This BaseDriver is for parsing sql and distribution tasks, as well as the
 * merged results and perform 2pc.
 * 
 */
public class BaseDriver implements Closeable {

  /** Logger **/
  public static final Log LOG = LogFactory.getLog(BaseDriver.class);

  /** Execute Engine. **/
  private final ExecutionEngine executeEngine;

  /** FServer **/
  private final FServerServices service;

  /** SQL parser engine. **/
  private final Parser parser;

  /** Connection instance. **/
  private FConnection connection = null;

  final Counter selectSQLCount = new Counter();
  final Counter insertSQLCount = new Counter();
  final Counter deleteSQLCount = new Counter();
  final Counter updateSQLCount = new Counter();
  private AtomicInteger readRequestCount = new AtomicInteger();
  private AtomicInteger writeRequestCount = new AtomicInteger();
  private AtomicInteger readRequestTime = new AtomicInteger();
  private AtomicInteger writeRequestTime = new AtomicInteger();

  /**
   * @param service
   */
  public BaseDriver(FServerServices service) throws MetaException,
      ZooKeeperConnectionException {
    super();
    this.service = service;
    this.executeEngine = new ExecutionEngine((FServer) this.service);
    Class<? extends Parser> parserClass = service.getConfiguration().getClass(
        FConstants.WASP_SQL_PARSER_CLASS, WaspParser.class, Parser.class);
    parser = ReflectionUtils.newInstance(parserClass,
        service.getConfiguration());
    this.connection = FConnectionManager.getConnection(service
        .getConfiguration());
  }

  public long getSelectSQLCount() {
    return selectSQLCount.get();
  }

  public long getInsertSQLCount() {
    return insertSQLCount.get();
  }

  public long getDeleteSQLCount() {
    return deleteSQLCount.get();
  }

  public long getUpdateSQLCount() {
    return updateSQLCount.get();
  }

  /**
   * This BaseDriver is for parsing sql and distribution tasks, as well as the
   * merged results and perform 2pc.
   * 
   * @param sql
   * @param readModel
   * @return
   * @throws com.google.protobuf.ServiceException
   */
  public ClientProtos.ExecuteResponse execute(String sql, String sessionId,
      ReadModel readModel, boolean closeSession, int fetchSize)
      throws ServiceException {
    try {
      long beforeGenPlan = EnvironmentEdgeManager.currentTimeMillis();
      ParseContext context = new ParseContext();
      context.setTsr(TableSchemaCacheReader.getInstance(service
          .getConfiguration()));
      context.setSql(sql);
      context.setReadModel(readModel);
      context.setSessionId(sessionId);
      try {
        if (StringUtils.isNotEmpty(sessionId)) {
          Parser.SQLType sqlType = parser.getSQLType(context);
          if(sqlType == Parser.SQLType.DQL) {
            context.setPlan(new DQLPlan());
          } else {
            parser.generatePlan(context);
          }
        } else {
          parser.generatePlan(context);
        }
      } catch (RuntimeException e) {
        if (e instanceof ParserException || e instanceof SQLParseException) {
          throw new DoNotRetryIOException(e.getMessage(), e);
        } else {
          throw new ServiceException(new IOException(e));
        }
      } catch (UnsupportedException e) {
        throw e;
      }
      final long afterGenPlan = EnvironmentEdgeManager.currentTimeMillis();
      ((FServer) this.service).getMetrics().updateGenPlan(
          afterGenPlan - beforeGenPlan);

      Plan executePlan = context.getPlan();
      if (executePlan instanceof DQLPlan) {
        selectSQLCount.increment();
        DQLPlan dqlPlan = (DQLPlan) executePlan;
        dqlPlan.setFetchRows(fetchSize);
        Pair<Boolean, Pair<String, Pair<List<QueryResultProto>, List<StringDataTypePair>>>> queryResults =
            executeEngine.execQueryPlan(dqlPlan, sessionId, closeSession);
        addReadMetricsCount(0, null, 1, EnvironmentEdgeManager.currentTimeMillis() - beforeGenPlan);
        return ResponseConverter.buildExecuteResponse(queryResults.getFirst(),
            queryResults.getSecond().getFirst(), queryResults.getSecond()
            .getSecond());
      } else if (executePlan instanceof DMLPlan) {
        if (executePlan instanceof InsertPlan) {
          insertSQLCount.increment();
          List<WriteResultProto> writeResults = executeEngine
              .execInsertPlan((InsertPlan) executePlan);
          addWriteMetricsCount(0, null, 1, EnvironmentEdgeManager.currentTimeMillis()
              - beforeGenPlan);
          return ResponseConverter.buildExecuteResponse(writeResults);
        } else if (executePlan instanceof UpdatePlan) {
          updateSQLCount.increment();
          List<WriteResultProto> writeResults = executeEngine
              .execUpdatePlan((UpdatePlan) executePlan);
          addWriteMetricsCount(0, null, 1, EnvironmentEdgeManager.currentTimeMillis()
              - beforeGenPlan);
          return ResponseConverter.buildExecuteResponse(writeResults);
        } else if (executePlan instanceof DeletePlan) {
          deleteSQLCount.increment();
          List<WriteResultProto> writeResults = executeEngine
              .execDeletePlan((DeletePlan) executePlan);
          addWriteMetricsCount(0, null, 1, EnvironmentEdgeManager.currentTimeMillis()
              - beforeGenPlan);
          return ResponseConverter.buildExecuteResponse(writeResults);
        } else {
          throw new ServiceException(
              "The instance of Plan is not supported. SQL:" + sql);
        }
      } else if (executePlan instanceof DDLPlan) {
        MasterAdminKeepAliveConnection masterAdminKeepAliveConnection = this.connection
            .getKeepAliveMasterAdmin();

        if(executePlan instanceof NotingTodoPlan) {
          return ResponseConverter.buildNotExecuteResponse();
        } else if (executePlan instanceof AlterTablePlan) {
          FTable hTableDesc = ((AlterTablePlan) executePlan).getNewTable();
          return modifyTable(masterAdminKeepAliveConnection, hTableDesc);
        } else if (executePlan instanceof CreateTablePlan) {
          CreateTablePlan createTable = (CreateTablePlan) executePlan;
          MasterAdminProtos.CreateTableResponse response = masterAdminKeepAliveConnection
              .createTable(
                  null,
                  RequestConverter.buildCreateTableRequest(
                      createTable.getTable(), createTable.getSplitKeys()));
          return ResponseConverter.buildExecuteResponse(response);
        } else if (executePlan instanceof CreateIndexPlan) {
          CreateIndexPlan createIndexPlan = (CreateIndexPlan) executePlan;
          Index index = createIndexPlan.getIndex();
          MasterAdminProtos.CreateIndexRequest request = RequestConverter
              .buildCreateIndexRequest(index);
          return ResponseConverter
              .buildExecuteResponse(masterAdminKeepAliveConnection.createIndex(
                  null, request));
        } else if (executePlan instanceof DropIndexPlan) {
          DropIndexPlan dropIndexPlan = (DropIndexPlan) executePlan;
          MasterAdminProtos.DropIndexRequest request = RequestConverter.buildDropIndexRequest(
              dropIndexPlan.getTableName(), dropIndexPlan.getIndexName());
          return ResponseConverter
              .buildExecuteResponse(masterAdminKeepAliveConnection.deleteIndex(
                  null, request));
        } else if (executePlan instanceof DropTablePlan) {
          List<DeleteTableResponse> responses = new ArrayList<DeleteTableResponse>();
          for (String tableName : ((DropTablePlan) executePlan).getTableNames()) {
            byte[] byteName = Bytes.toBytes(tableName);
            if (this.connection.isTableDisabled(byteName)) {
              MasterAdminProtos.DeleteTableRequest request = RequestConverter
                  .buildDeleteTableRequest(byteName);
              responses.add(masterAdminKeepAliveConnection.deleteTable(null,
                  request));
            } else {
              throw new TableNotDisabledException(tableName);
            }
          }
          return ResponseConverter.buildListExecuteResponse(responses);
        } else if (executePlan instanceof TruncateTablePlan) {
          List<TruncateTableResponse> responses = new ArrayList<TruncateTableResponse>();
          for (String tableName : ((TruncateTablePlan) executePlan)
              .getTableNames()) {
            byte[] byteName = Bytes.toBytes(tableName);
            if (this.connection.isTableDisabled(byteName)) {
              MasterAdminProtos.TruncateTableRequest request = RequestConverter
                  .buildTruncateTableRequest(byteName);
              responses.add(masterAdminKeepAliveConnection.truncateTable(null,
                  request));
            } else {
              throw new TableNotDisabledException(tableName);
            }
          }
        }
      }

      throw new ServiceException(new DoNotRetryIOException(
          "The instance of Plan is not supported. SQL:" + sql));
    } catch (ServiceException e) {
      LOG.error("ServiceException ", e);
      throw e;
    } catch (Throwable e) {
      LOG.error("Unexpected throwable object ", e);
      throw new ServiceException(e);
    }
  }

  public ExecuteResponse execute(List<String> sqls, boolean isTransaction, String sessionId) throws ServiceException {
    try {
      long beforeGenPlan = EnvironmentEdgeManager.currentTimeMillis();
      List<DMLPlan> dmlPlans = new ArrayList<DMLPlan>();
      for (String sql : sqls) {
        ParseContext context = new ParseContext();
        context.setTsr(TableSchemaCacheReader.getInstance(service
            .getConfiguration()));
        context.setSql(sql);
        context.setSessionId(sessionId);
        try {
            parser.generatePlan(context);
        } catch (RuntimeException e) {
          if (e instanceof ParserException || e instanceof SQLParseException) {
            throw new DoNotRetryIOException(e.getMessage(), e);
          } else {
            throw new ServiceException(new IOException(e));
          }
        } catch (UnsupportedException e) {
          throw e;
        }
        Plan executePlan = context.getPlan();
        if(executePlan instanceof DMLPlan) {
          dmlPlans.add((DMLPlan) executePlan);
        } else {
          //TODO throw exception
        }
      }

      ParseContext context = new ParseContext();
      context.setTsr(TableSchemaCacheReader.getInstance(service
          .getConfiguration()));
      context.setSessionId(sessionId);

      DMLTransactionPlan transactionPlan = WaspParser.generateTransactionPlan(context, dmlPlans);
      final long afterGenPlan = EnvironmentEdgeManager.currentTimeMillis();
      ((FServer) this.service).getMetrics().updateGenPlan(
          afterGenPlan - beforeGenPlan);
      addWriteMetricsCount(0, null, 1, EnvironmentEdgeManager.currentTimeMillis()
        - beforeGenPlan);

      List<WriteResultProto> writeResults = executeEngine
            .execDMLTransactionPlans(transactionPlan);
      return ResponseConverter.buildExecuteResponse(writeResults);
    } catch (ServiceException e) {
      LOG.error("ServiceException ", e);
      throw e;
    } catch (Throwable e) {
      LOG.error("Unexpected throwable object ", e);
      throw new ServiceException(e);
    }
  }

  private ClientProtos.ExecuteResponse modifyTable(
      MasterAdminKeepAliveConnection masterAdminKeepAliveConnection,
      FTable hTableDesc) throws ServiceException {
    MasterAdminProtos.ModifyTableRequest request = RequestConverter.buildModifyTableRequest(
        Bytes.toBytes(hTableDesc.getTableName()), hTableDesc);
    return ResponseConverter
        .buildExecuteResponse(masterAdminKeepAliveConnection.modifyTable(null,
            request));
  }

  private void addReadMetricsCount(int dataLen, String ip, int count, long time) {
    readRequestCount.addAndGet(count);
    readRequestTime.addAndGet((int) time);
  }

  private void addWriteMetricsCount(int dataLen, String ip, int count, long time) {
    writeRequestCount.addAndGet(count);
    writeRequestTime.addAndGet((int) time);
  }

  public float getAvgWriteTime() {
    int writeCount = writeRequestCount.get();
    int writeTime = writeRequestTime.get();
    if (writeCount > 0) {
      return (float) writeTime / writeCount;
    } else {
      return 0.0f;
    }
  }

  public float getAvgReadTime() {
    int readCount = readRequestCount.get();
    int readTime = readRequestTime.get();
    if (readCount > 0) {
      return (float) readTime / readCount;
    } else {
      return 0.0f;
    }
  }

  public void resetMetricsCount() {
    readRequestCount.getAndSet(0);
    readRequestTime.getAndSet(0);
    writeRequestCount.getAndSet(0);
    writeRequestTime.getAndSet(0);
  }

  @Override
  public void close() throws IOException {
    if(connection != null) {
      connection.close();
    }
    if(executeEngine != null) {
      executeEngine.close();
    }
  }

}
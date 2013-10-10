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
package com.alibaba.wasp.plan.execute;

import com.alibaba.wasp.DataType;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupLocation;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.UnknownSessionException;
import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.client.ClientProtocol;
import com.alibaba.wasp.client.FConnection;
import com.alibaba.wasp.client.FConnectionManager;
import com.alibaba.wasp.coprocessor.DoubleColumnInterpreter;
import com.alibaba.wasp.coprocessor.FloatColumnInterpreter;
import com.alibaba.wasp.coprocessor.IntegerColumnInterpreter;
import com.alibaba.wasp.coprocessor.WaspAggregationClient;
import com.alibaba.wasp.fserver.FServer;
import com.alibaba.wasp.fserver.FServerServices;
import com.alibaba.wasp.fserver.LeaseException;
import com.alibaba.wasp.fserver.LeaseListener;
import com.alibaba.wasp.fserver.Leases;
import com.alibaba.wasp.fserver.Leases.LeaseStillHeldException;
import com.alibaba.wasp.fserver.OperationStatus;
import com.alibaba.wasp.fserver.TwoPhaseCommit;
import com.alibaba.wasp.fserver.TwoPhaseCommitProtocol;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Field;
import com.alibaba.wasp.meta.StorageTableNameBuilder;
import com.alibaba.wasp.plan.AggregateQueryPlan;
import com.alibaba.wasp.plan.DMLTransactionPlan;
import com.alibaba.wasp.plan.DQLPlan;
import com.alibaba.wasp.plan.DeletePlan;
import com.alibaba.wasp.plan.GlobalQueryPlan;
import com.alibaba.wasp.plan.InsertPlan;
import com.alibaba.wasp.plan.LocalQueryPlan;
import com.alibaba.wasp.plan.UpdatePlan;
import com.alibaba.wasp.plan.action.Action;
import com.alibaba.wasp.plan.action.ColumnStruct;
import com.alibaba.wasp.plan.action.DeleteAction;
import com.alibaba.wasp.plan.action.GetAction;
import com.alibaba.wasp.plan.action.InsertAction;
import com.alibaba.wasp.plan.action.ScanAction;
import com.alibaba.wasp.plan.action.TransactionAction;
import com.alibaba.wasp.plan.action.UpdateAction;
import com.alibaba.wasp.plan.parser.AggregateInfo;
import com.alibaba.wasp.plan.parser.AggregateInfo.AggregateType;
import com.alibaba.wasp.protobuf.ProtobufUtil;
import com.alibaba.wasp.protobuf.RequestConverter;
import com.alibaba.wasp.protobuf.ResponseConverter;
import com.alibaba.wasp.protobuf.generated.ClientProtos;
import com.alibaba.wasp.protobuf.generated.MetaProtos;
import com.alibaba.wasp.session.ExecutionEngineSession;
import com.alibaba.wasp.session.SessionFactory;
import com.alibaba.wasp.util.ParserUtils;
import com.google.protobuf.ServiceException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 
 * ExecutionEngine responsibility to execute QueryPlan, InserPlan, UpdatePlan
 * and DeletePlan.
 * 
 */
public class ExecutionEngine implements Execution, Closeable {

  public static final Log LOG = LogFactory.getLog(ExecutionEngine.class);

  /** FServer reference **/
  private final FServer server;

  private Leases leases;
  protected final ConcurrentHashMap<String, ExecutionEngineSession> sessions = new ConcurrentHashMap<String, ExecutionEngineSession>();
  private final Random rand = new Random();
  private final FConnection connection;
  private final TwoPhaseCommitProtocol twoPhaseCommit;
  private final int sessionLeaseTimeoutPeriod;

  /**
   * Default constructor.
   * 
   * @param server
   */
  public ExecutionEngine(FServer server) throws MetaException,
      ZooKeeperConnectionException {
    super();
    this.server = server;
    this.connection = FConnectionManager.getConnection(server
        .getConfiguration());
    this.twoPhaseCommit = TwoPhaseCommit.getInstance(server.getActionManager());
    this.leases = new Leases(server.getConfiguration().getInt(
        FConstants.THREAD_WAKE_FREQUENCY,
        FConstants.DEFAULT_THREAD_WAKE_FREQUENCY));
    this.sessionLeaseTimeoutPeriod = this.server.getConfiguration().getInt(
        FConstants.WASP_CLIENT_SESSION_TIMEOUT_PERIOD,
        FConstants.DEFAULT_WASP_CLIENT_SESSION_TIMEOUT_PERIOD);
  }

  /**
   * Execute query plan.
   * 
   * @param plan
   * @return
   */
  @Override
  public Pair<Boolean, Pair<String, Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>>>> execQueryPlan(
      DQLPlan plan, String sessionId, boolean closeSession)
      throws ServiceException {
    Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>> results = null;
    try {
      String sessionName = null;
      if (sessionId != null) {
        sessionName = sessionId;
      }

      Leases.Lease lease = null;
      ExecutionEngineSession session = null;

      boolean lastScan = false;
      if (StringUtils.isNotEmpty(sessionId)) {
        session = sessions.get(sessionName);
        if (!closeSession) {
          if (session == null) {
            throw new UnknownSessionException("Name: " + sessionName
                + ", already closed? sessions size is " + sessions.size());
          }
        }
      } else {
        session = addSession();
        sessionName = session.getSessionId();
        if (plan instanceof LocalQueryPlan) {
          LocalQueryPlan localQueryPlan = (LocalQueryPlan) plan;
          if (localQueryPlan.getScanAction() != null) {
            session.setExecutor(new LocalScanExecutor(localQueryPlan));
          } else if (localQueryPlan.getGetAction() != null) {
            localQueryPlan.getGetAction().setSessionId(sessionName);
            session.setExecutor(new LocalGetExecutor(localQueryPlan));
          }
        } else if (plan instanceof GlobalQueryPlan) {
          session.setExecutor(new GlobalQueryExecutor((GlobalQueryPlan) plan,
              server));
        } else if (plan instanceof AggregateQueryPlan) {
          session.setExecutor(new AggregateQueryExecutor(
              (AggregateQueryPlan) plan, server));
        }
      }

      if (closeSession) {
        session = sessions.remove(sessionName);
        if (session != null) {
          session.close();
          try {
            leases.cancelLease(sessionName);
          } catch (LeaseException e) {
            LOG.warn("Lease " + sessionName
                + "does't exsit,may has been closed.");
          }
          session = null;
        }
        return new Pair<Boolean, Pair<String, Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>>>>(
            lastScan,
            new Pair<String, Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>>>(
                sessionName,
                new Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>>(
                    new ArrayList<ClientProtos.QueryResultProto>(),
                    new ArrayList<ClientProtos.StringDataTypePair>())));
      } else {
        ExecutionEngineSession.QueryExecutor executor = null;
        try {
          lease = leases.removeLease(sessionName);
          executor = (ExecutionEngineSession.QueryExecutor) session
              .getExecutor();
          results = executor.execute();
          lastScan = executor.isLastScan();
          return new Pair<Boolean, Pair<String, Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>>>>(
              lastScan,
              new Pair<String, Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>>>(
                  sessionName, results));
        } finally {
          // We're done. On way out re-add the above removed lease.
          // Adding resets expiration time on lease.
          if (!lastScan) {
            if (sessions.containsKey(sessionName)) {
              if (lease != null) {
                leases.addLease(lease);
              }
            }
          } else {
            session = sessions.remove(sessionName);
            if (session != null) {
              session.close();
              session = null;
            }
          }
        }
      }
    } catch (Throwable ie) {
      throw new ServiceException(ie);
    }
  }

  private ExecutionEngineSession addSession() throws LeaseStillHeldException {
    long sessionId = -1;
    ExecutionEngineSession session;
    while (true) {
      sessionId = rand.nextLong();
      if (sessionId == -1) {
        continue;
      }
      String sessionName = String.valueOf(sessionId);
      session = SessionFactory.createExecutionEngineSession(sessionName, null);
      ExecutionEngineSession existing = sessions.putIfAbsent(sessionName,
          session);
      if (existing == null) {
        this.leases.createLease(sessionName, this.sessionLeaseTimeoutPeriod,
            new SessionListener(sessionName));
        break;
      }
    }
    return session;
  }

  /**
   * find that if the target ServerName is the same as the FServer
   * 
   * @param services
   * @param egSeverName
   * @return
   */
  private boolean workingOnLocalServer(FServerServices services,
      ServerName egSeverName) {
    String egHostnamePort = egSeverName.getHostAndPort();
    String fsHostnamePort = services.getServerName().getHostAndPort();
    if (egHostnamePort == null || fsHostnamePort == null) {
      return false;
    }
    return egHostnamePort.equals(fsHostnamePort);
  }

  /**
   * Execute UpdatePlan with some regular 1. when only have one Action in the
   * UpdatePlan. it will update without 2pc 2. If not 2pc. if the Action execute
   * on local EntityGroup. just call
   * {@link com.alibaba.wasp.fserver.FServer#update(byte[], com.alibaba.wasp.plan.action.UpdateAction)} ,
   * otherwise it needed process the action by RPC. 3. If 2pc. just call
   * {@link com.alibaba.wasp.fserver.TwoPhaseCommitProtocol#submit(java.util.Map)}
   *
   * @param plan
   */
  @Override
  public List<ClientProtos.WriteResultProto> execUpdatePlan(UpdatePlan plan)
      throws ServiceException {
    List<ClientProtos.WriteResultProto> writeResultProtos = new ArrayList<ClientProtos.WriteResultProto>();
    List<UpdateAction> updateActions = plan.getActions();
    // if actions > 1. will commit by 2pc
    if (updateActions.size() == 1) {
      UpdateAction action = updateActions.get(0);
      EntityGroupInfo entityGroupInfo = action.getEntityGroupLocation()
          .getEntityGroupInfo();
      ServerName serverName = new ServerName(action.getEntityGroupLocation()
          .getHostname(), action.getEntityGroupLocation().getPort(),
          ServerName.NON_STARTCODE);
      try {
        if (workingOnLocalServer(server, serverName)) {
          ClientProtos.UpdateResponse response = server.update(
              entityGroupInfo.getEntityGroupName(), action);
          writeResultProtos.add(response.getResult());
        } else {
          ClientProtocol clientProtocol = connection.getClient(
              serverName.getHostname(), serverName.getPort());
          ClientProtos.UpdateResponse response = clientProtocol.update(null,
              RequestConverter.buildUpdateRequest(action));
          writeResultProtos.add(response.getResult());
        }
      } catch (ServiceException e) {
        if (e.getCause() != null && e.getCause() instanceof IOException) {
          connection.clearCaches(serverName.getHostAndPort());
        }
        throw e;
      } catch (Exception e) {
        if (e instanceof IOException) {
          connection.clearCaches(serverName.getHostAndPort());
        }
        throw new ServiceException(e);
      }
    } else if (updateActions.size() > 1) {
      Map<EntityGroupInfo, List<Action>> actionMap = getActionMap(plan
          .getActions());
      boolean ret = twoPhaseCommit.submit(actionMap);
      OperationStatus status = ret ? OperationStatus.SUCCESS
          : OperationStatus.FAILURE;
      writeResultProtos.add(ResponseConverter.buildUpdateResponse(status)
          .getResult());
    }
    LOG.debug("ExecUpdatePlan:" + plan.toString() + ",SUCCESS:"
        + writeResultProtos.size());
    return writeResultProtos;
  }

  /**
   * for 2pc, the Actions in the same EntityGroup will be in the identity
   * submit.
   *
   * @param actions
   * @return
   */
  private Map<EntityGroupInfo, List<Action>> getActionMap(
      List<? extends Action> actions) {
    Map<EntityGroupInfo, List<Action>> actionMap = new HashMap<EntityGroupInfo, List<Action>>();
    for (Action action : actions) {
      EntityGroupInfo egi = action.getEntityGroupLocation()
          .getEntityGroupInfo();
      List<Action> egActions = actionMap.get(egi);
      if (egActions == null) {
        egActions = new ArrayList<Action>();
        actionMap.put(egi, egActions);
      }
      egActions.add(action);
    }
    return actionMap;
  }

  /**
   * Execute InsertPlan with some regular 1. when only have one Action in the
   * InsertPlan. it will insert without 2pc 2. If not 2pc. if the Action execute
   * on local EntityGroup. just call
   * {@link com.alibaba.wasp.fserver.FServer#insert(byte[], com.alibaba.wasp.plan.action.InsertAction)} ,
   * otherwise it needed process the action by RPC. 3. If 2pc. just call
   * {@link com.alibaba.wasp.fserver.TwoPhaseCommitProtocol#submit(java.util.Map)}
   *
   * @param plan
   * @return
   */
  @Override
  public List<ClientProtos.WriteResultProto> execInsertPlan(InsertPlan plan)
      throws ServiceException {
    List<ClientProtos.WriteResultProto> writeResultProtos = new ArrayList<ClientProtos.WriteResultProto>();
    List<InsertAction> actions = plan.getActions();
    InsertAction action = actions.get(0);
    EntityGroupInfo entityGroupInfo = action.getEntityGroupLocation()
        .getEntityGroupInfo();
    ServerName serverName = new ServerName(action.getEntityGroupLocation()
        .getHostname(), action.getEntityGroupLocation().getPort(),
        ServerName.NON_STARTCODE);
    // if actions > 1. will commit by 2pc
    if (actions.size() == 1) {
      try {

        if (workingOnLocalServer(server, serverName)) {
          ClientProtos.InsertResponse response = server.insert(
              entityGroupInfo.getEntityGroupName(), action);
          writeResultProtos.add(response.getResult());
        } else {
          ClientProtocol clientProtocol = connection.getClient(
              serverName.getHostname(), serverName.getPort());
          ClientProtos.InsertResponse response = clientProtocol.insert(null,
              RequestConverter.buildInsertRequest(action));
          writeResultProtos.add(response.getResult());
        }
      } catch (ServiceException e) {
        if (e.getCause() != null && e.getCause() instanceof IOException) {
          connection.clearCaches(serverName.getHostAndPort());
        }
        throw e;
      } catch (Exception e) {
        if (e instanceof IOException) {
          connection.clearCaches(serverName.getHostAndPort());
        }
        throw new ServiceException(e);
      }
    } else if (actions.size() > 1) {
      Map<EntityGroupInfo, List<Action>> actionMap = getActionMap(plan
          .getActions());
      boolean ret = twoPhaseCommit.submit(actionMap);
      OperationStatus status = ret ? OperationStatus.SUCCESS
          : OperationStatus.FAILURE;
      writeResultProtos.add(ResponseConverter.buildInsertResponse(status)
          .getResult());
    }
    LOG.debug("ExecInsertPlan:" + plan.toString() + ",SUCCESS:"
        + writeResultProtos.size());
    return writeResultProtos;
  }

  /**
   * Execute DeletePlan with some regular 1. when only have one Action in the
   * DeletePlan. it will delete without 2pc 2. If not 2pc. if the Action execute
   * on local EntityGroup. just call
   * {@link com.alibaba.wasp.fserver.FServer#delete(byte[], com.alibaba.wasp.plan.action.DeleteAction)} ,
   * otherwise it needed process the action by RPC. 3. If 2pc. just call
   * {@link com.alibaba.wasp.fserver.TwoPhaseCommitProtocol#submit(java.util.Map)}
   * 
   * @param plan
   * @return
   */
  @Override
  public List<ClientProtos.WriteResultProto> execDeletePlan(DeletePlan plan)
      throws ServiceException {
    List<ClientProtos.WriteResultProto> writeResultProtos = new ArrayList<ClientProtos.WriteResultProto>();
    List<DeleteAction> actions = plan.getActions();
    // if actions > 1. will commit by 2pc
    if (actions.size() == 1) {
      DeleteAction action = actions.get(0);
      EntityGroupInfo entityGroupInfo = action.getEntityGroupLocation()
          .getEntityGroupInfo();
      ServerName serverName = new ServerName(action.getEntityGroupLocation()
          .getHostname(), action.getEntityGroupLocation().getPort(),
          ServerName.NON_STARTCODE);
      try {
        if (workingOnLocalServer(server, serverName)) {
          ClientProtos.DeleteResponse response = server.delete(
              entityGroupInfo.getEntityGroupName(), action);
          writeResultProtos.add(response.getResult());
        } else {
          ClientProtocol clientProtocol = connection.getClient(
              serverName.getHostname(), serverName.getPort());
          ClientProtos.DeleteResponse response = clientProtocol.delete(null,
              RequestConverter.buildDeleteRequest(action));
          writeResultProtos.add(response.getResult());
        }
      } catch (ServiceException e) {
        if (e.getCause() != null && e.getCause() instanceof IOException) {
          connection.clearCaches(serverName.getHostAndPort());
        }
        throw e;
      } catch (Exception e) {
        if (e instanceof IOException) {
          connection.clearCaches(serverName.getHostAndPort());
        }
        throw new ServiceException(e);
      }
    } else if (actions.size() > 1) {
      Map<EntityGroupInfo, List<Action>> actionMap = getActionMap(plan
          .getActions());
      boolean ret = twoPhaseCommit.submit(actionMap);
      OperationStatus status = ret ? OperationStatus.SUCCESS
          : OperationStatus.FAILURE;
      writeResultProtos.add(ResponseConverter.buildDeleteResponse(status)
          .getResult());
    }
    LOG.debug("ExecDeletePlan:" + plan.toString() + ",SUCCESS:"
        + writeResultProtos.size());
    return writeResultProtos;
  }

  @Override
  public List<ClientProtos.WriteResultProto> execDMLTransactionPlans(
      DMLTransactionPlan plan) throws ServiceException {
    List<ClientProtos.WriteResultProto> writeResultProtos = new ArrayList<ClientProtos.WriteResultProto>();
    List<TransactionAction> actions = plan.getActions();
    if (actions.size() == 1) {
      TransactionAction action = actions.get(0);
      EntityGroupInfo entityGroupInfo = action.getEntityGroupLocation()
          .getEntityGroupInfo();
      ServerName serverName = new ServerName(action.getEntityGroupLocation()
          .getHostname(), action.getEntityGroupLocation().getPort(),
          ServerName.NON_STARTCODE);
      try {
        if (workingOnLocalServer(server, serverName)) {
          ClientProtos.TransactionResponse response = server.transaction(
              entityGroupInfo.getEntityGroupName(), action);
          writeResultProtos.add(response.getResult());
        } else {
          ClientProtocol clientProtocol = connection.getClient(
              serverName.getHostname(), serverName.getPort());
          ClientProtos.TransactionResponse response = clientProtocol
              .transaction(null,
                  RequestConverter.buildTransactionRequest(action));
          writeResultProtos.add(response.getResult());
        }
      } catch (ServiceException e) {
        if (e.getCause() != null && e.getCause() instanceof IOException) {
          connection.clearCaches(serverName.getHostAndPort());
        }
        throw e;
      } catch (Exception e) {
        if (e instanceof IOException) {
          connection.clearCaches(serverName.getHostAndPort());
        }
        throw new ServiceException(e);
      }
    } else {
      // TODO
    }

    LOG.debug("execDMLPlans:" + plan.toString() + ",SUCCESS:"
        + writeResultProtos.size());
    return writeResultProtos;
  }

  @Override
  public void close() throws IOException {
    if (connection != null) {
      connection.close();
    }
  }

  /**
   * Instantiated as a session lease. If the lease times out, the scanner is
   * closed
   */
  public class SessionListener implements LeaseListener {
    private final String sessionName;

    protected SessionListener(final String n) {
      this.sessionName = n;
    }

    public void leaseExpired() {
      ExecutionEngineSession s = sessions.remove(this.sessionName);
      if (s != null) {
        LOG.info("Session " + this.sessionName + " lease expired.");
        try {
          s.close();
          s = null;
        } catch (IOException e) {
          LOG.error("Closing session for " + sessionName, e);
        }
      } else {
        LOG.info("Session " + this.sessionName + " lease expired.");
      }
    }
  }

  /**
   * Global query plan executor.
   */
  private class GlobalQueryExecutor implements
      ExecutionEngineSession.QueryExecutor {

    private boolean lastScan = false;

    private final FTable tableDesc;

    private final int fetchSize;

    private final GlobalQueryPlan plan;

    private final List<ClientProtos.QueryResultProto> queryResultProtos;

    private final List<ClientProtos.StringDataTypePair> nameDataTypePairs;

    private List<ClientProtos.QueryResultProto> resultList;

    private final FServer server;

    private long scannerId = -1;

    private boolean closed = true;

    private int limit = -1;

    /**
     * @param plan
     */
    public GlobalQueryExecutor(GlobalQueryPlan plan, FServer server) {
      this.plan = plan;
      this.fetchSize = this.plan.getFetchRows();
      this.queryResultProtos = new ArrayList<ClientProtos.QueryResultProto>(
          this.fetchSize);
      this.nameDataTypePairs = new ArrayList<ClientProtos.StringDataTypePair>();
      this.resultList = new ArrayList<ClientProtos.QueryResultProto>();
      this.server = server;
      this.tableDesc = plan.getTableDesc();
      this.limit = this.plan.getAction().getLimit();
    }

    /**
     * @see com.alibaba.wasp.session.ExecutionEngineSession.Executor#execute()
     */
    @Override
    public Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>> execute()
        throws ServiceException {
      try {
        queryResultProtos.clear();
        nameDataTypePairs.clear();
        ClientProtos.ScanResponse response = null;
        ScanAction action = plan.getAction();
        do {
          response = server.scan(null, action, scannerId == -1 ? false : true,
              scannerId, false, true, tableDesc);
          scannerId = response.getScannerId();
          resultList.addAll(response.getResultList());
          if (nameDataTypePairs.isEmpty()) {
            nameDataTypePairs.addAll(response.getMetaList());
          }
          if (limit != -1) {
            queryResultProtos.addAll(resultList);
            resultList.clear();
            server.scan(null, action, true, scannerId, true, true, tableDesc);
            lastScan = true;
            scannerId = -1;
            closed = true;
            break;
          } else {
            if (response.getResultList().size() == 0) {
              server.scan(null, action, true, scannerId, true, true, tableDesc);
              lastScan = true;
              scannerId = -1;
              closed = true;
              break;
            } else {
              if (resultList.size() <= fetchSize) {
                queryResultProtos.addAll(resultList);
                resultList.clear();
                continue;
              } else {
                queryResultProtos.addAll(resultList.subList(0, fetchSize));
                resultList = resultList.subList(fetchSize,
                    resultList.size() - 1);
                break;
              }
            }
          }
        } while (scannerId != -1);
      } catch (ServiceException e) {
        throw e;
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      return new Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>>(
          queryResultProtos, nameDataTypePairs);
    }

    /**
     * @see com.alibaba.wasp.session.ExecutionEngineSession.Executor#close()
     */
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      } else {
        // queryResultProtos.clear();
        // nameDataTypePairs.clear();
        // resultList.clear();
        if (scannerId != -1) {
          try {
            server.scan(null, plan.getAction(), true, scannerId, true, true,
                tableDesc);
          } catch (ServiceException e) {
            throw new IOException(e.getCause());
          }
          scannerId = -1;
        }
      }
      closed = true;
    }

    /**
     * @see com.alibaba.wasp.session.ExecutionEngineSession.QueryExecutor#isLastScan()
     */
    @Override
    public boolean isLastScan() {
      return lastScan;
    }
  }

  /**
   * Aggregate query plan executor.
   */
  private class AggregateQueryExecutor implements
      ExecutionEngineSession.QueryExecutor {

    private boolean lastScan = true;

    private final FTable tableDesc;

    private final AggregateQueryPlan plan;

    private final List<ClientProtos.QueryResultProto> queryResultProtos;

    private final List<ClientProtos.StringDataTypePair> nameDataTypePairs;

    private final FServer server;

    private boolean closed = true;

    private final WaspAggregationClient aggregationClient;

    /**
     * @param plan
     */
    public AggregateQueryExecutor(AggregateQueryPlan plan, FServer server) {
      this.plan = plan;
      this.queryResultProtos = new ArrayList<ClientProtos.QueryResultProto>();
      this.nameDataTypePairs = new ArrayList<ClientProtos.StringDataTypePair>();
      this.server = server;
      this.tableDesc = plan.getTableDesc();
      this.aggregationClient = new WaspAggregationClient(server.getConfiguration());
    }

    /**
     * @see com.alibaba.wasp.session.ExecutionEngineSession.Executor#execute()
     */
    @Override
    public Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>> execute()
        throws ServiceException {
      queryResultProtos.clear();
      nameDataTypePairs.clear();
      ScanAction action = plan.getAction();
      List<ColumnStruct> columnStructs = action.getNotIndexConditionColumns();
      AggregateInfo info = plan.getQueryInfo().getAggregateInfo();
      Scan scan = new Scan();

      scan.addColumn(Bytes.toBytes(info.getField().getFamily()),
          Bytes.toBytes(info.getField().getName()));

      for (ColumnStruct col : columnStructs) {
        if(info.getField().getFamily().equals(col.getFamilyName())
            && info.getField().getName().equals(col.getColumnName())) {
          continue;
        }
        scan.addColumn(Bytes.toBytes(col.getFamilyName()),
            Bytes.toBytes(col.getColumnName()));
      }

      FilterList filters = new FilterList();
      // Filter columnNotNullFilter = new SingleColumnValueFilter(
      // Bytes.toBytes(info.getField().getFamily()),
      // Bytes.toBytes(info.getField().getName()),
      // CompareFilter.CompareOp.NOT_EQUAL,
      // HConstants.EMPTY_BYTE_ARRAY
      // );
      for (ColumnStruct columnStruct : columnStructs) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
            Bytes.toBytes(columnStruct.getFamilyName()),
            Bytes.toBytes(columnStruct.getColumnName()),
            ParserUtils.convertIntValueToCompareOp(columnStruct.getCompareOp()),
            columnStruct.getValue());
        filters.addFilter(filter);
      }
      scan.setFilter(filters);

      String tableName =  StorageTableNameBuilder.buildEntityTableName(plan.getTableDesc().getTableName());

      AggregateType type = info.getAggregateType();

      byte[] value = null;
      DataType resultDataType = DataType.STRING;

      try {
        switch (type) {
          case COUNT: {
            long count = aggregationClient.count(Bytes.toBytes(tableName),
                null, scan);
            value = Bytes.toBytes(count);
            resultDataType = DataType.INT64;
            break;
          }
          case SUM: {
            value = getSumValue(tableName, info.getField(), scan);
            resultDataType = info.getField().getType();
            break;
          }
        }
      } catch (Throwable throwable) {
        throw new ServiceException(throwable);
      }

      ClientProtos.QueryResultProto.Builder builder = ClientProtos.QueryResultProto.newBuilder();
            builder.addResult(ProtobufUtil.toStringBytesPair(new KeyValue(Bytes.toBytes(type.getMethodName()),
                Bytes.toBytes(info.getField().getFamily()), Bytes.toBytes(type.getMethodName()), value)));
            queryResultProtos.add(builder.build());

      MetaProtos.DataTypeProtos dataTypeProtos = DataType.convertDataTypeToDataTypeProtos(resultDataType) ;
            ClientProtos.StringDataTypePair.Builder stringDataTypePairBuilder = ClientProtos.StringDataTypePair
              .newBuilder();
      stringDataTypePairBuilder.setDataType(dataTypeProtos);
      stringDataTypePairBuilder.setName(type.getMethodName());
      nameDataTypePairs.add(stringDataTypePairBuilder.build());

      return new Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>>(
          queryResultProtos, nameDataTypePairs);
    }

    private byte[] getSumValue(String tableName, Field field, Scan scan) throws Throwable {
      switch (field.getType()) {
        case INT32: {
          Integer sum = aggregationClient
                    .sum(Bytes.toBytes(tableName), new IntegerColumnInterpreter(),
                        scan, Bytes.toBytes(field.getName()));
          return Bytes.toBytes(sum == null ? 0 : sum.intValue());
        }
        case INT64: {
          Long sum = aggregationClient
                    .sum(Bytes.toBytes(tableName), new LongColumnInterpreter(),
                        scan, Bytes.toBytes(field.getName()));
          return Bytes.toBytes(sum == null ? 0 : sum.longValue());
        }
        case FLOAT: {
          Float sum = aggregationClient
                    .sum(Bytes.toBytes(tableName), new FloatColumnInterpreter(),
                        scan, Bytes.toBytes(field.getName()));
          return Bytes.toBytes(sum == null ? 0 : sum.floatValue());
        }
        case DOUBLE: {
          Double sum = aggregationClient
                    .sum(Bytes.toBytes(tableName), new DoubleColumnInterpreter(),
                        scan, Bytes.toBytes(field.getName()));
          return Bytes.toBytes(sum == null ? 0 : sum.doubleValue());
        }
      }
      return null;
    }

    /**
     * @see com.alibaba.wasp.session.ExecutionEngineSession.Executor#close()
     */
    @Override
    public void close() throws IOException {
      closed = true;
    }

    /**
     * @see com.alibaba.wasp.session.ExecutionEngineSession.QueryExecutor#isLastScan()
     */
    @Override
    public boolean isLastScan() {
      return lastScan;
    }
  }

  /**
   * Local query plan executor.
   */
  private class LocalScanExecutor implements
      ExecutionEngineSession.QueryExecutor {

    private boolean closed = false;

    private boolean lastScan = false;

    private final int fetchSize;

    private final LocalQueryPlan plan;

    private final List<ClientProtos.QueryResultProto> queryResultProtos;

    private final List<ClientProtos.StringDataTypePair> nameDataTypePairs;

    private List<ClientProtos.QueryResultProto> resultList;

    private long scannerId = -1;

    private boolean localScan = false;

    private ServerName serverName = null;

    private int limit = -1;

    /**
     * @param plan
     */
    public LocalScanExecutor(LocalQueryPlan plan) {
      this.plan = plan;
      this.fetchSize = this.plan.getFetchRows();
      this.queryResultProtos = new ArrayList<ClientProtos.QueryResultProto>(
          this.fetchSize);
      this.nameDataTypePairs = new ArrayList<ClientProtos.StringDataTypePair>();
      this.resultList = new ArrayList<ClientProtos.QueryResultProto>();
      this.limit = this.plan.getScanAction().getLimit();
    }

    @Override
    public Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>> execute()
        throws ServiceException {
      try {
        queryResultProtos.clear();
        ScanAction action = plan.getScanAction();
        EntityGroupLocation entityGroupLocation = action
            .getEntityGroupLocation();
        EntityGroupInfo entityGroupInfo = entityGroupLocation
            .getEntityGroupInfo();
        serverName = new ServerName(entityGroupLocation.getHostname(),
            entityGroupLocation.getPort(), ServerName.NON_STARTCODE);
        ClientProtos.ScanResponse response = null;
        // local
        localScan = workingOnLocalServer(server, serverName);
        do {
          if (localScan) {// if the target entityGroup's server is current
                          // server, run it in local.
            response = server.scan(entityGroupInfo.getEntityGroupName(),
                action, scannerId == -1 ? false : true, scannerId, false);
            scannerId = response.getScannerId();
          } else {// rpc
            ClientProtocol clientProtocol = connection.getClient(
                serverName.getHostname(), serverName.getPort());
            response = clientProtocol.scan(null,
                RequestConverter.buildScanRequest(action, scannerId, false));
            scannerId = response.getScannerId();
          }
          if (nameDataTypePairs.isEmpty()) {
            nameDataTypePairs.addAll(response.getMetaList());
          }
          resultList.addAll(response.getResultList());
          if (limit != -1) {
            queryResultProtos.addAll(resultList);
            resultList.clear();
            if (localScan) {
              server.scan(null, action, true, scannerId, true);
            } else {
              ClientProtocol clientProtocol = connection.getClient(
                  serverName.getHostname(), serverName.getPort());
              clientProtocol.scan(null,
                  RequestConverter.buildScanRequest(action, scannerId, true));
            }
            lastScan = true;
            scannerId = -1;
            closed = true;
            break;
          } else {
            if (response.getResultList().size() == 0 && resultList.size() == 0) {
              if (localScan) {
                server.scan(null, action, true, scannerId, true);
              } else {
                ClientProtocol clientProtocol = connection.getClient(
                    serverName.getHostname(), serverName.getPort());
                clientProtocol.scan(null,
                    RequestConverter.buildScanRequest(action, scannerId, true));
              }
              lastScan = true;
              scannerId = -1;
              closed = true;
              break;
            } else {
              if (resultList.size() <= fetchSize) {
                queryResultProtos.addAll(resultList);
                resultList.clear();
                continue;
              } else {
                queryResultProtos.addAll(resultList.subList(0, fetchSize));
                resultList = resultList.subList(fetchSize - 1,
                    resultList.size() - 1);
                break;
              }
            }
          }
        } while (scannerId != -1);
      } catch (ServiceException e) {
        throw e;
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      return new Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>>(
          queryResultProtos, nameDataTypePairs);
    }

    /**
     * @return the lastScan
     */
    public boolean isLastScan() {
      return lastScan;
    }

    /**
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      } else {
        if (localScan) {
          // queryResultProtos.clear();
          // nameDataTypePairs.clear();
          // resultList.clear();
          try {
            server.scan(null, null, true, scannerId, true);
          } catch (ServiceException e) {
            throw new IOException(e.getCause());
          }
        } else {
          ClientProtocol clientProtocol = connection.getClient(
              serverName.getHostname(), serverName.getPort());
          try {
            clientProtocol.scan(null, RequestConverter.buildScanRequest(
                plan.getScanAction(), scannerId, true));
          } catch (ServiceException e) {
            throw new IOException(e.getCause());
          }
        }
        scannerId = -1;
      }
      closed = true;
    }
  }

  /**
   * Local query plan executor.
   */
  private class LocalGetExecutor implements
      ExecutionEngineSession.QueryExecutor {

    private final LocalQueryPlan plan;

    private final List<ClientProtos.QueryResultProto> queryResultProtos = new ArrayList<ClientProtos.QueryResultProto>(
        1);

    private final List<ClientProtos.StringDataTypePair> nameDataTypePairs;

    /**
     * @param plan
     */
    public LocalGetExecutor(LocalQueryPlan plan) {
      this.plan = plan;
      this.nameDataTypePairs = new ArrayList<ClientProtos.StringDataTypePair>();
    }

    @Override
    public Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>> execute()
        throws ServiceException {
      try {
        queryResultProtos.clear();

        GetAction action = plan.getGetAction();
        EntityGroupLocation entityGroupLocation = action
            .getEntityGroupLocation();
        EntityGroupInfo entityGroupInfo = entityGroupLocation
            .getEntityGroupInfo();
        ServerName serverName = new ServerName(
            entityGroupLocation.getHostname(), entityGroupLocation.getPort(),
            ServerName.NON_STARTCODE);
        ClientProtos.GetResponse response = null;
        // local
        boolean localGet = workingOnLocalServer(server, serverName);
        if (localGet) {// if the target entityGroup's server is current
                       // server, run it in local.
          response = server.get(entityGroupInfo.getEntityGroupName(), action);
        } else {// rpc
          ClientProtocol clientProtocol = connection.getClient(
              serverName.getHostname(), serverName.getPort());
          response = clientProtocol.get(null,
              RequestConverter.buildGetRequest(action));
        }
        if (response.getExists()) {
          queryResultProtos.add(response.getResult());
        }
        if (nameDataTypePairs.isEmpty()) {
          nameDataTypePairs.addAll(response.getMetaList());
        }
      } catch (ServiceException e) {
        throw e;
      } catch (Exception e) {
        throw new ServiceException(e);
      }
      return new Pair<List<ClientProtos.QueryResultProto>, List<ClientProtos.StringDataTypePair>>(
          queryResultProtos, nameDataTypePairs);
    }

    /**
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
      // queryResultProtos.clear();
      // nameDataTypePairs.clear();
    }

    @Override
    public boolean isLastScan() {
      return true;
    }
  }
}

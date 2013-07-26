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
package com.alibaba.wasp.jdbc.result;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Pair;
import com.alibaba.wasp.ClientConcernedException;
import com.alibaba.wasp.DataType;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.client.ExecuteResult;
import com.alibaba.wasp.client.FClient;
import com.alibaba.wasp.fserver.OperationStatus;
import com.alibaba.wasp.jdbc.JdbcException;
import com.alibaba.wasp.jdbc.value.Value;
import com.alibaba.wasp.jdbc.value.ValueInt;
import com.alibaba.wasp.session.QuerySession;
import com.alibaba.wasp.util.New;
import com.alibaba.wasp.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The client side part of a result set that is kept on the server. In many
 * cases, the complete data is kept on the client side, but for large results
 * only a subset is in-memory.
 */
public class ResultRemote implements ResultInterface {

  private Log log = LogFactory.getLog(ResultRemote.class);

  private int fetchSize;
  private QuerySession session;
  private int id;
  // private final ResultColumn[] columns;
  private Value[] currentRow;
  private int rowCount;
  private int rowId, rowOffset;
  private ArrayList<Value[]> result;
  private String tablename;
  private HashMap<Integer, String> aliasLabelMap = New.hashMap();
  private boolean lableAliasInited = false;
  private final FClient fClient;
  private final String sql;
  private boolean isQuery = false;
  // the interface is query
  private boolean isExecuteQuery = false;

  private boolean isLastScan = false;

  private transient boolean isClose;

  public ResultRemote(FClient fClient, QuerySession session,
      ResultTransfer transfer, String sql, int id, int fetchSize,
      boolean isExecuteQuery, ReadModel readModel) throws IOException,
      SQLException {
    this.session = session;
    this.id = id;
    this.tablename = transfer.getTablename();
    rowId = -1;
    result = New.arrayList();
    this.fetchSize = fetchSize;
    this.sql = sql;
    this.fClient = fClient;
    this.isExecuteQuery = isExecuteQuery;
    fetchRows(false);
  }

  public ResultRemote(FClient fClient, QuerySession session,
      ResultTransfer transfer, String sql, ReadModel readModel)
      throws IOException, SQLException {
    this(fClient, session, transfer, sql, 0, 0, false, readModel);
  }

  private static void mapColumn(HashMap<Integer, String> map, String label,
      int index) {
    // put the index (usually that's the only operation)
    String old = map.put(index, label);
    if (old != null) {
      // if there was a clash (which is seldom),
      // put the old one back
      map.put(index, old);
    }
  }

  @Override
  public String getColumnName(int i) {
    return aliasLabelMap.get(i);
  }

  @Override
  public long getColumnPrecision(int i) {
    return -1;
  }

  @Override
  public int getColumnScale(int i) {
    return -1;
  }

  @Override
  public int getNullable(int i) {
    return -1;
  }

  @Override
  public void reset() {
    rowId = -1;
    currentRow = null;
    if (session == null) {
      return;
    }
    synchronized (session) {
      session.checkClosed();
    }
  }

  @Override
  public boolean isQuery() {
    return this.isQuery || this.isExecuteQuery;
  }

  @Override
  public Value[] currentRow() {
    return currentRow;
  }

  @Override
  public boolean next() throws SQLException {
    if (rowId < rowCount) {
      rowId++;
      remapIfOld();
      if (rowId < rowCount) {
        if (rowId - rowOffset >= result.size()) {
          if (isLastScan) {
            return false;
          }
          fetchRows(true);
        }
        currentRow = result.get(rowId - rowOffset);
        return true;
      }
      currentRow = null;
    }
    return false;
  }

  @Override
  public int getRowId() {
    return rowId;
  }

  @Override
  public int getVisibleColumnCount() {
    return aliasLabelMap.size();
  }

  @Override
  public int getRowCount() {
    return rowCount;
  }

  private void sendClose() throws IOException {
    if (isClose || session == null) {
      return;
    }
    this.fClient.closeSession(this.session.getSessionId());
    session = null;
  }

  @Override
  public synchronized void close() {
    result = null;
    try {
      sendClose();
      isClose = true;
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void remapIfOld() {
    if (session == null) {
      return;
    }
    if (id <= session.getCurrentId()) {
      // object is too old - we need to map it to a new id
      int newId = session.getNextId();
      id = newId;
    }
  }

  private void fetchRows(boolean sendFetch) throws SQLException {
    synchronized (session) {
      rowOffset += result.size();
      result.clear();
      if (isClose) {
        return;
      }
      int fetch = Math.min(fetchSize, rowCount - rowOffset);
      Pair<String, Pair<Boolean, List<ExecuteResult>>> pair = null;
      try {
        if (rowOffset == 0 && rowOffset < fetchSize
            && session.getSessionId() == null) {
          pair = fClient.execute(sql, ReadModel.CURRENT, fetchSize);
          session.setSessionId(pair.getFirst());
        } else if (rowOffset < fetch && session.getSessionId() != null) {
          pair = fClient.next(session.getSessionId());
        } else if (!isExecuteQuery) {
          pair = fClient.execute(sql);
        }
      } catch (IOException ioe) {
        if (ioe instanceof ClientConcernedException) {
          ClientConcernedException exception = (ClientConcernedException) ioe;
          throw new SQLException(JdbcException.get(exception.getErrorCode(),
              ioe, ioe.getMessage()));
        }

        log.error("FClinet execute sql error", ioe);
        throw new RuntimeException(ioe);
      }

      // if the query result is null
      if (pair == null) {
        return;
      }

      Pair<Boolean, List<ExecuteResult>> resultPair = pair.getSecond();
      if (resultPair == null) {
        return;
      }

      List<ExecuteResult> executeResults = resultPair.getSecond();

      isLastScan = resultPair.getFirst();

      int effectNums = 0;

      for (ExecuteResult executeResult : executeResults) {
        isQuery = executeResult.isQuery();
        // Query
        if (executeResult.isQuery()) {
          Map<String, Pair<DataType, byte[]>> ret = executeResult.getMap();

          List<Value> values = New.arrayList();

          int i = 0;
          for (Map.Entry<String, Pair<DataType, byte[]>> stringPairEntry : ret
              .entrySet()) {
            String columnLabel = stringPairEntry.getKey();
            if (columnLabel.equalsIgnoreCase("rowkey")) {
              continue;
            }
            if (lableAliasInited == false) {
              mapColumn(aliasLabelMap, columnLabel, i++);
            }
            Pair<DataType, byte[]> typeAndValue = ret.get(columnLabel);
            Value v = Value.toValue(typeAndValue.getSecond(),
                typeAndValue.getFirst());
            values.add(v);
          }
          result.add(values.toArray(new Value[values.size()]));
          lableAliasInited = true;
        } else {
          OperationStatus operationStatus = executeResult.getStatus();

          // write
          if ((FConstants.OperationStatusCode.SUCCESS.name())
              .equals(operationStatus.getOperationStatusCode().name())) {
            effectNums++;
          } else if (((FConstants.OperationStatusCode.FAILURE.name())
              .equals(operationStatus.getOperationStatusCode().name()))) {
            String exceptionClassName = operationStatus.getExceptionClassname();
            String exceptionMsg = operationStatus.getExceptionMsg();

            if (StringUtils.isNullOrEmpty(exceptionClassName)) {
              continue;
            }

            try {
              Class exceptionClazz = Class.forName(exceptionClassName);
              Constructor constructor = exceptionClazz
                  .getConstructor(String.class);
              Object exceptionObject = constructor.newInstance(exceptionMsg);
              if (exceptionObject instanceof ClientConcernedException) {
                ClientConcernedException exception = (ClientConcernedException) exceptionObject;
                throw new SQLException(JdbcException.get(
                    exception.getErrorCode(), exceptionMsg));
              }
            } catch (ClassNotFoundException e) {
              log.error("ClassNotFoundException with Exception class name "
                  + exceptionClassName, e);
              throw new RuntimeException(e);
            } catch (InstantiationException e) {
              log.error("InstantiationException ", e);
              throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
              log.error("IllegalAccessException ", e);
              throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
              log.error("NoSuchMethodException ", e);
              throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
              log.error("InvocationTargetException ", e);
              throw new RuntimeException(e);
            }
          }
        }
      }

      if (effectNums > 0) {
        Value[] values = new Value[1];
        values[0] = ValueInt.get(effectNums);
        result.clear();
        result.add(values);
      }
      rowCount = result.size();
    }
  }

  @Override
  public String toString() {
    return "columns: " + aliasLabelMap.size() + " rows: " + rowCount + " pos: "
        + rowId;
  }

  @Override
  public int getFetchSize() {
    return fetchSize;
  }

  @Override
  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  @Override
  public String getAlias(int i) {
    if (i > aliasLabelMap.size() - 1) {
      throw new JdbcException("the alias greater than the columns size");
    }
    return aliasLabelMap.get(i);
  }

  @Override
  public String getTableName(int i) {
    return tablename;
  }

  public boolean needToClose() {
    return true;
  }

}

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
package org.apache.wasp.jdbc.command;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.wasp.ReadModel;
import org.apache.wasp.SQLErrorCode;
import org.apache.wasp.client.FClient;
import org.apache.wasp.jdbc.JdbcException;
import org.apache.wasp.jdbc.expression.ParameterInterface;
import org.apache.wasp.jdbc.result.ResultInterface;
import org.apache.wasp.jdbc.result.ResultRemote;
import org.apache.wasp.jdbc.result.ResultTransfer;
import org.apache.wasp.jdbc.value.Value;
import org.apache.wasp.jdbc.value.ValueInt;
import org.apache.wasp.meta.Field;
import org.apache.wasp.session.RemoteSession;
import org.apache.wasp.session.SessionFactory;
import org.apache.wasp.util.New;

/**
 * Represents the client-side part of a SQL statement. This class is not used in
 * embedded mode.
 */
public class CommandRemote implements CommandInterface {

  private Log log = LogFactory.getLog(CommandRemote.class);

  private final ArrayList<ParameterInterface> parameters;
  private final String sql;
  private final int fetchSize;
  private RemoteSession session;
  private final FClient fClient;
  private int id;
  private boolean isQuery;
  private final int created;
  private final ReadModel readModel;

  public CommandRemote(FClient fClient, RemoteSession session, String sql,
      int fetchSize, ReadModel readModel) {
    this.sql = sql;
    parameters = New.arrayList();
    prepare(session, true);
    // set session late because prepare might fail - in this case we don't
    // need to close the object
    this.session = session;
    this.fClient = fClient;
    this.fetchSize = fetchSize;
    this.readModel = readModel;
    created = session.getLastReconnect();
  }

  private void prepare(RemoteSession s, boolean createParams) {
    if (fetchSize > 0) {
      isQuery = true;
    }
  }

  @Override
  public boolean isQuery() {
    return isQuery;
  }

  @Override
  public ArrayList<ParameterInterface> getParameters() {
    return parameters;
  }

  private void prepareIfRequired() {
    if (session.getLastReconnect() != created) {
      // in this case we need to prepare again in every case
      id = Integer.MIN_VALUE;
    }
    session.checkClosed();
    if (id <= session.getCurrentId()) {
      // object is too old - we need to prepare again
      prepare(session, false);
    }
  }

  @Override
  public ResultInterface getMetaData() {
    return null;
  }

  @Override
  public ResultInterface executeQuery(int maxRows) throws SQLException {
    checkParameters();
    synchronized (session) {
      int objectId = session.getNextId();
      ResultRemote result = null;
      prepareIfRequired();
      try {
        List<Field> fields = getFileds(sql);
        String tablename = getTableName(sql);
        ResultTransfer transfer = new ResultTransfer(fields,
            tablename, RemoteSession.COMMAND_EXECUTE_QUERY);
        result = new ResultRemote(fClient, SessionFactory.createQuerySession(), transfer, sql, objectId,
            fetchSize, true, readModel);
        isQuery = result.isQuery();
        if (!isQuery() && result.getRowCount() > 0) {
          throw JdbcException.get(SQLErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY,
              "sql:" + sql);
        }
      } catch (IOException e) {
        session.removeServer(e);
      }
      return result;
    }
  }

  private String getTableName(String sql) {
    return null; // To change body of created methods use File | Settings | File
                 // Templates.
  }

  private List<Field> getFileds(String sql) {
    return null; // To change body of created methods use File | Settings | File
                 // Templates.
  }

  @Override
  public int executeUpdate() throws SQLException {
    checkParameters();
    synchronized (session) {
      prepareIfRequired();
      ResultRemote result = null;
      String tablename = getTableName(sql);
      ResultTransfer transfer = new ResultTransfer(null, tablename,
          RemoteSession.COMMAND_EXECUTE_QUERY);
      try {
        result = new ResultRemote(fClient, SessionFactory.createQuerySession(), transfer, sql, readModel);
        result.next();
        isQuery = result.isQuery();
        if (isQuery()) {
          throw JdbcException.get(SQLErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY,
              "sql:" + sql);
        }
        Value[] values = result.currentRow();
        if (values == null || values.length == 0) {
          return 0;
        }
        if (values[0] instanceof ValueInt) {
          return ((ValueInt) values[0]).getInt();
        }

      } catch (IOException e) {
        session.removeServer(e);
      }
      return transfer.getCounts();
    }
  }

  private void checkParameters() {
    for (ParameterInterface p : parameters) {
      p.checkSet();
    }
  }

  @Override
  public void close() {
    if (session == null || session.isClosed()) {
      return;
    }
    session = null;
    try {
      for (ParameterInterface p : parameters) {
        Value v = p.getParamValue();
        if (v != null) {
          v.close();
        }
      }
    } catch (JdbcException e) {
      log.error("close", e);
    }
    parameters.clear();
  }

  /**
   * Cancel this current statement.
   */
  @Override
  public void cancel() {
    session.cancelStatement(id);
  }

  @Override
  public String toString() {
    return sql;
  }

  @Override
  public int getCommandType() {
    return UNKNOWN;
  }
}
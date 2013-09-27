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
package com.alibaba.wasp.protobuf;

import com.alibaba.wasp.DataType;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.fserver.OperationStatus;
import com.alibaba.wasp.ipc.ServerRpcController;
import com.alibaba.wasp.plan.action.ColumnStruct;
import com.alibaba.wasp.protobuf.generated.ClientProtos;
import com.alibaba.wasp.protobuf.generated.ClientProtos.QueryResultProto;
import com.alibaba.wasp.protobuf.generated.ClientProtos.StringDataTypePair;
import com.alibaba.wasp.protobuf.generated.ClientProtos.WriteResultProto;
import com.alibaba.wasp.protobuf.generated.ClientProtos.WriteResultProto.StatusCode;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetServerInfoResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.ServerInfo;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.SplitEntityGroupResponse;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DeleteTableResponse;
import com.alibaba.wasp.protobuf.generated.MetaProtos;
import com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper utility to build protocol buffer responses, or retrieve data from
 * protocol buffer responses.
 */
public final class ResponseConverter {

  private ResponseConverter() {
  }

  // Start utilities for Client

  // End utilities for Client
  // Start utilities for Admin

  /**
   * Get the list of entityGroup info from a GetOnlineRegionResponse
   * 
   * @param proto
   *          the GetOnlineRegionResponse
   * @return the list of entityGroup info
   */
  public static List<EntityGroupInfo> getEntityGroupInfos(
      final GetOnlineEntityGroupsResponse proto) {
    if (proto == null || proto.getEntityGroupInfosCount() == 0)
      return null;
    return ProtobufUtil.getEntityGroupInfos(proto);
  }

  /**
   * Get the entityGroup opening state from a OpenEntityGroupResponse
   * 
   * @param proto
   *          the OpenEntityGroupResponse
   * @return the entityGroup opening state
   */
  public static com.alibaba.wasp.fserver.EntityGroupOpeningState getEntityGroupOpeningState(
      final OpenEntityGroupResponse proto) {
    if (proto == null || proto.getOpeningStateCount() != 1)
      return null;
    return com.alibaba.wasp.fserver.EntityGroupOpeningState.valueOf(proto
        .getOpeningState(0).name());
  }

  /**
   * Get a list of entityGroup opening state from a OpenEntityGroupResponse
   * 
   * @param proto
   *          the OpenEntityGroupResponse
   * @return the list of entityGroup opening state
   */
  public static List<com.alibaba.wasp.fserver.EntityGroupOpeningState> getEntityGroupOpeningStateList(
      final OpenEntityGroupResponse proto) {
    if (proto == null)
      return null;
    List<com.alibaba.wasp.fserver.EntityGroupOpeningState> entityGroupOpeningStates = new ArrayList<com.alibaba.wasp.fserver.EntityGroupOpeningState>();
    for (int i = 0; i < proto.getOpeningStateCount(); i++) {
      entityGroupOpeningStates.add(com.alibaba.wasp.fserver.EntityGroupOpeningState
          .valueOf(proto.getOpeningState(i).name()));
    }
    return entityGroupOpeningStates;
  }

  /**
   * Check if the entityGroup is closed from a CloseEntityGroupResponse
   * 
   * @param proto
   *          the CloseEntityGroupResponse
   * @return the entityGroup close state
   */
  public static boolean isClosed(final CloseEntityGroupResponse proto) {
    if (proto == null || !proto.hasSuccess())
      return false;
    return proto.getSuccess();
  }

  /**
   * A utility to build a GetServerInfoResponse.
   * 
   * @param serverName
   * @param webuiPort
   * @return the response
   */
  public static GetServerInfoResponse buildGetServerInfoResponse(
      final ServerName serverName, final int webuiPort) {
    GetServerInfoResponse.Builder builder = GetServerInfoResponse.newBuilder();
    ServerInfo.Builder serverInfoBuilder = ServerInfo.newBuilder();
    serverInfoBuilder.setServerName(ProtobufUtil.toServerName(serverName));
    if (webuiPort >= 0) {
      serverInfoBuilder.setWebuiPort(webuiPort);
    }
    builder.setServerInfo(serverInfoBuilder.build());
    return builder.build();
  }

  /**
   * Stores an exception encountered during RPC invocation so it can be passed
   * back through to the client.
   * 
   * @param controller
   *          the controller instance provided by the client when calling the
   *          service
   * @param ioe
   *          the exception encountered
   */
  public static void setControllerException(RpcController controller,
      IOException ioe) {
    if (controller != null) {
      if (controller instanceof ServerRpcController) {
        ((ServerRpcController) controller).setFailedOn(ioe);
      } else {
        controller.setFailed(StringUtils.stringifyException(ioe));
      }
    }
  }

  /**
   * A utility to build a GetResponse.
   * 
   * 
   * @param result
   *          result from hbase
   * @return
   */
  public static ClientProtos.GetResponse buildGetResponse(Result result,
      List<ColumnStruct> columns) {
    ClientProtos.GetResponse.Builder builder = ClientProtos.GetResponse.newBuilder();
    boolean existence = checkResultExist(result);
    if (existence) {
      ClientProtos.QueryResultProto queryResultProto = ProtobufUtil
          .toQeuryResultProto(result);
      builder.setResult(queryResultProto);
      builder.setExists(true);
      List<StringDataTypePair> StringDataTypePairs = buildStringDataTypePairs(columns);
      for (ClientProtos.StringDataTypePair StringDataTypePair : StringDataTypePairs) {
        builder.addMeta(StringDataTypePair);
      }
    } else {
      builder.setExists(false);
    }
    return builder.build();
  }

  private static boolean checkResultExist(Result result) {
    if (result.list() == null) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * A utility to build a InsertResponse.
   *
   * @param status
   * @return
   */
  public static ClientProtos.InsertResponse buildInsertResponse(OperationStatus status) {
    ClientProtos.InsertResponse.Builder builder = ClientProtos.InsertResponse.newBuilder();
    ClientProtos.WriteResultProto writeResultProto = ProtobufUtil.toWriteResultProto(status);
    builder.setResult(writeResultProto);
    return builder.build();
  }

  /**
   * A utility to build a UpdateResponse.
   *
   * @param status
   * @return
   */
  public static ClientProtos.UpdateResponse buildUpdateResponse(OperationStatus status) {
    ClientProtos.UpdateResponse.Builder builder = ClientProtos.UpdateResponse.newBuilder();
    ClientProtos.WriteResultProto writeResultProto = ProtobufUtil.toWriteResultProto(status);
    builder.setResult(writeResultProto);
    return builder.build();
  }

  /**
   * A utility to build a DeleteResponse.
   *
   * @param status
   * @return
   */
  public static ClientProtos.DeleteResponse buildDeleteResponse(OperationStatus status) {
    ClientProtos.DeleteResponse.Builder builder = ClientProtos.DeleteResponse.newBuilder();
    ClientProtos.WriteResultProto writeResultProto = ProtobufUtil.toWriteResultProto(status);
    builder.setResult(writeResultProto);
    return builder.build();
  }

  /**
   * A utility to build a TransactionResponse.
   *
   * @param status
   * @return
   */
  public static ClientProtos.TransactionResponse buildTransactionResponse(OperationStatus status) {
    ClientProtos.TransactionResponse.Builder builder = ClientProtos.TransactionResponse.newBuilder();
    ClientProtos.WriteResultProto writeResultProto = ProtobufUtil.toWriteResultProto(status);
    builder.setResult(writeResultProto);
    return builder.build();
  }

  /**
   * A utility to build a ExecuteResponse.
   *
   * @param queryResults
   * @return
   */
  public static ClientProtos.ExecuteResponse buildExecuteResponse(boolean lastSacn,
      String sessionName,
      Pair<List<QueryResultProto>, List<StringDataTypePair>> queryResults) {
    ClientProtos.ExecuteResponse.Builder executeResponseBuilder = ClientProtos.ExecuteResponse
        .newBuilder();
    executeResponseBuilder.setSessionId(sessionName);
    for (ClientProtos.QueryResultProto queryResultProto : queryResults.getFirst()) {
      ClientProtos.ExecuteResultProto.Builder executeResultProtoBuilder = ClientProtos.ExecuteResultProto
          .newBuilder();
      executeResultProtoBuilder.setType(ClientProtos.ExecuteResultProto.ResultType.QUERY);
      executeResultProtoBuilder.setQueryResult(queryResultProto);
      executeResponseBuilder.addResult(executeResultProtoBuilder.build());
    }
    executeResponseBuilder.setLastScan(lastSacn);
    executeResponseBuilder.addAllMeta(queryResults.getSecond());
    return executeResponseBuilder.build();
  }

  /**
   * A utility to build a ExecuteResponse.
   *
   * @param writeResults
   * @return
   */
  public static ClientProtos.ExecuteResponse buildExecuteResponse(
      List<WriteResultProto> writeResults) {
    ClientProtos.ExecuteResponse.Builder executeResponseBuilder = ClientProtos.ExecuteResponse
        .newBuilder();
    for (ClientProtos.WriteResultProto writeResultProto : writeResults) {
      ClientProtos.ExecuteResultProto.Builder executeResultProtoBuilder = ClientProtos.ExecuteResultProto
          .newBuilder();
      executeResultProtoBuilder.setType(ClientProtos.ExecuteResultProto.ResultType.WRITE);
      executeResultProtoBuilder.setWriteResult(writeResultProto);
      executeResponseBuilder.addResult(executeResultProtoBuilder.build());
    }
    return executeResponseBuilder.build();
  }

  /**
   * A utility to build a ExecuteResponse.
   *
   * @param response
   * @return
   */
  public static ClientProtos.ExecuteResponse buildExecuteResponse(
      MasterAdminProtos.CreateTableResponse response) {
    return buildExecuteResponse();
  }

  /**
   * A utility to build a ExecuteResponse.
   *
   * @param response
   * @return
   */
  public static ClientProtos.ExecuteResponse buildExecuteResponse(
      MasterAdminProtos.CreateIndexResponse response) {
    return buildExecuteResponse();
  }

  /**
   * A utility to build a ExecuteResponse.
   *
   * @param response
   * @return
   */
  public static ClientProtos.ExecuteResponse buildExecuteResponse(MasterAdminProtos.DropIndexResponse response) {
    return buildExecuteResponse();
  }

  /**
   * A utility to build a ExecuteResponse.It is same to buildExecuteResponse.
   *
   * @param responses
   * @return
   */
  public static ClientProtos.ExecuteResponse buildListExecuteResponse(
      List<DeleteTableResponse> responses) {
    ClientProtos.ExecuteResponse.Builder executeResponseBuilder = ClientProtos.ExecuteResponse
        .newBuilder();
    ClientProtos.ExecuteResultProto.Builder executeResultProtoBuilder = ClientProtos.ExecuteResultProto
        .newBuilder();
    int size = responses.size();
    for (int i = 0; i < size; i++) {
      ClientProtos.WriteResultProto.Builder writeResultProtoBuilder = ClientProtos.WriteResultProto
          .newBuilder();
      writeResultProtoBuilder.setCode(ClientProtos.WriteResultProto.StatusCode.SUCCESS);
      executeResultProtoBuilder.setType(ClientProtos.ExecuteResultProto.ResultType.WRITE);
      executeResultProtoBuilder.setWriteResult(writeResultProtoBuilder.build());
      executeResponseBuilder.addResult(executeResultProtoBuilder.build());
    }
    return executeResponseBuilder.build();
  }

  /**
   * A utility to build a ExecuteResponse.
   *
   * @param response
   * @return
   */
  public static ClientProtos.ExecuteResponse buildExecuteResponse(
      MasterAdminProtos.ModifyTableResponse response) {
    return buildExecuteResponse();
  }

  /**
   * A utility to build a ExecuteResponse.
   *
   * @param response
   * @return
   */
  public static ClientProtos.ExecuteResponse buildExecuteResponse(
      MasterAdminProtos.DeleteTableResponse response) {
    return buildExecuteResponse();
  }

  private static ClientProtos.ExecuteResponse buildExecuteResponse() {
    ClientProtos.ExecuteResponse.Builder executeResponseBuilder = ClientProtos.ExecuteResponse
        .newBuilder();
    ClientProtos.ExecuteResultProto.Builder executeResultProtoBuilder = ClientProtos.ExecuteResultProto
        .newBuilder();
    ClientProtos.WriteResultProto.Builder writeResultProtoBuilder = ClientProtos.WriteResultProto
        .newBuilder();
    writeResultProtoBuilder.setCode(ClientProtos.WriteResultProto.StatusCode.SUCCESS);
    executeResultProtoBuilder.setType(ClientProtos.ExecuteResultProto.ResultType.WRITE);
    executeResultProtoBuilder.setWriteResult(writeResultProtoBuilder.build());
    executeResponseBuilder.addResult(executeResultProtoBuilder.build());
    return executeResponseBuilder.build();
  }

  public static ClientProtos.ExecuteResponse buildNotExecuteResponse() {
    ClientProtos.ExecuteResponse.Builder executeResponseBuilder = ClientProtos.ExecuteResponse
        .newBuilder();
    ClientProtos.ExecuteResultProto.Builder executeResultProtoBuilder = ClientProtos.ExecuteResultProto
        .newBuilder();
    ClientProtos.WriteResultProto.Builder writeResultProtoBuilder = ClientProtos.WriteResultProto
        .newBuilder();
    writeResultProtoBuilder.setCode(StatusCode.FAILURE);
    executeResultProtoBuilder.setType(ClientProtos.ExecuteResultProto.ResultType.WRITE);
    executeResultProtoBuilder.setWriteResult(writeResultProtoBuilder.build());
    executeResponseBuilder.addResult(executeResultProtoBuilder.build());
    return executeResponseBuilder.build();
  }

  /**
   * A utility to build a SplitEntityGroupResponse.
   *
   * @return
   */
  public static SplitEntityGroupResponse buildSplitEntityGroupResponse() {
    SplitEntityGroupResponse.Builder builder = SplitEntityGroupResponse
        .newBuilder();
    return builder.build();
  }

  public static ClientProtos.ScanResponse buildScanResponse(ClientProtos.ScanResponse.Builder builder,
      List<ColumnStruct> columns) {
    List<StringDataTypePair> stringDataTypePairs = buildStringDataTypePairs(columns);
    for (ClientProtos.StringDataTypePair StringDataTypePair : stringDataTypePairs) {
      builder.addMeta(StringDataTypePair);
    }
    return builder.build();
  }

  private static List<StringDataTypePair> buildStringDataTypePairs(
      List<ColumnStruct> columns) {
    List<StringDataTypePair> stringDataTypePairs = new ArrayList<StringDataTypePair>();
    if (columns == null || columns.size() == 0) {
      return stringDataTypePairs;
    }

    for (ColumnStruct column : columns) {
      MetaProtos.DataTypeProtos dataTypeProtos = DataType
          .convertDataTypeToDataTypeProtos(column.getDataType());
      ClientProtos.StringDataTypePair.Builder StringDataTypePairBuilder = ClientProtos.StringDataTypePair
          .newBuilder();
      StringDataTypePairBuilder.setDataType(dataTypeProtos);
      StringDataTypePairBuilder.setName(column.getColumnName());
      stringDataTypePairs.add(StringDataTypePairBuilder.build());
    }
    return stringDataTypePairs;
  }
}
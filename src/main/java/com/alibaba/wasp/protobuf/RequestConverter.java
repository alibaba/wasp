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

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.wasp.DeserializationException;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.Index;
import com.alibaba.wasp.plan.action.DeleteAction;
import com.alibaba.wasp.plan.action.GetAction;
import com.alibaba.wasp.plan.action.InsertAction;
import com.alibaba.wasp.plan.action.ScanAction;
import com.alibaba.wasp.plan.action.UpdateAction;
import com.alibaba.wasp.protobuf.generated.ClientProtos.DeleteRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ExecuteRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.GetRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.InsertRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.ScanRequest;
import com.alibaba.wasp.protobuf.generated.ClientProtos.UpdateRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEncodedEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.CloseEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.GetServerInfoRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.SplitEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.FServerAdminProtos.StopServerRequest;
import com.alibaba.wasp.protobuf.generated.FServerStatusProtos.FServerReportRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.AssignEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.BalanceRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.CreateIndexRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.CreateTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DeleteTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DropIndexRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.EnableTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.FetchEntityGroupSizeRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupWithScanRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetEntityGroupsRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.GetTableEntityGroupsRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.IsTableAvailableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.IsTableLockedRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.ModifyTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.MoveEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.OfflineEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.SetBalancerRunningRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.SetTableStateRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.TableExistsRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.TruncateTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.UnassignEntityGroupRequest;
import com.alibaba.wasp.protobuf.generated.MasterAdminProtos.UnlockTableRequest;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetClusterStatusRequest;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusRequest;
import com.alibaba.wasp.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsRequest;
import com.alibaba.wasp.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import com.alibaba.wasp.protobuf.generated.WaspProtos;
import com.alibaba.wasp.protobuf.generated.WaspProtos.EntityGroupSpecifier;
import com.alibaba.wasp.protobuf.generated.WaspProtos.EntityGroupSpecifier.EntityGroupSpecifierType;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

/**
 * Helper utility to build protocol buffer requests, or build components for
 * protocol buffer requests.
 */
public final class RequestConverter {

  private RequestConverter() {
  }

  /**
   * Create a protocol buffer OpenRegionRequest to open a list of entityGroups
   * 
   * @param entityGroups
   *          the list of entityGroups to open
   * @return a protocol buffer OpenRegionRequest
   */
  public static OpenEntityGroupRequest buildOpenEntityGroupRequest(
      final List<EntityGroupInfo> entityGroups) {
    OpenEntityGroupRequest.Builder builder = OpenEntityGroupRequest
        .newBuilder();
    for (EntityGroupInfo entityGroup : entityGroups) {
      builder.addEntityGroup(EntityGroupInfo.convert(entityGroup));
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer OpenRegionRequest for a given entityGroup
   * 
   * @param entityGroup
   *          the entityGroup to open
   * @return a protocol buffer OpenEntityGroupRequest
   */
  public static OpenEntityGroupRequest buildOpenEntityGroupRequest(
      final EntityGroupInfo entityGroup) {
    return buildOpenEntityGroupRequest(entityGroup, -1);
  }

  /**
   * Create a protocol buffer OpenEntityGroupRequest for a given entityGroup
   * 
   * @param entityGroup
   *          the entityGroup to open
   * @param versionOfOfflineNode
   *          that needs to be present in the offline node
   * @return a protocol buffer OpenEntityGroupRequest
   */
  public static OpenEntityGroupRequest buildOpenEntityGroupRequest(
      final EntityGroupInfo entityGroup, final int versionOfOfflineNode) {
    OpenEntityGroupRequest.Builder builder = OpenEntityGroupRequest
        .newBuilder();
    builder.addEntityGroup(EntityGroupInfo.convert(entityGroup));
    if (versionOfOfflineNode >= 0) {
      builder.setVersionOfOfflineNode(versionOfOfflineNode);
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer ExecuteRequest.
   * 
   * @param sql
   * @param readModel
   * @return
   */
  public static ExecuteRequest buildExecuteRequest(String sql, ReadModel readModel, int fetchSize,
      String sessionId) {
    ExecuteRequest.Builder builder = ExecuteRequest.newBuilder();
    builder.setSql(sql);
    builder.setFetchSize(fetchSize);
    builder.setReadModel(ProtobufUtil.toReadModelProto(readModel));
    if (sessionId != null) {
      builder.setSessionId(sessionId);
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer ExecuteRequest.
   * 
   * @param sessionId
   * @param closeSession
   * @return
   */
  public static ExecuteRequest buildExecuteRequest(String sessionId,
      String sql, boolean closeSession) {
    ExecuteRequest.Builder builder = ExecuteRequest.newBuilder();
    builder.setSql(sql);
    builder.setSessionId(sessionId);
    builder.setCloseSession(closeSession);
    return builder.build();
  }

  /**
   * Create a protocol buffer GetOnlineEntityGroupsRequest.
   * 
   * @return
   */
  public static GetOnlineEntityGroupsRequest buildGetOnlineEntityGroupsRequest() {
    GetOnlineEntityGroupsRequest.Builder builder = GetOnlineEntityGroupsRequest
        .newBuilder();
    return builder.build();
  }

  /**
   * Create a protocol buffer GetRegionInfoRequest for a given entityGroupName
   * name
   * 
   * @param entityGroupName
   *          the name of the entityGroupName to get info
   * @return a protocol buffer GetRegionInfoRequest
   */
  public static GetEntityGroupInfoRequest buildGetEntityGroupInfoRequest(
      final byte[] entityGroupName) {
    return buildGetEntityGroupInfoRequest(entityGroupName, false);
  }

  /**
   * Create a protocol buffer GetRegionInfoRequest for a given entityGroup name
   * 
   * @param entityGroupName
   *          the name of the entityGroup to get info
   * @param includeCompactionState
   *          indicate if the compaction state is requested
   * @return a protocol buffer GetRegionInfoRequest
   */
  public static GetEntityGroupInfoRequest buildGetEntityGroupInfoRequest(
      final byte[] entityGroupName, final boolean includeCompactionState) {
    GetEntityGroupInfoRequest.Builder builder = GetEntityGroupInfoRequest
        .newBuilder();
    EntityGroupSpecifier entityGroup = buildEntityGroupSpecifier(
        EntityGroupSpecifierType.ENTITYGROUP_NAME, entityGroupName);
    builder.setEntityGroup(entityGroup);
    return builder.build();
  }

  /**
   * Create CloseEncodedEntityGroupRequest for a given encode entityGroup name.
   * 
   * @return
   */
  public static CloseEncodedEntityGroupRequest buildCloseEncodedEntityGroupRequest(
      final String encodedEntityGroupName, boolean zk) {
    CloseEncodedEntityGroupRequest.Builder builder = CloseEncodedEntityGroupRequest
        .newBuilder();
    builder.setEntityGroup(getEntityGroupSpecifier(Bytes
        .toBytes(encodedEntityGroupName)));
    builder.setZk(zk);
    return builder.build();
  }

  /**
   * Create a CloseEntityGroupRequest for a given entityGroup name
   * 
   * @param egi
   *          the entityGroup info
   * @param transitionInZK
   *          indicator if to transition in ZK
   * @return a CloseEntityGroupRequest
   */
  public static CloseEntityGroupRequest buildCloseEntityGroupRequest(
      final EntityGroupInfo egi, final boolean transitionInZK) {
    CloseEntityGroupRequest.Builder builder = CloseEntityGroupRequest
        .newBuilder();
    builder.setEntityGroup(egi.convert());
    builder.setZk(transitionInZK);
    return builder.build();
  }

  /**
   * Create a CloseRegionRequest for a given entityGroup name
   * 
   * @param egi
   *          the name of the entityGroup to close
   * @param versionOfClosingNode
   *          the version of znode to compare when FS transitions the znode from
   *          CLOSING state.
   * @return a CloseRegionRequest
   */
  public static CloseEntityGroupRequest buildCloseEntityGroupRequest(
      final EntityGroupInfo egi, final int versionOfClosingNode) {
    CloseEntityGroupRequest.Builder builder = CloseEntityGroupRequest
        .newBuilder();
    builder.setEntityGroup(egi.convert());
    builder.setVersionOfClosingNode(versionOfClosingNode);
    return builder.build();
  }

  public static CloseEntityGroupRequest buildCloseEntityGroupRequest(
      final EntityGroupInfo egi, final int versionOfClosingNode,
      final boolean transitionInZK) {
    CloseEntityGroupRequest.Builder builder = CloseEntityGroupRequest
        .newBuilder();
    builder.setEntityGroup(egi.convert());
    builder.setVersionOfClosingNode(versionOfClosingNode);
    builder.setZk(transitionInZK);
    return builder.build();
  }

  /**
   * Create a SplitRegionRequest for a given entityGroup name
   * 
   * @param entityGroupName
   *          the name of the entityGroup to split
   * @param splitPoint
   *          the split point
   * @return a SplitRegionRequest
   */
  public static SplitEntityGroupRequest buildSplitEntityGroupRequest(
      final byte[] entityGroupName, final byte[] splitPoint) {
    SplitEntityGroupRequest.Builder builder = SplitEntityGroupRequest
        .newBuilder();
    EntityGroupSpecifier entityGroup = buildEntityGroupSpecifier(
        EntityGroupSpecifierType.ENTITYGROUP_NAME, entityGroupName);
    builder.setEntityGroup(entityGroup);
    if (splitPoint != null) {
      builder.setSplitPoint(ByteString.copyFrom(splitPoint));
    }
    return builder.build();
  }

  /**
   * Create a new GetServerInfoRequest
   * 
   * @return a GetServerInfoRequest
   */
  public static GetServerInfoRequest buildGetServerInfoRequest() {
    GetServerInfoRequest.Builder builder = GetServerInfoRequest.newBuilder();
    return builder.build();
  }

  /**
   * Create a new StopServerRequest
   * 
   * @param reason
   *          the reason to stop the server
   * @return a StopServerRequest
   */
  public static StopServerRequest buildStopServerRequest(final String reason) {
    StopServerRequest.Builder builder = StopServerRequest.newBuilder();
    builder.setReason(reason);
    return builder.build();
  }

  /**
   * Convert a byte array to a protocol buffer RegionSpecifier
   * 
   * @param type
   *          the entityGroup specifier type
   * @param value
   *          the entityGroup specifier byte array value
   * @return a protocol buffer RegionSpecifier
   */
  public static EntityGroupSpecifier buildEntityGroupSpecifier(
      final EntityGroupSpecifierType type, final byte[] value) {
    EntityGroupSpecifier.Builder entityGroupBuilder = EntityGroupSpecifier
        .newBuilder();
    entityGroupBuilder.setValue(ByteString.copyFrom(value));
    entityGroupBuilder.setType(type);
    return entityGroupBuilder.build();
  }

  /**
   * Create a protocol buffer MoveRegionRequest
   * 
   * @param encodedEntityGroupName
   * @param destServerName
   * @return A MoveRegionRequest
   * @throws DeserializationException
   */
  public static MoveEntityGroupRequest buildMoveEntityGroupRequest(
      final byte[] encodedEntityGroupName, final byte[] destServerName) {
    MoveEntityGroupRequest.Builder builder = MoveEntityGroupRequest
        .newBuilder();
    builder.setEntityGroup(buildEntityGroupSpecifier(
        EntityGroupSpecifierType.ENCODED_ENTITYGROUP_NAME,
        encodedEntityGroupName));
    if (destServerName != null) {
      builder.setDestServerName(ProtobufUtil.toServerName(new ServerName(Bytes
          .toString(destServerName))));
    }
    return builder.build();
  }

  /**
   * Create a protocol buffer AssignRegionRequest
   * 
   * @param entityGroupName
   * @return an AssignRegionRequest
   */
  public static AssignEntityGroupRequest buildAssignEntityGroupRequest(
      final byte[] entityGroupName) {
    AssignEntityGroupRequest.Builder builder = AssignEntityGroupRequest
        .newBuilder();
    builder.setEntityGroup(buildEntityGroupSpecifier(
        EntityGroupSpecifierType.ENTITYGROUP_NAME, entityGroupName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer UnassignRegionRequest
   * 
   * @param entityGroupName
   * @param force
   * @return an UnassignRegionRequest
   */
  public static UnassignEntityGroupRequest buildUnassignEntityGroupRequest(
      final byte[] entityGroupName, final boolean force) {
    UnassignEntityGroupRequest.Builder builder = UnassignEntityGroupRequest
        .newBuilder();
    builder.setEntityGroup(buildEntityGroupSpecifier(
        EntityGroupSpecifierType.ENTITYGROUP_NAME, entityGroupName));
    builder.setForce(force);
    return builder.build();
  }

  /**
   * Creates a protocol buffer OfflineRegionRequest
   * 
   * @param entityGroupName
   * @return an OfflineRegionRequest
   */
  public static OfflineEntityGroupRequest buildOfflineEntityGroupRequest(
      final byte[] entityGroupName) {
    OfflineEntityGroupRequest.Builder builder = OfflineEntityGroupRequest
        .newBuilder();
    builder.setEntityGroup(buildEntityGroupSpecifier(
        EntityGroupSpecifierType.ENTITYGROUP_NAME, entityGroupName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer DeleteTableRequest
   * 
   * @param tableName
   * @return a DeleteTableRequest
   */
  public static DeleteTableRequest buildDeleteTableRequest(
      final byte[] tableName) {
    DeleteTableRequest.Builder builder = DeleteTableRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer TruncateTableRequest
   * 
   * @param tableName
   * @return a TruncateTableRequest
   */
  public static TruncateTableRequest buildTruncateTableRequest(
      final byte[] tableName) {
    TruncateTableRequest.Builder builder = TruncateTableRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer EnableTableRequest
   * 
   * @param tableName
   * @return an EnableTableRequest
   */
  public static EnableTableRequest buildEnableTableRequest(
      final byte[] tableName) {
    EnableTableRequest.Builder builder = EnableTableRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer DisableTableRequest
   * 
   * @param tableName
   * @return a DisableTableRequest
   */
  public static com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DisableTableRequest buildMasterDisableTableRequest(
      final byte[] tableName) {
    com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DisableTableRequest.Builder builder = com.alibaba.wasp.protobuf.generated.MasterAdminProtos.DisableTableRequest
        .newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer DisableTableRequest
   * 
   * @param tableName
   * @return a DisableTableRequest
   */
  public static com.alibaba.wasp.protobuf.generated.FServerAdminProtos.DisableTableRequest buildServerDisableTableRequest(
      final String tableName) {
    com.alibaba.wasp.protobuf.generated.FServerAdminProtos.DisableTableRequest.Builder builder = com.alibaba.wasp.protobuf.generated.FServerAdminProtos.DisableTableRequest
        .newBuilder();
    builder.setTableName(tableName);
    return builder.build();
  }

  /**
   * Creates a protocol buffer EnableTableRequest
   * 
   * @param tableName
   * @return a DisableTableRequest
   */
  public static com.alibaba.wasp.protobuf.generated.FServerAdminProtos.EnableTableRequest buildServerEnableTableRequest(
      final String tableName) {
    com.alibaba.wasp.protobuf.generated.FServerAdminProtos.EnableTableRequest.Builder builder = com.alibaba.wasp.protobuf.generated.FServerAdminProtos.EnableTableRequest
        .newBuilder();
    builder.setTableName(tableName);
    return builder.build();
  }

  /**
   * Creates a protocol buffer CreateTableRequest
   * 
   * @param hTableDesc
   * @param splitKeys
   * @return a CreateTableRequest
   * @throws ServiceException
   * @throws MetaException
   */
  public static CreateTableRequest buildCreateTableRequest(
      final FTable hTableDesc, final byte[][] splitKeys)
      throws ServiceException {
    CreateTableRequest.Builder builder = CreateTableRequest.newBuilder();
    try {
      builder.setTableSchema(hTableDesc.convert());
    } catch (MetaException e) {
      throw new ServiceException(e);
    }
    if (splitKeys != null) {
      for (byte[] splitKey : splitKeys) {
        builder.addSplitKeys(ByteString.copyFrom(splitKey));
      }
    }
    return builder.build();
  }

  /**
   * Creates a protocol buffer CreateTableRequest
   * 
   * @param tableName
   * @return a CreateTableRequest
   * @throws ServiceException
   * @throws MetaException
   */
  public static FetchEntityGroupSizeRequest buildFetchEntityGroupSizeRequest(
      final byte[] tableName) throws ServiceException {
    FetchEntityGroupSizeRequest.Builder builder = FetchEntityGroupSizeRequest
        .newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  /**
   * Creates a protocol buffer CreateIndexRequest
   * 
   * @param index
   * @return a CreateIndexRequest
   * 
   */
  public static CreateIndexRequest buildCreateIndexRequest(Index index)
      throws ServiceException {
    CreateIndexRequest.Builder builder = CreateIndexRequest.newBuilder();
    builder.setIndexSchema(index.convert());
    return builder.build();
  }

  /**
   * Creates a protocol buffer DropIndexRequest
   * 
   * @param indexName
   * @return a CreateIndexRequest
   * 
   */
  public static DropIndexRequest buildDropIndexRequest(String tableName,
      String indexName) throws ServiceException {
    DropIndexRequest.Builder builder = DropIndexRequest.newBuilder();
    builder.setIndexName(indexName);
    builder.setTableName(tableName);
    return builder.build();
  }

  /**
   * Creates a protocol buffer ModifyTableRequest
   * 
   * @param table
   * @param hTableDesc
   * @return a ModifyTableRequest
   * @throws ServiceException
   */
  public static ModifyTableRequest buildModifyTableRequest(final byte[] table,
      final FTable hTableDesc) throws ServiceException {
    ModifyTableRequest.Builder builder = ModifyTableRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(table));
    try {
      builder.setTableSchema(hTableDesc.convert());
    } catch (MetaException e) {
      throw new ServiceException(e);
    }
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetSchemaAlterStatusRequest
   * 
   * @param table
   * @return a GetSchemaAlterStatusRequest
   */
  public static GetSchemaAlterStatusRequest buildGetSchemaAlterStatusRequest(
      final byte[] table) {
    GetSchemaAlterStatusRequest.Builder builder = GetSchemaAlterStatusRequest
        .newBuilder();
    builder.setTableName(ByteString.copyFrom(table));
    return builder.build();
  }

  /**
   * Creates a protocol buffer GetTableDescriptorsRequest
   * 
   * @param tableNames
   * @return a GetTableDescriptorsRequest
   */
  public static GetTableDescriptorsRequest buildGetTableDescriptorsRequest(
      final List<String> tableNames) {
    GetTableDescriptorsRequest.Builder builder = GetTableDescriptorsRequest
        .newBuilder();
    if (tableNames != null) {
      for (String str : tableNames) {
        builder.addTableNames(str);
      }
    }
    return builder.build();
  }

  /**
   * Creates a protocol buffer IsMasterRunningRequest
   * 
   * @return a IsMasterRunningRequest
   */
  public static IsMasterRunningRequest buildIsMasterRunningRequest() {
    return IsMasterRunningRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer BalanceRequest
   * 
   * @return a BalanceRequest
   */
  public static BalanceRequest buildBalanceRequest() {
    return BalanceRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer SetBalancerRunningRequest
   * 
   * @param on
   * @param synchronous
   * @return a SetBalancerRunningRequest
   */
  public static SetBalancerRunningRequest buildSetBalancerRunningRequest(
      boolean on, boolean synchronous) {
    return SetBalancerRunningRequest.newBuilder().setOn(on)
        .setSynchronous(synchronous).build();
  }

  /**
   * Creates a protocol buffer GetClusterStatusRequest
   * 
   * @return A GetClusterStatusRequest
   */
  public static GetClusterStatusRequest buildGetClusterStatusRequest() {
    return GetClusterStatusRequest.newBuilder().build();
  }

  /**
   * Creates a protocol buffer UpdateRequest.
   * 
   * @param action
   * @return
   */
  public static UpdateRequest buildUpdateRequest(UpdateAction action) {
    UpdateRequest.Builder builder = UpdateRequest.newBuilder();
    builder.setEntityGroup(getEntityGroupSpecifier(action
        .getEntityGroupLocation().getEntityGroupInfo()));
    builder.setUpdateAction(ProtobufUtil.convertUpdateAction(action)
        .getUpdate());
    return builder.build();
  }

  /**
   * 
   * @param encodedEntityGroupName
   * @return
   */
  public static EntityGroupSpecifier getEntityGroupSpecifier(
      byte[] encodedEntityGroupName) {
    EntityGroupSpecifier.Builder builder = EntityGroupSpecifier.newBuilder();
    builder.setValue(ByteString.copyFrom(encodedEntityGroupName));
    builder
        .setType(EntityGroupSpecifier.EntityGroupSpecifierType.ENCODED_ENTITYGROUP_NAME);
    return builder.build();
  }

  /**
   * 
   * @param entityGroupInfo
   * @return
   */
  public static EntityGroupSpecifier getEntityGroupSpecifier(
      EntityGroupInfo entityGroupInfo) {
    EntityGroupSpecifier.Builder builder = EntityGroupSpecifier.newBuilder();
    builder.setValue(ByteString.copyFrom(entityGroupInfo.getEntityGroupName()));
    builder
        .setType(EntityGroupSpecifier.EntityGroupSpecifierType.ENTITYGROUP_NAME);
    return builder.build();
  }

  /**
   * Creates a protocol buffer ScanRequest.
   * 
   * @param scanAction
   * @param scanId
   * @param closeScanner
   * @return
   */
  public static ScanRequest buildScanRequest(ScanAction scanAction,
      long scanId, boolean closeScanner) {
    ScanRequest.Builder builder = ScanRequest.newBuilder();
    builder.setEntityGroup(getEntityGroupSpecifier(scanAction
        .getEntityGroupLocation().getEntityGroupInfo()));
    builder.setScannerId(scanId);
    builder.setCloseScanner(closeScanner);
    builder.setScanAction(ProtobufUtil.convertScanAction(scanAction));
    return builder.build();
  }

  /**
   * Creates a protocol buffer InsertRequest.
   * 
   * @param action
   * @return
   */
  public static InsertRequest buildInsertRequest(InsertAction action) {
    InsertRequest.Builder builder = InsertRequest.newBuilder();
    builder.setEntityGroup(getEntityGroupSpecifier(action
        .getEntityGroupLocation().getEntityGroupInfo()));
    builder.setInsertAction(ProtobufUtil.convertInsertAction(action)
        .getInsert());
    return builder.build();
  }

  /**
   * Creates a protocol buffer DeleteRequest.
   * 
   * @param action
   * @return
   */
  public static DeleteRequest buildDeleteRequest(DeleteAction action) {
    DeleteRequest.Builder builder = DeleteRequest.newBuilder();
    builder.setEntityGroup(getEntityGroupSpecifier(action
        .getEntityGroupLocation().getEntityGroupInfo()));
    builder.setDeleteAction(ProtobufUtil.convertDeleteAction(action)
        .getDelete());
    return builder.build();
  }

  /**
   * Creates a protocol buffer FServerReportRequest.
   * 
   * @param sl
   *          WaspProtos.ServerLoadProtos
   * @param sn
   *          WaspProtos.ServerName
   * @return
   */
  public static FServerReportRequest buildFServerReportRequest(
      WaspProtos.ServerLoadProtos sl, WaspProtos.ServerName sn) {
    FServerReportRequest.Builder request = FServerReportRequest.newBuilder();
    request.setServer(sn);
    request.setLoad(sl);
    return request.build();
  }

  public static GetRequest buildGetRequest(GetAction action) {
    GetRequest.Builder builder = GetRequest.newBuilder();
    builder.setEntityGroup(getEntityGroupSpecifier(action
        .getEntityGroupLocation().getEntityGroupInfo()));
    builder.setGet(ProtobufUtil.convertGetAction(action));
    return builder.build();
  }

  public static GetEntityGroupRequest buildGetEntityGroupRequest(
      final byte[] entityGroupName) {
    GetEntityGroupRequest.Builder builder = GetEntityGroupRequest.newBuilder();
    builder.setEntityGroupName(ByteString.copyFrom(entityGroupName));
    return builder.build();
  }

  public static GetEntityGroupWithScanRequest buildGetEntityGroupWithScanRequest(
      final byte[] entityGroupName) {
    GetEntityGroupWithScanRequest.Builder builder = GetEntityGroupWithScanRequest
        .newBuilder();
    builder.setTableNameOrEntityGroupName(ByteString.copyFrom(entityGroupName));
    return builder.build();
  }

  public static GetEntityGroupsRequest buildGetEntityGroupsRequest(
      final byte[] tableName) {
    GetEntityGroupsRequest.Builder builder = GetEntityGroupsRequest
        .newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  public static GetTableEntityGroupsRequest buildGetTableEntityGroupsRequest(
      final byte[] tableName) {
    GetTableEntityGroupsRequest.Builder builder = GetTableEntityGroupsRequest
        .newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  public static TableExistsRequest buildTableExistsRequest(
      final byte[] tableName) {
    TableExistsRequest.Builder builder = TableExistsRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  public static IsTableAvailableRequest buildIsTableAvailableRequest(
      final byte[] tableName) {
    IsTableAvailableRequest.Builder builder = IsTableAvailableRequest
        .newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  public static IsTableLockedRequest buildIsTableLockedRequest(
      final byte[] tableName) {
    IsTableLockedRequest.Builder builder = IsTableLockedRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  public static UnlockTableRequest buildUnlockTableRequest(final byte[] tableName) {
    UnlockTableRequest.Builder builder = UnlockTableRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    return builder.build();
  }

  public static SetTableStateRequest buildSetTableStateRequest(final byte[] tableName,
      final byte[] state) {
    SetTableStateRequest.Builder builder = SetTableStateRequest.newBuilder();
    builder.setTableName(ByteString.copyFrom(tableName));
    builder.setState(ByteString.copyFrom(state));
    return builder.build();
  }
}
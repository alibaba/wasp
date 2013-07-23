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
package org.apache.wasp.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.wasp.DataType;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.FConstants;
import org.apache.wasp.MetaException;
import org.apache.wasp.ReadModel;
import org.apache.wasp.ServerName;
import org.apache.wasp.client.ExecuteResult;
import org.apache.wasp.client.QueryResult;
import org.apache.wasp.client.WriteResult;
import org.apache.wasp.fserver.AdminProtocol;
import org.apache.wasp.fserver.OperationStatus;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.meta.Index;
import org.apache.wasp.plan.action.Action;
import org.apache.wasp.plan.action.ColumnStruct;
import org.apache.wasp.plan.action.DeleteAction;
import org.apache.wasp.plan.action.GetAction;
import org.apache.wasp.plan.action.InsertAction;
import org.apache.wasp.plan.action.ScanAction;
import org.apache.wasp.plan.action.UpdateAction;
import org.apache.wasp.protobuf.generated.ClientProtos.ExecuteResultProto;
import org.apache.wasp.protobuf.generated.ClientProtos.ExecuteResultProto.ResultType;
import org.apache.wasp.protobuf.generated.ClientProtos.QueryResultProto;
import org.apache.wasp.protobuf.generated.ClientProtos.StringDataTypePair;
import org.apache.wasp.protobuf.generated.ClientProtos.WriteResultProto;
import org.apache.wasp.protobuf.generated.ClientProtos.WriteResultProto.StatusCode;
import org.apache.wasp.protobuf.generated.ComparatorProtos;
import org.apache.wasp.protobuf.generated.ComparatorProtos.ByteArrayComparable;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.CloseEntityGroupRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.CloseEntityGroupResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.GetEntityGroupInfoResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.GetOnlineEntityGroupsResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.GetServerInfoRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.GetServerInfoResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupRequest;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.OpenEntityGroupResponse;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.ServerInfo;
import org.apache.wasp.protobuf.generated.FServerAdminProtos.SplitEntityGroupRequest;
import org.apache.wasp.protobuf.generated.MasterAdminProtos.CreateTableRequest;
import org.apache.wasp.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsResponse;
import org.apache.wasp.protobuf.generated.MasterMonitorProtos.ITableSchema;
import org.apache.wasp.protobuf.generated.MetaProtos.ColumnStructProto;
import org.apache.wasp.protobuf.generated.MetaProtos.DeleteActionProto;
import org.apache.wasp.protobuf.generated.MetaProtos.GetActionProto;
import org.apache.wasp.protobuf.generated.MetaProtos.IndexSchema;
import org.apache.wasp.protobuf.generated.MetaProtos.InsertActionProto;
import org.apache.wasp.protobuf.generated.MetaProtos.MessageProto;
import org.apache.wasp.protobuf.generated.MetaProtos.MessageProto.ActionType;
import org.apache.wasp.protobuf.generated.MetaProtos.ReadModelProto;
import org.apache.wasp.protobuf.generated.MetaProtos.ScanActionProto;
import org.apache.wasp.protobuf.generated.MetaProtos.UpdateActionProto;
import org.apache.wasp.protobuf.generated.WaspProtos;
import org.apache.wasp.protobuf.generated.WaspProtos.EntityGroupLoadProtos;
import org.apache.wasp.protobuf.generated.WaspProtos.Mutate;
import org.apache.wasp.protobuf.generated.WaspProtos.Mutate.ColumnValue;
import org.apache.wasp.protobuf.generated.WaspProtos.Mutate.ColumnValue.QualifierValue;
import org.apache.wasp.protobuf.generated.WaspProtos.Mutate.DeleteType;
import org.apache.wasp.protobuf.generated.WaspProtos.Mutate.MutateType;
import org.apache.wasp.protobuf.generated.WaspProtos.StringBytesPair;
import org.apache.wasp.protobuf.generated.WaspProtos.StringStringPair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Protobufs utility.
 */
public final class ProtobufUtil {

  /**
   * Return the IOException thrown by the remote server wrapped in
   * ServiceException as cause.
   * 
   * @param se
   *          ServiceException that wraps IO exception thrown by the server
   * @return Exception wrapped in ServiceException or a new IOException that
   *         wraps the unexpected ServiceException.
   */
  public static IOException getRemoteException(ServiceException se) {
    Throwable e = se.getCause();
    if (e == null) {
      return new IOException(se);
    }
    return e instanceof IOException ? (IOException) e : new IOException(se);
  }

  /**
   * Convert a ServerName to a protocol buffer ServerName
   * 
   * @param serverName
   *          the ServerName to convert
   * @return the converted protocol buffer ServerName
   * @see #toServerName(org.apache.wasp.protobuf.generated.WaspProtos.ServerName)
   */
  public static WaspProtos.ServerName toServerName(final ServerName serverName) {
    if (serverName == null) {
      return null;
    }
    return serverName.convert();
  }

  /**
   * Convert a protocol buffer ServerName to a ServerName
   * 
   * @param proto
   *          the protocol buffer ServerName to convert
   * @return the converted ServerName
   */
  public static ServerName toServerName(final WaspProtos.ServerName proto) {
    return ServerName.convert(proto);
  }

  /**
   * Create a protocol buffer Mutate based on a client Mutation
   * 
   * @param mutateType
   * @param mutation
   * @return a mutate
   * @throws IOException
   */
  public static Mutate toMutate(final MutateType mutateType,
      final Mutation mutation, final String tableName) throws IOException {
    Mutate.Builder mutateBuilder = Mutate.newBuilder();
    mutateBuilder.setTableName(tableName);
    mutateBuilder.setRow(ByteString.copyFrom(mutation.getRow()));
    mutateBuilder.setMutateType(mutateType);
    mutateBuilder.setWriteToWAL(mutation.getWriteToWAL());
    if (mutation.getLockId() >= 0) {
      mutateBuilder.setLockId(mutation.getLockId());
    }
    mutateBuilder.setTimestamp(mutation.getTimeStamp());
    Map<String, byte[]> attributes = mutation.getAttributesMap();
    if (!attributes.isEmpty()) {
      StringBytesPair.Builder attributeBuilder = StringBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute : attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        mutateBuilder.addAttribute(attributeBuilder.build());
      }
    }
    ColumnValue.Builder columnBuilder = ColumnValue.newBuilder();
    QualifierValue.Builder valueBuilder = QualifierValue.newBuilder();
    for (Map.Entry<byte[], List<KeyValue>> family : mutation.getFamilyMap()
        .entrySet()) {
      columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
      columnBuilder.clearQualifierValue();
      for (KeyValue value : family.getValue()) {
        valueBuilder.setQualifier(ByteString.copyFrom(value.getQualifier()));
        valueBuilder.setValue(ByteString.copyFrom(value.getValue()));
        valueBuilder.setTimestamp(value.getTimestamp());
        if (mutateType == MutateType.DELETE) {
          KeyValue.Type keyValueType = KeyValue.Type
              .codeToType(value.getType());
          valueBuilder.setDeleteType(toDeleteType(keyValueType));
        }
        columnBuilder.addQualifierValue(valueBuilder.build());
      }
      mutateBuilder.addColumnValue(columnBuilder.build());
    }
    return mutateBuilder.build();
  }

  /**
   * Convert a protocol buffer Mutate to a Put
   * 
   * @param proto
   *          the protocol buffer Mutate to convert
   * @return the converted client Put
   * @throws DoNotRetryIOException
   */
  public static Put toPut(final Mutate proto, Long timestampTransaction)
      throws DoNotRetryIOException {
    MutateType type = proto.getMutateType();
    assert type == MutateType.PUT : type.name();
    byte[] row = proto.getRow().toByteArray();
    Long timestamp = FConstants.LATEST_TIMESTAMP;
    if (proto.hasTimestamp()) {
      timestamp = proto.getTimestamp();
    }
    if (timestampTransaction != null) {
      timestamp = timestampTransaction;
    }
    RowLock lock = null;
    if (proto.hasLockId()) {
      lock = new RowLock(proto.getLockId());
    }
    Put put = new Put(row, timestamp, lock);
    put.setWriteToWAL(proto.getWriteToWAL());
    for (StringBytesPair attribute : proto.getAttributeList()) {
      put.setAttribute(attribute.getName(), attribute.getValue().toByteArray());
    }
    for (ColumnValue column : proto.getColumnValueList()) {
      byte[] family = column.getFamily().toByteArray();
      for (QualifierValue qv : column.getQualifierValueList()) {
        byte[] qualifier = qv.getQualifier().toByteArray();
        if (!qv.hasValue()) {
          throw new DoNotRetryIOException(
              "Missing required field: qualifer value");
        }
        byte[] value = qv.getValue().toByteArray();
        put.add(family, qualifier, value);
      }
    }
    return put;
  }

  /**
   * Convert a delete KeyValue type to protocol buffer DeleteType.
   * 
   * @param type
   * @return
   * @throws IOException
   */
  public static DeleteType toDeleteType(KeyValue.Type type) throws IOException {
    switch (type) {
    case Delete:
      return DeleteType.DELETE_ONE_VERSION;
    case DeleteColumn:
      return DeleteType.DELETE_MULTIPLE_VERSIONS;
    case DeleteFamily:
      return DeleteType.DELETE_FAMILY;
    default:
      throw new IOException("Unknown delete type: " + type);
    }
  }

  public static Delete toDelete(final Mutate proto) {
    return toDelete(proto, null);
  }

  /**
   * Convert a protocol buffer Mutate to a Delete
   * 
   * @param proto
   *          the protocol buffer Mutate to convert
   * @return the converted client Delete
   */
  public static Delete toDelete(final Mutate proto, Long timestampTransaction) {
    MutateType type = proto.getMutateType();
    assert type == MutateType.DELETE : type.name();
    byte[] row = proto.getRow().toByteArray();
    long timestamp = HConstants.LATEST_TIMESTAMP;
    if (proto.hasTimestamp()) {
      timestamp = proto.getTimestamp();
    }
    if (timestampTransaction != null) {
      timestamp = timestampTransaction;
    }
    RowLock lock = null;
    if (proto.hasLockId()) {
      lock = new RowLock(proto.getLockId());
    }
    Delete delete = new Delete(row, timestamp, lock);
    delete.setWriteToWAL(proto.getWriteToWAL());
    for (StringBytesPair attribute : proto.getAttributeList()) {
      delete.setAttribute(attribute.getName(), attribute.getValue()
          .toByteArray());
    }
    for (ColumnValue column : proto.getColumnValueList()) {
      byte[] family = column.getFamily().toByteArray();
      for (QualifierValue qv : column.getQualifierValueList()) {
        DeleteType deleteType = qv.getDeleteType();
        byte[] qualifier = null;
        if (qv.hasQualifier()) {
          qualifier = qv.getQualifier().toByteArray();
        }
        long ts = HConstants.LATEST_TIMESTAMP;
        if (qv.hasTimestamp()) {
          ts = qv.getTimestamp();
        }
        if (deleteType == DeleteType.DELETE_ONE_VERSION) {
          delete.deleteColumn(family, qualifier, ts);
        } else if (deleteType == DeleteType.DELETE_MULTIPLE_VERSIONS) {
          delete.deleteColumns(family, qualifier, ts);
        } else {
          delete.deleteFamily(family, ts);
        }
      }
    }
    return delete;
  }

  /**
   * Convert ReadModelProto to ReadModel.
   * 
   * @param readModel
   * @return
   */
  public static ReadModel toReadModel(ReadModelProto readModel) {
    return ReadModel.valueOf(readModel.name());
  }

  /**
   * Convert ReadModel to ReadModelProto.
   * 
   * @param readModel
   * @return
   */
  public static ReadModelProto toReadModelProto(ReadModel readModel) {
    return ReadModelProto.valueOf(readModel.name());
  }

  /**
   * Convert OperationStatus to WriteResultProto.
   * 
   * @return WriteResultProto
   */
  public static WriteResultProto toWriteResultProto(OperationStatus status) {
    WriteResultProto.Builder writeResultProtoBuilder = WriteResultProto
        .newBuilder();
    if (status.getExceptionMsg() != null) {
      writeResultProtoBuilder.setExceptionMsg(status.getExceptionMsg());
    }
    if (status.getExceptionClassname() != null) {
      writeResultProtoBuilder.setExceptionClassName(status
          .getExceptionClassname());
    }
    writeResultProtoBuilder.setCode(StatusCode.valueOf(status
        .getOperationStatusCode().name()));
    return writeResultProtoBuilder.build();
  }

  /**
   * Convert ExecuteResultProto to ExecuteResult.
   * 
   * @param protoResults
   * @param metaDatas
   * @return
   */
  public static List<ExecuteResult> toExecuteResult(
      List<ExecuteResultProto> protoResults, List<StringDataTypePair> metaDatas) {

    List<ExecuteResult> results = new ArrayList<ExecuteResult>();
    for (ExecuteResultProto result : protoResults) {
      if (result.getType() == ResultType.QUERY) {
        QueryResultProto queryResultProto = result.getQueryResult();
        Map<String, Pair<DataType, byte[]>> resultMap = newResultMap(metaDatas);
        for (StringBytesPair pair : queryResultProto.getResultList()) {
          if (pair.getName().equalsIgnoreCase("rowkey")) {
            continue;
          }
          Pair<DataType, byte[]> dataTypePair = resultMap.get(pair.getName());
          // if dataTypePair is null, the column isn't selected in this case;
          if (dataTypePair == null) {
            continue;
          }
          resultMap.put(pair.getName(), Pair.newPair(dataTypePair.getFirst(),
              pair.getValue().toByteArray()));
        }
        QueryResult queryResult = new QueryResult(resultMap);
        results.add(queryResult);
      } else {
        WriteResultProto writeResultProto = result.getWriteResult();
        OperationStatus status = new OperationStatus(
            OperationStatusCode.valueOf(writeResultProto.getCode().name()),
            writeResultProto.getExceptionMsg(),
            writeResultProto.getExceptionClassName());
        WriteResult writeResult = new WriteResult(status);
        results.add(writeResult);
      }
    }
    return results;
  }

  private static Map<String, Pair<DataType, byte[]>> newResultMap(
      List<StringDataTypePair> metaDatas) {
    Map<String, Pair<DataType, byte[]>> resultMap;
    resultMap = new LinkedHashMap<String, Pair<DataType, byte[]>>();
    for (StringDataTypePair metaData : metaDatas) {
      resultMap.put(metaData.getName(), Pair.newPair(
          DataType.convertDataTypeProtosToDataType(metaData.getDataType()),
          new byte[] {}));
    }
    return resultMap;
  }

  /**
   * Convert DeleteActionProto to DeleteAction.
   * 
   * @param deleteActionProto
   * @return
   */
  public static DeleteAction toDeleteAction(DeleteActionProto deleteActionProto) {
    return DeleteAction.convert(deleteActionProto);
  }

  /**
   * Convert UpdateActionProto to UpdateAction.
   * 
   * @param updateActionProto
   * @return
   */
  public static UpdateAction toUpdateAction(UpdateActionProto updateActionProto) {
    return UpdateAction.convert(updateActionProto);
  }

  /**
   * Convert ColumnStructProto to ColumnAction.
   * 
   * @param col
   * @return
   */
  public static ColumnStruct toColumnAction(ColumnStructProto col) {
    return ColumnStruct.convert(col);
  }

  /**
   * Convert ColumnStructProto to ColumnAction.
   * 
   * @param col
   * @return
   */
  public static ColumnStructProto toColumnStructProto(ColumnStruct col) {
    return ColumnStruct.convert(col);
  }

  /**
   * Convert InsertActionProto to InsertAction.
   * 
   * @param insertActionProto
   * @return
   */
  public static InsertAction toInsertAction(InsertActionProto insertActionProto) {
    return InsertAction.convert(insertActionProto);
  }

  /**
   * Convert GetActionProto to GetAction.
   * 
   * @param getAction
   * @return
   */
  public static GetAction toGetAction(GetActionProto getAction) {
    return GetAction.convert(getAction);
  }

  /**
   * Convert GetAction to GetActionProto.
   * 
   * @param getAction
   * @return
   */
  public static GetActionProto toGetActionProto(GetAction getAction) {
    return GetAction.convert(getAction);
  }

  /**
   * Convert protobuf value to message object.This method will be used by
   * message queue.
   * 
   * @param value
   * @return
   * @throws InvalidProtocolBufferException
   */
  public static Action convertWriteAction(byte[] value)
      throws InvalidProtocolBufferException {
    MessageProto.Builder builder = MessageProto.newBuilder();
    builder = builder.mergeFrom(value);
    MessageProto proto = builder.build();
    return convertWriteAction(proto);
  }

  /**
   * Convert protobuf object MessageProto to write Action.
   * 
   * @param proto
   * @return
   * @throws InvalidProtocolBufferException
   */
  public static Action convertWriteAction(MessageProto proto)
      throws InvalidProtocolBufferException {
    if (proto.getType() == ActionType.UPDATE) {
      return UpdateAction.convert(proto.getUpdate());
    } else if (proto.getType() == ActionType.DELETE) {
      return DeleteAction.convert(proto.getDelete());
    } else if (proto.getType() == ActionType.INSERT) {
      return InsertAction.convert(proto.getInsert());
    }
    throw new InvalidProtocolBufferException(String.valueOf(proto.getType()));
  }

  /**
   * convert Action to MessageProto.
   * 
   * @param action
   * @return
   * @throws InvalidProtocolBufferException
   */
  public static MessageProto convertWriteAction(Action action)
      throws InvalidProtocolBufferException {
    if (action instanceof DeleteAction) {
      return ProtobufUtil.convertDeleteAction((DeleteAction) action);
    } else if (action instanceof UpdateAction) {
      return ProtobufUtil.convertUpdateAction((UpdateAction) action);
    } else if (action instanceof InsertAction) {
      return ProtobufUtil.convertInsertAction((InsertAction) action);
    }
    throw new InvalidProtocolBufferException(action.toString());
  }

  /**
   * convert Action to MessageProto.
   * 
   * @param action
   * @return
   */
  public static MessageProto convertInsertAction(InsertAction action) {
    MessageProto.Builder builder = MessageProto.newBuilder();
    InsertActionProto proto = InsertAction.convert(action);
    builder.setType(ActionType.INSERT);
    builder.setInsert(proto);
    return builder.build();
  }

  /**
   * convert Action to MessageProto.
   * 
   * @param action
   * @return
   */
  public static MessageProto convertUpdateAction(UpdateAction action) {
    MessageProto.Builder builder = MessageProto.newBuilder();
    UpdateActionProto proto = UpdateAction.convert(action);
    builder.setType(ActionType.UPDATE);
    builder.setUpdate(proto);
    return builder.build();
  }

  /**
   * Convert Action to MessageProto.
   * 
   * @param action
   * @return
   */
  public static MessageProto convertDeleteAction(DeleteAction action) {
    MessageProto.Builder builder = MessageProto.newBuilder();
    DeleteActionProto proto = DeleteAction.convert(action);
    builder.setType(ActionType.DELETE);
    builder.setDelete(proto);
    return builder.build();
  }

  /**
   * Convert ScanActionProto to ScanAction.
   * 
   * @param scanActionProto
   * @return
   */
  public static ScanAction convertScanAction(ScanActionProto scanActionProto) {
    return ScanAction.convert(scanActionProto);
  }

  /**
   * Convert ScanAction to ScanActionProto.
   * 
   * @param scanAction
   * @return
   */
  public static ScanActionProto convertScanAction(ScanAction scanAction) {
    return ScanAction.convert(scanAction);
  }

  /**
   * Convert result to QueryResultProto.
   * 
   * @param result
   * @return
   */
  public static QueryResultProto toQeuryResultProto(
      org.apache.hadoop.hbase.client.Result result) {
    QueryResultProto.Builder builder = QueryResultProto.newBuilder();
    KeyValue[] raw = result.raw();
    for (KeyValue kv : raw) {
      builder.addResult(toStringBytesPair(kv));
    }
    return builder.build();
  }

  /**
   * Combine result1 and result2,then convert it to QueryResultProto.
   * 
   * @param result1
   * @param result2
   * @return
   */
  public static QueryResultProto toQeuryResultProto(
      org.apache.hadoop.hbase.client.Result result1,
      org.apache.hadoop.hbase.client.Result result2) {
    QueryResultProto.Builder builder = QueryResultProto.newBuilder();
    KeyValue[] raw = result1.raw();
    for (KeyValue kv : raw) {
      builder.addResult(toStringBytesPair(kv));
    }
    if(result1.raw().length == 0) {
      raw = result2.raw();
      for (KeyValue kv : raw) {
        builder.addResult(toStringBytesPair(kv));
      }
    }
    return builder.build();
  }

  /**
   * Convert result to NameDataTypeBytesPair.
   * 
   * @param kv
   * @return
   */
  public static StringBytesPair toStringBytesPair(KeyValue kv) {
    StringBytesPair.Builder builder = StringBytesPair.newBuilder();
    String column = Bytes.toString(kv.getQualifier());
    builder.setName(column);
    builder.setValue(ByteString.copyFrom(kv.getValue()));
    return builder.build();
  }

  /**
   * Convert UpdateActionProto to UpdateAction.
   * 
   * @param updateActionProto
   * @return
   */
  public static UpdateAction convertUpdateAction(
      UpdateActionProto updateActionProto) {
    return UpdateAction.convert(updateActionProto);
  }

  /**
   * Convert InsertActionProto to InsertAction.
   * 
   * @param insertActionProto
   * @return
   */
  public static InsertAction convertInsertAction(
      InsertActionProto insertActionProto) {
    return InsertAction.convert(insertActionProto);
  }

  /**
   * Convert DeleteActionProto to DeleteAction.
   * 
   * @param deleteActionProto
   * @return
   */
  public static DeleteAction convertDeleteAction(
      DeleteActionProto deleteActionProto) {
    return DeleteAction.convert(deleteActionProto);
  }

  /**
   * Get HTableDescriptor[] from GetTableDescriptorsResponse protobuf
   * 
   * @param proto
   *          the GetTableDescriptorsResponse
   * @return HTableDescriptor[]
   */
  public static FTable[] getHTableDescriptorArray(
      GetTableDescriptorsResponse proto) {
    if (proto == null)
      return null;

    FTable[] ret = new FTable[proto.getTableSchemaCount()];
    for (int i = 0; i < proto.getTableSchemaCount(); ++i) {
      ret[i] = convertITableSchema(proto.getTableSchema(i));
    }
    return ret;
  }

  public static FTable convertITableSchema(ITableSchema proto) {
    FTable table = FTable.convert(proto.getTableSchema());
    LinkedHashMap<String, Index> indexs = new LinkedHashMap<String, Index>();
    for (IndexSchema indexSchema : proto.getIndexSchemaList()) {
      Index index = Index.convert(indexSchema);
      indexs.put(index.getIndexName(), index);
    }
    table.setIndex(indexs);
    return table;
  }

  public static ITableSchema convertITableSchema(FTable table)
      throws MetaException {
    ITableSchema.Builder iBuilder = ITableSchema.newBuilder();
    iBuilder.setTableSchema(table.convert());
    for (Index index : table.getIndex().values()) {
      iBuilder.addIndexSchema(index.convert());
    }
    return iBuilder.build();
  }

  /**
   * get the split keys in form "byte [][]" from a CreateTableRequest proto
   * 
   * @param proto
   *          the CreateTableRequest
   * @return the split keys
   */
  public static byte[][] getSplitKeysArray(final CreateTableRequest proto) {
    byte[][] splitKeys = new byte[proto.getSplitKeysCount()][];
    for (int i = 0; i < proto.getSplitKeysCount(); ++i) {
      splitKeys[i] = proto.getSplitKeys(i).toByteArray();
    }
    return splitKeys;
  }

  /**
   * Convert a ByteArrayComparable to a protocol buffer Comparator
   * 
   * @param comparator
   *          the ByteArrayComparable to convert
   * @return the converted protocol buffer Comparator
   */
  public static ComparatorProtos.Comparator toComparator(
      ByteArrayComparable comparator) {
    ComparatorProtos.Comparator.Builder builder = ComparatorProtos.Comparator
        .newBuilder();
    builder.setName(comparator.getClass().getName());
    builder.setSerializedComparator(ByteString.copyFrom(comparator
        .toByteArray()));
    return builder.build();
  }

  /**
   * Convert a protocol buffer Comparator to a ByteArrayComparable
   * 
   * @param proto
   *          the protocol buffer Comparator to convert
   * @return the converted ByteArrayComparable
   */
  @SuppressWarnings("unchecked")
  public static ByteArrayComparable toComparator(
      ComparatorProtos.Comparator proto) throws IOException {
    String type = proto.getName();
    String funcName = "parseFrom";
    byte[] value = proto.getSerializedComparator().toByteArray();
    try {
      Class<? extends ByteArrayComparable> c = (Class<? extends ByteArrayComparable>) (Class
          .forName(type));
      Method parseFrom = c.getMethod(funcName, byte[].class);
      if (parseFrom == null) {
        throw new IOException("Unable to locate function: " + funcName
            + " in type: " + type);
      }
      return (ByteArrayComparable) parseFrom.invoke(null, value);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Convert a stringified protocol buffer exception Parameter to a Java
   * Exception
   * 
   * @param parameter
   *          the protocol buffer Parameter to convert
   * @return the converted Exception
   * @throws IOException
   *           if failed to deserialize the parameter
   */
  @SuppressWarnings("unchecked")
  public static Throwable toException(final StringBytesPair parameter)
      throws IOException {
    if (parameter == null || !parameter.hasValue())
      return null;
    String desc = parameter.getValue().toStringUtf8();
    String type = parameter.getName();
    try {
      Class<? extends Throwable> c = (Class<? extends Throwable>) Class
          .forName(type);
      Constructor<? extends Throwable> cn = c
          .getDeclaredConstructor(String.class);
      return cn.newInstance(desc);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * A helper to retrieve entityGroup info given a entityGroup name using admin
   * protocol.
   * 
   * @param admin
   * @param entityGroupName
   * @return the retrieved entityGroup info
   * @throws IOException
   */
  public static EntityGroupInfo getEntityGroupInfo(final AdminProtocol admin,
      final byte[] entityGroupName) throws IOException {
    try {
      GetEntityGroupInfoRequest request = RequestConverter
          .buildGetEntityGroupInfoRequest(entityGroupName);
      GetEntityGroupInfoResponse response = admin.getEntityGroupInfo(null,
          request);
      return EntityGroupInfo.convert(response.getEntityGroupInfo());
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to close a entityGroup given a entityGroup name using admin
   * protocol.
   * 
   * @param admin
   * @param egi
   * @param transitionInZK
   * @throws IOException
   */
  public static void closeEntityGroup(final AdminProtocol admin,
      final EntityGroupInfo egi, final boolean transitionInZK)
      throws IOException {
    CloseEntityGroupRequest closeEntityGroupRequest = RequestConverter
        .buildCloseEntityGroupRequest(egi, transitionInZK);
    try {
      admin.closeEntityGroup(null, closeEntityGroupRequest);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to close a entityGroup given a entityGroup name using admin
   * protocol.
   * 
   * @param admin
   * @param egi
   * @param versionOfClosingNode
   * @return true if the entityGroup is closed
   * @throws IOException
   */
  public static boolean closeEntityGroup(final AdminProtocol admin,
      final EntityGroupInfo egi, final int versionOfClosingNode,
      final boolean transitionInZK) throws IOException {
    CloseEntityGroupRequest closeEntityGroupRequest = RequestConverter
        .buildCloseEntityGroupRequest(egi, versionOfClosingNode, transitionInZK);
    try {
      CloseEntityGroupResponse response = admin.closeEntityGroup(null,
          closeEntityGroupRequest);
      return ResponseConverter.isClosed(response);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to close a entityGroup given a entityGroup name using admin
   * protocol.
   * 
   * @param admin
   * @param egi
   * @param versionOfClosingNode
   * @return true if the entityGroup is closed
   * @throws IOException
   */
  public static boolean closeEntityGroup(final AdminProtocol admin,
      final EntityGroupInfo egi, final int versionOfClosingNode)
      throws IOException {
    CloseEntityGroupRequest closeEntityGroupRequest = RequestConverter
        .buildCloseEntityGroupRequest(egi, versionOfClosingNode);
    try {
      CloseEntityGroupResponse response = admin.closeEntityGroup(null,
          closeEntityGroupRequest);
      return ResponseConverter.isClosed(response);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to open a entityGroup using admin protocol.
   * 
   * @param admin
   * @param entityGroup
   * @throws IOException
   */
  public static void openEntityGroup(final AdminProtocol admin,
      final EntityGroupInfo entityGroup) throws IOException {
    OpenEntityGroupRequest request = RequestConverter
        .buildOpenEntityGroupRequest(entityGroup, -1);
    try {
      admin.openEntityGroup(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * A helper to open a list of entityGroups using admin protocol.
   * 
   * @param admin
   * @param entityGroups
   * @return OpenEntityGroupResponse
   * @throws IOException
   */
  public static OpenEntityGroupResponse openEntityGroup(
      final AdminProtocol admin, final List<EntityGroupInfo> entityGroups)
      throws IOException {
    OpenEntityGroupRequest request = RequestConverter
        .buildOpenEntityGroupRequest(entityGroups);
    try {
      return admin.openEntityGroup(null, request);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to get the all the online entityGroups on a fserver using admin
   * protocol.
   * 
   * @param admin
   * @return a list of online entityGroup info
   * @throws IOException
   */
  public static List<EntityGroupInfo> getOnlineEntityGroups(
      final AdminProtocol admin) throws IOException {
    GetOnlineEntityGroupsRequest request = RequestConverter
        .buildGetOnlineEntityGroupsRequest();
    GetOnlineEntityGroupsResponse response = null;
    try {
      response = admin.getOnlineEntityGroups(null, request);
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
    return getEntityGroupInfos(response);
  }

  /**
   * Get the list of entityGroup info from a GetOnlineEntityGroupsResponse
   * 
   * @param proto
   *          the GetOnlineEntityGroupsResponse
   * @return the list of entityGroup info or null if <code>proto</code> is null
   */
  static List<EntityGroupInfo> getEntityGroupInfos(
      final GetOnlineEntityGroupsResponse proto) {
    if (proto == null)
      return null;
    List<EntityGroupInfo> entityGroupInfos = new ArrayList<EntityGroupInfo>();
    for (WaspProtos.EntityGroupInfoProtos entityGroupInfo : proto
        .getEntityGroupInfosList()) {
      entityGroupInfos.add(EntityGroupInfo.convert(entityGroupInfo));
    }
    return entityGroupInfos;
  }

  /**
   * A helper to get the info of a fserver using admin protocol.
   * 
   * @param admin
   * @return the server name
   * @throws IOException
   */
  public static ServerInfo getServerInfo(final AdminProtocol admin)
      throws IOException {
    GetServerInfoRequest request = RequestConverter.buildGetServerInfoRequest();
    try {
      GetServerInfoResponse response = admin.getServerInfo(null, request);
      return response.getServerInfo();
    } catch (ServiceException se) {
      throw getRemoteException(se);
    }
  }

  /**
   * A helper to split a entityGroup using admin protocol.
   * 
   * @param admin
   * @param egi
   * @param splitPoint
   * @throws IOException
   */
  public static void split(final AdminProtocol admin,
      final EntityGroupInfo egi, byte[] splitPoint) throws IOException {
    SplitEntityGroupRequest request = RequestConverter
        .buildSplitEntityGroupRequest(egi.getEntityGroupName(), splitPoint);
    try {
      admin.splitEntityGroup(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  // End helpers for Admin

  /*
   * Get the total (read + write) requests from a EntityGroupLoad pb
   * 
   * @param rl - EntityGroupLoad pb
   * 
   * @return total (read + write) requests
   */
  public static long getTotalRequestsCount(EntityGroupLoadProtos rl) {
    if (rl == null) {
      return 0;
    }

    return rl.getReadRequestsCount() + rl.getWriteRequestsCount();
  }

  /**
   * @param m
   *          Message to get delimited pb serialization of (with pb magic
   *          prefix)
   */
  public static byte[] toDelimitedByteArray(final Message m) throws IOException {
    // Allocate arbitrary big size so we avoid resizing.
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    m.writeDelimitedTo(baos);
    baos.close();
    return baos.toByteArray();
  }

  /**
   * Unwraps an exception from a protobuf service into the underlying (expected)
   * IOException. This method will <strong>always</strong> throw an exception.
   * 
   * @param se
   *          the {@code ServiceException} instance to convert into an
   *          {@code IOException}
   */
  public static void toIOException(ServiceException se) throws IOException {
    if (se == null) {
      throw new NullPointerException("Null service exception passed!");
    }

    Throwable cause = se.getCause();
    if (cause != null && cause instanceof IOException) {
      throw (IOException) cause;
    }
    throw new IOException(se);
  }

  public static List<StringStringPair> toStringStringPairList(
      Map<String, String> parameter) {
    List<StringStringPair> pairs = new ArrayList<StringStringPair>();
    for (Map.Entry<String, String> entry : parameter.entrySet()) {
      StringStringPair.Builder builder = StringStringPair.newBuilder();
      builder.setName(entry.getKey());
      builder.setName(entry.getValue());
      pairs.add(builder.build());
    }
    return pairs;
  }

  public static Map<String, String> toMap(List<StringStringPair> pairs) {
    Map<String, String> parameters = new HashMap<String, String>();
    for (StringStringPair pair : pairs) {
      parameters.put(pair.getName(), pair.getValue());
    }
    return parameters;
  }

  public static StringStringPair toStringStringPair(String key, String value) {
    StringStringPair.Builder builder = StringStringPair.newBuilder();
    builder.setName(key);
    builder.setValue(value);
    return builder.build();
  }

  public static GetActionProto convertGetAction(GetAction action) {
    return GetAction.convert(action);
  }
}
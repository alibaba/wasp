package org.apache.wasp.storage;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.plan.action.DeleteAction;
import org.apache.wasp.plan.action.InsertAction;
import org.apache.wasp.plan.action.UpdateAction;

import java.io.IOException;

public class NullStorageServices implements StorageServices {

  @Override
  public Result getRow(String entityTableName, Get get) throws IOException,
      StorageTableNotFoundException {
    return new Result();
  }

  @Override
  public void putRow(String entityTableName, Put put) throws IOException,
      StorageTableNotFoundException {
  }

  @Override
  public void deleteRow(String entityTableName, Delete delete)
      throws IOException, StorageTableNotFoundException {
  }

  @Override
  public void checkRowExistsBeforeInsert(InsertAction insert,
      String entityTableName, Put entityPut) throws IOException,
      StorageTableNotFoundException {
  }

  @Override
  public Result getRowBeforeUpdate(UpdateAction update, String entityTableName,
      Get get) throws IOException, StorageTableNotFoundException {
    KeyValue[] keyValues = new KeyValue[1];
    keyValues[0] = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"),
        Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
    Result result = new Result(keyValues);
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.wasp.storage.StorageServices#getRowBeforeDelete(org.apache.wasp
   * .plan.action.DeleteAction, java.lang.String,
   * org.apache.hadoop.hbase.client.Get)
   */
  @Override
  public Result getRowBeforeDelete(DeleteAction delete, String entityTableName,
      Get get) throws IOException, StorageTableNotFoundException {
    KeyValue[] keyValues = new KeyValue[1];
    keyValues[0] = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"),
        Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
    Result result = new Result(keyValues);
    return result;
  }
}

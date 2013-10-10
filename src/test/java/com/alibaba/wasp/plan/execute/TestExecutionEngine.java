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
package com.alibaba.wasp.plan.execute;

import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.ReadModel;
import com.alibaba.wasp.WaspTestingUtility;
import com.alibaba.wasp.client.FClient;
import com.alibaba.wasp.fserver.FServer;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.DeletePlan;
import com.alibaba.wasp.plan.InsertPlan;
import com.alibaba.wasp.plan.LocalQueryPlan;
import com.alibaba.wasp.plan.UpdatePlan;
import com.alibaba.wasp.plan.parser.ParseContext;
import com.alibaba.wasp.plan.parser.Parser;
import com.alibaba.wasp.plan.parser.WaspParser;
import com.alibaba.wasp.protobuf.generated.ClientProtos;
import com.alibaba.wasp.protobuf.generated.ClientProtos.QueryResultProto;
import com.alibaba.wasp.protobuf.generated.ClientProtos.StringDataTypePair;
import com.alibaba.wasp.protobuf.generated.ClientProtos.WriteResultProto;
import com.alibaba.wasp.util.ResultInHBasePrinter;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestExecutionEngine {

  final Log LOG = LogFactory.getLog(getClass());

  private static ExecutionEngine engine;

  private static WaspTestingUtility TEST_UTIL = new WaspTestingUtility();

  private static final String TABLE_NAME = "TEST_TABLE";

  public static final byte[] TABLE = Bytes.toBytes(TABLE_NAME);

  private static FTable table;

  private static FClient client;

  public static final String INDEX_NAME = "test_index";

  /** SQL parser engine. **/
  private static Parser parser;

  @BeforeClass
  public static void before() throws Exception {
    WaspTestingUtility.adjustLogLevel();
    Logger.getLogger(ZooKeeperServer.class).setLevel(Level.DEBUG);
    Logger.getLogger(ZooKeeper.class).setLevel(Level.DEBUG);
    Logger.getLogger(PrepRequestProcessor.class).setLevel(Level.DEBUG);
    Logger.getLogger(NIOServerCnxn.class).setLevel(Level.DEBUG);
    TEST_UTIL.startMiniCluster(1);

    engine = new ExecutionEngine(new FServer(TEST_UTIL.getConfiguration()));

    table = TEST_UTIL.createTable(Bytes.toBytes(TABLE_NAME));
    TEST_UTIL.waitTableEnabled(TABLE, 180000);
    TEST_UTIL.getWaspAdmin().disableTable(TABLE);
    client = new FClient(TEST_UTIL.getConfiguration());
    client.execute("create index " + INDEX_NAME + " on " + TABLE_NAME
        + "(column3);");
    TEST_UTIL.getWaspAdmin().waitTableNotLocked(TABLE);
    TEST_UTIL.getWaspAdmin().enableTable(TABLE);

    Class<? extends Parser> parserClass = TEST_UTIL.getConfiguration()
        .getClass(FConstants.WASP_SQL_PARSER_CLASS, WaspParser.class,
            Parser.class);
    parser = ReflectionUtils.newInstance(parserClass,
        TEST_UTIL.getConfiguration());
    assertNotNull(table);
  }

  @AfterClass
  public static void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testExecInsertPlan() throws ServiceException, IOException {
    String sql = "Insert into " + TABLE_NAME
        + " (column1,column2,column3) values (123,456,'abc');";
    ParseContext context = new ParseContext();
    context.setTsr(TableSchemaCacheReader.getInstance(TEST_UTIL
        .getConfiguration()));
    context.setSql(sql);
    context.setReadModel(ReadModel.CURRENT);
    parser.generatePlan(context);

    InsertPlan plan = (InsertPlan) context.getPlan();
    List<WriteResultProto> writeResultProtos = engine.execInsertPlan(plan);
    assertTrue(writeResultProtos.size() == 1);

    for (ClientProtos.WriteResultProto writeResultProto : writeResultProtos) {
      assertTrue(writeResultProto.getCode() == ClientProtos.WriteResultProto.StatusCode.SUCCESS);
    }
  }

  @Test
  public void testExecQueryPlan() throws ServiceException, IOException {
    printTablesRs("insert");
    testExecQueryPlan("select column1,column2,column3 from " + TABLE_NAME
        + " where column1=123 and column2=456;", true);
  }

  /**
   *
   * @param sql
   *          the columns -> value Map
   * @param exits
   *          if exits the target by PK. if test delete. it will be false, and
   *          query result is null
   * @throws com.google.protobuf.ServiceException
   */
  private void testExecQueryPlan(String sql, boolean exits)
      throws ServiceException, IOException {

    ParseContext context = new ParseContext();
    context.setTsr(TableSchemaCacheReader.getInstance(TEST_UTIL
        .getConfiguration()));
    context.setSql(sql);
    context.setReadModel(ReadModel.CURRENT);
    parser.generatePlan(context);

    LocalQueryPlan plan = (LocalQueryPlan) context.getPlan();

    Pair<Boolean, Pair<String, Pair<List<QueryResultProto>, List<StringDataTypePair>>>> rets = engine
        .execQueryPlan(plan, "", false);
    if (!exits) {
      assertTrue(rets.getSecond().getSecond().getFirst().size() == 0);
      return;
    } else {
      assertTrue(rets.getSecond().getSecond().getFirst().size() > 0);
    }
  }

  private void printTablesRs(String type) throws IOException {
    ResultInHBasePrinter.printTablesRs(type, TEST_UTIL.getConfiguration(),
        TABLE_NAME, INDEX_NAME, LOG);
  }

  @Test
  public void testExecUpdatePlan() throws ServiceException, IOException {
    String sql = "UPDATE " + TABLE_NAME
        + " SET column3 = 'def' WHERE column1 = 123 and column2=456;";
    ParseContext context = new ParseContext();
    context.setTsr(TableSchemaCacheReader.getInstance(TEST_UTIL
        .getConfiguration()));
    context.setSql(sql);
    context.setReadModel(ReadModel.CURRENT);
    parser.generatePlan(context);

    UpdatePlan plan = (UpdatePlan) context.getPlan();
    List<WriteResultProto> writeResultProtos = engine.execUpdatePlan(plan);
    assertTrue(writeResultProtos.size() == 1);

    for (ClientProtos.WriteResultProto writeResultProto : writeResultProtos) {
      assertTrue(writeResultProto.getCode() == ClientProtos.WriteResultProto.StatusCode.SUCCESS);
    }
    testExecQueryPlan("select column1,column2,column3 from " + TABLE_NAME
        + " where column1=123 and column2=456;", true);
  }

  @Test
  public void testExecDeletePlan() throws ServiceException, IOException {
    String sql = "delete from " + TABLE_NAME
        + " where column1=123 and column2=456;";
    ParseContext context = new ParseContext();
    context.setTsr(TableSchemaCacheReader.getInstance(TEST_UTIL
        .getConfiguration()));
    context.setSql(sql);
    context.setReadModel(ReadModel.CURRENT);
    parser.generatePlan(context);

    DeletePlan plan = (DeletePlan) context.getPlan();
    List<WriteResultProto> writeResultProtos = engine.execDeletePlan(plan);
    assertTrue(writeResultProtos.size() == 1);

    for (ClientProtos.WriteResultProto writeResultProto : writeResultProtos) {
      assertTrue(writeResultProto.getCode() == ClientProtos.WriteResultProto.StatusCode.SUCCESS);
    }
    testExecQueryPlan("select column1,column2,column3 from " + TABLE_NAME
        + " where column1=123 and column3='def';", false);
  }

  @Test
  public void testQueryNext() {
    // TODO
  }
}

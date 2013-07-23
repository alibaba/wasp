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
package org.apache.wasp.fserver;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.wasp.DataType;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.WaspTestingUtility;
import org.apache.wasp.ZooKeeperConnectionException;
import org.apache.wasp.fserver.redo.Redo;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.meta.Field;
import org.apache.wasp.meta.Index;
import org.apache.wasp.meta.TableSchemaCacheReader;
import org.apache.wasp.plan.CreateIndexPlan;
import org.apache.wasp.plan.CreateTablePlan;
import org.apache.wasp.plan.DeletePlan;
import org.apache.wasp.plan.GlobalQueryPlan;
import org.apache.wasp.plan.InsertPlan;
import org.apache.wasp.plan.LocalQueryPlan;
import org.apache.wasp.plan.Plan;
import org.apache.wasp.plan.UpdatePlan;
import org.apache.wasp.plan.action.DeleteAction;
import org.apache.wasp.plan.action.GetAction;
import org.apache.wasp.plan.action.InsertAction;
import org.apache.wasp.plan.action.ScanAction;
import org.apache.wasp.plan.action.UpdateAction;
import org.apache.wasp.plan.parser.ParseContext;
import org.apache.wasp.plan.parser.WaspParser;
import org.apache.wasp.plan.parser.druid.DruidDDLParser;
import org.apache.wasp.plan.parser.druid.DruidDMLParser;
import org.apache.wasp.plan.parser.druid.DruidDQLParser;
import org.apache.wasp.plan.parser.druid.DruidParserTestUtil;
import org.apache.wasp.protobuf.generated.ClientProtos.QueryResultProto;
import org.apache.wasp.protobuf.generated.WaspProtos.StringBytesPair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Tests that need to spin up a cluster testing an {@link EntityGroup}. Use
 * {@link TestEntityGroup} if you don't need a cluster, if you can test w/ a
 * standalone {@link EntityGroup}.
 */
@Category(MediumTests.class)
public class TestEntityGroupOnCluster {
  static final Log LOG = LogFactory.getLog(TestEntityGroupOnCluster.class);

  // EntityGroup entityGroup = null;
  private static final WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  private static Configuration conf = null;
  private static ParseContext context = new ParseContext();
  private static WaspParser druidParser = null;
  private static FTable[] table = null;
  private static EntityGroup eg = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TableSchemaCacheReader.globalFMetaservice = null;
    print("Before setUpBeforeClass");
    WaspTestingUtility.adjustLogLevel();
    TEST_UTIL.startMiniCluster(3);
    conf = TEST_UTIL.getConfiguration();
    context.setGenWholePlan(false);
    DruidDQLParser dqlParser = new DruidDQLParser(conf);
    DruidDDLParser ddlParser = new DruidDDLParser(conf);
    DruidDMLParser dmlParser = new DruidDMLParser(conf);
    druidParser = new WaspParser(ddlParser, dqlParser, dmlParser);
    TableSchemaCacheReader reader = TableSchemaCacheReader.getInstance(conf);
    reader.clearCache();
    context.setTsr(reader);
    // create table
    table = new FTable[DruidParserTestUtil.SEED.length];
    for (int i = 0; i < DruidParserTestUtil.SEED.length; i++) {
      String createTable = DruidParserTestUtil.SEED[i];
      context.setSql(createTable);
      druidParser.generatePlan(context);
      Plan plan = context.getPlan();
      if (plan instanceof CreateTablePlan) {
        CreateTablePlan createPlan = (CreateTablePlan) plan;
        table[i] = createPlan.getTable();
        TableSchemaCacheReader.getService(conf).createTable(table[i]);
        reader.addSchema(table[i].getTableName(), table[i]);
      }
    }
    for (int i = 0; i < DruidParserTestUtil.INDEX_SEED.length; i++) {
      String createIndex = DruidParserTestUtil.INDEX_SEED[i];
      context.setSql(createIndex);
      druidParser.generatePlan(context);
      Plan plan = context.getPlan();
      if (plan instanceof CreateIndexPlan) {
        CreateIndexPlan createIndexPlan = (CreateIndexPlan) plan;
        Index index = createIndexPlan.getIndex();
        TableSchemaCacheReader.getService(conf).addIndex(
            index.getDependentTableName(), index);
        reader.refreshSchema(index.getDependentTableName());
      }
    }
    for (FTable ftable : table) {
      TableSchemaCacheReader.getInstance(conf).refreshSchema(
          ftable.getTableName());
    }
    EntityGroupInfo egi = new EntityGroupInfo(table[0].getTableName(), null,
        null);
    // MockFServerServices service = new MockFServerServices();
    eg = new EntityGroup(conf, egi, table[0], TEST_UTIL.getMiniWaspCluster()
        .getFServer(0));
    eg.initialize();
    print("After setUpBeforeClass");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TableSchemaCacheReader.getInstance(conf, null);
    TEST_UTIL.shutdownMiniCluster();
  }

  public static void print(String message) {
    System.out.println("\n\n\n" + message + "\n\n\n");
  }

  @Test
  public void testInsert() throws Throwable {
    print("Before testInsert");
    boolean success = true;
    String[] insert = {
        // 2147483647
        "Insert into User(user_id,name,value) values(1,'binlijin', 9999999999);",
        "Insert into User(user_id,name,value) values(2,'binlijin', 9.9);",
        "Insert into User(user_id,name,value) values(3,'binlijin', 99999999.99);",
        "Insert into Photo(user_id,photo_id,time,full_url) values(1,1000,10000,'dads1');",
        "Insert into Photo(user_id,photo_id,time,full_url) values(1,2000,20000,'dads2');",
        "Insert into Photo(user_id,photo_id,time,full_url,tag) values(1,3000,30000,'dads3','daizhiyuan');",
        "UPDATE Photo SET full_url = 'hhh' WHERE user_id = 1 and photo_id=1000;",
        "UPDATE Photo SET time = 10001 WHERE user_id = 1 and photo_id=1000;",
        "DELETE FROM Photo WHERE user_id = 1 and photo_id=1000;" };
    String[] select = {
        "Select user_id,name,value from User where user_id=1;",
        "Select user_id,name,value from User where user_id=2;",
        "Select user_id,name,value from User where user_id=3;",
        "Select user_id,photo_id,time,full_url from Photo where user_id=1 and photo_id=1000;",
        "Select user_id,photo_id,time,full_url from Photo where user_id=1 and time=20000;",
        "Select user_id,photo_id,time,full_url,tag from Photo where tag='daizhiyuan';",
        "Select user_id,photo_id,time,full_url from Photo where user_id=1 and photo_id=1000;",
        "Select user_id,photo_id,time,full_url from Photo where user_id=1 and time=10001;",
        "Select user_id,photo_id,time,full_url from Photo where user_id=1 and photo_id=1000;" };

    FTable[] testFTable = { table[0], table[0], table[0], table[1], table[1],
        table[1], table[1], table[1], table[1] };

    for (int i = 0; i < insert.length; i++) {
      success = true;
      print("Execute " + insert[i]);
      try {
        // insert
        context.setSql(insert[i]);
        druidParser.generatePlan(context);
        Plan plan = context.getPlan();
        if (plan instanceof InsertPlan) {
          InsertPlan insertPlan = (InsertPlan) plan;
          List<InsertAction> units = insertPlan.getActions();
          Assert.assertEquals(units.size(), 1);
          // EntityGroup execute insert
          OperationStatus status = eg.insert(units.get(0));
          if (status.getOperationStatusCode() != org.apache.hadoop.hbase.HConstants.OperationStatusCode.SUCCESS) {
            success = false;
          }
          Redo redo = eg.getLog();
          try {
            while (redo.peekLastUnCommitedTransaction() != null) {
              Thread.sleep(500);
            }
          } catch (Exception e) {
            success = false;
          }
        } else if (plan instanceof UpdatePlan) {
          UpdatePlan updatePlan = (UpdatePlan) plan;
          List<UpdateAction> units = updatePlan.getActions();
          Assert.assertEquals(units.size(), 1);
          // EntityGroup execute insert
          OperationStatus status = eg.update(units.get(0));
          if (status.getOperationStatusCode() != org.apache.hadoop.hbase.HConstants.OperationStatusCode.SUCCESS) {
            success = false;
          }
          Redo redo = eg.getLog();
          try {
            while (redo.peekLastUnCommitedTransaction() != null) {
              Thread.sleep(500);
            }
          } catch (Exception e) {
            success = false;
          }
        } else if (plan instanceof DeletePlan) {
          LOG.info(" DeletePlan is " + plan.toString());
          DeletePlan deletePlan = (DeletePlan) plan;
          List<DeleteAction> units = deletePlan.getActions();
          Assert.assertEquals(units.size(), 1);
          OperationStatus status = eg.delete(units.get(0));
          if (status.getOperationStatusCode() != org.apache.hadoop.hbase.HConstants.OperationStatusCode.SUCCESS) {
            success = false;
          }
          Redo redo = eg.getLog();
          try {
            while (redo.peekLastUnCommitedTransaction() != null) {
              Thread.sleep(500);
            }
          } catch (Exception e) {
            success = false;
          }
        } else {
          Assert.assertTrue(false);
        }

        context.setSql(select[i]);
        druidParser.generatePlan(context);
        plan = context.getPlan();
        LOG.info("Plan is " + plan.toString());
        if (plan instanceof LocalQueryPlan) {
          LocalQueryPlan localQueryPlan = (LocalQueryPlan) plan;
          GetAction getAction = localQueryPlan.getGetAction();
          if (getAction != null) {
            LOG.info("GetAction " + getAction.toString());
            Result rs = eg.get(getAction);
            LOG.info("Result " + rs.toString());
            if (!rs.isEmpty()) {
              Collection<Field> columns = testFTable[i].getColumns().values();
              for (Field f : columns) {
                try {
                  byte[] value = rs.getValue(Bytes.toBytes(f.getFamily()),
                      Bytes.toBytes(f.getName()));
                  if (f.getType() == DataType.INT64) {
                    LOG.info("Column:" + f.getName() + " , value:"
                        + Bytes.toLong(value));
                  } else if (f.getType() == DataType.STRING) {
                    LOG.info("Column:" + f.getName() + " , value:"
                        + Bytes.toString(value));
                  } else if (f.getType() == DataType.INT32) {
                    LOG.info("Column:" + f.getName() + " , value:"
                        + Bytes.toInt(value));
                  } else if (f.getType() == DataType.DOUBLE) {
                    LOG.info("Column:" + f.getName() + " , value:"
                        + Bytes.toDouble(value));
                  } else if (f.getType() == DataType.FLOAT) {
                    LOG.info("Column:" + f.getName() + " , value:"
                        + Bytes.toFloat(value));
                  }
                } catch (Throwable e) {
                  LOG.error("Why?(" + f.getFamily() + ":" + f.getName() + ")",
                      e);
                  throw e;
                }
              }
            }
          } else {
            ScanAction scanAction = localQueryPlan.getScanAction();
            executeScanAction(eg, scanAction, testFTable[i]);
          }
        } else {
          GlobalQueryPlan globalQueryPlan = (GlobalQueryPlan) plan;
          ScanAction scanAction = globalQueryPlan.getAction();
          executeScanAction(eg, scanAction, testFTable[i]);
        }
      } catch (ZooKeeperConnectionException e) {
        e.printStackTrace();
        success = false;
      } catch (IOException e) {
        e.printStackTrace();
        success = false;
      }
      Assert.assertTrue(success);
    }
    print("After testInsert");
  }

  public void executeScanAction(EntityGroup eg, ScanAction scanAction,
      FTable testFTable) throws IOException {
    LOG.info(" ScanAction " + scanAction.toString());
    EntityGroupScanner scanner = eg.getScanner(scanAction);
    List<QueryResultProto> results = new ArrayList<QueryResultProto>(
        scanner.getCaching());
    // Collect values to be returned here
    scanner.next(results);
    scanner.close();
    for (QueryResultProto rs : results) {
      for (StringBytesPair pair : rs.getResultList()) {
        byte[] value = pair.getValue().toByteArray();
        Field f = testFTable.getColumn(pair.getName());
        LOG.info(" StringBytesPair " + pair.getName());
        if (f != null) {
          if (f.getType() == DataType.INT64) {
            LOG.info("Column:" + f.getName() + " , value:"
                + Bytes.toLong(value));
          } else if (f.getType() == DataType.STRING) {
            LOG.info("Column:" + f.getName() + " , value:"
                + Bytes.toString(value));
          } else if (f.getType() == DataType.INT32) {
            LOG.info("Column:" + f.getName() + " , value:" + Bytes.toInt(value));
          }
        }
      }
    }
  }
}
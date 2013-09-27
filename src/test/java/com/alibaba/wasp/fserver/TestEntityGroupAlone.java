package com.alibaba.wasp.fserver;

import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.fserver.redo.MemRedoLog;
import com.alibaba.wasp.fserver.redo.Redo;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.MemFMetaStore;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.CreateTablePlan;
import com.alibaba.wasp.plan.DeletePlan;
import com.alibaba.wasp.plan.InsertPlan;
import com.alibaba.wasp.plan.Plan;
import com.alibaba.wasp.plan.UpdatePlan;
import com.alibaba.wasp.plan.action.DeleteAction;
import com.alibaba.wasp.plan.action.InsertAction;
import com.alibaba.wasp.plan.action.UpdateAction;
import com.alibaba.wasp.plan.parser.ParseContext;
import com.alibaba.wasp.plan.parser.WaspParser;
import com.alibaba.wasp.plan.parser.druid.DruidDDLParser;
import com.alibaba.wasp.plan.parser.druid.DruidDMLParser;
import com.alibaba.wasp.plan.parser.druid.DruidDQLParser;
import com.alibaba.wasp.plan.parser.druid.DruidParserTestUtil;
import com.alibaba.wasp.storage.NullStorageServices;
import com.alibaba.wasp.util.MockFServerServices;
import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestEntityGroupAlone {
  static final Log LOG = LogFactory.getLog(TestEntityGroupAlone.class);
  private static Configuration conf = WaspConfiguration.create();
  private static ParseContext context = new ParseContext();
  private static WaspParser druidParser = null;
  private static FTable table = null;
  private static EntityGroup eg = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    context.setGenWholePlan(false);
    conf.setClass(FConstants.REDO_IMPL, MemRedoLog.class, Redo.class);
    DruidDQLParser dqlParser = new DruidDQLParser(conf, null);
    DruidDDLParser ddlParser = new DruidDDLParser(conf);
    DruidDMLParser dmlParser = new DruidDMLParser(conf, null);
    druidParser = new WaspParser(ddlParser, dqlParser, dmlParser);
    MemFMetaStore fmetaServices = new MemFMetaStore();
    TableSchemaCacheReader reader = TableSchemaCacheReader.getInstance(conf,
        fmetaServices);
    reader.clearCache();
    context.setTsr(reader);
    // create table
    String createTable = DruidParserTestUtil.SEED[0];
    context.setSql(createTable);
    druidParser.generatePlan(context);
    Plan plan = context.getPlan();
    if (plan instanceof CreateTablePlan) {
      CreateTablePlan createPlan = (CreateTablePlan) plan;
      table = createPlan.getTable();
      TableSchemaCacheReader.getService(conf).createTable(table);
      reader.addSchema(table.getTableName(), table);
    }
    EntityGroupInfo egi = new EntityGroupInfo(table.getTableName(), null, null);
    MockFServerServices service = new MockFServerServices();
    eg = new EntityGroup(conf, egi, table, service);
    NullStorageServices storageServices = new NullStorageServices();
    eg.setStorageServices(storageServices);
    eg.initialize();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TableSchemaCacheReader.getInstance(conf, null);
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testInsert() throws IOException {
    String insert = "Insert into User(user_id,name) values(1,'binlijin');";
    // insert
    context.setSql(insert);
    druidParser.generatePlan(context);
    Plan plan = context.getPlan();
    if (plan instanceof InsertPlan) {
      InsertPlan insertPlan = (InsertPlan) plan;
      List<InsertAction> units = insertPlan.getActions();
      Assert.assertEquals(units.size(), 1);
      // EntityGroup execute insert
      eg.insert(units.get(0));
      Redo redo = eg.getLog();
      if (redo instanceof MemRedoLog) {
        MemRedoLog memRedo = (MemRedoLog) redo;
        Assert.assertEquals(0, memRedo.size());
      }
    }
  }

  @Test
  public void testUpdate() throws IOException {
    String update = "UPDATE User SET name = 'Mike' WHERE user_id = 123;";
    // update
    context.setSql(update);
    druidParser.generatePlan(context);
    Plan plan = context.getPlan();
    if (plan instanceof UpdatePlan) {
      UpdatePlan updatePlan = (UpdatePlan) plan;
      List<UpdateAction> units = updatePlan.getActions();
      Assert.assertEquals(units.size(), 1);
      // EntityGroup execute update
      eg.update(units.get(0));
      Redo redo = eg.getLog();
      if (redo instanceof MemRedoLog) {
        MemRedoLog memRedo = (MemRedoLog) redo;
        Assert.assertEquals(memRedo.size(), 0);
      }
    }
  }

  @Test
  public void testDelete() throws IOException {
    String delete = "DELETE FROM User WHERE user_id = 123;";
    // delete
    context.setSql(delete);
    druidParser.generatePlan(context);
    Plan plan = context.getPlan();
    if (plan instanceof DeletePlan) {
      DeletePlan deletePlan = (DeletePlan) plan;
      List<DeleteAction> units = deletePlan.getActions();
      Assert.assertEquals(units.size(), 1);
      // EntityGroup execute delete
      eg.delete(units.get(0));
      Redo redo = eg.getLog();
      if (redo instanceof MemRedoLog) {
        MemRedoLog memRedo = (MemRedoLog) redo;
        Assert.assertEquals(memRedo.size(), 0);
      }
    }
  }
}
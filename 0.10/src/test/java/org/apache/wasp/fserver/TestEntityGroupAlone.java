package org.apache.wasp.fserver;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.FConstants;
import org.apache.wasp.conf.WaspConfiguration;
import org.apache.wasp.fserver.redo.MemRedoLog;
import org.apache.wasp.fserver.redo.Redo;
import org.apache.wasp.meta.FTable;
import org.apache.wasp.meta.MemFMetaStore;
import org.apache.wasp.meta.TableSchemaCacheReader;
import org.apache.wasp.plan.CreateTablePlan;
import org.apache.wasp.plan.DeletePlan;
import org.apache.wasp.plan.InsertPlan;
import org.apache.wasp.plan.Plan;
import org.apache.wasp.plan.UpdatePlan;
import org.apache.wasp.plan.action.DeleteAction;
import org.apache.wasp.plan.action.InsertAction;
import org.apache.wasp.plan.action.UpdateAction;
import org.apache.wasp.plan.parser.ParseContext;
import org.apache.wasp.plan.parser.WaspParser;
import org.apache.wasp.plan.parser.druid.DruidDDLParser;
import org.apache.wasp.plan.parser.druid.DruidDMLParser;
import org.apache.wasp.plan.parser.druid.DruidDQLParser;
import org.apache.wasp.plan.parser.druid.DruidParserTestUtil;
import org.apache.wasp.storage.NullStorageServices;
import org.apache.wasp.util.MockFServerServices;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
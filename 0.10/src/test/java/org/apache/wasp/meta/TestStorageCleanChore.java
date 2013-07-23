package org.apache.wasp.meta;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.wasp.WaspTestingUtility;
import org.apache.wasp.plan.CreateIndexPlan;
import org.apache.wasp.plan.CreateTablePlan;
import org.apache.wasp.plan.Plan;
import org.apache.wasp.plan.parser.ParseContext;
import org.apache.wasp.plan.parser.WaspParser;
import org.apache.wasp.plan.parser.druid.DruidDDLParser;
import org.apache.wasp.plan.parser.druid.DruidDMLParser;
import org.apache.wasp.plan.parser.druid.DruidDQLParser;
import org.apache.wasp.plan.parser.druid.DruidParserTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStorageCleanChore {

  static final Log LOG = LogFactory.getLog(TestStorageCleanChore.class);

  private static final WaspTestingUtility TEST_UTIL = new WaspTestingUtility();
  private static Configuration conf = null;
  private static ParseContext context = new ParseContext();
  private static WaspParser druidParser = null;
  private static FTable[] table = null;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TableSchemaCacheReader.globalFMetaservice = null;
    WaspTestingUtility.adjustLogLevel();
    TEST_UTIL.startMiniCluster(3);
    conf = TEST_UTIL.getConfiguration();
    context.setGenWholePlan(false);
    DruidDQLParser dqlParser = new DruidDQLParser(conf, null);
    DruidDDLParser ddlParser = new DruidDDLParser(conf);
    DruidDMLParser dmlParser = new DruidDMLParser(conf, null);
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
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TableSchemaCacheReader.getInstance(conf, null);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCleanChore() {
    try {
      FMetaServices fMetaServices = TableSchemaCacheReader.getService(conf);
      StorageCleanChore clean = new StorageCleanChore(1000, null, conf,
          fMetaServices);
      HTableDescriptor[] htables1 = fMetaServices.listStorageTables();
      for (HTableDescriptor h : htables1) {
        LOG.info("HTable : " + h.getNameAsString() + "  ");
      }
      Assert.assertEquals(htables1.length, 5);
      LOG.info("HTable========");
      clean.chore();
      HTableDescriptor[] htables2 = fMetaServices.listStorageTables();
      for (HTableDescriptor h : htables2) {
        LOG.info("HTable : " + h.getNameAsString() + "  ");
      }
      Assert.assertEquals(htables2.length, 5);

      HBaseTestingUtility hbaseTestingUtility = TEST_UTIL
          .getHBaseTestingUtility();
      HBaseAdmin hbaseAdmin = hbaseTestingUtility.getHBaseAdmin();
      String testTableName = "tr";
      String fami = "test";
      // test delete TransactionTable
      String tHTableName = StorageTableNameBuilder
          .buildTransactionTableName(testTableName);
      HTableDescriptor desc = new HTableDescriptor(tHTableName);
      HColumnDescriptor hcolumn = new HColumnDescriptor(fami);
      desc.addFamily(hcolumn);
      hbaseAdmin.createTable(desc);
      HTableDescriptor[] htables3 = fMetaServices.listStorageTables();
      Assert.assertEquals(htables3.length, 6);
      clean.chore();
      HTableDescriptor[] htables4 = fMetaServices.listStorageTables();
      Assert.assertEquals(htables4.length, 5);

      // test delete IndexTable
      Index index = new Index("testIndex", "testTable", null);
      String indexHTableName = StorageTableNameBuilder
          .buildIndexTableName(index);
      HTableDescriptor desc2 = new HTableDescriptor(indexHTableName);
      desc2.addFamily(hcolumn);
      hbaseAdmin.createTable(desc2);
      HTableDescriptor[] htables5 = fMetaServices.listStorageTables();
      Assert.assertEquals(htables5.length, 6);
      clean.chore();
      HTableDescriptor[] htables6 = fMetaServices.listStorageTables();
      Assert.assertEquals(htables6.length, 5);

      // test delete EntityTable
      String eHTableName = StorageTableNameBuilder
          .buildEntityTableName(testTableName);
      HTableDescriptor desc3 = new HTableDescriptor(eHTableName);
      desc3.addFamily(hcolumn);
      hbaseAdmin.createTable(desc3);
      HTableDescriptor[] htables7 = fMetaServices.listStorageTables();
      Assert.assertEquals(htables7.length, 6);
      clean.chore();
      HTableDescriptor[] htables8 = fMetaServices.listStorageTables();
      Assert.assertEquals(htables8.length, 5);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

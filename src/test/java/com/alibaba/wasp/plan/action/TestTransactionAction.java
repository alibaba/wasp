/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package com.alibaba.wasp.plan.action;

import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.parser.SQLParseException;
import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.conf.WaspConfiguration;
import com.alibaba.wasp.meta.MemFMetaStore;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.DMLPlan;
import com.alibaba.wasp.plan.DMLTransactionPlan;
import com.alibaba.wasp.plan.Plan;
import com.alibaba.wasp.plan.parser.ParseContext;
import com.alibaba.wasp.plan.parser.UnsupportedException;
import com.alibaba.wasp.plan.parser.WaspParser;
import com.alibaba.wasp.plan.parser.druid.DruidDDLParser;
import com.alibaba.wasp.plan.parser.druid.DruidDMLParser;
import com.alibaba.wasp.plan.parser.druid.DruidDQLParser;
import com.alibaba.wasp.plan.parser.druid.DruidParserTestUtil;
import com.alibaba.wasp.protobuf.generated.MetaProtos;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestTransactionAction {

  private static Configuration conf = WaspConfiguration.create();
  private static ParseContext context = new ParseContext();
  private static TableSchemaCacheReader reader;
  private static MemFMetaStore fmetaServices = new MemFMetaStore();

  @BeforeClass
  public static void setUp() throws Exception {
    context.setGenWholePlan(false);

    reader = TableSchemaCacheReader.getInstance(conf, fmetaServices);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    reader.clearCache();
    TableSchemaCacheReader.getInstance(conf, null);
  }

  @Test
  public void testGenerateTransactionAction() {

    try {
      TableSchemaCacheReader reader = TableSchemaCacheReader.getInstance(conf);
      context.setTsr(reader);

      DruidDQLParser dqlParser = new DruidDQLParser(conf, null);
      DruidDDLParser ddlParser = new DruidDDLParser(conf);
      DruidDMLParser dmlParser = new DruidDMLParser(conf, null);
      WaspParser druidParser = new WaspParser(ddlParser, dqlParser, dmlParser);

      DruidParserTestUtil.loadTable(context, druidParser.getDruidDDLParser(),
          fmetaServices);

      DMLTransactionPlan transactionPlan = getTransactionPlan(druidParser);
      List<TransactionAction> actions = transactionPlan.getActions();
      Assert.assertEquals(actions.size(), 1);

      List<DMLAction> dmlActions = actions.get(0).getDmlActions();
      Assert.assertEquals(dmlActions.size(), 2);

      for (DMLAction dmlAction : dmlActions) {
        Assert.assertTrue(dmlAction.getTableName().equals("User") || dmlAction.getTableName().equals("Photo"));
      }


    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ServiceException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Test
  public void testTransactionAction() {
    TransactionAction transactionAction = ActionTestUtil
        .makeTestTransactionAction();
    MetaProtos.TransactionActionProto insertActionProto = TransactionAction
        .convert(transactionAction);
    TransactionAction derTransactionAction = TransactionAction
        .convert(insertActionProto);
    Equals(transactionAction, derTransactionAction);
  }

  public static boolean Equals(TransactionAction one, TransactionAction two) {
    if (one == two) {
      return true;
    }
    if (!one.getFTableName().equals(two.getFTableName())) {
      return false;
    }

    List<DMLAction> oneDMLActions = one.getDmlActions();
    List<DMLAction> twoDMLActions = two.getDmlActions();
    if (oneDMLActions.size() != twoDMLActions.size()) {
      return false;
    }
    // for (int i = 0; i < oneDMLActions.size(); i++) {
    // if (!oneColumns.get(i).equals(twoColumns.get(i))) {
    // return false;
    // }
    // }
    return true;
  }

  public DMLTransactionPlan getTransactionPlan(WaspParser druidParser)
      throws IOException, ServiceException {
    List<DMLPlan> dmlPlans = new ArrayList<DMLPlan>();

    List<String> sqls = new ArrayList<String>();
    sqls.add("Insert into User(user_id,name) values(1,'binlijin');");
    sqls.add("Insert into Photo(user_id,photo_id,tag) values(1,1,'tag');");

    for (String sql : sqls) {
      ParseContext context = new ParseContext();
      context.setGenWholePlan(false);
      context.setTsr(reader);
      context.setSql(sql);
      try {
        druidParser.generatePlan(context);
      } catch (RuntimeException e) {
        if (e instanceof ParserException || e instanceof SQLParseException) {
          throw new DoNotRetryIOException(e.getMessage(), e);
        } else {
          throw new ServiceException(new IOException(e));
        }
      } catch (UnsupportedException e) {
        throw e;
      }
      Plan executePlan = context.getPlan();
      if (executePlan instanceof DMLPlan) {
        dmlPlans.add((DMLPlan) executePlan);
      } else {
        // TODO throw exception
      }
    }

    ParseContext context = new ParseContext();
    context.setTsr(reader);

    DMLTransactionPlan transactionPlan = WaspParser.generateTransactionPlan(
        context, dmlPlans);
    return transactionPlan;
  }
}

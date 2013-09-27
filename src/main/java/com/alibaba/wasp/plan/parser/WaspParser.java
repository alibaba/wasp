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
package com.alibaba.wasp.plan.parser;

import com.alibaba.wasp.EntityGroupLocation;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.TransactionParseException;
import com.alibaba.wasp.ZooKeeperConnectionException;
import com.alibaba.wasp.client.FConnection;
import com.alibaba.wasp.client.FConnectionManager;
import com.alibaba.wasp.meta.FTable;
import com.alibaba.wasp.meta.TableSchemaCacheReader;
import com.alibaba.wasp.plan.DMLPlan;
import com.alibaba.wasp.plan.DMLTransactionPlan;
import com.alibaba.wasp.plan.action.DMLAction;
import com.alibaba.wasp.plan.action.TransactionAction;
import com.alibaba.wasp.plan.parser.druid.DruidDDLParser;
import com.alibaba.wasp.plan.parser.druid.DruidDMLParser;
import com.alibaba.wasp.plan.parser.druid.DruidDQLParser;
import com.alibaba.wasp.plan.parser.druid.DruidParser;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Wrap class for DruidDDLParser DruidDMLParser
 * 
 */
public class WaspParser extends DruidParser {

  private FConnection connection;

  private DruidDDLParser ddlParser = null;
  private DruidDQLParser dqlParser = null;
  private DruidDMLParser dmlParser = null;

  /**
   * Empty constructor.
   * 
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   */
  public WaspParser() throws ZooKeeperConnectionException {
  }
  
  /**
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   */
  public WaspParser(DruidDDLParser ddlParser, DruidDQLParser dqlParser,
      DruidDMLParser dmlParser) throws ZooKeeperConnectionException {
    this.ddlParser = ddlParser;
    this.dmlParser = dmlParser;
    this.dqlParser = dqlParser;
  }

  /**
   * @return the connection
   */
  public FConnection getConnection() {
    return connection;
  }

  /**
   * @return the ddlParser
   */
  public DruidDDLParser getDruidDDLParser() {
    return ddlParser;
  }

  /**
   * @throws com.alibaba.wasp.ZooKeeperConnectionException
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration configuration) {
    super.setConf(configuration);
    try {
      connection = FConnectionManager.getConnection(this.getConf());
      if (ddlParser == null) {
        ddlParser = new DruidDDLParser(this.configuration);
      }
      if (dqlParser == null) {
        dqlParser = new DruidDQLParser(this.configuration);
      }
      if (dmlParser == null) {
        dmlParser = new DruidDMLParser(this.configuration);
      }
    } catch (ZooKeeperConnectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generate plan(insert plan,update plan,delete plan,query plan) in the parse
   * context
   *
   * @param context
   * @throws java.io.IOException
   */
  @Override
  public void generatePlan(ParseContext context) throws IOException {
    SQLType sqlType = getSQLType(context);
    switch (sqlType) {
      case DDL: ddlParser.generatePlan(context);break;
      case DQL: dqlParser.generatePlan(context);break;
      case DML: dmlParser.generatePlan(context);break;
    }
  }

  public static DMLTransactionPlan generateTransactionPlan(ParseContext context, List<DMLPlan> dmlPlans) throws MetaException, TransactionParseException {
    Set<String> checkTables = new HashSet<String>();
    List<DMLAction> dmlActions = new ArrayList<DMLAction>();
    for (DMLPlan dmlPlan : dmlPlans) {
      for (DMLAction dmlAction : dmlPlan.getActions()) {
        checkTables.add(dmlAction.getTableName());
        dmlActions.add(dmlAction);
      }
    }
    String parentTable = checkTablesAndGetParent(checkTables, context);
    List<TransactionAction> transactions = new ArrayList<TransactionAction>();
    transactions.add(createTransactionAction(parentTable, dmlActions));
    DMLTransactionPlan transactionPlan = new DMLTransactionPlan(transactions);
    return transactionPlan;
  }

  private static TransactionAction createTransactionAction(String parentTable, List<DMLAction> dmlActions) throws TransactionParseException {
    TransactionAction transactionAction = new TransactionAction(parentTable, dmlActions);
    EntityGroupLocation entityGroupLocation = null;
    for (DMLAction dmlAction : dmlActions) {
      if(entityGroupLocation == null) {
        entityGroupLocation = dmlAction.getEntityGroupLocation();
      } else {
        if(entityGroupLocation.compareTo(dmlAction.getEntityGroupLocation()) != 0) {
          throw new TransactionParseException("child table row must be the same entityGoup with parent row. it means value of egk row must be same");
        }
      }
    }
    transactionAction.setEntityGroupLocation(entityGroupLocation);
    return transactionAction;
  }

  private static String checkTablesAndGetParent(Set<String> checkTables, ParseContext context) throws MetaException, TransactionParseException {
    TableSchemaCacheReader reader = context.getTsr();
    String parentTable = null;
    for (String checkTable : checkTables) {
      FTable table = reader.getSchema(checkTable);
      if(table.getTableType() == FTable.TableType.ROOT) {
        if(parentTable != null && !parentTable.equals(table.getTableName())) {
          throw new TransactionParseException("in a transaction can be no more than one parent table");
        } else {
          parentTable = table.getTableName();
        }
      } else {
        if(parentTable != null && (!parentTable.equals(table.getParentName()))) {
          throw new TransactionParseException("in a transaction can be no more than one parent table. and others must be the parent's child");
        } else {
          parentTable = table.getParentName();
        }
      }

    }
    return parentTable;
  }
}
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
package org.apache.wasp.meta;

import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.wasp.MetaException;
import org.apache.wasp.conf.WaspConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.ServiceException;

public class TestTableSchemaCacheReader {

  private static Configuration conf = WaspConfiguration.create();

  @BeforeClass
  public static void setUp() throws Exception {
    MemFMetaStore fmetaServices = new MemFMetaStore();
    TableSchemaCacheReader.getInstance(conf, fmetaServices);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TableSchemaCacheReader.getInstance(conf, null);
  }

  @Test
  public void testTableSchemaCacheReader() throws ServiceException,
      MetaException {
    TableSchemaCacheReader reader = TableSchemaCacheReader.getInstance(conf);
    FTable User = FMetaTestUtil.User;
    reader.addSchema(User.getTableName(), User);

    FTable Photo = FMetaTestUtil.Photo;
    Photo.addIndex(FMetaTestUtil.PhotosByTime);
    Photo.addIndex(FMetaTestUtil.PhotosByTag);
    reader.addSchema(Photo.getTableName(), Photo);

    try {
      FTable newUser = reader.getSchema(User.getTableName());
      TestFMetaStore.compare(User, newUser);

      Set<Index> indexs = reader.getIndexsByField(Photo.getTableName(),
          "user_id");
      Assert.assertEquals(indexs.size(), 1);
      Index i1 = indexs.iterator().next();
      TestFMetaStore.compare(FMetaTestUtil.PhotosByTime, i1);
      // no exist column name
      indexs = reader.getIndexsByField(Photo.getTableName(),
          "no_exists_user_id");
      Assert.assertEquals(indexs.size(), 0);

      Set<Index> indexs2 = reader.getIndexsByField(Photo.getTableName(),
          "user_id");
      Assert.assertEquals(indexs2.size(), 1);
      TestFMetaStore.compare(FMetaTestUtil.PhotosByTime, indexs2.iterator()
          .next());
      // no exist column name
      indexs2 = reader.getIndexsByField(Photo.getTableName(),
          "no_exists_user_id");
      Assert.assertEquals(indexs2.size(), 0);

      // No exist tableName
      FTable noExists = reader.getSchema("noExists");
      Assert.assertNull(noExists);
      indexs = reader.getIndexsByField("noExists", "user_id");
      Assert.assertEquals(indexs.size(), 0);
      indexs2 = reader.getIndexsByField("noExists", "user_id");
      Assert.assertEquals(indexs2.size(), 0);

      // test refreshSchema
      reader.refreshSchema(User.getTableName());
      newUser = reader.getSchema(User.getTableName());
      TestFMetaStore.compare(User, newUser);
      reader.refreshSchema("noExists"); // No Exception
      newUser = reader.getSchema("noExists");
      Assert.assertNull(newUser);

      // test removeSchema
      newUser = reader.removeSchema(User.getTableName());
      TestFMetaStore.compare(User, newUser);
      newUser = reader.getSchema(User.getTableName());
      Assert.assertNull(newUser);
      reader.removeSchema("noExists"); // No Exception
      newUser = reader.getSchema("noExists");
      Assert.assertNull(newUser);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertTrue(false);
    }
  }
}
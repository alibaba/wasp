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
package com.alibaba.wasp.jdbcx;

import org.apache.hadoop.conf.Configuration;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Hashtable;

/**
 * This class is used to create new DataSource objects. An application should
 * not use this class directly.
 */
public class JdbcDataSourceFactory implements ObjectFactory {

  private Configuration conf;

  static {
    com.alibaba.wasp.jdbc.Driver.load();
  }

  /**
   * The public constructor to create new factory objects.
   */
  public JdbcDataSourceFactory(Configuration conf) {
    this.conf = conf;

  }

  /**
   * Creates a new object using the specified location or reference information.
   * 
   * @param obj
   *          the reference (this factory only supports objects of type
   *          javax.naming.Reference)
   * @param name
   *          unused
   * @param nameCtx
   *          unused
   * @param environment
   *          unused
   * @return the new JdbcDataSource, or null if the reference class name is not
   *         JdbcDataSource.
   */
  public synchronized Object getObjectInstance(Object obj, Name name,
      Context nameCtx, Hashtable<?, ?> environment) {
    if (obj instanceof Reference) {
      Reference ref = (Reference) obj;
      if (ref.getClassName().equals(JdbcDataSource.class.getName())) {
        JdbcDataSource dataSource = new JdbcDataSource(conf);
        dataSource.setURL((String) ref.get("url").getContent());
        dataSource.setUser((String) ref.get("user").getContent());
        dataSource.setPassword((String) ref.get("password").getContent());
        dataSource.setDescription((String) ref.get("description").getContent());
        String s = (String) ref.get("loginTimeout").getContent();
        dataSource.setLoginTimeout(Integer.parseInt(s));
        return dataSource;
      }
    }
    return null;
  }

}

/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.wasp.conf;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.wasp.util.VersionInfo;

/**
 * Adds Wasp configuration files to a Configuration
 */
public class WaspConfiguration extends Configuration {

  private static void checkDefaultsVersion(Configuration conf) {
    if (conf.getBoolean("wasp.defaults.for.version.skip", Boolean.TRUE)) return;
    String defaultsVersion = conf.get("wasp.defaults.for.version");
    String thisVersion = VersionInfo.getVersion();
    if (!thisVersion.equals(defaultsVersion)) {
      throw new RuntimeException(
        "wasp-default.xml file seems to be for and old version of Wasp (" +
        defaultsVersion + "), this version is " + thisVersion);
    }
  }

  public static Configuration addWaspResources(Configuration conf) {
    conf.addResource("wasp-default.xml");
    conf.addResource("wasp-site.xml");

    checkDefaultsVersion(conf);
    return conf;
  }

  /**
   * Creates a Configuration with HBase and Wasp resources
   * 
   * @return a Configuration with HBase and Wasp resources
   */
  public static Configuration create() {
    Configuration conf = HBaseConfiguration.create();
    return addWaspResources(conf);
  }

  /**
   * Creates a clone of passed configuration.
   * @param that Configuration to clone.
   * @return a clone of passed configuration.
   */
  public static Configuration create(final Configuration that) {
    Configuration conf = create();
    merge(conf, that);
    return conf;
  }

  /**
   * Merge two configurations.
   * @param destConf the configuration that will be overwritten with items
   *                 from the srcConf
   * @param srcConf the source configuration
   **/
  public static void merge(Configuration destConf, Configuration srcConf) {
    for (Entry<String, String> e : srcConf) {
      destConf.set(e.getKey(), e.getValue());
    }
  }
  
  /**
   * 
   * @return whether to show Wasp Configuration in servlet
   */
  public static boolean isShowConfInServlet() {
    boolean isShowConf = false;
    try {
      if (Class.forName("org.apache.hadoop.conf.ConfServlet") != null) {
        isShowConf = true;  
      }
    } catch (Exception e) {
      
    }
    return isShowConf;
  }
}
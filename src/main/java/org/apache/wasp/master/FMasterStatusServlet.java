/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.wasp.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.wasp.ServerName;
import org.apache.wasp.client.WaspAdmin;
import org.apache.wasp.tmpl.master.MasterStatusTmpl;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * The servlet responsible for rendering the index page of the master.
 */
public class FMasterStatusServlet extends HttpServlet {
  private static final Log LOG = LogFactory.getLog(FMasterStatusServlet.class);
  private static final long serialVersionUID = 1L;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    FMaster master = (FMaster) getServletContext().getAttribute(FMaster.MASTER);
    assert master != null : "No Master in context!";

    Configuration conf = master.getConfiguration();
    WaspAdmin admin = new WaspAdmin(conf);

    List<ServerName> servers = master.getFServerManager()
        .getOnlineServersList();
    Set<ServerName> deadServers = master.getFServerManager().getDeadServers();

    response.setContentType("text/html");
    MasterStatusTmpl tmpl = new MasterStatusTmpl()
        .setShowAppendWarning(shouldShowAppendWarning(conf))
        .setServers(servers).setDeadServers(deadServers);
    if (request.getParameter("filter") != null)
      tmpl.setFilter(request.getParameter("filter"));
    if (request.getParameter("format") != null)
      tmpl.setFormat(request.getParameter("format"));
    tmpl.render(response.getWriter(), master, admin);
  }

  static boolean shouldShowAppendWarning(Configuration conf) {
    try {
      return !FSUtils.isAppendSupported(conf) && FSUtils.isHDFS(conf);
    } catch (IOException e) {
      LOG.warn("Unable to determine if append is supported", e);
      return false;
    }
  }
}

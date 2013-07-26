/**
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
package com.alibaba.wasp.fserver;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.monitoring.LogMonitoring;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.util.ReflectionUtils;
import com.alibaba.wasp.monitoring.StateDumpServlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;

@InterfaceAudience.Private
public class FSDumpServlet extends StateDumpServlet {
  private static final long serialVersionUID = 1L;
  private static final String LINE =
    "===========================================================";
  
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    FServer fs = (FServer)getServletContext().getAttribute(
        FServer.FSERVER);
    assert fs != null : "No FS in context!";
    
    Configuration hrsconf = (Configuration)getServletContext().getAttribute(
        FServer.FSERVER_CONF);
    assert hrsconf != null : "No FS conf in context";

    response.setContentType("text/plain");
    OutputStream os = response.getOutputStream();
    PrintWriter out = new PrintWriter(os);
    
    out.println("Master status for " + fs.getServerName()
        + " as of " + new Date());
    
    out.println("\n\nVersion Info:");
    out.println(LINE);
    dumpVersionInfo(out);

    out.println("\n\nTasks:");
    out.println(LINE);
    TaskMonitor.get().dumpAsText(out);
    
    out.println("\n\nExecutors:");
    out.println(LINE);
    dumpExecutors(fs.getExecutorService(), out);
    
    out.println("\n\nStacks:");
    out.println(LINE);
    ReflectionUtils.printThreadInfo(out, "");
    
    out.println("\n\nFS Configuration:");
    out.println(LINE);
    Configuration conf = fs.getConfiguration();
    out.flush();
    conf.writeXml(os);
    os.flush();
    
    out.println("\n\nLogs");
    out.println(LINE);
    long tailKb = getTailKbParam(request);
    LogMonitoring.dumpTailOfLogs(out, tailKb);
    
    out.println("\n\nFS Queue:");
    out.println(LINE);
    if(isShowQueueDump(hrsconf)) {
      dumpQueue(fs, out);
    } 
    
    out.flush();
  }
  
  private boolean isShowQueueDump(Configuration conf){
    return conf.getBoolean("wasp.fserver.servlet.show.queuedump", true);
  }
    
  private void dumpQueue(FServer fs, PrintWriter out)
      throws IOException {
    // 1. Print out Compaction/Split Queue
    out.println("Split Queue summary: "
        + fs.splitThread.toString() );
    out.println(fs.splitThread.dumpQueue());

  }
  
}

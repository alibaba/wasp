<%--
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
--%>
<%@ page contentType="text/html;charset=UTF-8"
         import="org.apache.commons.lang.StringEscapeUtils"
        %>
<%@ page import="org.apache.hadoop.conf.Configuration" %>
<%@ page import="org.apache.hadoop.hbase.util.Bytes" %>
<%@ page import="org.apache.wasp.EntityGroupInfo" %>
<%@ page import="org.apache.wasp.EntityGroupLoad" %>
<%@ page import="org.apache.wasp.ServerLoad" %>
<%@ page import="org.apache.wasp.ServerName" %>
<%@ page import="org.apache.wasp.client.FConnectionManager" %>
<%@ page import="org.apache.wasp.client.WaspAdmin" %>
<%@ page import="org.apache.wasp.master.FMaster" %>
<%@ page import="org.apache.wasp.meta.FTable" %>
<%@ page import="java.util.HashMap" %>
<%@ page import="java.util.Map" %>
<%
    FMaster master = (FMaster) getServletContext().getAttribute(FMaster.MASTER);
    Configuration conf = master.getConfiguration();
    WaspAdmin admin = new WaspAdmin(conf);
    String tableName = request.getParameter("name");
    FTable table = admin.getTableDescriptor(Bytes.toBytes(tableName));
    Map<EntityGroupInfo, ServerName> entityGroups = admin.getEntityGroupLocations(Bytes.toBytes(tableName));
    String tableHeader = "<h2>Table EntityGroups</h2><table class=\"table table-striped\"><tr><th>Name</th><th>FServer</th><th>Start Key</th><th>End Key</th><th>Requests</th></tr>";
    ServerName rl = master.getServerName();
    boolean showFragmentation = conf.getBoolean("wasp.master.ui.fragmentation.enabled", false);
    boolean readOnly = conf.getBoolean("wasp.master.ui.readonly", false);
    Map<String, Integer> frags = new HashMap<String, Integer>();
//  if (showFragmentation) {
//      frags = FSUtils.getTableFragmentation(master);
//  }
    // HARDCODED FOR NOW TODO: FIX GET FROM ZK
    // This port might be wrong if RS actually ended up using something else.
    int infoPort = conf.getInt("wasp.fserver.info.port", 50030);
%>

<?xml version="1.0" encoding="UTF-8" ?>
<!-- Commenting out DOCTYPE so our blue outline shows on hadoop 0.20.205.0, etc.
See tail of HBASE-2110 for explaination.
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
-->
<html xmlns="http://www.w3.org/1999/xhtml">

<%
    String action = request.getParameter("action");
    String key = request.getParameter("key");
    if (!readOnly && action != null) {
%>
<head>
    <meta charset="utf-8">
    <title>Wasp FMaster: <%= master.getServerName() %>
    </title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="">


    <link href="/static/css/bootstrap.css" rel="stylesheet">
    <link href="/static/css/wasp.css" rel="stylesheet">
    <link href="/static/css/bootstrap-responsive.css" rel="stylesheet">
    <!--[if lt IE 9]>
    <script src="/static/js/html5shiv.js"></script>
    <![endif]-->
    <meta http-equiv="refresh" content="5,javascript:history.back()"/>
</head>
<body>
<div class="navbar navbar-fixed-top">
    <div class="navbar-inner">
        <div class="container">
            <a class="btn btn-navbar" data-toggle="collapse" data-target=".nav-collapse">
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
            </a>
            <a class="brand" href="/master-status"><img src="/static/wasp_logo_small.jpg" alt="HBase Logo"/></a>

            <div class="nav-collapse">
                <ul class="nav">
                    <li><a href="/">Home</a></li>
                    <li><a href="/tablesDetailed.jsp">Table Details</a></li>
                    <li><a href="/logs/">Local logs</a></li>
                    <li><a href="/logLevel">Log Level</a></li>
                    <li><a href="/dump">Debug dump</a></li>
                    <li><a href="/jmx">Metrics Dump</a></li>
                </ul>
            </div>
            <!--/.nav-collapse -->
        </div>
    </div>
</div>
<div class="container">


    <div class="container">
        <div class="row inner_header">
            <div class="page-header">
                <h1>Table action request accepted</h1>
            </div>
        </div>
    </div>
    <p>
    <hr>
    <p>
            <%
  if (action.equals("split")) {
      admin.split(tableName, key);
    }

    %> Split request accepted.

    <p>Go <a href="javascript:history.back()">Back</a>, or wait for the redirect.
</div>
<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>
</body>
<%
} else {
%>
<head>
    <meta charset="utf-8">
    <title>Table: <%= tableName %>
    </title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="">


    <link href="/static/css/bootstrap.css" rel="stylesheet">
    <link href="/static/css/wasp.css" rel="stylesheet">
    <link href="/static/css/bootstrap-responsive.css" rel="stylesheet">
    <!--[if lt IE 9]>
    <script src="/static/js/html5shiv.js"></script>
    <![endif]-->
</head>
<body>
<div class="navbar navbar-fixed-top">
    <div class="navbar-inner">
        <div class="container">
            <a class="btn btn-navbar" data-toggle="collapse" data-target=".nav-collapse">
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
            </a>
            <a class="brand" href="/master-status">Wasp FMaster</a>

            <div class="nav-collapse">
                <ul class="nav">
                    <li><a href="/">Home</a></li>
                    <li><a href="/tablesDetailed.jsp">Table Details</a></li>
                    <li><a href="/logs/">Local logs</a></li>
                    <li><a href="/logLevel">Log Level</a></li>
                    <li><a href="/dump">Debug dump</a></li>
                </ul>
            </div>
            <!--/.nav-collapse -->
        </div>
    </div>
</div>
<div class="container">


    <div class="container">
        <div class="row inner_header">
            <div class="page-header">
                <h1>Table
                    <small><%= tableName %>
                    </small>
                </h1>
            </div>
        </div>
    </div>
    <div class="row">
        <%
            try {
        %>
        <h2>Table Attributes</h2>
        <table class="table table-striped">
            <tr>
                <th>Attribute Name</th>
                <th>Value</th>
                <th>Description</th>
            </tr>
            <tr>
                <td>Enabled</td>
                <td><%= admin.isTableEnabled(table.getTableName()) %>
                </td>
                <td>Is the table enabled</td>
            </tr>

        </table>
        <%
            Map<String, Integer> regDistribution = new HashMap<String, Integer>();
            if (entityGroups != null && entityGroups.size() > 0) { %>
        <%=     tableHeader %>
        <%
            for (Map.Entry<EntityGroupInfo, ServerName> hriEntry : entityGroups.entrySet()) {
                EntityGroupInfo entityGroupInfo = hriEntry.getKey();
                ServerName addr = hriEntry.getValue();
                long req = 0;

                String urlFServer = null;

                if (addr != null) {
                    ServerLoad sl = master.getFServerManager().getLoad(addr);
                    if (sl != null) {
                        Map<byte[], EntityGroupLoad> map = sl.getEntityGroupLoad();
                        if (map.containsKey(entityGroupInfo.getEntityGroupName())) {
                            req = map.get(entityGroupInfo.getEntityGroupName()).getRequestsCount();
                        }
                        // This port might be wrong if RS actually ended up using something else.
                        urlFServer =
                                "http://" + addr.getHostname().toString() + ":" + infoPort + "/fserver.jsp";
                        Integer i = regDistribution.get(urlFServer);
                        if (null == i) i = Integer.valueOf(0);
                        regDistribution.put(urlFServer, i + 1);
                    }
                }
        %>
        <tr>
            <td><%= StringEscapeUtils.escapeXml(Bytes.toStringBinary(entityGroupInfo.getEntityGroupName())) %>
            </td>
            <%
                if (urlFServer != null) {
            %>
            <td>
                <a href="<%= urlFServer %>"><%= addr.getHostname().toString() + ":" + infoPort %>
                </a>
            </td>
            <%
            } else {
            %>
            <td class="undeployed-region">not deployed</td>
            <%
                }
            %>
            <td><%= StringEscapeUtils.escapeXml(Bytes.toStringBinary(entityGroupInfo.getStartKey())) %>
            </td>
            <td><%= StringEscapeUtils.escapeXml(Bytes.toStringBinary(entityGroupInfo.getEndKey())) %>
            </td>
            <td><%= req%>
            </td>
        </tr>
        <% } %>
        </table>
        <h2>EntityGroups by FServer</h2>
        <table class="table table-striped">
            <tr>
                <th>FServer</th>
                <th>EntityGroup Count</th>
            </tr>
            <%
                for (Map.Entry<String, Integer> rdEntry : regDistribution.entrySet()) {
            %>
            <tr>
                <td><%= rdEntry.getKey()%>
                </td>
                <td><%= rdEntry.getValue()%>
                </td>
            </tr>
            <% } %>
        </table>
        <% }
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
        } // end else

            FConnectionManager.deleteConnection(admin.getConfiguration(), false);
        %>


        <% if (!readOnly) { %>
        <p>
        <hr>
        <p>
            Actions:

        <p>
        <center>
            <table class="table" width="90%">
                <tr>
                    <td style="border-style: none" colspan="4">&nbsp;</td>
                </tr>
                <tr>
                    <form method="get">
                        <input type="hidden" name="action" value="split">
                        <input type="hidden" name="name" value="<%= tableName %>">
                        <td style="border-style: none; text-align: center">
                            <input style="font-size: 12pt; width: 10em" type="submit" value="Split" class="btn"></td>
                        <td style="border-style: none" width="5%">&nbsp;</td>
                        <td style="border-style: none">EntityGroup Key (optional):<input type="text" name="key" size="40">
                        </td>
                        <td style="border-style: none">This action will force a split of all eligible
                            entityGroups of the table, or, if a key is supplied, only the EntityGroup containing the
                            given key. An eligible EntityGroup is one that does not contain any references to
                            other entityGroups. Split requests for noneligible entityGroups will be ignored.
                        </td>
                    </form>
                </tr>
            </table>
        </center>
        <p>
    </div>
</div>
<% } %>

<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>


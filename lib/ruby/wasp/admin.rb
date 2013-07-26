#
# Copyright 2010 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

include Java
java_import org.apache.hadoop.hbase.util.Pair

module Wasp
  class Admin
    include WaspConstants

    def initialize(configuration, formatter)
      @admin = com.alibaba.wasp.client.WaspAdmin.new(configuration)
      connection = @admin.getConnection()
      @conf = configuration
      @zk_wrapper = connection.getZooKeeperWatcher()
      zk = @zk_wrapper.getRecoverableZooKeeper().getZooKeeper()
      @zk_main = org.apache.zookeeper.ZooKeeperMain.new(zk)
      @formatter = formatter
      com.alibaba.wasp.jdbc.Driver.load();
      @info = java.util.Properties.new();
      @info.put("wasp.zookeeper.quorum", configuration.get("wasp.zookeeper.quorum"));
      @info.put("wasp.zookeeper.property.clientPort", configuration.getInt("wasp.zookeeper.property.clientPort", 2181));
      @conn = java.sql.DriverManager.getConnection("jdbc:wasp:", @info);
      @stat = @conn.createStatement();
      @formatter = com.alibaba.wasp.jdbc.JdbcResultFormatter.new(@conn);
    end
    
    #----------------------------------------------------------------------------------------------
    # Execute a sql
    def executeSQL(sql_sentence,args = {})
      @stat.execute(sql_sentence)
    end

    # Execute a query
    def executeQuery(sql_sentence,args = {})
      @formatter.format(@stat, sql_sentence)
    end

    #----------------------------------------------------------------------------------------------
    # Returns a list of tables in wasp
    def show_tables
      @admin.listTables.map { |t| t.getTableName }
    end
    #----------------------------------------------------------------------------------------------

    #----------------------------------------------------------------------------------------------
    # Returns a list of tables in wasp
    def show_indexes(table_name)
      @admin.listIndexes(table_name).map { |index| index.getIndexName }
    end
    #----------------------------------------------------------------------------------------------

    # Returns a table describe
    def describe_table(table_name)
      @admin.describeTable(table_name)
    end
    #----------------------------------------------------------------------------------------------

    # Returns a index describe
    def describe_index(table_name, index_name)
      @admin.describeIndex(table_name, index_name)
    end
    #----------------------------------------------------------------------------------------------

    # Requests a table or entityGroup split
    def split(table_or_entityGroup_name, split_point)
      if split_point == nil
        @admin.split(table_or_entityGroup_name)
      else
        @admin.split(table_or_entityGroup_name, split_point)
      end
    end

    #----------------------------------------------------------------------------------------------
    # Requests a cluster balance
    # Returns true if balancer ran
    def balancer()
      @admin.balancer()
    end

    #----------------------------------------------------------------------------------------------
    # Enable/disable balancer
    # Returns previous balancer switch setting.
    def balance_switch(enableDisable)
      @admin.setBalancerRunning(java.lang.Boolean::valueOf(enableDisable))
    end

    #----------------------------------------------------------------------------------------------
    # Enables a table
    def enable(table_name)
      tableExists(table_name)
      return if enabled?(table_name)
      @admin.enableTable(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Enables all tables matching the given regex
    def enable_all(regex)
      regex = regex.to_s
      @admin.enableTables(regex)
    end

    #----------------------------------------------------------------------------------------------
    # Disables a table
    def disable(table_name)
      tableExists(table_name)
      return if disabled?(table_name)
      @admin.disableTable(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Disables all tables matching the given regex
    def disable_all(regex)
      regex = regex.to_s
      @admin.disableTables(regex).map { |t| t.getNameAsString }
    end

    #---------------------------------------------------------------------------------------------
    # Throw exception if table doesn't exist
    def tableExists(table_name)
      raise ArgumentError, "Table #{table_name} does not exist.'" unless exists?(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Is table disabled?
    def disabled?(table_name)
      @admin.isTableDisabled(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Drops a table
    def drop(table_name)
      tableExists(table_name)
      raise ArgumentError, "Table #{table_name} is enabled. Disable it first.'" if enabled?(table_name)

      @admin.deleteTable(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Returns ZooKeeper status dump
    def zk_dump
      com.alibaba.wasp.zookeeper.ZKUtil::dump(@zk_wrapper)
    end

    #----------------------------------------------------------------------------------------------
    # Closes a entityGroup.
    # If server name is nil, we presume entityGroup_name is full entityGroup name (HRegionInfo.getRegionName).
    # If server name is not nil, we presume it is the entityGroup's encoded name (HRegionInfo.getEncodedName)
    def close_entityGroup(entityGroup_name, server)
      if (server == nil || !closeEncodedEntityGroup?(entityGroup_name, server))         
      	@admin.closeEntityGroup(entityGroup_name, server)
      end	
    end

    #----------------------------------------------------------------------------------------------
    #----------------------------------------------------------------------------------------------
    # Assign a entityGroup
    def assign(entityGroup_name)
      @admin.assign(entityGroup_name.to_java_bytes)
    end

    #----------------------------------------------------------------------------------------------
    # Unassign a entityGroup
    def unassign(entityGroup_name, force)
      @admin.unassign(entityGroup_name.to_java_bytes, java.lang.Boolean::valueOf(force))
    end

    #----------------------------------------------------------------------------------------------
    # Move a entityGroup
    def move(encoded_entityGroup_name, server = nil)
      @admin.move(encoded_entityGroup_name.to_java_bytes, server ? server.to_java_bytes: nil)
    end

    #----------------------------------------------------------------------------------------------
    # Truncates table (deletes all records by recreating the table)
    def truncate(table_name, conf = @conf)
      admin.truncate(table_name)
    end

    def status(format)
      status = @admin.getClusterStatus()
      if format == "detailed"
        puts("version %s" % [ status.getWaspVersion() ])
        # Put entityGroups in transition first because usually empty
        puts("%d entityGroupsInTransition" % status.getEntityGroupsInTransition().size())
        for k, v in status.getEntityGroupsInTransition()
          puts("    %s" % [v])
        end
        puts("%d live servers" % [ status.getServersSize() ])
        for server in status.getServers()
          puts("    %s:%d %d" % \
            [ server.getHostname(), server.getPort(), server.getStartcode() ])
          puts("        %s" % [ status.getLoad(server).toString() ])
          for name, entityGroup in status.getLoad(server).getEntityGroupsLoad()
            puts("        %s" % [ entityGroup.getNameAsString() ])
            puts("            %s" % [ entityGroup.toString() ])
          end
        end
        puts("%d dead servers" % [ status.getDeadServers() ])
        for server in status.getDeadServerNames()
          puts("    %s" % [ server ])
        end
      elsif format == "simple"
        load = 0
        entityGroups = 0
        puts("%d live servers" % [ status.getServersSize() ])
        for server in status.getServers()
          puts("    %s:%d %d" % \
            [ server.getHostname(), server.getPort(), server.getStartcode() ])
          puts("        %s" % [ status.getLoad(server).toString() ])
          load += status.getLoad(server).getNumberOfRequests()
          entityGroups += status.getLoad(server).getNumberOfEntityGroups()
        end
        puts("%d dead servers" % [ status.getDeadServers() ])
        for server in status.getDeadServerNames()
          puts("    %s" % [ server ])
        end
        puts("Aggregate load: %d, entityGroups: %d" % [ load , entityGroups ] )
      else
        puts "#{status.getServersSize} servers, #{status.getDeadServers} dead, #{'%.4f' % status.getAverageLoad} average load"
      end
    end

    #----------------------------------------------------------------------------------------------
    #
    # Helper methods
    #

    # Does table exist?
    def exists?(table_name)
      @admin.tableExists(table_name)
    end

    #----------------------------------------------------------------------------------------------
    # Is table enabled
    def enabled?(table_name)
      @admin.isTableEnabled(table_name)
    end

    #----------------------------------------------------------------------------------------------
    #Is supplied entityGroup name is encoded entityGroup name
    def closeEncodedEntityGroup?(entityGroup_name, server)
       @admin.closeEntityGroupWithEncodedEntityGroupName(entityGroup_name, server)
    end

    def closeAdmin()
      @admin.close()
    end
  end
end

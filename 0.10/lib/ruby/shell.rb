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

# Shell commands module
module Shell
  @@commands = {}
  def self.commands
    @@commands
  end

  @@command_groups = {}
  def self.command_groups
    @@command_groups
  end

  def self.load_command(name, group)
    return if commands[name]

    # Register command in the group
    raise ArgumentError, "Unknown group: #{group}" unless command_groups[group]
    command_groups[group][:commands] << name

    # Load command
    begin
      require "shell/commands/#{name}"
      klass_name = name.to_s.gsub(/(?:^|_)(.)/) { $1.upcase } # camelize
      commands[name] = eval("Commands::#{klass_name}")
    rescue => e
      raise "Can't load wasp shell command: #{name}. Error: #{e}\n#{e.backtrace.join("\n")}"
    end
  end

  def self.load_command_group(group, opts)
    raise ArgumentError, "No :commands for group #{group}" unless opts[:commands]

    command_groups[group] = {
      :commands => [],
      :command_names => opts[:commands],
      :full_name => opts[:full_name] || group,
      :comment => opts[:comment]
    }

    opts[:commands].each do |command|
      load_command(command, group)
    end
  end

  #----------------------------------------------------------------------
  class Shell
    attr_accessor :wasp
    attr_accessor :formatter

    @debug = false
    attr_accessor :debug

    def initialize(wasp, formatter)
      self.wasp = wasp
      self.formatter = formatter
    end

    def wasp_admin
      @wasp_admin ||= wasp.admin(formatter)
    end

    def wasp_table(name)
      wasp.table(name, formatter)
    end

    def export_commands(where)
      ::Shell.commands.keys.each do |cmd|
        where.send :instance_eval, <<-EOF
          def #{cmd}(*args)
            @shell.command('#{cmd}', *args)
            puts
          end
        EOF
      end
    end

    def command_instance(command)
      ::Shell.commands[command.to_s].new(self)
    end

    def command(command, *args)
      command_instance(command).command_safe(self.debug, *args)
    end

    def print_banner
      puts "Wasp Shell; enter 'help<RETURN>' for list of supported commands."
      puts 'Type "exit<RETURN>" to leave the Wasp Shell'
      print 'Version '
      command('version')
      puts
    end

    def help_multi_command(command)
      puts "Command: #{command}"
      puts command_instance(command).help
      puts
      return nil
    end

    def help_command(command)
      puts command_instance(command).help
      return nil
    end

    def help_group(group_name)
      group = ::Shell.command_groups[group_name.to_s]
      group[:commands].sort.each { |cmd| help_multi_command(cmd) }
      if group[:comment]
        puts '-' * 80
        puts
        puts group[:comment]
        puts
      end
      return nil
    end

    def help(command = nil)
      if command
        return help_command(command) if ::Shell.commands[command.to_s]
        return help_group(command) if ::Shell.command_groups[command.to_s]
        puts "ERROR: Invalid command or command group name: #{command}"
        puts
      end

      puts help_header
      puts
      puts 'COMMAND GROUPS:'
      ::Shell.command_groups.each do |name, group|
        puts "  Group name: " + name
        puts "  Commands: " + group[:command_names].sort.join(', ')
        puts
      end
      unless command
        puts 'SHELL USAGE:'
        help_footer
      end
      return nil
    end

    def help_header
      return "Wasp Shell, version #{org.apache.wasp.util.VersionInfo.getVersion()}, " +
             "r#{org.apache.wasp.util.VersionInfo.getRevision()}, " +
             "#{org.apache.wasp.util.VersionInfo.getDate()}" + "\n" +
        "Type 'help \"COMMAND\"', (e.g. 'help \"get\"' -- the quotes are necessary) for help on a specific command.\n" +
        "Commands are grouped. Type 'help \"COMMAND_GROUP\"', (e.g. 'help \"general\"') for help on a command group."
    end

    def help_footer
      puts <<-HERE
      HERE
    end
  end
end

# Load commands base class
require 'shell/commands'

# Load all commands
Shell.load_command_group(
  'general',
  :full_name => 'GENERAL WASP SHELL COMMANDS',
  :commands => %w[
    status
    version
  ]
)

Shell.load_command_group(
  'ddl',
  :full_name => 'TABLES MANAGEMENT COMMANDS',
  :commands => %w[
    describe_table
    describe_index
    disable
    truncate
    disable_all
    is_disabled
    drop
    enable
    enable_all
    is_enabled
    exists
    show_tables
    show_indexes
  ]
)

Shell.load_command_group(
  'sql',
  :full_name => 'DATA MANIPULATION COMMANDS',
  :commands => %w[
    sql
    query
  ]
)

Shell.load_command_group(
  'tools',
  :full_name => 'WASP SURGERY TOOLS',
  :comment => "WARNING: Above commands are for 'experts'-only as misuse can damage an install",
  :commands => %w[
    assign
    balancer
    balance_switch
    move
    split
    unassign
    zk_dump
  ]
)
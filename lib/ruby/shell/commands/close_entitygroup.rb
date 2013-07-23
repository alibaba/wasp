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

module Shell
  module Commands
    class CloseEntityGroup < Command
      def help
        return <<-EOF
Close a single entityGroup.  Ask the master to close a entityGroup out on the cluster
or if 'SERVER_NAME' is supplied, ask the designated hosting entityGroupserver to
close the entityGroup directly.  Closing a entityGroup, the master expects 'entityGroupNAME'
to be a fully qualified entityGroup name.  When asking the hosting entityGroupserver to
directly close a entityGroup, you pass the entityGroups' encoded name only. A entityGroup
name looks like this:
 
 TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.

The trailing period is part of the entityGroupserver name. A entityGroup's encoded name
is the hash at the end of a entityGroup name; e.g. 527db22f95c8a9e0116f0cc13c680396 
(without the period).  A 'SERVER_NAME' is its host, port plus startcode. For
example: host187.example.com,60020,1289493121758 (find servername in master ui
or when you do detailed status in shell).  This command will end up running
close on the entityGroup hosting entityGroupserver.  The close is done without the
master's involvement (It will not know of the close).  Once closed, entityGroup will
stay closed.  Use assign to reopen/reassign.  Use unassign or move to assign
the entityGroup elsewhere on cluster. Use with caution.  For experts only.
Examples:

  hbase> close_entityGroup 'entityGroupNAME'
  hbase> close_entityGroup 'entityGroupNAME', 'SERVER_NAME'
EOF
      end

      def command(entityGroup_name, server = nil)
        format_simple_command do
          admin.close_entityGroup(entityGroup_name, server)
        end
      end
    end
  end
end

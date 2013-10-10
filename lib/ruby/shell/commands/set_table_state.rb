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
    class SetTableState < Command
      def help
        return <<-EOF
Set table state. State are DISABLED,DISABLING,ENABLING,ENABLED. For example:
  wasp> set_table_state 't1', 'DISABLED'
EOF
      end
      def command(table_name, state)
        now = Time.now

        desc = admin.set_table_state(table_name, state)

        puts desc
        formatter.footer(now)
      end
    end
  end
end

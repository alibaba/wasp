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
    class Sql < Command
      def help
        return <<-EOF
Execute a sql sentence
if a select sql please use ( query 'select * from t1' )

Examples:

  wasp> sql "create table tabname(col1 type1 [not null] [primary key],col2 type2 [not null],..)"
  wasp> sql "insert into table1(field1,field2) values(value1,value2)"
EOF
      end

      def command(sql_sentence,args = {})
          admin.executeSQL(sql_sentence,args)
      end
    end
  end
end

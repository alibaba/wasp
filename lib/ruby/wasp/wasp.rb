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

require 'wasp/admin'

module Wasp
  class Wasp
    attr_accessor :configuration

    def initialize(config = nil)
      # Create configuration
      if config
        self.configuration = config
      else
        self.configuration = com.alibaba.wasp.conf.WaspConfiguration.create
        # Turn off retries in wasp and ipc.  Human doesn't want to wait on N retries.
        configuration.setInt("wasp.client.retries.number", 7)
        configuration.setInt("ipc.client.connect.max.retries", 3)
      end
    end

    def admin(formatter)
      ::Wasp::Admin.new(configuration, formatter)
    end
  end
end

#!/usr/bin/env bash
#
#/**
# * Copyright 2007 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Modelled after $HBASE_HOME/bin/start-hbase.sh.

# Start hadoop wasp daemons.
# Run this on master node.
usage="Usage: start-wasp.sh"

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/wasp-config.sh

# start wasp daemons
errCode=$?
if [ $errCode -ne 0 ]
then
  exit $errCode
fi


if [ "$1" = "autorestart" ]
then
  commandToRun="autorestart"
else 
  commandToRun="start"
fi

# only take the first line of the output in case verbose gc is on
distMode=`$bin/wasp --config "$WASP_CONF_DIR" com.alibaba.wasp.util.WaspConfTool wasp.cluster.distributed | head -n 1`


if [ "$distMode" == 'false' ] 
then
  "$bin"/wasp-daemon.sh $commandToRun master
else
  "$bin"/wasp-daemons.sh --config "${WASP_CONF_DIR}" $commandToRun zookeeper
  "$bin"/wasp-daemon.sh --config "${WASP_CONF_DIR}" $commandToRun master 
  "$bin"/wasp-daemons.sh --config "${WASP_CONF_DIR}" \
    --hosts "${WASP_FSERVERS}" $commandToRun fserver
  "$bin"/wasp-daemons.sh --config "${WASP_CONF_DIR}" \
    --hosts "${WASP_BACKUP_MASTERS}" $commandToRun master-backup
fi

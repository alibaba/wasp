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
# 
# Run a shell command on all fserver hosts.
#
# Environment Variables
#
#   WASP_FSERVERS    File naming remote hosts.
#     Default is ${WASP_CONF_DIR}/fservers
#   WASP_CONF_DIR  Alternate wasp conf dir. Default is ${WASP_HOME}/conf.
#   WASP_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   WASP_SSH_OPTS Options passed to ssh when running remote commands.
#
# Modelled after $HBASE_HOME/bin/regionservers.sh.

usage="Usage: fservers [--config <wasp-confdir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/wasp-config.sh

# If the fservers file is specified in the command line,
# then it takes precedence over the definition in 
# wasp-env.sh. Save it here.
HOSTLIST=$WASP_FSERVERS

if [ "$HOSTLIST" = "" ]; then
  if [ "$WASP_FSERVERS" = "" ]; then
    export HOSTLIST="${WASP_CONF_DIR}/fservers"
  else
    export HOSTLIST="${WASP_FSERVERS}"
  fi
fi

for fserver in `cat "$HOSTLIST"`; do
  if ${WASP_SLAVE_PARALLEL:-true}; then 
    ssh $WASP_SSH_OPTS $fserver $"${@// /\\ }" \
      2>&1 | sed "s/^/$fserver: /" &
  else # run each command serially 
    ssh $WASP_SSH_OPTS $fserver $"${@// /\\ }" \
      2>&1 | sed "s/^/$fserver: /"
  fi
  if [ "$WASP_SLAVE_SLEEP" != "" ]; then
    sleep $WASP_SLAVE_SLEEP
  fi
done

wait

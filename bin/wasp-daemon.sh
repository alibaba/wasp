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
# Runs a Wasp command as a daemon.
#
# Environment Variables
#
#   WASP_CONF_DIR   Alternate wasp conf dir. Default is ${WASP_HOME}/conf.
#   WASP_LOG_DIR    Where log files are stored.  PWD by default.
#   WASP_PID_DIR    The pid files are stored. /tmp by default.
#   WASP_IDENT_STRING   A string representing this instance of wasp. $USER by default
#   WASP_NICENESS The scheduling priority for daemons. Defaults to 0.
#
# Modelled after $HBASE_HOME/bin/hbase-daemon.sh

usage="Usage: wasp-daemon.sh [--config <conf-dir>]\
 (start|stop|restart|autorestart) <wasp-command> \
 <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`

. "$bin"/wasp-config.sh

# get arguments
startStop=$1
shift

command=$1
shift

wasp_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
    num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
    while [ $num -gt 1 ]; do
        prev=`expr $num - 1`
        [ -f "$log.$prev" ] && mv -f "$log.$prev" "$log.$num"
        num=$prev
    done
    mv -f "$log" "$log.$num";
    fi
}

cleanZNode() {
  if [ -f $WASP_ZNODE_FILE ]; then
    if [ "$command" = "master" ]; then
      $bin/wasp master clear > /dev/null 2>&1
    else
      #call ZK to delete the node
      ZNODE=`cat $WASP_ZNODE_FILE`
      $bin/wasp zkcli delete $ZNODE > /dev/null 2>&1
    fi
    rm $WASP_ZNODE_FILE
  fi
}

check_before_start(){
    #ckeck if the process is not running
    mkdir -p "$WASP_PID_DIR"
    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi
}

wait_until_done ()
{
    p=$1
    cnt=${WASP_SLAVE_TIMEOUT:-300}
    origcnt=$cnt
    while kill -0 $p > /dev/null 2>&1; do
      if [ $cnt -gt 1 ]; then
        cnt=`expr $cnt - 1`
        sleep 1
      else
        echo "Process did not complete after $origcnt seconds, killing."
        kill -9 $p
        exit 1
      fi
    done
    return 0
}

# get log directory
if [ "$WASP_LOG_DIR" = "" ]; then
  export WASP_LOG_DIR="$WASP_HOME/logs"
fi
mkdir -p "$WASP_LOG_DIR"

if [ "$WASP_PID_DIR" = "" ]; then
  WASP_PID_DIR=/tmp
fi

if [ "$WASP_IDENT_STRING" = "" ]; then
  export WASP_IDENT_STRING="$USER"
fi

# Some variables
# Work out java location so can print version into log.
if [ "$JAVA_HOME" != "" ]; then
  #echo "run java in $JAVA_HOME"
  JAVA_HOME=$JAVA_HOME
fi
if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi

JAVA=$JAVA_HOME/bin/java
export WASP_LOG_PREFIX=wasp-$WASP_IDENT_STRING-$command-$HOSTNAME
export WASP_LOGFILE=$WASP_LOG_PREFIX.log
export WASP_ROOT_LOGGER=${WASP_ROOT_LOGGER:-"INFO,RFA"}
export WASP_SECURITY_LOGGER=${WASP_SECURITY_LOGGER:-"INFO,RFAS"}
logout=$WASP_LOG_DIR/$WASP_LOG_PREFIX.out
loggc=$WASP_LOG_DIR/$WASP_LOG_PREFIX.gc
loglog="${WASP_LOG_DIR}/${WASP_LOGFILE}"
pid=$WASP_PID_DIR/wasp-$WASP_IDENT_STRING-$command.pid
export WASP_ZNODE_FILE=$WASP_PID_DIR/wasp-$WASP_IDENT_STRING-$command.znode
export WASP_START_FILE=$WASP_PID_DIR/wasp-$WASP_IDENT_STRING-$command.autorestart


if [ "$WASP_USE_GC_LOGFILE" = "true" ]; then
  export WASP_GC_OPTS=" -Xloggc:${loggc}"
fi

# Set default scheduling priority
if [ "$WASP_NICENESS" = "" ]; then
    export WASP_NICENESS=0
fi

thiscmd=$0
args=$@

case $startStop in

(start)
    check_before_start
    nohup $thiscmd --config "${WASP_CONF_DIR}" internal_start $command $args < /dev/null > /dev/null 2>&1  &
  ;;

(autorestart)
    check_before_start
    nohup $thiscmd --config "${WASP_CONF_DIR}" internal_autorestart $command $args < /dev/null > /dev/null 2>&1  &
  ;;

(internal_start)
    wasp_rotate_log $logout
    wasp_rotate_log $loggc
    echo starting $command, logging to $logout
    # Add to the command log file vital stats on our environment.
    echo "`date` Starting $command on `hostname`" >> $loglog
    echo "`ulimit -a`" >> $loglog 2>&1
    nice -n $WASP_NICENESS "$WASP_HOME"/bin/wasp \
        --config "${WASP_CONF_DIR}" \
        $command "$@" start > "$logout" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$logout"
    wait
    cleanZNode
  ;;

(internal_autorestart)
    touch "$WASP_START_FILE"
    #keep starting the command until asked to stop. Reloop on software crash
    while true
      do
        lastLaunchDate=`date +%s`
        $thiscmd --config "${WASP_CONF_DIR}" internal_start $command $args

        #if the file does not exist it means that it was not stopped properly by the stop command
        if [ ! -f "$WASP_START_FILE" ]; then
          exit 1
        fi

        #if the cluster is being stopped then do not restart it again.
        zparent=`$bin/wasp com.alibaba.wasp.util.WaspConfTool zookeeper.znode.parent`
        if [ "$zparent" == "null" ]; then zparent="/wasp"; fi
        zshutdown=`$bin/wasp com.alibaba.wasp.util.WaspConfTool zookeeper.znode.state`
        if [ "$zshutdown" == "null" ]; then zshutdown="shutdown"; fi
        zFullShutdown=$zparent/$zshutdown
        $bin/wasp zkcli stat $zFullShutdown 2>&1 | grep "Node does not exist"  1>/dev/null 2>&1
        #grep returns 0 if it found something, 1 otherwise
        if [ $? -eq 0 ]; then
          exit 1
        fi

        #If ZooKeeper cannot be found, then do not restart
        $bin/wasp zkcli stat $zFullShutdown 2>&1 | grep Exception | grep ConnectionLoss  1>/dev/null 2>&1
        if [ $? -eq 0 ]; then
          exit 1
        fi

        #if it was launched less than 5 minutes ago, then wait for 5 minutes before starting it again.
        curDate=`date +%s`
        limitDate=`expr $lastLaunchDate + 300`
        if [ $limitDate -gt $curDate ]; then
          sleep 300
        fi
      done
    ;;

(stop)
    rm -f "$WASP_START_FILE"
    if [ -f $pid ]; then
      pidToKill=`cat $pid`
      # kill -0 == see if the PID exists
      if kill -0 $pidToKill > /dev/null 2>&1; then
        echo -n stopping $command
        echo "`date` Terminating $command" >> $loglog
        kill $pidToKill > /dev/null 2>&1
        while kill -0 $pidToKill > /dev/null 2>&1;
         do
           echo -n "."
           sleep 1;
         done
        rm $pid
        echo
      else
        retval=$?
        echo no $command to stop because kill -0 of pid $pidToKill failed with status $retval
      fi
    else
      echo no $command to stop because no pid file $pid
    fi
  ;;

(restart)
    # stop the command
    $thiscmd --config "${WASP_CONF_DIR}" stop $command $args &
    wait_until_done $!
    # wait a user-specified sleep period
    sp=${WASP_RESTART_SLEEP:-3}
    if [ $sp -gt 0 ]; then
      sleep $sp
    fi
    # start the command
    $thiscmd --config "${WASP_CONF_DIR}" start $command $args &
    wait_until_done $!
  ;;

(*)
  echo $usage
  exit 1
  ;;
esac
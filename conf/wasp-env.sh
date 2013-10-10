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
# Modelled after $HBASE_HOME/conf/hbase-env.sh.

# Set environment variables here.

# The java implementation to use.  Java 1.6 required.
# export JAVA_HOME=/usr/java/jdk1.6.0/

# Extra Java CLASSPATH elements.  Optional.
# export WASP_CLASSPATH=

# The maximum amount of heap to use, in MB. Default is 1000.
# export WASP_HEAPSIZE=1000

# Extra Java runtime options.
# Below are what we set by default.  May only work with SUN JVM.
# For more on why as well as other possible settings,
# see http://wiki.apache.org/hadoop/PerformanceTuning
export WASP_OPTS="$WASP_OPTS -XX:+UseConcMarkSweepGC"

# Uncomment below to enable java garbage collection logging in the .out file.
# export WASP_OPTS="$WASP_OPTS -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps $WASP_GC_OPTS" 

# Uncomment below (along with above GC logging) to put GC information in its own logfile (will set WASP_GC_OPTS)
# export WASP_USE_GC_LOGFILE=true

# Uncomment and adjust to enable JMX exporting
# See jmxremote.password and jmxremote.access in $JRE_HOME/lib/management to configure remote password access.
# More details at: http://java.sun.com/javase/6/docs/technotes/guides/management/agent.html
#
# export WASP_JMX_BASE="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false"
# export WASP_MASTER_OPTS="$WASP_MASTER_OPTS $WASP_JMX_BASE -Dcom.sun.management.jmxremote.port=10101"
# export WASP_FSERVER_OPTS="$WASP_FSERVER_OPTS $WASP_JMX_BASE -Dcom.sun.management.jmxremote.port=10102"
# export WASP_THRIFT_OPTS="$WASP_THRIFT_OPTS $WASP_JMX_BASE -Dcom.sun.management.jmxremote.port=10103"
# export WASP_ZOOKEEPER_OPTS="$WASP_ZOOKEEPER_OPTS $WASP_JMX_BASE -Dcom.sun.management.jmxremote.port=10104"

# File naming hosts on which FServers will run.  $WASP_HOME/conf/fservers by default.
# export WASP_FSERVERS=${WASP_HOME}/conf/fservers

# Uncomment and adjust to keep all the FServer pages mapped to be memory resident
#WASP_FSERVER_MLOCK=true
#WASP_FSERVER_UID="wasp"

# File naming hosts on which backup HMaster will run.  $WASP_HOME/conf/backup-masters by default.
# export WASP_BACKUP_MASTERS=${WASP_HOME}/conf/backup-masters

# Extra ssh options.  Empty by default.
# export WASP_SSH_OPTS="-o ConnectTimeout=1 -o SendEnv=WASP_CONF_DIR"

# Where log files are stored.  $WASP_HOME/logs by default.
# export WASP_LOG_DIR=${WASP_HOME}/logs

# Enable remote JDWP debugging of major Wasp processes. Meant for Core Developers 
# export WASP_MASTER_OPTS="$WASP_MASTER_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8070"
# export WASP_FSERVER_OPTS="$WASP_FSERVER_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8071"
# export WASP_THRIFT_OPTS="$WASP_THRIFT_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8072"
# export WASP_ZOOKEEPER_OPTS="$WASP_ZOOKEEPER_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8073"

# A string representing this instance of wasp. $USER by default.
# export WASP_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export WASP_NICENESS=10

# The directory where pid files are stored. /tmp by default.
# export WASP_PID_DIR=/var/hadoop/pids

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export WASP_SLAVE_SLEEP=0.1

# Tell Wasp whether it should manage it's own instance of Zookeeper or not.
# export WASP_MANAGES_ZK=true

# The default log rolling policy is RFA, where the log file is rolled as per the size defined for the 
# RFA appender. Please refer to the log4j.properties file to see more details on this appender.
# In case one needs to do log rolling on a date change, one should set the environment property
# WASP_ROOT_LOGGER to "<DESIRED_LOG LEVEL>,DRFA".
# For example:
# WASP_ROOT_LOGGER=INFO,DRFA
# The reason for changing default to RFA is to avoid the boundary case of filling out disk space as 
# DRFA doesn't put any cap on the log size.

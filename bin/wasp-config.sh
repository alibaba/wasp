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
# included in all the wasp scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*
# Modelled after $HADOOP_HOME/bin/hadoop-env.sh.

# resolve links - "${BASH_SOURCE-$0}" may be a softlink

this="${BASH_SOURCE-$0}"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin">/dev/null; pwd`
this="$bin/$script"

# the root of the wasp installation
if [ -z "$WASP_HOME" ]; then
  export WASP_HOME=`dirname "$this"`/..
fi

#check to see if the conf dir or wasp home are given as an optional arguments
while [ $# -gt 1 ]
do
  if [ "--config" = "$1" ]
  then
    shift
    confdir=$1
    shift
    WASP_CONF_DIR=$confdir
  elif [ "--hosts" = "$1" ]
  then
    shift
    hosts=$1
    shift
    WASP_FSERVERS=$hosts
  else
    # Presume we are at end of options and break
    break
  fi
done
 
# Allow alternate wasp conf dir location.
WASP_CONF_DIR="${WASP_CONF_DIR:-$WASP_HOME/conf}"
# List of wasp regions servers.
WASP_FSERVERS="${WASP_FSERVERS:-$WASP_CONF_DIR/fservers}"
# List of wasp secondary masters.
WASP_BACKUP_MASTERS="${WASP_BACKUP_MASTERS:-$WASP_CONF_DIR/backup-masters}"
# Thrift jmx opts
if [ -z "$WASP_THRIFT_JMX_OPTS" ]; then
  WASP_THRIFT_JMX_OPTS="$WASP_JMX_OPTS -Dcom.sun.management.jmxremote.port=8093"
fi
# Thrift opts
if [ -z "$WASP_THRIFT_OPTS" ]; then
  export WASP_THRIFT_OPTS="$WASP_THRIFT_JMX_OPTS"
fi

# Source the wasp-env.sh.  Will have JAVA_HOME defined.
if [ -f "${WASP_CONF_DIR}/wasp-env.sh" ]; then
  . "${WASP_CONF_DIR}/wasp-env.sh"
fi

# Set default value for fserver uid if not present
if [ -z "$WASP_FSERVER_UID" ]; then
  WASP_FSERVER_UID="wasp"
fi

# Verify if wasp has the mlock agent
if [ "$WASP_FSERVER_MLOCK" = "true" ]; then
  MLOCK_AGENT="$WASP_HOME/native/libmlockall_agent.so"
  if [ ! -f "$MLOCK_AGENT" ]; then
    cat 1>&2 <<EOF
Unable to find mlockall_agent, wasp must be compiled with -Pnative
EOF
    exit 1
  fi

  WASP_FSERVER_OPTS="$WASP_FSERVER_OPTS -agentpath:$MLOCK_AGENT=user=$WASP_FSERVER_UID"
fi

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode. Tune the variable down to prevent vmem explosion.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

if [ -z "$JAVA_HOME" ]; then
  for candidate in \
    /usr/lib/jvm/java-6-sun \
    /usr/lib/jvm/java-1.6.0-sun-1.6.0.*/jre \
    /usr/lib/jvm/java-1.6.0-sun-1.6.0.* \
    /usr/lib/j2sdk1.6-sun \
    /usr/java/jdk1.6* \
    /usr/java/jre1.6* \
    /Library/Java/Home ; do
    if [ -e $candidate/bin/java ]; then
      export JAVA_HOME=$candidate
      break
    fi
  done
  # if we didn't set it
  if [ -z "$JAVA_HOME" ]; then
    cat 1>&2 <<EOF
+======================================================================+
|      Error: JAVA_HOME is not set and Java could not be found         |
+----------------------------------------------------------------------+
| Please download the latest Sun JDK from the Sun Java web site        |
|       > http://java.sun.com/javase/downloads/ <                      |
|                                                                      |
| Wasp requires Java 1.6 or later.                                    |
| NOTE: This script will find Sun Java whether you install using the   |
|       binary or the RPM based installer.                             |
+======================================================================+
EOF
    exit 1
  fi
fi

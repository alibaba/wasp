/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.wasp;

import org.apache.hadoop.hbase.util.Bytes;

import java.sql.ResultSet;
import java.util.UUID;

/**
 * FConstants holds a bunch of wasp-related constants
 */
public final class FConstants {


  /**
   * Status codes used for return values of bulk operations.
   */
  public enum OperationStatusCode {
    NOT_RUN,
      SUCCESS,
      BAD_FAMILY,
      SANITY_CHECK_FAILURE,
      FAILURE;
  }

  // FMeta Constants
  /** long constant for zero */
  public static final Long ZERO_L = Long.valueOf(0L);
  public static final String NINES = "99999999999999";
  public static final String ZEROES = "00000000000000";

  /** Meta store stuff **/
  public static final String METASTORE_URIS = "wasp.metastore.uris";

  /** Which Table to store META Info in HBASE **/
  public static final String METASTORE_TABLE = "wasp.metastore.table";
  public static final String DEFAULT_METASTORE_TABLE = "_FMETA_";

  /** The lower-half split EntityGroup column qualifier */
  public static final byte[] SPLITA_QUALIFIER = Bytes.toBytes("splitA");

  /** The upper-half split EntityGroup column qualifier */
  public static final byte[] SPLITB_QUALIFIER = Bytes.toBytes("splitB");

  /**
   * In meta table for table row we add a prefix so table rows and eg rows are
   * apart, scan all the table row is easy to get all the table info
   */
  public static final String TABLEROW_PREFIX_STR = "#schema#";
  public static final byte[] TABLEROW_PREFIX = Bytes
      .toBytes(TABLEROW_PREFIX_STR);

  public static final byte[] TABLEINFO = Bytes.toBytes("tableinfo");

  /** The entityGroupinfo column qualifier */
  public static final byte[] EGINFO = Bytes.toBytes("eginfo");

  public static final byte[] EGLOCATION = Bytes.toBytes("eglocation");

  public static final String INDEXQUALIFIER_PREFIX_STR = "index.";
  public static final byte[] INDEXQUALIFIER_PREFIX = Bytes
      .toBytes(INDEXQUALIFIER_PREFIX_STR);

  /** delimiter used between portions of a entityGroup name */
  public static final int META_ROW_DELIMITER = ',';

  /** The catalog family as a string */
  public static final String CATALOG_FAMILY_STR = "info";

  /** The catalog family */
  public static final byte[] CATALOG_FAMILY = Bytes.toBytes(CATALOG_FAMILY_STR);

  /** The start code column qualifier */
  public static final byte[] STARTCODE_QUALIFIER = Bytes
      .toBytes("server.startcode");

  /** Meta version column qualifier **/
  public static final byte[] META_VERSION_QUALIFIER = Bytes.toBytes("v");

  /** Meta version **/
  public static final short META_VERSION = 1;

  public static final String INDEX_STORING_FAMILY_STR = "s"; // storing

  public static final byte[] INDEX_STORING_FAMILY_BYTES = Bytes
      .toBytes(INDEX_STORING_FAMILY_STR);

  public static final byte[] INDEX_STORE_ROW_QUALIFIER = Bytes
      .toBytes("rowkey");

  public static final byte[] DATA_ROW_SEP_STORE = { 0 };

  public static final byte[] DATA_ROW_SEP_QUERY = { 1 };

  /** Separate row key to table names, combine table name to one row key. **/
  public static final String TABLE_ROW_SEP = "_";

  public static final String STORAGE_TABLE_TYPE = "ENTITY";

  public static final String INDEX_TABLE_TYPE = "INDEX";

  public static final String TRANSACTION_TABLE_TYPE = "TRANSACTION";

  public static final String WASP_TABLE_PREFIX = "WASP" + TABLE_ROW_SEP;

  public static final String WASP_TABLE_ENTITY_PREFIX = WASP_TABLE_PREFIX
      + STORAGE_TABLE_TYPE + TABLE_ROW_SEP;

  public static final String WASP_TABLE_INDEX_PREFIX = WASP_TABLE_PREFIX
      + INDEX_TABLE_TYPE + TABLE_ROW_SEP;

  /** the prefix of transaction table **/
  public static final String WASP_TABLE_TRANSACTION_PREFIX = WASP_TABLE_PREFIX
      + TRANSACTION_TABLE_TYPE + TABLE_ROW_SEP;

  /** Default Column's Column family **/
  public static final String COLUMNFAMILYNAME_STR = "df"; // default

  public static final byte[] COLUMNFAMILYNAME = Bytes
      .toBytes(COLUMNFAMILYNAME_STR);

  // For migration

  /** name of version file */
  public static final String VERSION_FILE_NAME = "wasp.version";

  /**
   * This is a retry backoff multiplier table similar to the BSD TCP syn backoff
   * table, a bit more aggressive than simple exponential backoff.
   */
  public static int RETRY_BACKOFF[] = { 1, 1, 1, 2, 2, 4, 4, 8, 16, 32 };

  // Configuration parameters

  /** Cluster is in distributed mode or not */
  public static final String CLUSTER_DISTRIBUTED = "wasp.cluster.distributed";

  /** Config for pluggable sql parsers */
  public static final String WASP_SQL_PARSER_CLASS = "wasp.sql.parser.class";

  /** Config for pluggable load balancers */
  public static final String WASP_MASTER_LOADBALANCER_CLASS = "wasp.master.loadbalancer.class";

  /** Cluster is standalone or pseudo-distributed */
  public static final boolean CLUSTER_IS_LOCAL = false;

  /** Cluster is fully-distributed */
  public static final boolean CLUSTER_IS_DISTRIBUTED = true;

  /** Default value for cluster distributed mode */
  public static final boolean DEFAULT_CLUSTER_DISTRIBUTED = CLUSTER_IS_LOCAL;

  /** default host address */
  public static final String DEFAULT_HOST = "0.0.0.0";

  /** Parameter name for port master listens on. */
  public static final String MASTER_PORT = "wasp.master.port";

  /** default port that the master listens on */
  public static final int DEFAULT_MASTER_PORT = 60000;

  /** default port for master web api */
  public static final int DEFAULT_MASTER_INFOPORT = 60080;

  /** Configuration key for master web API port */
  public static final String MASTER_INFO_PORT = "wasp.master.info.port";

  /**
   * Parameter name for the master type being backup (waits for primary to go
   * inactive).
   */
  public static final String MASTER_TYPE_BACKUP = "wasp.master.backup";

  /**
   * by default every master is a possible primary master unless the conf
   * explicitly overrides it
   */
  public static final boolean DEFAULT_MASTER_TYPE_BACKUP = false;

  /** Parameter name for ZooKeeper session time out. */
  public static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.session.timeout";

  /** Name of ZooKeeper quorum configuration parameter. */
  public static final String ZOOKEEPER_QUORUM = "wasp.zookeeper.quorum";

  /** Name of ZooKeeper config file in conf/ directory. */
  public static final String ZOOKEEPER_CONFIG_NAME = "zoo.cfg";

  /** Common prefix of ZooKeeper configuration properties */
  public static final String ZK_CFG_PROPERTY_PREFIX = "wasp.zookeeper.property.";

  public static final int ZK_CFG_PROPERTY_PREFIX_LEN = ZK_CFG_PROPERTY_PREFIX
      .length();

  /**
   * The ZK client port key in the ZK properties map. The name reflects the fact
   * that this is not an Wasp configuration key.
   */
  public static final String CLIENT_PORT_STR = "clientPort";

  /** Parameter name for the client port that the zookeeper listens on */
  public static final String ZOOKEEPER_CLIENT_PORT = ZK_CFG_PROPERTY_PREFIX
      + CLIENT_PORT_STR;

  /** Default client port that the zookeeper listens on */
  public static final int DEFAULT_ZOOKEPER_CLIENT_PORT = 2181;

  /** Parameter name for the wait time for the recoverable zookeeper */
  public static final String ZOOKEEPER_RECOVERABLE_WAITTIME = "wasp.zookeeper.recoverable.waittime";

  /** Default wait time for the recoverable zookeeper */
  public static final long DEFAULT_ZOOKEPER_RECOVERABLE_WAITIME = 10000;

  /** Parameter name for the root dir in ZK for this cluster */
  public static final String ZOOKEEPER_ZNODE_PARENT = "zookeeper.wasp.znode.parent";

  public static final String DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/wasp";

  /**
   * Parameter name for the limit on concurrent client-side zookeeper
   * connections
   */
  public static final String ZOOKEEPER_MAX_CLIENT_CNXNS = ZK_CFG_PROPERTY_PREFIX
      + "maxClientCnxns";

  /** Parameter name for the ZK data directory */
  public static final String ZOOKEEPER_DATA_DIR = ZK_CFG_PROPERTY_PREFIX
      + "dataDir";

  /** Default limit on concurrent client-side zookeeper connections */
  public static final int DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS = 300;

  /** Configuration key for ZooKeeper session timeout */
  public static final String ZK_SESSION_TIMEOUT = "zookeeper.session.timeout";

  /** Default value for ZooKeeper session timeout */
  public static final int DEFAULT_ZK_SESSION_TIMEOUT = 180 * 1000;

  /** Parameter name for port fserver listens on. */
  public static final String WASP_TRANSACTION_RETRY_PAUSE = "wasp.transaction.retry.pause";

  /** Default port fserver listens on. */
  public static final int DEFAULT_WASP_TRANSACTION_RETRY_PAUSE = 10;

  /** Parameter name for port fserver listens on. */
  public static final String FSERVER_PORT = "wasp.fserver.port";

  /** Default port fserver listens on. */
  public static final int DEFAULT_FSERVER_PORT = 60020;

  /** default port for fserver web api */
  public static final int DEFAULT_FSERVER_INFOPORT = 60030;

  /** A configuration key for fserver info port */
  public static final String FSERVER_INFO_PORT = "wasp.fserver.info.port";

  /** A flag that enables automatic selection of fserver info port */
  public static final String FSERVER_INFO_PORT_AUTO = FSERVER_INFO_PORT
      + ".auto";

  /** Parameter name for what fserver implementation to use. */
  public static final String FSERVER_IMPL = "wasp.fserver.impl";

  /** Parameter name for what master implementation to use. */
  public static final String MASTER_IMPL = "wasp.fmaster.impl";

  /** Parameter name for what wasp client implementation to use. */
  public static final String WASPCLIENT_IMPL = "wasp.fclient.impl";

  /** Parameter name for how often threads should wake up */
  public static final String THREAD_WAKE_FREQUENCY = "wasp.server.thread.wakefrequency";

  /** Default value for thread wake frequency */
  public static final int DEFAULT_THREAD_WAKE_FREQUENCY = 10 * 1000;

  /**
   * Parameter name for how often we should try to write a version file, before
   * failing
   */
  public static final String VERSION_FILE_WRITE_ATTEMPTS = "wasp.server.versionfile.writeattempts";

  /**
   * Parameter name for how often we should try to write a version file, before
   * failing
   */
  public static final int DEFAULT_VERSION_FILE_WRITE_ATTEMPTS = 3;

  /** Parameter name for what entityGroup server interface to use. */
  public static final String FSERVER_CLASS = "wasp.fserver.class";

  /** Default entityGroup server interface class name. */
  public static final String DEFAULT_FSERVER_CLASS = EntityGroupInfo.class
      .getName();

  /**
   * Parameter name for maximum attempts, used to limit the number of times the
   * client will try to obtain the proxy for a given fserver.
   */
  public static final String WASP_CLIENT_RPC_MAXATTEMPTS = "wasp.client.rpc.maxattempts";

  /**
   * Default value of {@link #WASP_CLIENT_RPC_MAXATTEMPTS}.
   */
  public static final int DEFAULT_WASP_CLIENT_RPC_MAXATTEMPTS = 1;

  /**
   * Parameter name for client pause value, used mostly as value to wait before
   * running a retry of a failed get, entityGroup lookup, etc.
   */
  public static final String WASP_CLIENT_PAUSE = "wasp.client.pause";

  public static final String READ_MODEL = "READ_MODEL";

  /**
   * Default value of {@link #WASP_CLIENT_PAUSE}.
   */
  public static final long DEFAULT_WASP_CLIENT_PAUSE = 1000;

  /**
   * Parameter name for Wasp client operation timeout, which overrides RPC
   * timeout
   */
  public static final String WASP_CLIENT_OPERATION_TIMEOUT = "wasp.client.operation.timeout";

  /**
   * Default Wasp client operation timeout, which is tantamount to a blocking
   * call
   */
  public static final int DEFAULT_WASP_CLIENT_OPERATION_TIMEOUT = Integer.MAX_VALUE;

  public static final String WASP_CONNECTION_TIMEOUT_MILLIS = "wasp.connection.timeout";

  /** If not specified, the default connection timeout will be used (3 sec). */
  public static final long DEFAULT_CONNECTION_TIMEOUT_MILLIS = 3 * 1000L;
  
  public static final String CONNECTION_WRITE_TIMEOUT_SEC = "wasp.connection.write.timeout";
  
  /** If not specified, the default connection write timeout will be used (3 sec). */
  public static final int DEFAULT_CONNECTION_WRITE_TIMEOUT_SEC = 3;
  
  public static final String CONNECTION_READ_TIMEOUT_SEC = "wasp.connection.read.timeout";
  
  /** If not specified, the default connection read timeout will be used (3 sec). */
  public static final int DEFAULT_CONNECTION_READ_TIMEOUT_SEC = 3;

  public static final String WASP_CLIENT_SCANNER_TIMEOUT_PERIOD = "wasp.client.scanner.timeout.period";

  public static final int DEFAULT_WASP_CLIENT_SCANNER_TIMEOUT_PERIOD = 60 * 1000;

  public static final String WASP_CLIENT_SESSION_TIMEOUT_PERIOD = "wasp.client.session.timeout.period";

  public static final int DEFAULT_WASP_CLIENT_SESSION_TIMEOUT_PERIOD = 60 * 1000;

  public static final String WASP_EXECUTION_ENGINE_TIMEOUT_PERIOD = "wasp.execution.engine.timeout.period";

  public static final int DEFAULT_WASP_EXECUTION_ENGINE_TIMEOUT_PERIOD = 60 * 1000;

  public static final String WASP_FSEVER_SUBSCRIBER_TIMEOUT_PERIOD = "wasp.fserver.subscriber.timeout.period";

  public static final int DEFAULT_WASP_FSEVER_SUBSCRIBER_TIMEOUT_PERIOD = 60 * 1000;

  public static final String WASP_JDBC_FETCHSIZE = "wasp.jdbc.fetchsize";

  public static final int DEFAULT_WASP_JDBC_FETCHSIZE = 1000;

  /** Used by WBCK to sideline backup data */
  public static final String WBCK_SIDELINEDIR_NAME = ".wbck";

  public static final String WASP_EDITS_REPLAY_SKIP_ERRORS = "wasp.entitygroup.edits.replay.skip.errors";

  public static final boolean DEFAULT_ENTITYGROUP_EDITS_REPLAY_SKIP_ERRORS = false;

  /** name of the file for unique cluster ID */
  public static final String CLUSTER_ID_FILE_NAME = "wasp.id";

  /** Configuration key storing the cluster ID */
  public static final String CLUSTER_ID = "wasp.cluster.id";

  // entityGroup

  public static final String ENTITYGROUP_SCANNER_LIMIT = "entitygroup.scanner.limit";

  public static final int DEFAULT_ENTITYGROUP_SCANNER_LIMIT = 20;

  public static final String ENTITYGROUP_SCANNER_CACHING = "entitygroup.scanner.caching";

  public static final int DEFAULT_ENTITYGROUP_SCANNER_CACHING = 20;

  // Other constants

  /**
   * Table name of message queue
   */
  public static final String MESSAGEQUEUE_TABLENAME = "WASP_MESSAGE_QUEUE";

  public static final String MESSAGEQUEUE_FAMILIY = FConstants.CATALOG_FAMILY_STR;

  public static final byte[] BYTE_MESSAGEQUEUE_FAMILIY = Bytes
      .toBytes(MESSAGEQUEUE_FAMILIY);

  public static final String MESSAGEQUEUE_QUALIFIER = FConstants.CATALOG_FAMILY_STR;

  public static final byte[] BYTE_MESSAGEQUEUE_QUALIFIER = Bytes
      .toBytes(MESSAGEQUEUE_QUALIFIER);

  public static final byte[] BYTE_COMMIT_QUALIFIER = Bytes.toBytes("commit");

  /**
   * An empty instance.
   */
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  /**
   * Used by scanners, etc when they want to start at the beginning of a
   * entityGroup
   */
  public static final byte[] EMPTY_START_ROW = EMPTY_BYTE_ARRAY;

  /**
   * Last row in a table.
   */
  public static final byte[] EMPTY_END_ROW = EMPTY_START_ROW;

  /**
   * Used by scanners and others when they're trying to detect the end of a
   * table
   */
  public static final byte[] LAST_ROW = EMPTY_BYTE_ARRAY;

  /**
   * Max length a row can have because of the limitation in TFile.
   */
  public static final int MAX_ROW_LENGTH = Short.MAX_VALUE;

  /** When we encode strings, we always specify UTF8 encoding */
  public static final String UTF8_ENCODING = "UTF-8";

  /**
   * Timestamp to use when we want to refer to the latest cell. This is the
   * timestamp sent by clients when no timestamp is specified on commit.
   */
  public static final long LATEST_TIMESTAMP = Long.MAX_VALUE;

  /**
   * Timestamp to use when we want to refer to the oldest cell.
   */
  public static final long OLDEST_TIMESTAMP = Long.MIN_VALUE;

  /**
   * LATEST_TIMESTAMP in bytes form
   */
  public static final byte[] LATEST_TIMESTAMP_BYTES = Bytes
      .toBytes(LATEST_TIMESTAMP);

  /**
   * Define for 'return-all-versions'.
   */
  public static final int ALL_VERSIONS = Integer.MAX_VALUE;

  /**
   * Unlimited time-to-live.
   */
  public static final int FOREVER = Integer.MAX_VALUE;

  /**
   * Seconds in a week
   */
  public static final int WEEK_IN_SECONDS = 7 * 24 * 3600;

  public static final String NAME = "NAME";
  public static final String VERSIONS = "VERSIONS";
  public static final String IN_MEMORY = "IN_MEMORY";
  public static final String CONFIG = "CONFIG";

  public static final String ENTITYGROUP_IMPL = "wasp.entitygroup.impl";

  public static final String REDO_IMPL = "wasp.redo.impl";

  /** modifyTable op for replacing the table descriptor */
  public static enum Modify {
    CLOSE_ENTITYGROUP,
      TABLE_SPLIT
  }

  /**
   * Default cluster ID, cannot be used to identify a cluster so a key with this
   * value means it wasn't meant for replication.
   */
  public static final UUID DEFAULT_CLUSTER_ID = new UUID(0L, 0L);

  public static final String WASP_CLIENT_RETRIES_NUMBER = "wasp.client.retries.number";

  public static final int DEFAULT_WASP_CLIENT_RETRIES_NUMBER = 10;

  public static final String WASP_FMETA_RETRIES_NUMBER = "wasp.fmeta.retries.number";

  public static final int DEFAULT_WASP_FMETA_RETRIES_NUMBER = 10;

  /**
   * timeout for each RPC
   */
  public static final String WASP_RPC_TIMEOUT_KEY = "wasp.rpc.timeout";

  public static final String WASP_FSERVER_MAX_TABLE_COUNT = "wasp.fserver.max.table.count";

  /**
   * Default value of {@link #WASP_RPC_TIMEOUT_KEY}
   */
  public static final int DEFAULT_WASP_RPC_TIMEOUT = 60000;

  /** WBCK special code name used as server name when manipulating ZK nodes */
  public static final String WBCK_CODE_NAME = "WBCKServerName";

  public static final String WASP_MASTER_LOGCLEANER_PLUGINS = "wasp.master.logcleaner.plugins";

  public static final String WASP_ENTITYGROUP_SPLIT_POLICY_KEY = "wasp.fserver.entitygroup.split.policy";

  /** Host name of the local machine */
  public static final String LOCALHOST = "localhost";

  public static final String LOCALHOST_IP = "127.0.0.1";

  /** EntityGroup in Transition metrics threshold time */
  public static final String METRICS_EGIT_STUCK_WARNING_THRESHOLD = "wasp.metrics.egit.stuck.warning.threshold";

  public static final String LOAD_BALANCER_SLOP_KEY = "wasp.entitygroups.slop";

  public static final String KEY_FOR_HOSTNAME_SEEN_BY_MASTER = "key.for.hostname.seen.by.master";

  /** delimiter used between portions of a entityGroup name */
  public static final int DELIMITER = ',';

  private FConstants() {
    // Can't be instantiated with this ctor.
  }

  public static final String BUILD_ID = "1";

  /**
   * The major version of this database.
   */
  public static final int VERSION_MAJOR = 1;

  /**
   * The minor version of this database.
   */
  public static final int VERSION_MINOR = 3;

  /**
   * The database URL prefix of this database.
   */
  public static final String START_URL = "jdbc:wasp:";

  /**
   * The database URL format in simplified Backus-Naur form.
   */
  public static final String URL_FORMAT = START_URL
      + "{ {.|mem:}[name] | {tcp|ssl}:[//]server[:port][,server2[:port]]/name }[;key=value...]";

  public static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

  /**
   * Get the version of this product, consisting of major version, minor
   * version, and build id.
   * 
   * @return the version number
   */
  public static String getVersion() {
    String version = VERSION_MAJOR + "." + VERSION_MINOR + "." + BUILD_ID;
    return version;
  }

  /**
   * Get the complete version number of this database, consisting of the major
   * version, the minor version, the build id, and the build date.
   * 
   * @return the complete version
   */
  public static String getFullVersion() {
    return getVersion();
  }

  public static final String SCHEMA_MAIN = "PUBLIC";

  /**
   * The block size for I/O operations.
   */
  public static final int IO_BUFFER_SIZE = 4 * 1024;

  public static final String GLOBAL_ENTITYGROUP = "global.entity.group";
}

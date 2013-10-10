/**
 *
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

package com.alibaba.wasp.meta;

import com.alibaba.wasp.EnityGroupOfflineException;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.FConstants;
import com.alibaba.wasp.MetaException;
import com.alibaba.wasp.ServerName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Scanner class that contains the <code>.FMETA.</code> table scanning logic and
 * uses a Retryable scanner. Provided visitors will be called for each row.
 * 
 * Although public visibility, this is not a public-facing API and may evolve in
 * minor releases.
 * 
 * <p>
 * Note that during concurrent EG splits, the scanner might not see META changes
 * across rows (for parent and daughter entries) consistently. see {@link BlockingMetaScannerVisitor} for details.
 * </p>
 */
public class FMetaScanner extends AbstractMetaService {

  private static final Log LOG = LogFactory.getLog(FMetaScanner.class);

  /**
   * Scans the meta table and calls a visitor on each RowResult and uses a empty
   * start row value as table name.
   *
   * @param configuration
   *          conf
   * @param visitor
   *          A custom visitor
   * @throws java.io.IOException
   *           e
   */
  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor) throws IOException {
    metaScan(configuration, visitor, null);
  }

  /**
   * Need to add comments.
   *
   * @param conf
   * @param entityGroupName
   * @return
   * @throws java.io.IOException
   */
  public static Pair<EntityGroupInfo, ServerName> getEntityGroup(
      Configuration conf, byte[] entityGroupName) throws IOException {
    return getService(conf).getEntityGroupAndLocation(entityGroupName);
  }

  /**
   * Scans the meta table and calls a visitor on each RowResult. Uses a table
   * name to locate meta entitGroups.
   *
   * @param configuration
   *          config
   * @param visitor
   *          visitor object
   * @param userTableName
   *          User table name in meta table to start scan at. Pass null if not
   *          interested in a particular table.
   * @throws java.io.IOException
   *           e
   */
  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor, byte[] userTableName) throws IOException {
    metaScan(configuration, visitor, userTableName, null, Integer.MAX_VALUE);
  }

  public static void metaScan(Configuration configuration,
      MetaScannerVisitor visitor, byte[] tableName, byte[] row, int rowLimit)
      throws IOException {
    int rowUpperLimit = rowLimit > 0 ? rowLimit : Integer.MAX_VALUE;

    // if row is not null, we want to use the startKey of the row's entityGroup
    // as
    // the startRow for the meta scan.
    byte[] startRow = tableName;
    if (tableName == null || tableName.length == 0) {
      // Full META scan
      startRow = FConstants.EMPTY_START_ROW;
    }
    String metaTableString = configuration.get(FConstants.METASTORE_TABLE,
        FConstants.DEFAULT_METASTORE_TABLE);
    HTable fmetaTable = new HTable(configuration, metaTableString);
    // Scan over each meta entityGroup
    int rows = Math.min(rowLimit, configuration.getInt(
        HConstants.HBASE_META_SCANNER_CACHING,
        HConstants.DEFAULT_HBASE_META_SCANNER_CACHING));

    final Scan scan = new Scan(startRow);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Scanning " + metaTableString + " starting at row="
          + Bytes.toStringBinary(startRow) + " for max=" + rowUpperLimit
          + " rows ");
    }
    scan.addFamily(FConstants.CATALOG_FAMILY);
    scan.setCaching(rows);
    if (tableName == null || tableName.length == 0) {
      FilterList allFilters = new FilterList();
      allFilters.addFilter(new PrefixFilter(tableName));
      scan.setFilter(allFilters);
    }
    int processedRows = 0;
    try {
      ResultScanner scanner = fmetaTable.getScanner(scan);
      for (Result r = scanner.next(); r != null; r = scanner.next()) {
        if (processedRows >= rowUpperLimit) {
          break;
        }
        if (Bytes.startsWith(r.getRow(), FConstants.TABLEROW_PREFIX)) {
          continue;
        }
        if (!visitor.processRow(r))
          break; // exit completely
        processedRows++;
      }
      // here, we didn't break anywhere. Check if we have more rows
    } finally {
      // Close scanner
      fmetaTable.close();
    }
  }

  /**
   * Returns EntityGroupInfo object from the column
   * FConstants.CATALOG_FAMILY:FConstants.EGINFO of the catalog table Result.
   *
   * @param data a Result object from the catalog table scan
   * @return EntityGroupInfo or null
   */
  public static EntityGroupInfo getEntityGroupInfo(Result data) {
    byte[] bytes = data.getValue(FConstants.CATALOG_FAMILY, FConstants.EGINFO);
    if (bytes == null)
      return null;
    EntityGroupInfo info = EntityGroupInfo.parseFromOrNull(bytes);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Current INFO from scan results = " + info);
    }
    return info;
  }

  /**
   * Lists all of the EntityGroups currently in META.
   *
   * @param conf
   * @return List of all user-space EntityGroups.
   * @throws java.io.IOException
   */
  public static List<EntityGroupInfo> listAllEntityGroups(Configuration conf)
      throws IOException {
    return listAllEntityGroups(conf, true);
  }

  /**
   * Lists all of the EntityGroups currently in META.
   *
   * @param conf
   * @param offlined
   *          True if we are to include offlined EntityGroups, false and we'll
   *          leave out offlined EntityGroups from returned list.
   * @return List of all user-space EntityGroups.
   * @throws java.io.IOException
   */
  public static List<EntityGroupInfo> listAllEntityGroups(Configuration conf,
      final boolean offlined) throws IOException {
    final List<EntityGroupInfo> entityGroups = new ArrayList<EntityGroupInfo>();
    MetaScannerVisitor visitor = new BlockingMetaScannerVisitor(conf) {
      @Override
      public boolean processRowInternal(Result result) throws IOException {
        if (result == null || result.isEmpty()) {
          return true;
        }

        EntityGroupInfo entityGroupInfo = getEntityGroupInfo(result);
        if (entityGroupInfo == null) {
          LOG.warn("Null ENTITYGROUPINFO_QUALIFIER: " + result);
          return true;
        }

        // If entityGroup offline AND we are not to include offlined entityGroups,
        // return.
        if (entityGroupInfo.isOffline() && !offlined)
          return true;
        entityGroups.add(entityGroupInfo);
        return true;
      }
    };
    metaScan(conf, visitor);
    return entityGroups;
  }

  /**
   * Get root table name
   *
   * @param FTable
   * @return
   */
  public static String getRootTable(FTable table) {
    if (table.isRootTable()) {
      return table.getTableName();
    } else {
      return table.getParentName();
    }
  }

  /**
   * Lists all of the table EntityGroups currently in META.
   *
   * @param conf
   * @param offlined
   *          True if we are to include offlined EntityGroups, false and we'll
   *          leave out offlined EntityGroups from returned list.
   * @return Map of all user-space EntityGroups to servers
   * @throws java.io.IOException
   */
  public static NavigableMap<EntityGroupInfo, ServerName> allTableEntityGroups(
      Configuration conf, final byte[] tablename, final boolean offlined)
      throws IOException {
    FTable table = getService(conf).getTable(Bytes.toString(tablename));
    final byte[] parentTableName = Bytes.toBytes(getRootTable(table));
    final NavigableMap<EntityGroupInfo, ServerName> entityGroups = new TreeMap<EntityGroupInfo, ServerName>();
    MetaScannerVisitor visitor = new TableMetaScannerVisitor(conf,
        parentTableName) {
      @Override
      public boolean processRowInternal(Result rowResult) throws IOException {
        EntityGroupInfo entityGroupInfo = getEntityGroupInfo(rowResult);
        ServerName sn = ServerName.getServerName(rowResult);

        if (!(entityGroupInfo.isOffline() || entityGroupInfo.isSplit())) {
          entityGroups.put(entityGroupInfo, sn);
        }
        return true;
      }
    };
    metaScan(conf, visitor, parentTableName);
    return entityGroups;
  }

  /**
   * Visitor class called to process each row of the .META. table
   */
  public interface MetaScannerVisitor extends Closeable {
    /**
     * Visitor method that accepts a RowResult and the meta entityGroup location.
     * Implementations can return false to stop the entityGroup's loop if it
     * becomes unnecessary for some reason.
     *
     * @param rowResult
     *          result
     * @return A boolean to know if it should continue to loop in the
     *         entityGroup
     * @throws java.io.IOException
     *           e
     */
    public boolean processRow(Result rowResult) throws IOException;
  }

  public static abstract class MetaScannerVisitorBase implements
      MetaScannerVisitor {
    @Override
    public void close() throws IOException {
    }
  }

  /**
   * A MetaScannerVisitor that provides a consistent view of the table's META
   * entries during concurrent splits (see HBASE-5986 for details). This class
   * does not guarantee ordered traversal of meta entries, and can block until
   * the META entries for daughters are available during splits.
   */
  public static abstract class BlockingMetaScannerVisitor extends
      MetaScannerVisitorBase {

    private static final int DEFAULT_BLOCKING_TIMEOUT = 10000;
    private Configuration conf;
    private TreeSet<byte[]> daughterEntityGroups = new TreeSet<byte[]>(
        Bytes.BYTES_COMPARATOR);
    private int blockingTimeout;
    private HTable fmetaTable;

    public BlockingMetaScannerVisitor(Configuration conf) {
      this.conf = conf;
      this.blockingTimeout = conf.getInt(
          HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, DEFAULT_BLOCKING_TIMEOUT);
    }

    public abstract boolean processRowInternal(Result rowResult)
        throws IOException;

    @Override
    public void close() throws IOException {
      super.close();
      if (fmetaTable != null) {
        fmetaTable.close();
        fmetaTable = null;
      }
    }

    public HTable getFMetaTable() throws IOException {
      if (fmetaTable == null) {
        String metaTableString = conf.get(FConstants.METASTORE_TABLE,
            FConstants.DEFAULT_METASTORE_TABLE);
        fmetaTable = new HTable(conf, metaTableString);
      }
      return fmetaTable;
    }

    @Override
    public boolean processRow(Result rowResult) throws IOException {
      EntityGroupInfo info = getEntityGroupInfo(rowResult);
      if (info == null) {
        return true;
      }

      if (daughterEntityGroups.remove(info.getEntityGroupName())) {
        return true; // we have already processed this row
      }

      if (info.isSplitParent()) {
        /*
         * we have found a parent entityGroup which was split. We have to ensure
         * that it's daughters are seen by this scanner as well, so we block
         * until they are added to the META table. Even though we are waiting
         * for META entries, ACID semantics in HBase indicates that this scanner
         * might not see the new rows. So we manually query the daughter rows
         */
        PairOfSameType<EntityGroupInfo> daughters = EntityGroupInfo
            .getDaughterEntityGroups(rowResult);
        EntityGroupInfo splitA = daughters.getFirst();
        EntityGroupInfo splitB = daughters.getSecond();

        HTable fmetaTable = getFMetaTable();
        long start = System.currentTimeMillis();
        Result resultA = getEntityGroupResultBlocking(fmetaTable,
            blockingTimeout, splitA.getEntityGroupName());
        if (resultA != null) {
          processRow(resultA);
          daughterEntityGroups.add(splitA.getEntityGroupName());
        } else {
          throw new EnityGroupOfflineException("Split daughter entityGroup "
              + splitA.getEntityGroupNameAsString()
              + " cannot be found in META.");
        }
        long rem = blockingTimeout - (System.currentTimeMillis() - start);

        Result resultB = getEntityGroupResultBlocking(fmetaTable, rem,
            splitB.getEntityGroupName());
        if (resultB != null) {
          processRow(resultB);
          daughterEntityGroups.add(splitB.getEntityGroupName());
        } else {
          throw new EnityGroupOfflineException("Split daughter entityGroup "
              + splitB.getEntityGroupNameAsString()
              + " cannot be found in META.");
        }
      }

      return processRowInternal(rowResult);
    }

    private Result getEntityGroupResultBlocking(HTable fmetaTable, long timeout,
        byte[] entityGroupName) throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("blocking until entityGroup is in META: "
            + Bytes.toStringBinary(entityGroupName));
      }
      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < timeout) {
        Get get = new Get(entityGroupName);
        Result result = fmetaTable.get(get);
        EntityGroupInfo entityGroupInfo = getEntityGroupInfo(result);
        if (entityGroupInfo != null) {
          return result;
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      return null;
    }
  }

  /**
   * A MetaScannerVisitor for a table. Provides a consistent view of the table's
   * META entries during concurrent splits (see HBASE-5986 for details). This
   * class does not guarantee ordered traversal of meta entries, and can block
   * until the META entries for daughters are available during splits.
   */
  public static abstract class TableMetaScannerVisitor extends
      BlockingMetaScannerVisitor {
    private byte[] tableName;

    public TableMetaScannerVisitor(Configuration conf, byte[] tableName) {
      super(conf);
      this.tableName = tableName;
    }

    @Override
    public final boolean processRow(Result rowResult) throws IOException {
      EntityGroupInfo entityGroupInfo = getEntityGroupInfo(rowResult);
      if (entityGroupInfo == null) {
        return true;
      }
      if (!(Bytes.equals(entityGroupInfo.getTableName(), tableName))) {
        return false;
      }
      return super.processRow(rowResult);
    }

  }

  /**
   *
   * @param conf
   * @param disabledOrDisablingOrEnabling
   * @param excludeOfflinedSplitParents
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static Map<EntityGroupInfo, ServerName> fullScan(Configuration conf,
      Set<String> disabledOrDisablingOrEnabling,
      boolean excludeOfflinedSplitParents) throws MetaException {
    return getService(conf).fullScan(disabledOrDisablingOrEnabling,
        excludeOfflinedSplitParents);
  }

  /**
   *
   * @param conf
   * @return
   * @throws com.alibaba.wasp.MetaException
   */
  public static List<Result> fullScan(Configuration conf) throws MetaException {
    return getService(conf).fullScan();
  }

  /**
   *
   * @param conf
   * @param visitor
   * @param startrow
   * @param endrow
   * @throws com.alibaba.wasp.MetaException
   */
  public static void fullScan(final Configuration conf,
      final FMetaVisitor visitor, final byte[] startrow, final byte[] endrow)
      throws MetaException {
    getService(conf).fullScan(visitor, startrow, endrow);
  }
}
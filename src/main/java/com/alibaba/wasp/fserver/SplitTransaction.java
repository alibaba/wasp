/**
 * Copyright 2010 The Apache Software Foundation
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
package com.alibaba.wasp.fserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.PairOfSameType;
import com.alibaba.wasp.EntityGroupInfo;
import com.alibaba.wasp.EntityGroupTransaction;
import com.alibaba.wasp.Server;
import com.alibaba.wasp.ServerName;
import com.alibaba.wasp.executor.EventHandler;
import com.alibaba.wasp.meta.FMetaEditor;
import com.alibaba.wasp.zookeeper.ZKAssign;
import com.alibaba.wasp.zookeeper.ZKUtil;
import com.alibaba.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Executes entityGroup split as a "transaction". Call {@link #prepare()} to
 * setup the transaction, {@link #execute(Server, FServerServices)} to run the
 * transaction and {@link #rollback(Server, FServerServices)} to cleanup if
 * execute fails.
 * 
 * <p>
 * Here is an example of how you would use this class:
 * 
 * <pre>
 *  SplitTransaction st = new SplitTransaction(this.conf, parent, midKey)
 *  if (!st.prepare()) return;
 *  try {
 *    st.execute(server, services);
 *  } catch (IOException ioe) {
 *    try {
 *      st.rollback(server, services);
 *      return;
 *    } catch (RuntimeException e) {
 *      myAbortable.abort("Failed split, abort");
 *    }
 *  }
 * </Pre>
 * <p>
 * This class is not thread safe. Caller needs ensure split is run by one thread
 * only.
 */
public class SplitTransaction {
  private static final Log LOG = LogFactory.getLog(SplitTransaction.class);
  /*
   * EntityGroup to split
   */
  private final EntityGroup parent;
  private EntityGroupInfo egi_a;
  private EntityGroupInfo egi_b;
  private int znodeVersion = -1;

  /*
   * Row to split around
   */
  private final byte[] splitrow;

  /**
   * Types to add to the transaction journal. Each enum is a step in the split
   * transaction. Used to figure how much we need to rollback.
   */
  enum JournalEntry {
    /**
     * Before creating node in splitting state.
     */
    STARTED_SPLITTING,
      /**
       * Set entityGroup as in transition, set it into SPLITTING state.
       */
      SET_SPLITTING_IN_ZK,
      /**
       * We created the temporary split data directory.
       */
      CREATE_SPLIT_STOREAGE,
      /**
       * Closed the parent entityGroup.
       */
      CLOSED_PARENT_ENTITYGROUP,
      /**
       * The parent has been taken out of the server's online entityGroups list.
       */
      OFFLINED_PARENT,
      /**
       * Started in on creation of the first daughter entityGroup.
       */
      STARTED_ENTITYGROUP_A_CREATION,
      /**
       * Started in on the creation of the second daughter entityGroup.
       */
      STARTED_ENTITYGROUP_B_CREATION,
      /**
       * Point of no return. If we got here, then transaction is not recoverable
       * other than by crashing out the fserver.
       */
      PONR
  }

  /*
   * Journal of how far the split transaction has progressed.
   */
  private final List<JournalEntry> journal = new ArrayList<JournalEntry>();

  private IOException closedByOtherException = new IOException(
      "Failed to close entityGroup: already closed by another thread");

  /**
   * Constructor
   * 
   * @param eg
   *          EntityGroup to split
   * @param splitrow
   *          Row to split around
   * @throws IOException
   */
  // public SplitTransaction(final Configuration conf, final EntityGroup eg,
  // final byte[] splitrow)
  // throws IOException {
  // this(eg, splitrow, conf);
  // }

  public SplitTransaction(final EntityGroup eg, final byte[] splitrow,
      final Configuration conf) throws IOException {
    this.parent = eg;
    this.splitrow = splitrow;
  }

  /**
   * Does checks on split inputs.
   * 
   * @return <code>true</code> if the entityGroup is splittable else
   *         <code>false</code> if it is not (e.g. its already closed, etc.).
   */
  public boolean prepare() {
    if (this.parent.isClosed() || this.parent.isClosing()) {
      LOG.info("EntityGroup " + this.parent + " is not splittable,"
          + " opened = " + this.parent.isAvailable());
      return false;
    }
    // Split key can be null if this entityGroup is unsplittable; i.e. has refs.
    if (this.splitrow == null)
      return false;
    EntityGroupInfo egi = this.parent.getEntityGroupInfo();
    // Check splitrow.
    byte[] startKey = egi.getStartKey();
    byte[] endKey = egi.getEndKey();
    if (Bytes.equals(startKey, splitrow)
        || !this.parent.getEntityGroupInfo().containsRow(splitrow)) {
      LOG.info("Split row is not inside entityGroup key range or is equal to "
          + "startkey: " + Bytes.toStringBinary(this.splitrow));
      return false;
    }
    long egid = getDaughterEntityGroupIdTimestamp(egi);
    this.egi_a = new EntityGroupInfo(egi.getTableName(), startKey,
        this.splitrow, false, egid);
    this.egi_b = new EntityGroupInfo(egi.getTableName(), this.splitrow, endKey,
        false, egid);
    return true;
  }

  /**
   * Calculate daughter entityGroupId to use.
   * 
   * @param egi
   *          Parent {@link EntityGroupInfo}
   * @return Daughter entityGroup id (timestamp) to use.
   */
  private static long getDaughterEntityGroupIdTimestamp(
      final EntityGroupInfo egi) {
    long egid = System.currentTimeMillis();
    // EntityGroupId is timestamp. Can't be less than that of parent else will
    // insert
    // at wrong location in .FMETA.
    if (egid < egi.getEntityGroupId()) {
      LOG.warn("Clock skew; parent entityGroup id is " + egi.getEntityGroupId()
          + " but current time here is " + egid);
      egid = egi.getEntityGroupId() + 1;
    }
    return egid;
  }

  /*
   * Open daughter entityGroup in its own thread. If we fail, abort this hosting
   * server.
   */
  class DaughterOpener extends HasThread {
    private final Server server;
    private final EntityGroup entityGroup;
    private Throwable t = null;

    DaughterOpener(final Server s, final EntityGroup entityGroup) {
      super((s == null ? "null-services" : s.getServerName())
          + "-daughterOpener="
          + entityGroup.getEntityGroupInfo().getEncodedName());
      setDaemon(true);
      this.server = s;
      this.entityGroup = entityGroup;
    }

    /**
     * @return Null if open succeeded else exception that causes us fail open.
     *         Call it after this thread exits else you may get wrong view on
     *         result.
     */
    Throwable getException() {
      return this.t;
    }

    @Override
    public void run() {
      try {
        openDaughterEntityGroup(this.server, entityGroup);
      } catch (Throwable t) {
        this.t = t;
      }
    }
  }

  /**
   * Open daughter entityGroups, add them to online list and update meta.
   * 
   * @param server
   * @param daughter
   * @throws IOException
   * @throws KeeperException
   */
  public EntityGroup openDaughterEntityGroup(final Server server,
      final EntityGroup daughter) throws IOException, KeeperException {
    return daughter.openEntityGroup(null);
  }

  static class LoggingProgressable implements CancelableProgressable {
    private final EntityGroupInfo egi;
    private long lastLog = -1;
    private final long interval;

    LoggingProgressable(final EntityGroupInfo egi, final Configuration c) {
      this.egi = egi;
      this.interval = c.getLong(
          "wasp.fserver.split.daughter.open.log.interval", 10000);
    }

    @Override
    public boolean progress() {
      long now = System.currentTimeMillis();
      if (now - lastLog > this.interval) {
        LOG.info("Opening " + this.egi.getEntityGroupNameAsString());
        this.lastLog = now;
      }
      return true;
    }
  }

  /**
   * Run the transaction.
   * 
   * @param server
   *          Hosting server instance. Can be null when testing (won't try and
   *          update in zk if a null server)
   * @param services
   *          Used to online/offline entityGroups.
   * @throws IOException
   *           If thrown, transaction failed. Call
   *           {@link #rollback(Server, FServerServices)}
   * @return EntityGroups created
   * @throws IOException
   * @see #rollback(Server, FServerServices)
   */
  public PairOfSameType<EntityGroup> execute(final Server server,
      final FServerServices services) throws IOException {
    PairOfSameType<EntityGroup> entityGroups = createDaughters(server, services);
    openDaughters(server, services, entityGroups.getFirst(),
        entityGroups.getSecond());
    transitionZKNode(server, services, entityGroups.getFirst(),
        entityGroups.getSecond());
    return entityGroups;
  }

  /**
   * Prepare the entityGroups.
   * 
   * @param server
   *          Hosting server instance. Can be null when testing (won't try and
   *          update in zk if a null server)
   * @param services
   *          Used to online/offline entityGroups.
   * @throws IOException
   *           If thrown, transaction failed. Call
   *           {@link #rollback(Server, FServerServices)}
   * @return EntityGroups created
   */
  PairOfSameType<EntityGroup> createDaughters(final Server server,
      final FServerServices services) throws IOException {
    LOG.info("Starting split of entityGroup "
        + this.parent.getEntityGroupInfo().toString());
    if ((server != null && server.isStopped())
        || (services != null && services.isStopping())) {
      throw new IOException("Server is stopped or stopping");
    }
    assert !this.parent.lock.writeLock().isHeldByCurrentThread() : "Unsafe to hold write lock while performing RPCs";

    // If true, no cluster to write meta edits to or to update znodes in.
    boolean testing = server == null ? true : server.getConfiguration()
        .getBoolean("wasp.testing.nocluster", false);

    this.journal.add(JournalEntry.STARTED_SPLITTING);
    // Set ephemeral SPLITTING znode up in zk. Mocked servers sometimes don't
    // have zookeeper so don't do zk stuff if server or zookeeper is null
    if (server != null && server.getZooKeeper() != null) {
      try {
        this.znodeVersion = createNodeSplitting(server.getZooKeeper(),
            this.parent.getEntityGroupInfo(), server.getServerName());
      } catch (KeeperException e) {
        throw new IOException("Failed setting SPLITTING znode on "
            + this.parent.getEntityGroupNameAsString(), e);
      }
    }
    this.journal.add(JournalEntry.SET_SPLITTING_IN_ZK);

    this.journal.add(JournalEntry.CREATE_SPLIT_STOREAGE);

    Exception exceptionToThrow = null;
    boolean result = false;
    try {
      result = this.parent.close(false);
    } catch (Exception e) {
      exceptionToThrow = e;
    }
    if (exceptionToThrow == null && !result) {
      // The entityGroup was closed by a concurrent thread. We can't continue
      // with the split, instead we must just abandon the split. If we
      // reopen or split this could cause problems because the entityGroup has
      // probably already been moved to a different server, or is in the
      // process of moving to a different server.
      exceptionToThrow = closedByOtherException;
    }
    if (exceptionToThrow != closedByOtherException) {
      this.journal.add(JournalEntry.CLOSED_PARENT_ENTITYGROUP);
    }
    if (exceptionToThrow != null) {
      if (exceptionToThrow instanceof IOException)
        throw (IOException) exceptionToThrow;
      throw new IOException(exceptionToThrow);
    }

    if (!testing) {
      services.removeFromOnlineEntityGroups(this.parent.getEntityGroupInfo()
          .getEncodedName());
    }
    this.journal.add(JournalEntry.OFFLINED_PARENT);

    // Log to the journal that we are creating entityGroup A, the first daughter
    // entityGroup. We could fail halfway through. If we do, we could have left
    // stuff in fs that needs cleanup -- a storefile or two. Thats why we
    // add entry to journal BEFORE rather than AFTER the change.
    this.journal.add(JournalEntry.STARTED_ENTITYGROUP_A_CREATION);
    EntityGroup a = createDaughterEntityGroup(this.egi_a, this.parent.services);

    // Ditto
    this.journal.add(JournalEntry.STARTED_ENTITYGROUP_B_CREATION);
    EntityGroup b = createDaughterEntityGroup(this.egi_b, this.parent.services);

    // This is the point of no return. Adding subsequent edits to .META. as we
    // do below when we do the daughter opens adding each to .META. can fail in
    // various interesting ways the most interesting of which is a timeout
    // BUT the edits all go through (See HBASE-3872). IF we reach the PONR
    // then subsequent failures need to crash out this entityGroupserver; the
    // server shutdown processing should be able to fix-up the incomplete split.
    // The offlined parent will have the daughters as extra columns. If
    // we leave the daughter entityGroups in place and do not remove them when
    // we
    // crash out, then they will have their references to the parent in place
    // still and the server shutdown fixup of .META. will point to these
    // entityGroups.
    // We should add PONR JournalEntry before offlineParentInMeta,so even if
    // OfflineParentInMeta timeout,this will cause entityGroupserver exit,and
    // then
    // master ServerShutdownHandler will fix daughter & avoid data loss.
    this.journal.add(JournalEntry.PONR);

    // Edit parent in meta. Offlines parent entityGroup and adds splita and
    // splitb.
    if (!testing) {
      FMetaEditor.offlineParentInMeta(server.getConfiguration(),
          this.parent.getEntityGroupInfo(), a.getEntityGroupInfo(),
          b.getEntityGroupInfo());
    }
    return new PairOfSameType<EntityGroup>(a, b);
  }

  /**
   * @param egi
   *          Spec. for daughter entityGroup to open.
   * @return Created daughter EntityGroup.
   * @throws IOException
   */
  EntityGroup createDaughterEntityGroup(final EntityGroupInfo egi,
      final FServerServices rsServices) throws IOException {
    // Package private so unit tests have access.
    EntityGroup entityGroup = EntityGroup.newEntityGroup(this.parent.getConf(),
        egi, this.parent.getTableDesc(), rsServices);
    entityGroup.readRequestsCount.set(this.parent.getReadRequestsCount() / 2);
    entityGroup.writeRequestsCount.set(this.parent.getWriteRequestsCount() / 2);
    return entityGroup;
  }

  /**
   * Perform time consuming opening of the daughter entityGroups.
   * 
   * @param server
   *          Hosting server instance. Can be null when testing (won't try and
   *          update in zk if a null server)
   * @param services
   *          Used to online/offline entityGroups.
   * @param a
   *          first daughter entityGroup
   * @param a
   *          second daughter entityGroup
   * @throws IOException
   *           If thrown, transaction failed. Call
   *           {@link #rollback(Server, FServerServices)}
   */
  void openDaughters(final Server server, final FServerServices services,
      EntityGroup a, EntityGroup b) throws IOException {
    boolean stopped = server != null && server.isStopped();
    boolean stopping = services != null && services.isStopping();
    if (stopped || stopping) {
      LOG.info("Not opening daughters "
          + b.getEntityGroupInfo().getEntityGroupNameAsString() + " and "
          + a.getEntityGroupInfo().getEntityGroupNameAsString()
          + " because stopping=" + stopping + ", stopped=" + stopped);
    } else {
      // Open daughters in parallel.
      DaughterOpener aOpener = new DaughterOpener(server, a);
      DaughterOpener bOpener = new DaughterOpener(server, b);
      aOpener.start();
      bOpener.start();
      try {
        aOpener.join();
        bOpener.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted " + e.getMessage());
      }
      if (aOpener.getException() != null) {
        throw new IOException("Failed " + aOpener.getName(),
            aOpener.getException());
      }
      if (bOpener.getException() != null) {
        throw new IOException("Failed " + bOpener.getName(),
            bOpener.getException());
      }
      if (services != null) {
        // add 2nd daughter first
        services.postOpenDeployTasks(b, true);
        // Should add it to OnlineEntityGroups
        services.addToOnlineEntityGroups(b);
        services.postOpenDeployTasks(a, true);
        services.addToOnlineEntityGroups(a);
      }
    }
  }

  /**
   * Creates a new ephemeral node in the SPLITTING state for the specified
   * entityGroup. Create it ephemeral in case entityGroupserver dies mid-split.
   * 
   * <p>
   * Does not transition nodes from other states. If a node already exists for
   * this entityGroup, a
   * {@link org.apache.zookeeper.KeeperException.NodeExistsException} will be
   * thrown.
   * 
   * @param zkw
   *          zk reference
   * @param entityGroup
   *          entityGroup to be created as offline
   * @param serverName
   *          server event originates from
   * @return Version of znode created.
   * @throws KeeperException
   * @throws IOException
   */
  int createNodeSplitting(final ZooKeeperWatcher zkw,
      final EntityGroupInfo entityGroup, final ServerName serverName)
      throws KeeperException, IOException {
    LOG.debug(zkw.prefix("Creating ephemeral node for "
        + entityGroup.getEncodedName() + " in SPLITTING state"));
    EntityGroupTransaction data = EntityGroupTransaction
        .createEntityGroupTransition(
            EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLITTING,
            entityGroup.getEntityGroupName(), serverName);
    String node = ZKAssign.getNodeName(zkw, entityGroup.getEncodedName());
    if (!ZKUtil.createEphemeralNodeAndWatch(zkw, node, data.toByteArray())) {
      throw new IOException("Failed create of ephemeral " + node);
    }
    // Transition node from SPLITTING to SPLITTING and pick up version so we
    // can be sure this znode is ours; version is needed deleting.
    return transitionNodeSplitting(zkw, entityGroup, serverName, -1);
  }

  /**
   * Finish off split transaction, transition the zknode
   * 
   * @param server
   *          Hosting server instance. Can be null when testing (won't try and
   *          update in zk if a null server)
   * @param services
   *          Used to online/offline entityGroups.
   * @param a
   *          first daughter entityGroup
   * @param a
   *          second daughter entityGroup
   * @throws IOException
   *           If thrown, transaction failed. Call
   *           {@link #rollback(Server, FServerServices)}
   */
  /* package */void transitionZKNode(final Server server,
      final FServerServices services, EntityGroup a, EntityGroup b)
      throws IOException {
    // Tell master about split by updating zk. If we fail, abort.
    if (server != null && server.getZooKeeper() != null) {
      try {
        this.znodeVersion = transitionNodeSplit(server.getZooKeeper(),
            parent.getEntityGroupInfo(), a.getEntityGroupInfo(),
            b.getEntityGroupInfo(), server.getServerName(), this.znodeVersion);

        int spins = 0;
        // Now wait for the master to process the split. We know it's done
        // when the znode is deleted. The reason we keep tickling the znode is
        // that it's possible for the master to miss an event.
        do {
          if (spins % 10 == 0) {
            LOG.debug("Still waiting on the master to process the split for "
                + this.parent.getEntityGroupInfo().getEncodedName());
          }
          Thread.sleep(100);
          // When this returns -1 it means the znode doesn't exist
          this.znodeVersion = tickleNodeSplit(server.getZooKeeper(),
              parent.getEntityGroupInfo(), a.getEntityGroupInfo(),
              b.getEntityGroupInfo(), server.getServerName(), this.znodeVersion);
          spins++;
        } while (this.znodeVersion != -1 && !server.isStopped()
            && !services.isStopping());
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IOException("Failed telling master about split", e);
      }
    }

    // Leaving here, the splitdir with its dross will be in place but since the
    // split was successful, just leave it; it'll be cleaned when parent is
    // deleted and cleaned up.
  }

  /**
   * @param server
   *          Hosting server instance (May be null when testing).
   * @param services
   * @throws IOException
   *           If thrown, rollback failed. Take drastic action.
   * @return True if we successfully rolled back, false if we got to the point
   *         of no return and so now need to abort the server to minimize
   *         damage.
   */
  public boolean rollback(final Server server, final FServerServices services)
      throws IOException {
    boolean result = true;
    ListIterator<JournalEntry> iterator = this.journal
        .listIterator(this.journal.size());
    // Iterate in reverse.
    while (iterator.hasPrevious()) {
      JournalEntry je = iterator.previous();
      switch (je) {

      case STARTED_SPLITTING:
        if (server != null && server.getZooKeeper() != null) {
          cleanZK(server, this.parent.getEntityGroupInfo(), false);
        }
        break;

      case SET_SPLITTING_IN_ZK:
        if (server != null && server.getZooKeeper() != null) {
          cleanZK(server, this.parent.getEntityGroupInfo(), true);
        }
        break;

      case CREATE_SPLIT_STOREAGE:
        this.parent.writestate.writesEnabled = true;
        break;

      case CLOSED_PARENT_ENTITYGROUP:
        try {
          this.parent.initialize();
        } catch (IOException e) {
          LOG.error(
              "Failed rollbacking CLOSED_PARENT_ENTITYGROUP of entityGroup "
                  + this.parent.getEntityGroupNameAsString(), e);
          throw new RuntimeException(e);
        }
        break;

      case STARTED_ENTITYGROUP_A_CREATION:
        break;

      case STARTED_ENTITYGROUP_B_CREATION:
        break;

      case OFFLINED_PARENT:
        if (services != null)
          services.addToOnlineEntityGroups(this.parent);
        break;

      case PONR:
        // We got to the point-of-no-return so we need to just abort. Return
        // immediately. Do not clean up created daughter entityGroups. They need
        // to be in place so we don't delete the parent entityGroup mistakenly.
        // See HBASE-3872.
        return false;

      default:
        throw new RuntimeException("Unhandled journal entry: " + je);
      }
    }
    return result;
  }

  private static void cleanZK(final Server server, final EntityGroupInfo egi,
      boolean abort) {
    try {
      // Only delete if its in expected state; could have been hijacked.
      ZKAssign.deleteNode(server.getZooKeeper(), egi.getEncodedName(),
          EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLITTING);
    } catch (KeeperException.NoNodeException nn) {
      if (abort) {
        server.abort("Failed cleanup of " + egi.getEntityGroupNameAsString(),
            nn);
      }
    } catch (KeeperException e) {
      server.abort("Failed cleanup of " + egi.getEntityGroupNameAsString(), e);
    }
  }

  /**
   * Transitions an existing node for the specified entityGroup which is
   * currently in the SPLITTING state to be in the SPLIT state. Converts the
   * ephemeral SPLITTING znode to an ephemeral SPLIT node. Master cleans up
   * SPLIT znode when it reads it (or if we crash, zk will clean it up).
   * 
   * <p>
   * Does not transition nodes from other states. If for some reason the node
   * could not be transitioned, the method returns -1. If the transition is
   * successful, the version of the node after transition is returned.
   * 
   * <p>
   * This method can fail and return false for three different reasons:
   * <ul>
   * <li>Node for this entityGroup does not exist</li>
   * <li>Node for this entityGroup is not in SPLITTING state</li>
   * <li>After verifying SPLITTING state, update fails because of wrong version
   * (this should never actually happen since an RS only does this transition
   * following a transition to SPLITTING. if two RS are conflicting, one would
   * fail the original transition to SPLITTING and not this transition)</li>
   * </ul>
   * 
   * <p>
   * Does not set any watches.
   * 
   * <p>
   * This method should only be used by a FServer when completing the open of a
   * entityGroup.
   * 
   * @param zkw
   *          zk reference
   * @param parent
   *          entityGroup to be transitioned to opened
   * @param a
   *          Daughter a of split
   * @param b
   *          Daughter b of split
   * @param serverName
   *          server event originates from
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws KeeperException
   *           if unexpected zookeeper exception
   * @throws IOException
   */
  private static int transitionNodeSplit(ZooKeeperWatcher zkw,
      EntityGroupInfo parent, EntityGroupInfo a, EntityGroupInfo b,
      ServerName serverName, final int znodeVersion) throws KeeperException,
      IOException {
    byte[] payload = EntityGroupInfo.toDelimitedByteArray(a, b);
    return ZKAssign.transitionNode(zkw, parent, serverName,
        EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLITTING,
        EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLIT, znodeVersion,
        payload);
  }

  EntityGroupInfo getFirstDaughter() {
    return egi_a;
  }

  EntityGroupInfo getSecondDaughter() {
    return egi_b;
  }

  /**
   * 
   * @param zkw
   *          zk reference
   * @param parent
   *          entityGroup to be transitioned to splitting
   * @param serverName
   *          server event originates from
   * @param version
   *          znode version
   * @return version of node after transition, -1 if unsuccessful transition
   * @throws KeeperException
   * @throws IOException
   */
  int transitionNodeSplitting(final ZooKeeperWatcher zkw,
      final EntityGroupInfo parent, final ServerName serverName,
      final int version) throws KeeperException, IOException {
    return ZKAssign.transitionNode(zkw, parent, serverName,
        EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLITTING,
        EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLITTING, version);
  }

  private static int tickleNodeSplit(ZooKeeperWatcher zkw,
      EntityGroupInfo parent, EntityGroupInfo a, EntityGroupInfo b,
      ServerName serverName, final int znodeVersion) throws KeeperException,
      IOException {
    byte[] payload = EntityGroupInfo.toDelimitedByteArray(a, b);
    return ZKAssign.transitionNode(zkw, parent, serverName,
        EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLIT,
        EventHandler.EventType.FSERVER_ZK_ENTITYGROUP_SPLIT, znodeVersion,
        payload);
  }
}
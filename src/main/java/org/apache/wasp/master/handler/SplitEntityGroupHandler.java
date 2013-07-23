package org.apache.wasp.master.handler;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.MetaException;
import org.apache.wasp.Server;
import org.apache.wasp.ServerName;
import org.apache.wasp.executor.EventHandler;
import org.apache.wasp.master.AssignmentManager;
import org.apache.wasp.meta.FMetaEditor;
import org.apache.wasp.meta.StorageTableNameBuilder;
import org.apache.wasp.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * Handles SPLIT entityGroup event on Master.
 */
public class SplitEntityGroupHandler extends EventHandler {

  private static final Log LOG = LogFactory
      .getLog(SplitEntityGroupHandler.class);
  private final AssignmentManager assignmentManager;
  private final EntityGroupInfo parent;
  private final ServerName sn;
  private final List<EntityGroupInfo> daughters;

  public static boolean TEST_SKIP = false;

  public SplitEntityGroupHandler(Server server,
      AssignmentManager assignmentManager, EntityGroupInfo entityGroupInfo,
      ServerName sn, List<EntityGroupInfo> daughters) {
    super(server, EventType.FSERVER_ZK_ENTITYGROUP_SPLIT);
    this.assignmentManager = assignmentManager;
    this.parent = entityGroupInfo;
    this.sn = sn;
    this.daughters = daughters;
  }

  @Override
  public void process() {
    String encodedEntityGroupName = this.parent.getEncodedName();
    LOG.debug("Handling SPLIT event for " + encodedEntityGroupName
        + "; deleting node");
    // The below is for testing ONLY! We can't do fault injection easily, so
    if (TEST_SKIP) {
      LOG.warn("Skipping split message, TEST_SKIP is set");
      return;
    }

    this.assignmentManager.handleSplitReport(this.sn, this.parent,
        this.daughters.get(0), this.daughters.get(1));
    // Remove entityGroup from ZK
    try {

      boolean successful = false;
      while (!successful) {
        // It's possible that the RS tickles in between the reading of the
        // znode and the deleting, so it's safe to retry.
        successful = ZKAssign.deleteNode(this.server.getZooKeeper(),
            encodedEntityGroupName, EventType.FSERVER_ZK_ENTITYGROUP_SPLIT);
      }
      try {
        FMetaEditor.deleteStorageTable(server.getConfiguration(),
            StorageTableNameBuilder
                .buildTransactionTableName(encodedEntityGroupName));
      } catch (MetaException e) {
        LOG.error("SplitEntityGroupHandler, Delete " + encodedEntityGroupName
            + " 's Transaction error", e);
      }
    } catch (KeeperException e) {
      if (e instanceof KeeperException.NoNodeException) {
        LOG.debug("The znode does not exist.  May be deleted already.", e);
      } else {
        server.abort("Error deleting SPLIT node in ZK for transition ZK node ("
            + parent.getEncodedName() + ")", e);
      }
    }
    LOG.info("Handled SPLIT event; parent="
        + this.parent.getEntityGroupNameAsString() + " daughter a="
        + this.daughters.get(0).getEntityGroupNameAsString() + "daughter b="
        + this.daughters.get(1).getEntityGroupNameAsString());

  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if (server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    String parentEntityGroup = "UnknownEntityGroup";
    if (parent != null) {
      parentEntityGroup = parent.getEntityGroupNameAsString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-"
        + parentEntityGroup;
  }

  public EntityGroupInfo getEntityGroupInfo() {
    return this.parent;
  }
}
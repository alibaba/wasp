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
package org.apache.wasp.master;

import static org.junit.Assert.assertNotSame;

import org.apache.wasp.DeserializationException;
import org.apache.wasp.EntityGroupInfo;
import org.apache.wasp.EntityGroupTransaction;
import org.apache.wasp.ServerName;
import org.apache.wasp.executor.EventHandler.EventType;
import org.apache.wasp.zookeeper.ZKAssign;
import org.apache.wasp.zookeeper.ZKUtil;
import org.apache.wasp.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Package scoped mocking utility.
 */
public class Mocking {

  static void waitForEntityGroupPendingOpenInRIT(AssignmentManager am, String encodedName)
    throws InterruptedException {
    // We used to do a check like this:
    //!Mocking.verifyEntityGroupState(this.watcher, ENTITYGROUPINFO, EventType.M_ZK_ENTITYGROUP_OFFLINE)) {
    // There is a race condition with this: because we may do the transition to
    // FSERVER_ZK_ENTITYGROUP_OPENING before the RIT is internally updated. We need to wait for the
    // RIT to be as we need it to be instead. This cannot happen in a real cluster as we
    // update the RIT before sending the openEntityGroup request.

    boolean wait = true;
    while (wait) {
      EntityGroupState state = am.getEntityGroupStates()
        .getEntityGroupsInTransition().get(encodedName);
      if (state != null && state.isPendingOpen()){
        wait = false;
      } else {
        Thread.sleep(1);
      }
    }

  }

  /**
   * Fakes the regionserver-side zk transitions of a region open.
   * @param w ZooKeeperWatcher to use.
   * @param sn Name of the regionserver doing the 'opening'
   * @param egInfo EntityGroup we're 'opening'.
   * @throws KeeperException
   * @throws DeserializationException
   */
  static void fakeEntityGroupServerEntityGroupOpenInZK(FMaster master,  final ZooKeeperWatcher w,
      final ServerName sn, final EntityGroupInfo egInfo)
    throws KeeperException, DeserializationException, InterruptedException {
    // Wait till the we region is ready to be open in RIT.
    waitForEntityGroupPendingOpenInRIT(master.getAssignmentManager(), egInfo.getEncodedName());

    // Get current versionid else will fail on transition from OFFLINE to OPENING below
    int versionid = ZKAssign.getVersion(w, egInfo);
    assertNotSame(-1, versionid);
    // This uglyness below is what the openregionhandler on FSERVER side does.  I
    // looked at exposing the method over in openregionhandler but its just a
    // one liner and its deep over in another package so just repeat it below.
    versionid = ZKAssign.transitionNode(w, egInfo, sn,
      EventType.M_ZK_ENTITYGROUP_OFFLINE, EventType.FSERVER_ZK_ENTITYGROUP_OPENING, versionid);
    assertNotSame(-1, versionid);
    // Move znode from OPENING to OPENED as FSERVER does on successful open.
    versionid = ZKAssign.transitionNodeOpened(w, egInfo, sn, versionid);
    assertNotSame(-1, versionid);
    // We should be done now.  The master open handler will notice the
    // transition and remove this regions znode.
  }

  /**
   * Verifies that the specified region is in the specified state in ZooKeeper.
   * <p>
   * Returns true if region is in transition and in the specified state in
   * ZooKeeper.  Returns false if the region does not exist in ZK or is in
   * a different state.
   * <p>
   * Method synchronizes() with ZK so will yield an up-to-date result but is
   * a slow read.
   * @param zkw
   * @param region
   * @param expectedState
   * @return true if region exists and is in expected state
   * @throws DeserializationException
   */
  static boolean verifyEntityGroupState(ZooKeeperWatcher zkw, EntityGroupInfo region, EventType expectedState)
  throws KeeperException, DeserializationException {
    String encoded = region.getEncodedName();

    String node = ZKAssign.getNodeName(zkw, encoded);
    zkw.sync(node);

    // Read existing data of the node
    byte [] existingBytes = null;
    try {
      existingBytes = ZKUtil.getDataAndWatch(zkw, node);
    } catch (KeeperException.NoNodeException nne) {
      return false;
    } catch (KeeperException e) {
      throw e;
    }
    if (existingBytes == null) return false;
    EntityGroupTransaction rt = EntityGroupTransaction.parseFrom(existingBytes);
    return rt.getEventType().equals(expectedState);
  }
}

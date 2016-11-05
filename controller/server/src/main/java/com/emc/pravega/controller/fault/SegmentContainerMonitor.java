/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.fault;

import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.util.ZKUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.utils.ZKPaths;

/**
 * Class used to monitor the Data nodes for failures and ensure the segment containers owned by them is assigned
 * to the other Data nodes.
 */
@Slf4j
public class SegmentContainerMonitor implements AutoCloseable {

    //The leader which monitors the data cluster and ensures all containers are mapped to available hosts.
    private final SegmentMonitorLeader segmentMonitorLeader;

    //The leader selector which competes with peer controller nodes. We are using this to ensure there is always only
    //one writer for the host to container map. This will prevent race conditions and also simplifies coordination
    //during load balancing.
    private final LeaderSelector leaderSelector;

    /**
     * Monitor to manage data node addition and removal in the cluster.
     *
     * @param hostStore     The store to read and write the host container mapping data
     * @param client        The curator client for coordination
     * @param clusterName   The unique name for this cluster
     */
    public SegmentContainerMonitor(HostControllerStore hostStore, CuratorFramework client, String clusterName) {

        //The ZK path which is monitored for leader selection
        final String leaderZKPath = ZKPaths.makePath("cluster", clusterName, "data", "faulthandlerleader");

        //TODO: Do this lazily to avoid controller startup failures.
        ZKUtils.createPathIfNotExists(client, leaderZKPath);

        segmentMonitorLeader = new SegmentMonitorLeader(clusterName, hostStore);
        leaderSelector = new LeaderSelector(client, leaderZKPath, segmentMonitorLeader);

        //Listen for any zookeeper connectivity error and relinquish leadership.
        client.getConnectionStateListenable().addListener(
                (curatorClient, newState) -> {
                    if (!newState.isConnected()) {
                        log.warn("Connection to zookeeper lost, attempting to interrrupt the leader thread");
                        leaderSelector.interruptLeadership();
                    }
                }
        );
    }

    /**
     * Start the leader selection process.
     */
    public void start() {
        //Ensure this process always competes for leadership.
        leaderSelector.autoRequeue();
        leaderSelector.start();
    }

    /**
     * Relinquish leadership and close.
     */
    @Override
    public void close() {
        leaderSelector.interruptLeadership();
        leaderSelector.close();
    }
}

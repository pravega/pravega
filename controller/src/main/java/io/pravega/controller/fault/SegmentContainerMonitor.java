/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.fault;

import io.pravega.controller.store.host.HostControllerStore;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.utils.ZKPaths;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class used to monitor the pravega host cluster for failures and ensure the segment containers owned by them are
 * assigned to the other pravega hosts.
 */
@Slf4j
public class SegmentContainerMonitor extends AbstractIdleService {

    //The leader which monitors the data cluster and ensures all containers are mapped to available hosts.
    private final SegmentMonitorLeader segmentMonitorLeader;

    //The leader selector which competes with peer controller nodes. We are using this to ensure there is always only
    //one writer for the host to container map. This will prevent race conditions and also simplifies coordination
    //during load balancing.
    private final LeaderSelector leaderSelector;

    //The ZK path which is monitored for leader selection.
    private final String leaderZKPath;

    private final AtomicBoolean zkConnected = new AtomicBoolean(false);

    /**
     * Monitor to manage pravega host addition and removal in the cluster.
     *
     * @param hostStore             The store to read and write the host container mapping data.
     * @param client                The curator client for coordination.
     * @param balancer              The host to segment container balancer implementation.
     * @param minRebalanceInterval  The minimum interval between any two rebalance operations in seconds.
     *                              0 indicates there can be no waits between retries.
     */
    public SegmentContainerMonitor(HostControllerStore hostStore, CuratorFramework client, ContainerBalancer balancer,
            int minRebalanceInterval) {
        Preconditions.checkNotNull(hostStore, "hostStore");
        Preconditions.checkNotNull(client, "client");
        Preconditions.checkNotNull(balancer, "balancer");

        leaderZKPath = ZKPaths.makePath("cluster", "faulthandlerleader");

        segmentMonitorLeader = new SegmentMonitorLeader(hostStore, balancer, minRebalanceInterval);
        leaderSelector = new LeaderSelector(client, leaderZKPath, segmentMonitorLeader);

        this.zkConnected.set(client.getZookeeperClient().isConnected());
        //Listen for any zookeeper connection state changes
        client.getConnectionStateListenable().addListener(
                (curatorClient, newState) -> {
                    this.zkConnected.set(newState.isConnected());
                    switch (newState) {
                        case LOST:
                            log.warn("Connection to zookeeper lost, attempting to interrrupt the leader thread");
                            leaderSelector.interruptLeadership();
                            break;
                        case SUSPENDED:
                            if (leaderSelector.hasLeadership()) {
                                log.info("Zookeeper session suspended, pausing the segment monitor");
                                segmentMonitorLeader.suspend();
                            }
                            break;
                        case RECONNECTED:
                            if (leaderSelector.hasLeadership()) {
                                log.info("Zookeeper session reconnected, resume the segment monitor");
                                segmentMonitorLeader.resume();
                            }
                            break;
                        //$CASES-OMITTED$
                        default:
                            log.debug("Connection state to zookeeper updated: " + newState.toString());
                    }
                }
        );
    }

    public boolean isZKConnected() {
        return zkConnected.get();
    }

    /**
     * Start the leader selection process.
     */
    @Override
    protected void startUp() {
        //Ensure this process always competes for leadership.
        leaderSelector.autoRequeue();
        leaderSelector.start();
    }

    /**
     * Relinquish leadership and close.
     */
    @Override
    protected void shutDown() {
        leaderSelector.interruptLeadership();
        leaderSelector.close();
    }
}

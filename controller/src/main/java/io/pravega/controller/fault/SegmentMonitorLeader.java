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

import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterException;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.controller.metrics.HostContainerMetrics;
import io.pravega.controller.store.host.HostControllerStore;
import com.google.common.base.Preconditions;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is the monitor leader which watches the pravega data node cluster and handles host level failures.
 * This ensures that all segment containers are owned by hosts which are alive. Containers on existing hosts are
 * also moved if necessary for load balancing.
 */
@Slf4j
class SegmentMonitorLeader implements LeaderSelectorListener {

    //The store for reading and writing the host to container mapping.
    private final HostControllerStore hostStore;

    //The host to containers balancer.
    private final ContainerBalancer segBalancer;

    //The pravega cluster which this host controller manages.
    private Cluster pravegaServiceCluster = null;

    //The minimum interval between any two rebalance operations. The minimum duration is not guaranteed when leadership
    //moves across controllers. Since this is uncommon and there are no significant side-effects to it, we don't
    //handle this scenario.
    private Duration minRebalanceInterval;

    //Semaphore to notify the leader thread to trigger a rebalance.
    private final Semaphore hostsChange = new Semaphore(0);

    //Semaphore to keep the current thread in suspended state.
    private final Semaphore suspendMonitor = new Semaphore(0);

    //Flag to check if monitor is suspended or not.
    private final AtomicBoolean suspended = new AtomicBoolean(false);

    // Container and host lifecycle metrics.
    private final HostContainerMetrics hostContainerMetrics = new HostContainerMetrics();

    /**
     * The leader instance which monitors the data node cluster.
     *
     * @param hostStore             The store for reading and writing the host to container mapping.
     * @param balancer              The host to segment container balancer implementation.
     * @param minRebalanceInterval  The minimum interval between any two rebalance operations in seconds.
     *                              0 indicates there can be no waits between retries.
     */
    public SegmentMonitorLeader(HostControllerStore hostStore, ContainerBalancer balancer, int minRebalanceInterval) {
        Preconditions.checkNotNull(hostStore, "hostStore");
        Preconditions.checkNotNull(balancer, "balancer");
        Preconditions.checkArgument(minRebalanceInterval >= 0, "minRebalanceInterval should not be negative");

        this.hostStore = hostStore;
        this.segBalancer = balancer;
        this.minRebalanceInterval = Duration.ofSeconds(minRebalanceInterval);
    }

    /**
     * Suspend the leader thread.
     */
    public void suspend() {
        suspended.set(true);
    }

    /**
     * Resume the suspended leader thread.
     */
    public void resume() {
        if (suspended.compareAndSet(true, false)) {
            suspendMonitor.release();
        }
    }

    /**
     * This function is called when the current instance is made the leader. The leadership is relinquished when this
     * function exits.
     *
     * @param client        The curator client.
     * @throws Exception    On any error. This would result in leadership being relinquished.
     */
    @Override
    @Synchronized
    public void takeLeadership(CuratorFramework client) throws Exception {
        log.info("Obtained leadership to monitor the Host to Segment Container Mapping");

        //Attempt a rebalance whenever leadership is obtained to ensure no host events are missed.
        hostsChange.release();

        //Start cluster monitor.
        pravegaServiceCluster = new ClusterZKImpl(client, ClusterType.HOST);

        //Add listener to track host changes on the monitored pravega cluster.
        pravegaServiceCluster.addListener((type, host) -> {
            switch (type) {
                case HOST_ADDED:
                case HOST_REMOVED:
                    //We don't keep track of the hosts and we always query for the entire set from the cluster
                    //when changes occur. This is to avoid any inconsistencies if we miss any notifications.
                    log.info("Received event: {} for host: {}. Wake up leader for rebalancing", type, host);
                    hostsChange.release();
                    break;
                case ERROR:
                    //This event should be due to ZK connection errors and would have been received by the monitor too,
                    //hence not handling it explicitly here.
                    log.info("Received error event when monitoring the pravega host cluster, ignoring...");
                    break;
            }
        });

        //Keep looping here as long as possible to stay as the leader and exclusively monitor the pravega cluster.
        while (true) {
            try {
                if (suspended.get()) {
                    log.info("Monitor is suspended, waiting for notification to resume");
                    suspendMonitor.acquire();
                    log.info("Resuming monitor");
                }

                hostsChange.acquire();
                log.info("Received rebalance event");

                // Wait here until rebalance can be performed.
                waitForRebalance();

                // Clear all events that has been received until this point since this will be included in the current
                // rebalance operation.
                hostsChange.drainPermits();
                triggerRebalance();
            } catch (InterruptedException e) {
                log.warn("Leadership interrupted, releasing monitor thread");

                //Stop watching the pravega cluster.
                pravegaServiceCluster.close();
                throw e;
            } catch (Exception e) {
                //We will not release leadership if in suspended mode.
                if (!suspended.get()) {
                    log.warn("Failed to perform rebalancing, relinquishing leadership");

                    //Stop watching the pravega cluster.
                    pravegaServiceCluster.close();
                    throw e;
                }
            }
        }
    }

    /**
     * Blocks until the rebalance interval. This wait serves multiple purposes:
     * -- Ensure rebalance does not happen in quick succession since its a costly cluster level operation.
     * -- Clubs multiple host events into one to reduce rebalance operations. For example:
     *      Fresh cluster start, cluster/multi-host/host restarts, etc.
     */
    private void waitForRebalance() throws InterruptedException {
        log.info("Waiting for {} seconds before attempting to rebalance", minRebalanceInterval.getSeconds());
        Thread.sleep(minRebalanceInterval.toMillis());
    }

    private void triggerRebalance() throws IOException {
        //Read the current mapping from the host store and write back the update after rebalancing.
        try {
            Map<Host, Set<Integer>> newMapping = segBalancer.rebalance(hostStore.getHostContainersMap(),
                    pravegaServiceCluster.getClusterMembers());
            Map<Host, Set<Integer>> oldMapping = hostStore.getHostContainersMap();
            hostStore.updateHostContainersMap(newMapping);
            hostContainerMetrics.updateHostContainerMetrics(oldMapping, newMapping);
        } catch (ClusterException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        //Nothing to do here. We are already monitoring the state changes for shutdown.
        log.info("Zookeeper connection state changed to: " + newState.toString());
    }
}

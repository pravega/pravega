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

import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.zkImpl.ClusterZKImpl;
import com.emc.pravega.controller.store.host.HostControllerStore;
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
 * also moved if neccessary for load balancing.
 */
@Slf4j
class SegmentMonitorLeader implements LeaderSelectorListener {

    //The store for reading and writing the host to container mapping.
    private final HostControllerStore hostStore;

    //The name of the cluster which has to be monitored.
    private final String clusterName;

    //The host to containers balancer.
    private final ContainerBalancer segBalancer;

    //The pravega cluster which this host controller manages.
    private Cluster pravegaServiceCluster = null;

    //The timer to ensure we maintain a minimum interval between expensive rebalance operations.
    private TimeoutTimer timeoutTimer = null;

    //The minimum interval between any two rebalance operations. The minimum duration is not guaranteed when leadership
    //moves across controllers. Since this is uncommon and there are no significant side-effects to it, we don't
    //handle this scenario.
    private Duration minRebalanceInterval;

    //Semaphore to notify the leader thread to trigger a rebalance.
    private final Semaphore hostsChange = new Semaphore(0);

    //Semaphore to keep the current thread in suspended state.
    private final Semaphore suspendMonitor = new Semaphore(0);

    //Flag to check if monitor is suspended or not.
    private AtomicBoolean suspended = new AtomicBoolean(false);

    /**
     * The leader instance which monitors the data node cluster.
     *
     * @param clusterName           The unique name for this cluster.
     * @param hostStore             The store for reading and writing the host to container mapping.
     * @param balancer              The host to segment container balancer implementation.
     * @param minRebalanceInterval  The minimum interval between any two rebalance operations in seconds.
     *                              0 indicates there can be no waits between retries.
     */
    public SegmentMonitorLeader(String clusterName, HostControllerStore hostStore, ContainerBalancer balancer,
            int minRebalanceInterval) {
        Preconditions.checkNotNull(clusterName, "clusterName");
        Preconditions.checkNotNull(hostStore, "hostStore");
        Preconditions.checkNotNull(balancer, "balancer");
        Preconditions.checkArgument(minRebalanceInterval >= 0, "minRebalanceInterval should not be negative");

        this.clusterName = clusterName;
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
        pravegaServiceCluster = new ClusterZKImpl(client, clusterName);

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
                log.debug("Received rebalance event");

                //Wait here until the rebalance timer is zero so that we honour the minimum rebalance interval.
                if (timeoutTimer != null && timeoutTimer.getRemaining().getSeconds() > 0) {
                    log.info("Waiting for {} seconds before attempting to rebalance",
                            timeoutTimer.getRemaining().getSeconds());
                    Thread.sleep(timeoutTimer.getRemaining().getSeconds() * 1000);
                }

                //Clear all events that has been received until this point.
                hostsChange.drainPermits();
                triggerRebalance();
            } catch (InterruptedException e) {
                log.error("Leadership interrupted, releasing monitor thread");

                //Stop watching the pravega cluster.
                pravegaServiceCluster.close();
                throw e;
            } catch (Exception e) {
                //We will not release leadership if in suspended mode.
                if (!suspended.get()) {
                    log.error("Failed to perform rebalancing, relinquishing leadership. error: " + e.getMessage());

                    //Stop watching the pravega cluster.
                    pravegaServiceCluster.close();
                    throw e;
                }
            }
        }
    }

    private void triggerRebalance() throws IOException {
        //Read the current mapping from the host store and write back the update after rebalancing.
        try {
            Map<Host, Set<Integer>> newMapping = segBalancer.rebalance(hostStore.getHostContainersMap(),
                    pravegaServiceCluster.getClusterMembers());
            hostStore.updateHostContainersMap(newMapping);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            //Reset the rebalance timer.
            timeoutTimer = new TimeoutTimer(minRebalanceInterval);
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        //Nothing to do here. We are already monitoring the state changes for shutdown.
        log.info("Zookeeper connection state changed to: " + newState.toString());
    }
}

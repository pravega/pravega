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
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class SegmentMonitorLeader implements LeaderSelectorListener {

    // The containers to host balancer
    private final ContainerBalancer<Host, Set<Integer>> segBalancer;

    // The store to retrieve and set the host to containers mapping
    private final SegContainerHostMapping<Host, Set<Integer>> segMapStore;

    // The name of the cluster to be monitored
    private final String clusterName;

    // The pravega cluster which this host controller manages
    private Cluster pravegaServiceCluster = null;

    // The timer to ensure we maintainer a minimum interval between expensive rebalance operations
    private TimeoutTimer timeoutTimer = null;

    // The minimum interval between any two rebalance operations
    private final Duration minRebalanceInterval = Duration.ofSeconds(120);

    // Boolean to track if any rebalance operations are pending
    private AtomicBoolean hostsChanged = new AtomicBoolean(false);

    public SegmentMonitorLeader(CuratorFramework client, String clusterName) {
        this.clusterName = clusterName;
        segBalancer = new UniformContainerBalancer();
        segMapStore = new SegContainerHostMappingZK(client, clusterName);
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {

        // Attempt a rebalance whenever leadership is obtained to ensure no host events are missed
        hostsChanged.set(true);

        // Start cluster monitor
        pravegaServiceCluster = new ClusterZKImpl(client, clusterName);

        // Add listener to track host changes on the monitored pravega cluster
        pravegaServiceCluster.addListener((type, host) -> {
            log.info("Received event: {} for host: {}", type, host);
            synchronized (this) {
                hostsChanged.set(true);
                notify();
            }
        });

        try {
            // Keep looping here as long as possible to stay as the leader and exclusively monitor the pravega cluster
            while (true) {
                synchronized (this) {
                    // loop and check for changed flag to avoid spurious wakeups in wait()
                    while (!hostsChanged.get()) {
                        wait();
                    }
                }

                // wait here so we don't execute rebalancer often
                if (timeoutTimer != null && timeoutTimer.getRemaining().getSeconds() > 0) {
                    Thread.sleep(timeoutTimer.getRemaining().getSeconds() * 1000);
                }
                triggerRebalance();
            }
        } catch (Exception e) {
            // On any errors (exceptions) we relinquish leadership and start afresh
            log.warn("Failed to rebalance, relinquishing leadership. error: " + e.getMessage());
            throw e;
        } finally {
            // stop watching the pravega cluster
            pravegaServiceCluster.close();
        }
    }

    private void triggerRebalance() throws Exception {

        // clear the cluster modified flag
        if (hostsChanged.compareAndSet(true, false)) {
            Optional<Map<Host, Set<Integer>>> newMapping = segBalancer.rebalance(segMapStore.getSegmentContainerHostMapping(),
                    pravegaServiceCluster.getClusterMembers());
            if (newMapping.isPresent()) {
                segMapStore.updateSegmentContainerHostMapping(newMapping.get());
                // reset the timer
                timeoutTimer = new TimeoutTimer(minRebalanceInterval);
            }
        }
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        // Nothing to do here. We are already monitoring the state changes for shutdown
    }
}

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

package io.pravega.controller.server.bucket;

import com.google.common.base.Preconditions;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.common.util.ReusableLatch;
import io.pravega.controller.store.stream.BucketStore;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

/**
 * BucketManagerLeader is responsible for distributing the buckets whenever any instance of controller is up/down.
 */
@Slf4j
public class BucketManagerLeader implements LeaderSelectorListener {

    private final BucketStore bucketStore;
    // ReusableLatch to notify the leader thread to trigger a redistribution.
    private final ReusableLatch controllerChange = new ReusableLatch();

    // ReusableLatch to keep the current thread in suspended state.
    private final ReusableLatch suspendMonitor = new ReusableLatch();

    // Flag to check if monitor is suspended or not.
    private final AtomicBoolean suspended = new AtomicBoolean(false);
    // The minimum interval between any two redistribution operations. The minimum duration is not guaranteed when leadership
    // moves across controllers. Since this is uncommon and there are no significant side-effects to it, we don't
    // handle this scenario.
    private final Duration minBucketRedistributionIntervalInSeconds;

    // The controller to bucket distributor.
    @Getter(AccessLevel.PACKAGE)
    private final BucketDistributor bucketDistributor;
    // Service type
    private final BucketStore.ServiceType serviceType;
    private final ScheduledExecutorService executorService;

    public BucketManagerLeader(BucketStore bucketStore, int minBucketRedistributionIntervalInSeconds,
                               BucketDistributor bucketDistributor, BucketStore.ServiceType serviceType,
                               ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(bucketStore, "bucketStore");
        Preconditions.checkArgument(minBucketRedistributionIntervalInSeconds >= 0,
                "minBucketRedistributionInterval should not be negative");

        this.bucketStore = bucketStore;
        this.minBucketRedistributionIntervalInSeconds = Duration.ofSeconds(minBucketRedistributionIntervalInSeconds);
        this.bucketDistributor = bucketDistributor;
        this.serviceType = serviceType;
        this.executorService = executorService;
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

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        log.info("{}: Obtained leadership to monitor the controller to buckets mapping.", serviceType);

        // Attempt a distribution whenever leadership is obtained to ensure no Controllers events are missed.
        controllerChange.release();

        // Start cluster monitor.
        Cluster pravegaServiceCluster = new ClusterZKImpl(client, ClusterType.CONTROLLER);

        // Add listener to track controller changes on the monitored pravega cluster.
        pravegaServiceCluster.addListener((type, controller) -> {
            switch (type) {
                case HOST_ADDED:
                case HOST_REMOVED:
                    // We don't keep track of the controllers and we always query for the entire set from the cluster
                    // when changes occur. This is to avoid any inconsistencies if we miss any notifications.
                    log.info("{}: Received event: {} for host: {}. Wake up leader for distribution", serviceType, type, controller);
                    controllerChange.release();
                    break;
                case ERROR:
                    // This event should be due to ZK connection errors and would have been received by the monitor too,
                    // hence not handling it explicitly here.
                    log.error("{}: Received error event when monitoring the pravega host cluster, ignoring...", serviceType);
                    break;
            }
        });

        // Keep looping here as long as possible to stay as the leader and exclusively monitor the pravega cluster.
        while (true) {
            try {
                if (suspended.get()) {
                    log.info("{}: Monitor is suspended, waiting for notification to resume.", serviceType);
                    suspendMonitor.await();
                    log.info("{}: Resuming monitor.", serviceType);
                }

                controllerChange.await();
                log.info("{}: Received distribute buckets.", serviceType);

                // Wait here until distribution can be performed.
                waitForReDistribute();

                // Clear all events that has been received until this point since this will be included in the current
                // distribution operation.
                controllerChange.reset();
                triggerDistribution(pravegaServiceCluster);
            } catch (InterruptedException e) {
                log.warn("{}: Leadership interrupted, releasing monitor thread.", serviceType);
                Thread.currentThread().interrupt();
                // Stop watching the pravega cluster.
                pravegaServiceCluster.close();

                throw e;
            } catch (Exception e) {
                // We will not release leadership if in suspended mode.
                if (!suspended.get()) {
                    log.warn("{}: Failed to perform distribution, relinquishing leadership.", serviceType);
                    // Stop watching the pravega cluster.
                    pravegaServiceCluster.close();
                    throw e;
                }
            }
        }

    }

    /**
     * Blocks until the redistribute interval. This wait serves multiple purposes:
     * -- Ensure redistribution does not happen in quick succession since its a costly cluster level operation.
     * -- Clubs multiple host events into one to reduce redistribution operations. For example:
     *      Fresh cluster start, cluster/multi-host/host restarts, etc.
     */
    private void waitForReDistribute() throws InterruptedException {
        log.info("{}: Waiting for {} seconds before attempting to distribute.", serviceType,
                minBucketRedistributionIntervalInSeconds.getSeconds());
        Thread.sleep(minBucketRedistributionIntervalInSeconds.toMillis());
    }

    /**
     * This method will distribute the bucket among available controller instances and update the bucket to controller
     * mapping on the znode path. Using this znode path all controller instances can fetch the controller to bucket
     * mapping.
     * @param pravegaServiceCluster Cluster having the controller's information.
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void triggerDistribution(final Cluster pravegaServiceCluster) throws ExecutionException, InterruptedException {
        //Get current controller instances.
        Set<String> currentControllers = pravegaServiceCluster.getClusterMembers().stream().map(Host::getHostId).collect(Collectors.toSet());

        //Read the current mapping from the bucket store and write back the update after distribution.
        bucketStore.getBucketControllerMap(serviceType)
                   .thenApply(currentControllerMapping -> bucketDistributor.distribute(currentControllerMapping,
                           currentControllers, bucketStore.getBucketCount(serviceType)))
                   .thenCompose(newMapping -> bucketStore.updateBucketControllerMap(newMapping, serviceType)).get();
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        //Nothing to do here. We are already monitoring the state changes for shutdown.
        log.info("{}: Zookeeper connection state changed to: " + newState.toString(), serviceType);
    }
}

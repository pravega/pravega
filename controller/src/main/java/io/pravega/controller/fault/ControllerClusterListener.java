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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterException;
import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.util.RetryHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Controller cluster listener service. This service when started, starts listening to
 * the controller cluster notifications. Whenever a controller instance leaves the
 * cluster, it does the following things.
 * 1. Try to complete the orphaned tasks running on the failed controller instance,
 * 2. Try to notify the commit and abort reader group about the loss of readers from failed controller instance, and
 * 3. Try to sweep transactions being tracked by the failed controller instance.
 *
 */
@Slf4j
public class ControllerClusterListener extends AbstractIdleService {

    private final String objectId;
    private final Host host;
    private final Cluster cluster;
    private final ScheduledExecutorService executor;
    private final List<FailoverSweeper> sweepers;

    public ControllerClusterListener(final Host host, final Cluster cluster,
                                     final ScheduledExecutorService executor, final List<FailoverSweeper> sweepers) {
        Preconditions.checkNotNull(host, "host");
        Preconditions.checkNotNull(cluster, "cluster");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkArgument(sweepers.stream().noneMatch(Objects::isNull));

        this.objectId = "ControllerClusterListener";
        this.host = host;
        this.cluster = cluster;
        this.executor = executor;
        this.sweepers = Lists.newArrayList(sweepers);
    }

    /**
     * Get the zookeeper health status.
     *
     * @return true if zookeeper is connected.
     */
    public boolean isMetadataServiceConnected() {
        return cluster.isHealthy();
    }

    /**
     * Get the sweepers status.
     *
     * @return true if all sweepers are ready.
     */
    public boolean areAllSweepersReady() {
        return sweepers.stream().allMatch(s -> s.isReady());
    }

    /**
     * Check service is ready.
     *
     * @return true if zookeeper is connected and all sweepers are ready.
     */
    public boolean isReady() {
        return isMetadataServiceConnected() && areAllSweepersReady();
    }

    @Override
    protected void startUp() throws InterruptedException {
        long traceId = LoggerHelpers.traceEnter(log, objectId, "startUp");
        try {
            log.info("Starting Controller cluster listener, registering host {} with controller cluster", host);
            cluster.registerHost(host);

            // It is important to first register the listener and then perform sweeps so that no notifications of HostRemoved
            // are lost.
            // While processing a notification we will first check if the handler is ready or not, if its not, then we can be
            // sure that handler.sweep has not happened yet either. And handler.sweep will take care of this host lost.
            // If sweep is happening and since listener is registered, any new notification can be concurrently handled
            // with the sweep.

            // Register cluster listener.
            log.info("Adding controller cluster listener");
            cluster.addListener((type, host) -> {
                switch (type) {
                    case HOST_ADDED:
                        // We need to do nothing when a new controller instance joins the cluster.
                        log.info("Received HOST_ADDED cluster event: {} for host: {}", type, host);
                        break;
                    case HOST_REMOVED:
                        log.info("Received HOST_REMOVED cluster event: {} for host: {}", type, host);
                        handleHostRemoved(host);
                        break;
                    case ERROR:
                        // This event should be due to ZK connection errors. If it is session lost error then
                        // ControllerServiceMain would handle it. Otherwise it is a fleeting error that can go
                        // away with retries, and hence we ignore it.
                        log.info("Received error event from controller host {}", host);
                        break;
                }
            }, executor);

            // processes: set of controller process running at unique IP:PORT
            Supplier<Set<String>> processes = () -> {
                try {
                    return cluster.getClusterMembers()
                            .stream()
                            .map(Host::getHostId)
                            .collect(Collectors.toSet());
                } catch (ClusterException e) {
                    log.error("Error fetching cluster members {}", e.getMessage());
                    throw new CompletionException(e);
                }
            };

            // This method should not block and process all sweepers asynchronously.
            // Also, it should retry any errors during processing as this is the only opportunity to process all failed hosts
            // that are no longer part of the cluster as we wont get any notification for them.
            sweepAll(processes);

            log.info("Controller cluster listener startUp complete");
        } finally {
            LoggerHelpers.traceLeave(log, objectId, "startUp", traceId);
        }
    }

    private CompletableFuture<Void> handleHostRemoved(Host host) {
        return Futures.allOf(sweepers.stream().map(sweeper -> {
            if (sweeper.isReady()) {
                // Note: if we find sweeper to be ready, it is possible that this processes can be swept by both
                // sweepFailedProcesses and handleFailedProcess. A sweep is safe and idempotent operation.
                return RetryHelper.withIndefiniteRetriesAsync(() -> sweeper.handleFailedProcess(host.getHostId()),
                        e -> log.warn(e.getMessage()), executor);
            } else {
                return CompletableFuture.completedFuture((Void) null);
            }
        }).collect(Collectors.toList()));
    }

    private CompletableFuture<Void> sweepAll(Supplier<Set<String>> processes) {
        return Futures.allOf(sweepers.stream().map(sweeper -> RetryHelper.withIndefiniteRetriesAsync(() -> {
            if (!sweeper.isReady()) {
                throw new RuntimeException(String.format("sweeper %s not ready, retrying with exponential backoff.", sweeper.getClass()));
            }
            return sweeper.sweepFailedProcesses(processes);
        }, e -> log.warn(e.getMessage()), executor)).collect(Collectors.toList()));
    }

    @Override
    protected void shutDown() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, objectId, "shutDown");
        try {
            log.info("De-registering host {} from controller cluster", host);
            cluster.deregisterHost(host);
            log.info("Controller cluster listener shutDown complete");
        } finally {
            LoggerHelpers.traceLeave(log, objectId, "shutDown", traceId);
        }
    }
}

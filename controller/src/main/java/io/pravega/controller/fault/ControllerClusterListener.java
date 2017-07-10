/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.fault;

import io.pravega.common.LoggerHelpers;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.util.RetryHelper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final Map<FailoverSweeper, PendingSweepQueue> failoverSweepers;

    public ControllerClusterListener(final Host host, final Cluster cluster,
                                     final ScheduledExecutorService executor,
                                     final FailoverSweeper... sweepers) {
        Preconditions.checkNotNull(host, "host");
        Preconditions.checkNotNull(cluster, "cluster");
        Preconditions.checkNotNull(executor, "executor");

        this.objectId = "ControllerClusterListener";
        this.host = host;
        this.cluster = cluster;
        this.executor = executor;
        failoverSweepers = Arrays.stream(sweepers).collect(Collectors.toMap(in -> in, in -> new PendingSweepQueue()));
    }

    @Override
    protected void startUp() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, objectId, "startUp");
        try {
            log.info("Registering host {} with controller cluster", host);
            cluster.registerHost(host);

            // Register cluster listener.
            log.info("Adding controller cluster listener");
            cluster.addListener((type, host) -> {
                switch (type) {
                    case HOST_ADDED:
                        // We need to do nothing when a new controller instance joins the cluster.
                        log.info("Received controller cluster event: {} for host: {}", type, host);
                        break;
                    case HOST_REMOVED:
                        log.info("Received controller cluster event: {} for host: {}", type, host);
                        handleFailedProcess(host);
                        break;
                    case ERROR:
                        // This event should be due to ZK connection errors. If it is session lost error then
                        // ControllerServiceMain would handle it. Otherwise it is a fleeting error that can go
                        // away with retries, and hence we ignore it.
                        log.info("Received error event when monitoring the controller host cluster, ignoring...");
                        break;
                }
            }, executor);

            log.info("Sweeping orphaned tasks at startup");
            Supplier<Set<String>> processes = () -> {
                try {
                    return cluster.getClusterMembers()
                            .stream()
                            .map(Host::getHostId)
                            .collect(Collectors.toSet());
                } catch (Exception e) {
                    log.error("error fetching cluster members {}", e);
                    throw new CompletionException(e);
                }
            };

            bootstrapSweep(processes);

            log.info("Controller cluster listener startUp complete");
        } finally {
            LoggerHelpers.traceLeave(log, objectId, "startUp", traceId);
        }
    }

    private void handleFailedProcess(Host host) {
        FutureHelpers.allOf(failoverSweepers.entrySet().stream().map(entry -> {
            Supplier<CompletableFuture<Void>> futureSupplier = () -> RetryHelper.withIndefiniteRetriesAsync(() -> {
                log.info("handling host removed", host.getHostId());
                return entry.getKey().handleFailedProcess(host.getHostId());
            }, e -> log.warn(e.getMessage()), executor);

            // if queue is not sealed, then add to queue. Else process asynchronously
            if (!entry.getValue().enqueue(futureSupplier)) {
                return futureSupplier.get();
            } else {
                CompletableFuture<Void> v = CompletableFuture.completedFuture(null);
                return v;
            }
        }).collect(Collectors.toList()));
    }

    private void bootstrapSweep(Supplier<Set<String>> processes) {
        FutureHelpers.allOf(failoverSweepers.entrySet().stream().map(entry -> {
            // Await initialization of eventProcesorsOpt
            return RetryHelper.withIndefiniteRetriesAsync(() -> {
                try {
                    entry.getKey().checkReady();
                } catch (SweeperNotReadyException e) {
                    throw new RuntimeException(e);
                }
                // Sweep orphaned tasks or readers at startup.
                return entry.getKey().sweepFailedProcesses(processes);
            }, e -> log.warn(e.getMessage()), executor).thenCompose((Void v) ->
                    // drain pending queue and process
                    FutureHelpers.allOf(entry.getValue().drainAndSeal()
                            .stream().map(Supplier::get).collect(Collectors.toList())));
        }).collect(Collectors.toList()));
    }

    @Override
    protected void shutDown() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, objectId, "shutDown");
        try {
            log.info("Deregistering host {} from controller cluster", host);
            cluster.deregisterHost(host);
            log.info("Controller cluster listener shutDown complete");
        } finally {
            LoggerHelpers.traceLeave(log, objectId, "shutDown", traceId);
        }
    }

    @VisibleForTesting
    static class PendingSweepQueue {
        private final ConcurrentLinkedQueue<Supplier<CompletableFuture<Void>>> workQueue = new ConcurrentLinkedQueue<>();
        private final AtomicBoolean isSealed = new AtomicBoolean(false);

        @Synchronized
        @VisibleForTesting
        boolean enqueue(Supplier<CompletableFuture<Void>> futureSupplier) {
            if (!isSealed.get()) {
                workQueue.add(futureSupplier);
            }
            return !isSealed.get();
        }

        @Synchronized
        @VisibleForTesting
        List<Supplier<CompletableFuture<Void>>> drainAndSeal() {
            Preconditions.checkState(!isSealed.get());
            isSealed.set(true);

            return Lists.newArrayList(workQueue);
        }
    }
}

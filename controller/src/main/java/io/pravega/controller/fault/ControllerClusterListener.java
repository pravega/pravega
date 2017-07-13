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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterException;
import io.pravega.common.cluster.Host;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.task.Stream.TxnSweeper;
import io.pravega.controller.task.TaskSweeper;
import io.pravega.controller.util.RetryHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
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
    private final Optional<ControllerEventProcessors> eventProcessorsOpt;
    private final TaskSweeper taskSweeper;
    private final Optional<TxnSweeper> txnSweeperOpt;

    public ControllerClusterListener(final Host host, final Cluster cluster,
                                     final Optional<ControllerEventProcessors> eventProcessorsOpt,
                                     final TaskSweeper taskSweeper,
                                     final Optional<TxnSweeper> txnSweeperOpt,
                                     final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(host, "host");
        Preconditions.checkNotNull(cluster, "cluster");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkNotNull(eventProcessorsOpt, "eventProcessorsOpt");
        Preconditions.checkNotNull(taskSweeper, "taskSweeper");
        Preconditions.checkNotNull(txnSweeperOpt, "txnSweeperOpt");

        this.objectId = "ControllerClusterListener";
        this.host = host;
        this.cluster = cluster;
        this.executor = executor;
        this.eventProcessorsOpt = eventProcessorsOpt;
        this.taskSweeper = taskSweeper;
        this.txnSweeperOpt = txnSweeperOpt;
    }

    @Override
    protected void startUp() throws InterruptedException {
        long traceId = LoggerHelpers.traceEnter(log, objectId, "startUp");
        try {
            log.info("Registering host {} with controller cluster", host);
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
                        log.info("Received controller cluster event: {} for host: {}", type, host);
                        break;
                    case HOST_REMOVED:
                        log.info("Received controller cluster event: {} for host: {}", type, host);
                        handleHostRemoved(host);
                        break;
                    case ERROR:
                        // This event should be due to ZK connection errors. If it is session lost error then
                        // ControllerServiceMain would handle it. Otherwise it is a fleeting error that can go
                        // away with retries, and hence we ignore it.
                        log.info("Received error event when monitoring the controller host cluster, ignoring...");
                        break;
                }
            }, executor);

            Supplier<Set<String>> processes = () -> {
                try {
                    return cluster.getClusterMembers()
                            .stream()
                            .map(Host::getHostId)
                            .collect(Collectors.toSet());
                } catch (ClusterException e) {
                    log.error("error fetching cluster members {}", e);
                    throw new CompletionException(e);
                }
            };

            // This method should not block and process all sweepers asynchronously.
            // Also, it should retry any errors during processing as this is the only opportunity to process all failed hosts
            // that are no longer part of the cluster as we wont get any notification for them.
            sweepEventProcessorReaders(processes);
            sweepTransactions(processes);
            sweepTasks(processes);

            log.info("Controller cluster listener startUp complete");
        } finally {
            LoggerHelpers.traceLeave(log, objectId, "startUp", traceId);
        }
    }

    private void handleHostRemoved(Host host) {
        RetryHelper.withIndefiniteRetriesAsync(() -> taskSweeper.sweepOrphanedTasks(host.getHostId()),
                e -> log.warn(e.getMessage()), executor);

        eventProcessorsOpt.ifPresent(controllerEventProcessors -> {
            RetryHelper.withIndefiniteRetriesAsync(() -> {
                if (controllerEventProcessors.isRunning()) {
                    log.info("handling host removed and reporting readers offline for host {}", host.getHostId());
                    return controllerEventProcessors.notifyProcessFailure(host.getHostId());
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            }, e -> log.warn(e.getMessage()), executor);
        });

        txnSweeperOpt.ifPresent(txnSweeper -> {
            RetryHelper.withIndefiniteRetriesAsync(() -> {
                if (txnSweeper.isReady()) {
                    log.info("Sweeping orphaned transactions for host {}", host.getHostId());
                    return txnSweeper.sweepOrphanedTxns(host.getHostId());
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            }, e -> log.warn(e.getMessage()), executor);
        });
    }

    private void sweepEventProcessorReaders(Supplier<Set<String>> processes) {
        eventProcessorsOpt.ifPresent(eventProcessors -> {
            // Await initialization of eventProcesorsOpt
            RetryHelper.withIndefiniteRetriesAsync(() -> {
                log.info("Awaiting controller event processors' start");
                eventProcessors.awaitRunning();
                // Sweep orphaned tasks or readers at startup.
                log.info("Sweeping orphaned readers at startup");
                return eventProcessors.handleOrphanedReaders(processes);
            }, e -> log.warn(e.getMessage()), executor);
        });
    }

    private void sweepTransactions(Supplier<Set<String>> processes) {
        txnSweeperOpt.ifPresent(txnSweeper -> {
            // Await initialization of transactionTasksOpt.
            RetryHelper.withIndefiniteRetriesAsync(() -> {
                log.info("Awaiting StreamTransactionMetadataTasks to get ready");
                Exceptions.handleInterrupted(txnSweeper::awaitInitialization);
                // Sweep orphaned transactions as startup.
                log.info("Sweeping orphaned transactions");
                return txnSweeper.sweepFailedHosts(processes);
            }, e -> log.warn(e.getMessage()), executor);
        });
    }

    private void sweepTasks(Supplier<Set<String>> processes) {
        log.info("Sweeping orphaned tasks at startup");
        RetryHelper.withIndefiniteRetriesAsync(() -> taskSweeper.sweepOrphanedTasks(processes),
                e -> log.warn(e.getMessage()), executor);
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
}

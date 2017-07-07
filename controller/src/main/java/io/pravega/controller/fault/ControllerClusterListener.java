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

import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.task.Stream.TxnSweeper;
import io.pravega.controller.task.TaskSweeper;
import io.pravega.controller.util.RetryHelper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
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
    private final Optional<ControllerEventProcessors> eventProcessorsOpt;
    private final TaskSweeper taskSweeper;
    private final Optional<TxnSweeper> txnSweeperOpt;
    private final PendingSweepQueue txnPendingQueue;
    private final PendingSweepQueue eventProcPendingQueue;

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
        this.txnPendingQueue = new PendingSweepQueue();
        this.eventProcPendingQueue = new PendingSweepQueue();
    }

    @Override
    protected void startUp() throws InterruptedException, Exception {
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

            sweepEventProcessorReaders(processes);
            sweepTransactions(processes);
            sweepTasks(processes);

            log.info("Controller cluster listener startUp complete");
        } finally {
            LoggerHelpers.traceLeave(log, objectId, "startUp", traceId);
        }
    }

    private void handleHostRemoved(Host host) {
        taskSweeper.sweepOrphanedTasks(host.getHostId());

        eventProcessorsOpt.ifPresent(controllerEventProcessors -> {
            Supplier<CompletableFuture<Void>> futureSupplier = () -> RetryHelper.withIndefiniteRetriesAsync(() -> {
                log.info("handling host removed and reporting readers offline for host {}", host.getHostId());
                return controllerEventProcessors.notifyProcessFailure(host.getHostId());
            }, e -> log.warn(e.getMessage()), executor);

            // if queue is not sealed, then add to queue. Else process asynchronously
            if (!eventProcPendingQueue.enqueue(futureSupplier)) {
                futureSupplier.get();
            }
        });

        txnSweeperOpt.ifPresent(txnSweeper -> {
            Supplier<CompletableFuture<Void>> futureSupplier = () -> RetryHelper.withIndefiniteRetriesAsync(() -> {
                log.info("Sweeping orphaned transactions for host {}", host.getHostId());
                return txnSweeper.sweepOrphanedTxns(host.getHostId());
            }, e -> log.warn(e.getMessage()), executor);

            // if queue is not sealed, then add to queue. Else process asynchronously
            if (!txnPendingQueue.enqueue(futureSupplier)) {
                futureSupplier.get();
            }
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
            }, e -> log.warn(e.getMessage()), executor).thenCompose((Void v) ->
                    // drain pending queue and process
                    FutureHelpers.allOf(eventProcPendingQueue.drainAndSeal()
                            .stream().map(Supplier::get).collect(Collectors.toList())));
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
            }, e -> log.warn(e.getMessage()), executor).thenCompose((Void v) ->
                    // drain pending queue and process
                    FutureHelpers.allOf(txnPendingQueue.drainAndSeal()
                            .stream().map(Supplier::get).collect(Collectors.toList())));
        });
    }

    private void sweepTasks(Supplier<Set<String>> processes) {
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

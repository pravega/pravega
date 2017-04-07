/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.fault;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import com.emc.pravega.controller.task.TaskSweeper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Controller cluster listener service. This service when started, starts listening to
 * the controller cluster notifications. Whenever a controller instance leaves the
 * cluster, it does the following two things.
 * 1. Try to complete the orphaned tasks running on the failed controller instance, and
 * 2. Try to notify the commit and abort reader group about the loss of readers from failed controller instance.
 *
 */
@Slf4j
public class ControllerClusterListener extends AbstractIdleService {

    private final String objectId;
    private final Host host;
    private final Cluster cluster;
    private final ExecutorService executor;
    private final Optional<ControllerEventProcessors> eventProcessorsOpt;
    private final TaskSweeper taskSweeper;

    public ControllerClusterListener(final Host host, final Cluster cluster,
                                     final Optional<ControllerEventProcessors> eventProcessorsOpt,
                                     final TaskSweeper taskSweeper,
                                     final ExecutorService executor) {
        Preconditions.checkNotNull(host, "host");
        Preconditions.checkNotNull(cluster, "cluster");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkNotNull(eventProcessorsOpt, "eventProcessorsOpt");
        Preconditions.checkNotNull(taskSweeper, "taskSweeper");

        this.objectId = "ControllerClusterListener";
        this.host = host;
        this.cluster = cluster;
        this.executor = executor;
        this.eventProcessorsOpt = eventProcessorsOpt;
        this.taskSweeper = taskSweeper;
    }

    @Override
    protected void startUp() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, objectId, "startUp");
        try {
            log.info("Registering host {} with controller cluster", host);
            cluster.registerHost(host);

            Set<String> activeProcesses = cluster.getClusterMembers()
                    .stream()
                    .map(Host::getHostId)
                    .collect(Collectors.toSet());

            // Await initialization of components.
            if (eventProcessorsOpt.isPresent()) {
                log.info("Awaiting controller event processors' start");
                eventProcessorsOpt.get().awaitRunning();
            }

            log.info("Awaiting taskSweeper to become ready");
            taskSweeper.awaitReady();

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
                        taskSweeper.sweepOrphanedTasks(host.getHostId());
                        if (eventProcessorsOpt.isPresent()) {
                            eventProcessorsOpt.get().notifyProcessFailure(host.getHostId());
                        }
                        break;
                    case ERROR:
                        // This event should be due to ZK connection errors. If it is session lost error then
                        // ControllerServiceMain would handle it. Otherwise it is a fleeting error that can go
                        // away with retries, and hence we ignore it.
                        log.info("Received error event when monitoring the controller host cluster, ignoring...");
                        break;
                }
            }, executor);

            // Sweep orphaned tasks or readers at startup.
            if (eventProcessorsOpt.isPresent()) {
                log.info("Sweeping orphaned readers at startup");
                eventProcessorsOpt.get().handleOrphanedReaders(activeProcesses);
            }

            log.info("Sweeping orphaned tasks at startup");
            taskSweeper.sweepOrphanedTasks(activeProcesses);

            log.info("Controller cluster listener startUp complete");
        } finally {
            LoggerHelpers.traceLeave(log, objectId, "startUp", traceId);
        }
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

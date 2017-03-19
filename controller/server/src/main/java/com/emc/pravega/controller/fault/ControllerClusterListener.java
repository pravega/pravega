/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.fault;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.zkImpl.ClusterZKImpl;
import com.emc.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import com.emc.pravega.controller.task.TaskSweeper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ExecutorService;

/**
 * Controller cluster listener.
 */
@Slf4j
public class ControllerClusterListener extends AbstractIdleService {

    private final String objectId;
    private final Host host;
    private final ExecutorService executor;
    private final ControllerEventProcessors eventProcessors;
    private final TaskSweeper taskSweeper;
    private final ClusterZKImpl clusterZK;

    public ControllerClusterListener(final Host host, final CuratorFramework client,
                                     final ControllerEventProcessors eventProcessors,
                                     final TaskSweeper taskSweeper,
                                     final ExecutorService executor) {
        Preconditions.checkNotNull(host, "host");
        Preconditions.checkNotNull(client, "client");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkNotNull(eventProcessors, "eventProcessors");
        Preconditions.checkNotNull(taskSweeper, "taskSweeper");

        this.objectId = "ControllerClusterListener";
        this.host = host;
        this.executor = executor;
        this.eventProcessors = eventProcessors;
        this.taskSweeper = taskSweeper;
        this.clusterZK = new ClusterZKImpl(client, ClusterZKImpl.ClusterType.Controller);
    }

    @Override
    protected void startUp() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, objectId, "startUp");
        try {
            log.info("Registering host {} with controller cluster", host);
            clusterZK.registerHost(host);

            // TODO: At startup find old failures that haven't been handled yet and handle them.

            // Await initialization of components
            log.info("Awaiting controller event processors' start");
            eventProcessors.awaitRunning();

            log.info("Awaiting taskSweeper to become ready");
            taskSweeper.awaitReady();

            log.info("Adding controller cluster listener");
            clusterZK.addListener((type, host) -> {
                switch (type) {
                    case HOST_ADDED:
                        break;
                    case HOST_REMOVED:
                        // TODO: Since events could be lost, find the correct diff and notify host failures accordingly.
                        log.info("Received host removed event for {}", host.toString());
                        taskSweeper.sweepOrphanedTasks(host.toString());
                        eventProcessors.notifyProcessFailure(host.toString());
                        break;
                    case ERROR:
                        break;
                }
            }, executor);
        } finally {
            LoggerHelpers.traceLeave(log, objectId, "startUp", traceId);
        }
    }

    @Override
    protected void shutDown() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, objectId, "shutDown");
        try {
            log.info("Deregistering host {} from controller cluster", host);
            clusterZK.deregisterHost(host);
        } finally {
            LoggerHelpers.traceLeave(log, objectId, "shutDown", traceId);
        }
    }
}

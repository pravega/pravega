/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.ClusterType;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.zkImpl.ClusterZKImpl;
import io.pravega.segmentstore.server.SegmentContainerManager;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

/**
 * ZK based implementation for SegmentContainerManager.
 */
@Slf4j
class ZKSegmentContainerManager implements SegmentContainerManager {

    private final Host host;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Cluster cluster;
    private final ZKSegmentContainerMonitor containerMonitor;

    /**
     * Creates a new instance of the ZKSegmentContainerManager class.
     *
     * @param containerRegistry      The SegmentContainerRegistry to manage.
     * @param zkClient               ZooKeeper client.
     * @param pravegaServiceEndpoint Pravega service endpoint details.
     * @param executor               Executor service for running async operations.
     */
    ZKSegmentContainerManager(SegmentContainerRegistry containerRegistry, CuratorFramework zkClient,
                              Host pravegaServiceEndpoint, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(containerRegistry, "containerRegistry");
        Preconditions.checkNotNull(zkClient, "zkClient");
        this.host = Preconditions.checkNotNull(pravegaServiceEndpoint, "pravegaServiceEndpoint");
        this.cluster = new ClusterZKImpl(zkClient, ClusterType.HOST);
        this.containerMonitor = new ZKSegmentContainerMonitor(containerRegistry, zkClient, pravegaServiceEndpoint, executor);
    }

    @Override
    public void initialize() {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "initialize");

        try {
            // Initialize the container monitor.
            this.containerMonitor.initialize();

            // Advertise this segment store to the cluster.
            this.cluster.registerHost(this.host);
            log.info("Initialized.");
            LoggerHelpers.traceLeave(log, "initialize", traceId);
        } catch (Exception ex) {
            // Need to make sure we clean up resources if we failed to initialize.
            log.error("Initialization error. Cleaning up.", ex);
            close();
            throw ex;
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            close(this.containerMonitor);
            close(this.cluster);
        }
    }

    private void close(final AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception e) {
            log.error("Error while closing resource", e);
        }
    }
}

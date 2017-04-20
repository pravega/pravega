/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.cluster.Cluster;
import com.emc.pravega.common.cluster.ClusterType;
import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.cluster.zkImpl.ClusterZKImpl;
import com.emc.pravega.service.server.SegmentContainerManager;
import com.emc.pravega.service.server.SegmentContainerRegistry;
import com.google.common.base.Preconditions;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
    private final ScheduledExecutorService executor;

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
        this.executor = Preconditions.checkNotNull(executor, "executor");

        this.cluster = new ClusterZKImpl(zkClient, ClusterType.HOST);
        this.containerMonitor = new ZKSegmentContainerMonitor(containerRegistry, zkClient, pravegaServiceEndpoint, this.executor);
    }

    @Override
    public CompletableFuture<Void> initialize() {
        Exceptions.checkNotClosed(closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "initialize");

        return CompletableFuture
                .runAsync(() -> {
                    // Initialize the container monitor.
                    this.containerMonitor.initialize();

                    // Advertise this segment store to the cluster.
                    this.cluster.registerHost(this.host);
                    log.info("Initialized.");
                    LoggerHelpers.traceLeave(log, "initialize", traceId);
                }, this.executor)
                .exceptionally(ex -> {
                    // Need to make sure we clean up resources if we failed to initialize.
                    log.error("Initialization error. Cleaning up.", ex);
                    close();
                    throw new CompletionException(ex);
                });
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

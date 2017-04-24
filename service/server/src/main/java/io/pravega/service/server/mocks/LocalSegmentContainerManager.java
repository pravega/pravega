/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.server.mocks;

import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.segment.SegmentToContainerMapper;
import io.pravega.service.server.ContainerHandle;
import io.pravega.service.server.SegmentContainerManager;
import io.pravega.service.server.SegmentContainerRegistry;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import lombok.extern.slf4j.Slf4j;

/**
 * Local (sandbox) implementation for SegmentContainerManager. Nothing that happens here ever leaves the confines
 * of this class. All information is lost upon closing and/or garbage-collection.
 * <p>
 * The SegmentName -> ContainerId mapping is done by taking the hash of the StreamSegment name and then modulo the
 * number of containers (result is in hex).
 */
@Slf4j
@ThreadSafe
public class LocalSegmentContainerManager implements SegmentContainerManager {
    //region Members
    private static final Duration INIT_TIMEOUT_PER_CONTAINER = Duration.ofSeconds(30L);
    private static final Duration CLOSE_TIMEOUT_PER_CONTAINER = Duration.ofSeconds(30L);
    private final SegmentContainerRegistry registry;
    private final SegmentToContainerMapper segmentToContainerMapper;
    @GuardedBy("handles")
    private final HashMap<Integer, ContainerHandle> handles;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the LocalSegmentContainerManager class.
     *
     * @param containerRegistry        The SegmentContainerRegistry to manage.
     * @param segmentToContainerMapper A SegmentToContainerMapper that is used to determine the configuration of the cluster
     *                                 (i.e., number of containers).
     * @throws NullPointerException     If containerRegistry is null.
     * @throws NullPointerException     If segmentToContainerMapper is null.
     * @throws NullPointerException     If logger is null.
     * @throws IllegalArgumentException If containerRegistry already has Containers registered in it.
     */
    public LocalSegmentContainerManager(SegmentContainerRegistry containerRegistry, SegmentToContainerMapper segmentToContainerMapper) {
        Preconditions.checkNotNull(containerRegistry, "containerRegistry");
        Preconditions.checkNotNull(segmentToContainerMapper, "segmentToContainerMapper");
        Exceptions.checkArgument(containerRegistry.getContainerCount() == 0, "containerRegistry", "containerRegistry already has containers registered.");

        this.registry = containerRegistry;
        this.segmentToContainerMapper = segmentToContainerMapper;
        this.handles = new HashMap<>();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            // Close all containers that are still open.
            ArrayList<CompletableFuture<Void>> results = new ArrayList<>();
            synchronized (this.handles) {
                ArrayList<ContainerHandle> toClose = new ArrayList<>(this.handles.values());
                for (ContainerHandle handle : toClose) {
                    results.add(this.registry.stopContainer(handle, CLOSE_TIMEOUT_PER_CONTAINER));
                }
            }

            // Wait for all the containers to be closed.
            FutureHelpers.await(FutureHelpers.allOf(results), CLOSE_TIMEOUT_PER_CONTAINER.toMillis());
        }
    }

    //endregion

    //region SegmentContainerManager Implementation

    @Override
    public CompletableFuture<Void> initialize() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "initialize");
        long containerCount = this.segmentToContainerMapper.getTotalContainerCount();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int containerId = 0; containerId < containerCount; containerId++) {
            futures.add(this.registry.startContainer(containerId, INIT_TIMEOUT_PER_CONTAINER)
                                     .thenAccept(this::registerHandle));
        }

        return FutureHelpers.allOf(futures)
                            .thenRun(() -> LoggerHelpers.traceLeave(log, "initialize", traceId));
    }

    //endregion

    //region Helpers

    private void unregisterHandle(ContainerHandle handle) {
        synchronized (this.handles) {
            assert this.handles.containsKey(handle.getContainerId()) : "found unregistered handle " + handle.getContainerId();
            this.handles.remove(handle.getContainerId());
        }

        log.info("Container {} has been unregistered.", handle.getContainerId());
    }

    private void registerHandle(ContainerHandle handle) {
        assert handle != null : "handle is null.";
        synchronized (this.handles) {
            assert !this.handles.containsKey(handle.getContainerId()) : "handle is already registered " + handle.getContainerId();
            this.handles.put(handle.getContainerId(), handle);

            handle.setContainerStoppedListener(id -> unregisterHandle(handle));
        }

        log.info("Container {} has been registered.", handle.getContainerId());
    }

    //endregion
}

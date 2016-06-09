package com.emc.logservice.server.mocks;

import com.emc.logservice.common.*;
import com.emc.logservice.server.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

/**
 * Local (sandbox) implementation for SegmentContainerManager. Nothing that happens here ever leaves the confines
 * of this class. All information is lost upon closing and/or garbage-collection.
 * <p>
 * The SegmentName -> ContainerId mapping is done by taking the hash of the StreamSegment name and then modulo the
 * number of containers (result is in hex).
 */
@Slf4j
public class LocalSegmentContainerManager implements SegmentContainerManager {
    //region Members

    private static final Duration CloseTimeout = Duration.ofSeconds(30);//TODO: config?
    private final SegmentContainerRegistry registry;
    private final SegmentToContainerMapper segmentToContainerMapper;
    private final HashMap<String, ContainerHandle> handles;
    private boolean closed;

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
        Exceptions.throwIfNull(containerRegistry, "containerRegistry");
        Exceptions.throwIfNull(segmentToContainerMapper, "segmentToContainerMapper");
        Exceptions.throwIfIllegalArgument(containerRegistry.getContainerCount() == 0, "containerRegistry", "containerRegistry already has containers registered.");

        this.registry = containerRegistry;
        this.segmentToContainerMapper = segmentToContainerMapper;
        this.handles = new HashMap<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.closed = true;

        // Close all containers that are still open.
        ArrayList<CompletableFuture<Void>> results = new ArrayList<>();
        ArrayList<ContainerHandle> toClose = new ArrayList<>(this.handles.values());
        for (ContainerHandle handle : toClose) {
            results.add(this.registry.stopContainer(handle, CloseTimeout).thenAccept(v -> unregisterHandle(handle)));
        }

        // Wait for all the containers to be closed.
        FutureHelpers.allOf(results).join();
    }

    //endregion

    //region SegmentContainerManager Implementation

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, "initialize");
        ensureNotClosed();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int containerNumber = 0; containerNumber < this.segmentToContainerMapper.getTotalContainerCount(); containerNumber++) {
            String id = this.segmentToContainerMapper.getContainerId(containerNumber);
            futures.add(this.registry.startContainer(id, timer.getRemaining()).thenAccept(this::registerHandle));
        }

        return FutureHelpers.allOf(futures)
                            .thenRun(() -> LoggerHelpers.traceLeave(log, "initialize", traceId));
    }

    //endregion

    //region Helpers

    private void unregisterHandle(ContainerHandle handle) {
        assert this.handles.containsKey(handle.getContainerId());
        this.handles.remove(handle.getContainerId());
        log.info("Container {} has been unregistered.", handle.getContainerId());
    }

    private void registerHandle(ContainerHandle handle) {
        assert handle != null;
        assert !this.handles.containsKey(handle.getContainerId());
        this.handles.put(handle.getContainerId(), handle);

        handle.setContainerStoppedListener(id -> {
            log.error("Container {} has failed and is now in a Stopped state.", handle.getContainerId());
            unregisterHandle(handle);
        });

        log.info("Container {} has been registered.", handle.getContainerId());
    }

    private void ensureNotClosed() {
        Exceptions.throwIfClosed(this.closed, this);
    }

    //endregion
}

/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.server.mocks;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.segment.SegmentToContainerMapper;
import com.emc.pravega.service.server.ContainerHandle;
import com.emc.pravega.service.server.SegmentContainerManager;
import com.emc.pravega.service.server.SegmentContainerRegistry;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
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
    private static final Duration CLOSE_TIMEOUT_PER_CONTAINER = Duration.ofSeconds(30L); //TODO: config?
    private final SegmentContainerRegistry registry;
    private final SegmentToContainerMapper segmentToContainerMapper;
    @GuardedBy("handles")
    private final HashMap<Integer, ContainerHandle> handles;
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
        Preconditions.checkNotNull(containerRegistry, "containerRegistry");
        Preconditions.checkNotNull(segmentToContainerMapper, "segmentToContainerMapper");
        Exceptions.checkArgument(containerRegistry.getContainerCount() == 0, "containerRegistry", "containerRegistry already has containers registered.");

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
        synchronized (this.handles) {
            ArrayList<ContainerHandle> toClose = new ArrayList<>(this.handles.values());
            for (ContainerHandle handle : toClose) {
                results.add(this.registry.stopContainer(handle, CLOSE_TIMEOUT_PER_CONTAINER));
            }
        }

        // Wait for all the containers to be closed.
        FutureHelpers.allOf(results).join();
    }

    //endregion

    //region SegmentContainerManager Implementation

    @Override
    public CompletableFuture<Void> initialize() {
        long traceId = LoggerHelpers.traceEnter(log, "initialize");
        long containerCount = this.segmentToContainerMapper.getTotalContainerCount();
        ensureNotClosed();
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

            handle.setContainerStoppedListener(id -> {
                unregisterHandle(handle);
                //TODO: need to restart container. BUT ONLY IF WE HAVE A FLAG SET. In benchmark mode,
                // we rely on not auto-restarting containers.
            });
        }

        log.info("Container {} has been registered.", handle.getContainerId());
    }

    private void ensureNotClosed() {
        Exceptions.checkNotClosed(this.closed, this);
    }

    //endregion
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.server.mocks;

import com.emc.nautilus.common.Exceptions;
import com.emc.nautilus.common.FutureHelpers;
import com.emc.nautilus.common.LoggerHelpers;
import com.emc.nautilus.common.TimeoutTimer;
import com.emc.logservice.server.ContainerHandle;
import com.emc.logservice.server.SegmentContainerManager;
import com.emc.logservice.server.SegmentContainerRegistry;
import com.emc.logservice.server.SegmentToContainerMapper;
import com.google.common.base.Preconditions;
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

    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30); //TODO: config?
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
                results.add(this.registry.stopContainer(handle, CLOSE_TIMEOUT));
            }
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
                //TODO: need to restart container. BUT ONLY IF WE HAVE A FLAG SET. In benchmark mode, we rely on not auto-restarting containers.
            });
        }

        log.info("Container {} has been registered.", handle.getContainerId());
    }

    private void ensureNotClosed() {
        Exceptions.checkNotClosed(this.closed, this);
    }

    //endregion
}

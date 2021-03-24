/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.mocks;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.server.ContainerHandle;
import io.pravega.segmentstore.server.SegmentContainerManager;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * Local (sandbox) implementation for SegmentContainerManager. Nothing that happens here ever leaves the confines
 * of this class. All information is lost upon closing and/or garbage-collection.
 * <p>
 * The SegmentName -&lt; ContainerId mapping is done by taking the hash of the StreamSegment name and then modulo the
 * number of containers (result is in hex).
 * </p>
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
            Futures.await(Futures.allOf(results), CLOSE_TIMEOUT_PER_CONTAINER.toMillis());
        }
    }

    //endregion

    //region SegmentContainerManager Implementation

    @Override
    public void initialize() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        long traceId = LoggerHelpers.traceEnter(log, "initialize");
        long containerCount = this.segmentToContainerMapper.getTotalContainerCount();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int containerId = 0; containerId < containerCount; containerId++) {
            futures.add(this.registry.startContainer(containerId, INIT_TIMEOUT_PER_CONTAINER)
                                     .thenAccept(this::registerHandle));
        }
        try {
            Futures.join(Futures.allOf(futures), INIT_TIMEOUT_PER_CONTAINER.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException("Failed to start SegmentContainerManager", e);
        }
        LoggerHelpers.traceLeave(log, "initialize", traceId);
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

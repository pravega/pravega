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

package com.emc.logservice.server.service;

import com.emc.logservice.common.CallbackHelpers;
import com.emc.logservice.common.Exceptions;
import com.emc.logservice.common.FutureHelpers;
import com.emc.logservice.common.ObjectClosedException;
import com.emc.logservice.contracts.ContainerNotFoundException;
import com.emc.logservice.server.ContainerHandle;
import com.emc.logservice.server.SegmentContainer;
import com.emc.logservice.server.SegmentContainerFactory;
import com.emc.logservice.server.SegmentContainerRegistry;
import com.emc.logservice.server.ServiceShutdownListener;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Service;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Registry for SegmentContainers.
 */
@Slf4j
public class StreamSegmentContainerRegistry implements SegmentContainerRegistry {
    //region Members

    private final SegmentContainerFactory factory;
    private final AbstractMap<String, ContainerWithHandle> containers;
    private final Executor executor;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentContainerRegistry.
     *
     * @param containerFactory The SegmentContainerFactory to use.
     * @param executor         The Executor to use for async tasks.
     * @throws NullPointerException If any of the arguments are null.
     */
    public StreamSegmentContainerRegistry(SegmentContainerFactory containerFactory, Executor executor) {
        Preconditions.checkNotNull(containerFactory, "containerFactory");
        Preconditions.checkNotNull(executor, "executor");

        this.factory = containerFactory;
        this.executor = executor;
        this.containers = new ConcurrentHashMap<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            // Mark the class as Closed first; this will prevent others from
            this.closed = true;

            ArrayList<ContainerWithHandle> toClose = new ArrayList<>(this.containers.values());
            for (ContainerWithHandle c : toClose) {
                closeContainer(c, true);
            }

            assert this.containers.size() == 0 : "Not all containers have been unregistered.";
        }
    }

    //endregion

    //region SegmentContainerRegistry Implementation

    @Override
    public int getContainerCount() {
        return this.containers.size();
    }

    @Override
    public SegmentContainer getContainer(String containerId) throws ContainerNotFoundException {
        ensureNotClosed();
        ContainerWithHandle result = this.containers.getOrDefault(containerId, null);
        if (result == null) {
            throw new ContainerNotFoundException(containerId);
        }

        return result.container;
    }

    @Override
    public CompletableFuture<ContainerHandle> startContainer(String containerId, Duration timeout) {
        ensureNotClosed();

        // Check if container exists
        Exceptions.checkArgument(!this.containers.containsKey(containerId), "containerId", "Container %s is already registered.", containerId);

        // If not, create one and register it.
        ContainerWithHandle newContainer = new ContainerWithHandle(this.factory.createStreamSegmentContainer(containerId), new SegmentContainerHandle(containerId));
        ContainerWithHandle existingContainer = this.containers.putIfAbsent(containerId, newContainer);
        if (existingContainer != null) {
            // We had a race and some other request beat us to it.
            newContainer.container.close();
            throw new IllegalArgumentException(String.format("Container %s is already registered.", containerId));
        }

        log.info("Registered SegmentContainer {}.", containerId);

        // Attempt to Start the container.
        ServiceShutdownListener failureListener = new ServiceShutdownListener(
                () -> closeContainer(newContainer, true),
                ex -> handleContainerFailure(newContainer, ex));
        newContainer.container.addListener(failureListener, this.executor);
        newContainer.container.startAsync();

        return CompletableFuture.supplyAsync(
                () -> {
                    newContainer.container.awaitRunning();
                    return newContainer.handle;
                }, this.executor);
    }

    @Override
    public CompletableFuture<Void> stopContainer(ContainerHandle handle, Duration timeout) {
        ensureNotClosed();
        ContainerWithHandle result = this.containers.getOrDefault(handle.getContainerId(), null);
        if (result == null) {
            return FutureHelpers.failedFuture(new ContainerNotFoundException(handle.getContainerId()));
        }

        // Stop the container and then unregister it.
        result.container.stopAsync();
        return CompletableFuture.runAsync(result.container::awaitTerminated, this.executor)
                                .thenRun(() -> closeContainer(result, false));
    }

    //endregion

    //region Helpers

    private void ensureNotClosed() {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }
    }

    private void handleContainerFailure(ContainerWithHandle containerWithHandle, Throwable exception) {
        closeContainer(containerWithHandle, true);
        log.error("Critical failure for SegmentContainer {}. {}", containerWithHandle, exception);
    }

    private void closeContainer(ContainerWithHandle toClose, boolean notifyHandle) {
        assert toClose != null : "toClose is null.";
        toClose.container.close();
        assert toClose.container.state() == Service.State.TERMINATED : "Container is not stopped.";
        this.containers.remove(toClose.handle.getContainerId());
        log.info("Unregistered SegmentContainer {}.", toClose.handle.getContainerId());
        if (notifyHandle) {
            toClose.handle.notifyContainerStopped();
        }
    }

    //endregion

    //region ContainerWithHandle

    private static class ContainerWithHandle {
        public final SegmentContainer container;
        public final SegmentContainerHandle handle;

        public ContainerWithHandle(SegmentContainer container, SegmentContainerHandle handle) {
            assert container.getId().equals(handle.getContainerId()) : "Mismatch between container id and handle container id.";
            this.container = container;
            this.handle = handle;
        }

        @Override
        public String toString() {
            return String.format("Container Id = %s, State = %s", this.container.getId(), this.container.state());
        }
    }

    //endregion

    //region SegmentContainerHandle

    private static class SegmentContainerHandle implements ContainerHandle {
        private final String containerId;
        private Consumer<String> containerStoppedListener;

        /**
         * Creates a new instance of the ContainerHandle class.
         *
         * @param containerId The Id of the container.
         */
        public SegmentContainerHandle(String containerId) {
            this.containerId = containerId;
        }

        @Override
        public String getContainerId() {
            return this.containerId;
        }

        @Override
        public void setContainerStoppedListener(Consumer<String> handler) {
            this.containerStoppedListener = handler;
        }

        /**
         * Notifies the Container Stopped Listener that the container for this handle has stopped processing.
         */
        void notifyContainerStopped() {
            Consumer<String> handler = containerStoppedListener;
            if (handler != null) {
                CallbackHelpers.invokeSafely(handler, this.containerId, null);
            }
        }

        @Override
        public String toString() {
            return String.format("SegmentContainerId = %s", this.containerId);
        }
    }

    //endregion
}

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
package io.pravega.segmentstore.server.store;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Services;
import io.pravega.common.function.Callbacks;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.server.ContainerHandle;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Registry for SegmentContainers.
 */
@Slf4j
class StreamSegmentContainerRegistry implements SegmentContainerRegistry {
    //region Members

    private final SegmentContainerFactory factory;
    private final ConcurrentHashMap<Integer, ContainerWithHandle> containers;
    private final Executor executor;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentContainerRegistry.
     *
     * @param containerFactory The SegmentContainerFactory to use.
     * @param executor         The Executor to use for async tasks.
     * @throws NullPointerException If any of the arguments are null.
     */
    StreamSegmentContainerRegistry(SegmentContainerFactory containerFactory, Executor executor) {
        Preconditions.checkNotNull(containerFactory, "containerFactory");
        Preconditions.checkNotNull(executor, "executor");

        this.factory = containerFactory;
        this.executor = executor;
        this.containers = new ConcurrentHashMap<>();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            // Close all open containers and notify their handles - as this was an unrequested stop.
            ArrayList<ContainerWithHandle> toClose = new ArrayList<>(this.containers.values());
            for (ContainerWithHandle c : toClose) {
                c.container.close();
            }
        }
    }


    @Override
    public boolean isClosed() {
        return this.closed.get();
    }

    //endregion

    //region SegmentContainerRegistry Implementation

    @Override
    public int getContainerCount() {
        return this.containers.size();
    }

    @Override
    public SegmentContainer getContainer(int containerId) throws ContainerNotFoundException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        ContainerWithHandle result = this.containers.getOrDefault(containerId, null);
        if (result == null || Services.isTerminating(result.container.state())) {
            throw new ContainerNotFoundException(containerId);
        }

        return result.container;
    }

    @Override
    public Collection<SegmentContainer> getContainers() {
        List<SegmentContainer> segmentContainers = new ArrayList<SegmentContainer>();
        for (ContainerWithHandle containerHandle: containers.values()) {
            segmentContainers.add(containerHandle.container);
        }
        return segmentContainers;
    }

    @Override
    public CompletableFuture<ContainerHandle> startContainer(int containerId, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        // Check if container exists
        ContainerWithHandle existingContainer = this.containers.get(containerId);
        if (existingContainer != null) {
            if (!Services.isTerminating(existingContainer.container.state())) {
                // Container is already registered and not in the process of shutting down.
                throw new IllegalArgumentException(String.format("Container %d is already registered.", containerId));
            }

            // Wait for the container to shut down, and then start a new one.
            return existingContainer.shutdownNotifier
                                    .thenComposeAsync(v -> startContainerInternal(containerId), this.executor);
        } else {
            // Start the container right away.
            return startContainerInternal(containerId);
        }
    }

    @Override
    public CompletableFuture<Void> stopContainer(ContainerHandle handle, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        ContainerWithHandle result = this.containers.getOrDefault(handle.getContainerId(), null);
        if (result == null) {
            return CompletableFuture.completedFuture(null); // This could happen due to some race (or AutoClose) in the caller.
        }

        // Stop the container and then unregister it.
        return Services.stopAsync(result.container, this.executor);
    }

    //endregion

    //region Helpers

    /**
     * Creates a new Container and attempts to register it. This method works in an optimistic manner: it creates the
     * Container first and then attempts to register it, which should prevent us from having to lock on this entire method.
     * Creating new containers is cheap (we don't start them yet), so this operation should not take any extra resources.
     *
     * @param containerId The Id of the Container to start.
     * @return A CompletableFuture which will be completed with a ContainerHandle once the container has been started.
     */
    private CompletableFuture<ContainerHandle> startContainerInternal(int containerId) {
        ContainerWithHandle newContainer = new ContainerWithHandle(this.factory.createStreamSegmentContainer(containerId),
                new SegmentContainerHandle(containerId));
        ContainerWithHandle existingContainer = this.containers.putIfAbsent(containerId, newContainer);
        if (existingContainer != null) {
            // We had multiple concurrent calls to start this Container and some other request beat us to it.
            newContainer.container.close();
            throw new IllegalArgumentException(String.format("Container %d is already registered.", containerId));
        }

        log.info("Registered SegmentContainer {}.", containerId);

        // Attempt to Start the container, but first, attach a shutdown listener so we know to unregister it when it's stopped.
        Services.onStop(
                newContainer.container,
                () -> unregisterContainer(newContainer),
                ex -> handleContainerFailure(newContainer, ex),
                this.executor);
        return Services.startAsync(newContainer.container, this.executor)
                       .thenApply(v -> newContainer.handle);
    }

    private void handleContainerFailure(ContainerWithHandle containerWithHandle, Throwable exception) {
        unregisterContainer(containerWithHandle);
        log.error("Critical failure for SegmentContainer {}. {}", containerWithHandle, exception);
    }

    private void unregisterContainer(ContainerWithHandle containerWithHandle) {
        assert containerWithHandle != null : "containerWithHandle is null.";
        assert containerWithHandle.container.state() == Service.State.TERMINATED || containerWithHandle.container.state() == Service.State.FAILED : "Container is not stopped.";

        // First, release all resources owned by this instance.
        containerWithHandle.container.close();

        // Unregister the container.
        this.containers.remove(containerWithHandle.handle.getContainerId());

        // Notify the handle that the container is now in a Stopped state.
        containerWithHandle.handle.notifyContainerStopped();
        log.info("Unregistered SegmentContainer {}.", containerWithHandle.handle.getContainerId());

        containerWithHandle.shutdownNotifier.complete(null);
    }


    //endregion

    //region ContainerWithHandle

    @RequiredArgsConstructor
    private static class ContainerWithHandle {
        final SegmentContainer container;
        final SegmentContainerHandle handle;
        final CompletableFuture<Void> shutdownNotifier = new CompletableFuture<>();

        @Override
        public String toString() {
            return String.format("Container Id = %d, State = %s", this.container.getId(), this.container.state());
        }
    }

    //endregion

    //region SegmentContainerHandle

    @RequiredArgsConstructor
    private static class SegmentContainerHandle implements ContainerHandle {
        @Getter
        private final int containerId;
        @Setter
        private Consumer<Integer> containerStoppedListener;

        /**
         * Notifies the Container Stopped Listener that the container for this handle has stopped processing,
         * whether normally or via an exception.
         */
        void notifyContainerStopped() {
            Consumer<Integer> handler = this.containerStoppedListener;
            if (handler != null) {
                Callbacks.invokeSafely(handler, this.containerId, null);
            }
        }

        @Override
        public String toString() {
            return String.format("SegmentContainerId = %d", this.containerId);
        }
    }
    //endregion
}

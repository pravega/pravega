/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.store;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.function.CallbackHelpers;
import com.emc.pravega.service.contracts.ContainerNotFoundException;
import com.emc.pravega.service.server.ContainerHandle;
import com.emc.pravega.service.server.SegmentContainer;
import com.emc.pravega.service.server.SegmentContainerFactory;
import com.emc.pravega.service.server.SegmentContainerRegistry;
import com.emc.pravega.common.concurrent.ServiceShutdownListener;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Service;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
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
    private final AbstractMap<Integer, ContainerWithHandle> containers;
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
            // Mark the class as Closed first; this will prevent others from creating new containers.
            this.closed = true;

            // Close all open containers and notify their handles - as this was an unrequested stop.
            ArrayList<ContainerWithHandle> toClose = new ArrayList<>(this.containers.values());
            for (ContainerWithHandle c : toClose) {
                c.container.close();
            }
        }
    }

    //endregion

    //region SegmentContainerRegistry Implementation

    @Override
    public int getContainerCount() {
        return this.containers.size();
    }

    @Override
    public Collection<Integer> getRegisteredContainerIds() {
        return this.containers.keySet();
    }

    @Override
    public SegmentContainer getContainer(int containerId) throws ContainerNotFoundException {
        Exceptions.checkNotClosed(this.closed, this);
        ContainerWithHandle result = this.containers.getOrDefault(containerId, null);
        if (result == null) {
            throw new ContainerNotFoundException(containerId);
        }

        return result.container;
    }

    @Override
    public CompletableFuture<ContainerHandle> startContainer(int containerId, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);

        // Check if container exists
        Exceptions.checkArgument(!this.containers.containsKey(containerId), "containerId", "Container %d is already registered.", containerId);

        // If not, create one and register it.
        ContainerWithHandle newContainer = new ContainerWithHandle(this.factory.createStreamSegmentContainer(containerId), new SegmentContainerHandle(containerId));
        ContainerWithHandle existingContainer = this.containers.putIfAbsent(containerId, newContainer);
        if (existingContainer != null) {
            // We had a race and some other request beat us to it.
            newContainer.container.close();
            throw new IllegalArgumentException(String.format("Container %d is already registered.", containerId));
        }

        log.info("Registered SegmentContainer {}.", containerId);

        // Attempt to Start the container, but first, attach a shutdown listener so we know to unregister it when it's stopped.
        ServiceShutdownListener shutdownListener = new ServiceShutdownListener(
                () -> unregisterContainer(newContainer),
                ex -> handleContainerFailure(newContainer, ex));
        newContainer.container.addListener(shutdownListener, this.executor);
        newContainer.container.startAsync();

        return CompletableFuture.supplyAsync(
                () -> {
                    newContainer.container.awaitRunning();
                    return newContainer.handle;
                }, this.executor);
    }

    @Override
    public CompletableFuture<Void> stopContainer(ContainerHandle handle, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        ContainerWithHandle result = this.containers.getOrDefault(handle.getContainerId(), null);
        if (result == null) {
            return CompletableFuture.completedFuture(null); // This could happen due to some race (or AutoClose) in the caller.
        }

        // Stop the container and then unregister it.
        result.container.stopAsync();
        return CompletableFuture.runAsync(result.container::awaitTerminated, this.executor);
    }

    //endregion

    //region Helpers

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
    }

    //endregion

    //region ContainerWithHandle

    private static class ContainerWithHandle {
        public final SegmentContainer container;
        public final SegmentContainerHandle handle;

        ContainerWithHandle(SegmentContainer container, SegmentContainerHandle handle) {
            assert container.getId() == handle.getContainerId() : "Mismatch between container id and handle container id.";
            this.container = container;
            this.handle = handle;
        }

        @Override
        public String toString() {
            return String.format("Container Id = %d, State = %s", this.container.getId(), this.container.state());
        }
    }

    //endregion

    //region SegmentContainerHandle

    private static class SegmentContainerHandle implements ContainerHandle {
        private final int containerId;
        private Consumer<Integer> containerStoppedListener;

        /**
         * Creates a new instance of the ContainerHandle class.
         *
         * @param containerId The Id of the container.
         */
        public SegmentContainerHandle(int containerId) {
            this.containerId = containerId;
        }

        @Override
        public int getContainerId() {
            return this.containerId;
        }

        @Override
        public void setContainerStoppedListener(Consumer<Integer> handler) {
            this.containerStoppedListener = handler;
        }

        /**
         * Notifies the Container Stopped Listener that the container for this handle has stopped processing,
         * whether normally or via an exception.
         */
        void notifyContainerStopped() {
            Consumer<Integer> handler = this.containerStoppedListener;
            if (handler != null) {
                CallbackHelpers.invokeSafely(handler, this.containerId, null);
            }
        }

        @Override
        public String toString() {
            return String.format("SegmentContainerId = %d", this.containerId);
        }
    }

    //endregion
}

package com.emc.logservice.server.service;

import com.emc.logservice.common.*;
import com.emc.logservice.contracts.ContainerNotFoundException;
import com.emc.logservice.server.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Registry for SegmentContainers.
 */
@Slf4j
public class StreamSegmentContainerRegistry implements SegmentContainerRegistry {
    //region Members

    private final SegmentContainerFactory factory;
    private final AbstractMap<String, ContainerWithHandle> containers;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentContainerRegistry.
     *
     * @param containerFactory The SegmentContainerFactory to use.
     * @throws NullPointerException If containerFactory is null.
     */
    public StreamSegmentContainerRegistry(SegmentContainerFactory containerFactory) {
        if (containerFactory == null) {
            throw new NullPointerException("containerFactory");
        }

        this.factory = containerFactory;
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

            assert this.containers.size() == 0;
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
        Exceptions.throwIfIllegalArgument(!this.containers.containsKey(containerId), "containerId","Container %s is already registered.", containerId);

        // If not, create one and register it.
        ContainerWithHandle newContainer = new ContainerWithHandle(this.factory.createStreamSegmentContainer(containerId), new SegmentContainerHandle(containerId));
        ContainerWithHandle existingContainer = this.containers.putIfAbsent(containerId, newContainer);
        if (existingContainer != null) {
            // We had a race and some other request beat us to it.
            newContainer.container.close();
            throw new IllegalArgumentException(String.format("Container %s is already registered.", containerId));
        }

        log.info("Registered SegmentContainer {}.", containerId);

        // Attempt to Initialize and Start the container.
        TimeoutTimer timer = new TimeoutTimer(timeout);
        CompletableFuture<ContainerHandle> startFuture = newContainer.container
                .initialize(timer.getRemaining())
                .thenCompose(v -> {
                    newContainer.container.registerFaultHandler(ex -> handleContainerFailure(containerId, ex));

                    // Start the container.
                    return newContainer.container.start(timer.getRemaining());
                })
                .thenApply(v -> {
                    // The Container Manager may have gotten closed in the meantime. Check again and halt if needed.
                    ensureNotClosed();
                    return newContainer.handle;
                });

        // If start failed, close and unregister the container we just created.
        FutureHelpers.exceptionListener(startFuture, ex -> {
            log.error("Start Failure for SegmentContainer {}. {}", containerId, ex);
            closeContainer(newContainer, true);
        });

        return startFuture;
    }

    @Override
    public CompletableFuture<Void> stopContainer(ContainerHandle handle, Duration timeout) {
        ensureNotClosed();
        ContainerWithHandle result = this.containers.getOrDefault(handle.getContainerId(), null);
        if (result == null) {
            return FutureHelpers.failedFuture(new ContainerNotFoundException(handle.getContainerId()));
        }

        // Stop the container and then unregister it.
        return result.container.stop(timeout).thenRun(() -> closeContainer(result, false));
    }

    //endregion

    //region Helpers

    private void ensureNotClosed() {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }
    }

    private void handleContainerFailure(String containerId, Throwable exception) {
        ContainerWithHandle toClose = this.containers.getOrDefault(containerId, null);
        closeContainer(toClose, true);
        log.error("Critical failure for SegmentContainer {}. {}", containerId, exception);
    }

    private void closeContainer(ContainerWithHandle toClose, boolean notifyHandle) {
        assert toClose != null;
        toClose.container.close();
        assert toClose.container.getState() == ContainerState.Closed;
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
            assert container.getId().equals(handle.getContainerId());
            this.container = container;
            this.handle = handle;
        }

        @Override
        public String toString() {
            return String.format("Container Id = %s, State = %s", this.container.getId(), this.container.getState());
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
            return String.format("ContainerId = %s", this.containerId);
        }
    }

    //endregion
}

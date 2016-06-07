package com.emc.logservice.server;

import com.emc.logservice.contracts.ContainerNotFoundException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Registry for Segment Containers, allowing access to SegmentContainers, as well as operations on them.
 */
public interface SegmentContainerRegistry extends AutoCloseable {
    /**
     * Gets the number of registered containers.
     * @return
     */
    int getContainerCount();

    /**
     * Gets a reference to the SegmentContainer with given Id.
     *
     * @param containerId The Id of the SegmentContainer.
     * @return The requested SegmentContainer, or null if no such container is started.
     */
    SegmentContainer getContainer(String containerId) throws ContainerNotFoundException;

    /**
     * Starts processing the container with given Id.
     *
     * @param containerId The Id of the container to start processing.
     * @param timeout     The timeout for the operation.
     * @return A CompletableFuture that, when this operation completes normally, will indicate that the container has
     * started successfully (in which case it contains a ContainerHandle that can be used to control the container). If
     * the operation failed, the Future will contain the reason for the failure.
     * @throws IllegalStateException If the container is already started.
     */
    CompletableFuture<ContainerHandle> startContainer(String containerId, Duration timeout);

    /**
     * Starts processing the container associated with the given handle.
     *
     * @param handle  The handle for the container to stop processing.
     * @param timeout The timeout for the operation.
     * @return A CompletableFuture that, when this operation completes normally, will indicate that the container has
     * been stopped successfully. If the operation failed, the Future will contain the reason for the failure.
     * @throws IllegalStateException If the container is already started.
     * @throws NullPointerException  If handle is null.
     */
    CompletableFuture<Void> stopContainer(ContainerHandle handle, Duration timeout);

    @Override
    void close();
}

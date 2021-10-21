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
package io.pravega.segmentstore.server;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Registry for Segment Containers, allowing access to SegmentContainers, as well as operations on them.
 */
public interface SegmentContainerRegistry extends AutoCloseable {
    /**
     * Gets the number of registered containers.
     * @return The number of registered containers.
     */
    int getContainerCount();

    /**
     * Gets a reference to the SegmentContainer with given Id.
     *
     * @param containerId The Id of the SegmentContainer.
     * @return The requested SegmentContainer, or null if no such container is started.
     * @throws ContainerNotFoundException If no container with the given Id is registered.
     */
    SegmentContainer getContainer(int containerId) throws ContainerNotFoundException;

    /**
     * Gets a reference to the all the SegmentContainers.
     *
     * @return collection of SegmentContainers within the registry.
     */
    Collection<SegmentContainer> getContainers();

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
    CompletableFuture<ContainerHandle> startContainer(int containerId, Duration timeout);

    /**
     * Tells if registry is closed.
     *
     * @return if the registry is closed.
     */
    boolean isClosed();

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

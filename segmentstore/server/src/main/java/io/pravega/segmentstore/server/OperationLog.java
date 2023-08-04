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

import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.storage.DurableDataLogException;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Sequential Log made of Log Operations.
 */
public interface OperationLog extends Container {
    /**
     * Adds a new {@link Operation} to the {@link OperationLog} with a given {@link OperationPriority}.
     *
     * @param operation The {@link Operation} to append.
     * @param priority  Operation Priority.
     * @param timeout   Timeout for the {@link Operation}.
     * @return A CompletableFuture that, when completed, will indicate that the {@link Operation} has been durably added.
     * If the {@link Operation} failed to be added, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Void> add(Operation operation, OperationPriority priority, Duration timeout);

    /**
     * Truncates the log up to the given sequence.
     *
     * @param upToSequence The Sequence up to where to truncate.
     * @param timeout      The timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the truncation completed. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Void> truncate(long upToSequence, Duration timeout);

    /**
     * Creates and persists a Metadata Checkpoint.
     *
     * @param timeout The timeout for the operation.
     * @return The Sequence Number of the Metadata Checkpoint.
     */
    CompletableFuture<Long> checkpoint(Duration timeout);

    /**
     * Reads a number of Operation from the log, starting with the first Operation that has not yet been read using this
     * method. If this method has not been invoked yet for this instance, the first Operation to be returned will be the
     * first one added via {@link #add}.
     *
     * @param maxCount The maximum number of entries to read.
     * @param timeout  Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a Queue with the result. If the operation
     * failed, this Future will complete with the appropriate exception. If there are Operations readily available for
     * reading, the returned Future will be already completed with the result. If no Operations are currently available
     * for reading, the Future will be completed when the first such Operation is added (via {@link #add}), or it will be
     * completed with a {@link java.util.concurrent.TimeoutException} if the given timeout expired prior to that happening.
     * The items in the returned Queue will not be returned for a subsequent call to this method.
     */
    CompletableFuture<Queue<Operation>> read(int maxCount, Duration timeout);

    /**
     * Waits until the OperationLog enters an Online State.
     *
     * @return A CompletableFuture that, when completed, will indicate that the OperationLog is Online. If the OperationLog
     * is already Online, this Future will be already completed when returned. If the OperationLog encounters an exception
     * while attempting to start (including it shutting down), this Future will be completed with the appropriate exception.
     */
    CompletableFuture<Void> awaitOnline();

    /**
     * Provides a method to determine if the OperationLog is initialized for the first time.
     * @return True if initializing for the first time.
     */
    boolean isInitialized();

    /**
     * Override epoch of a container. To be used in case of container recovering from data in storage,
     * hosting the container_epoch files with epoch information backed up in them.
     * @param epoch epoch value to override.
     * @throws DurableDataLogException Exception while overriding the epoch.
     */
    void overrideEpoch(long epoch) throws DurableDataLogException;
}


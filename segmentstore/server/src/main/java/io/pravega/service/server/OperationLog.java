/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server;

import io.pravega.service.server.logs.operations.Operation;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Sequential Log made of Log Operations.
 */
public interface OperationLog extends Container {
    /**
     * Adds a new Operation to the log.
     *
     * @param operation The Operation to append.
     * @param timeout   Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the Sequence for the Operation. If the entry failed to
     * be added, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Long> add(Operation operation, Duration timeout);

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
     * Reads a number of entries from the log.
     *
     * @param afterSequence The Sequence of the last entry before the first one to read.
     * @param maxCount      The maximum number of entries to read.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain an Iterator with the result. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Iterator<Operation>> read(long afterSequence, int maxCount, Duration timeout);

    /**
     * Waits until all currently pending Operations up to this point are processed (whether successfully or not).
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that all currently pending operations up to this
     * point will have completed (normally or exceptionally).
     */
    CompletableFuture<Void> operationProcessingBarrier(Duration timeout);
}


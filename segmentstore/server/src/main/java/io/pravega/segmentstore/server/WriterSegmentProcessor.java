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
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a general Segment Processor for the Storage Writer.
 */
public interface WriterSegmentProcessor extends AutoCloseable {

    @Override
    void close();

    /**
     * Gets a value indicating whether the Segment Processor is closed (for any kind of operations).
     * @return true if the Segment Processor is closed, false otherwise.
     */
    boolean isClosed();

    /**
     * Gets the SequenceNumber of the first operation that is not fully committed to Storage.
     * See {@link Operation#getSequenceNumber} for a definition of Sequence Number.
     * @return The SequenceNumber of the first operation that is not fully committed to Storage.
     */
    long getLowestUncommittedSequenceNumber();

    /**
     * Gets a value indicating whether a call to {@link #flush} is required given the current state of this Segment Processor.
     * @return true if a flush call is required.
     */
    boolean mustFlush();

    /**
     * Adds the given {@link SegmentOperation} to the Processor.
     *
     * @param operation the SegmentOperation to add.
     * @throws ServiceHaltException     If the validation of the given Operation indicates a possible data corruption in
     *                                  the code (offset gaps, out-of-order operations, etc.) or state in which the
     *                                  operation has been already acknowledged.
     * @throws IllegalArgumentException If the validation of the given Operation indicates a possible non-corrupting bug
     *                                  in the code.
     */
    void add(SegmentOperation operation) throws ServiceHaltException;

    /**
     * Flushes the contents of the Processor.
     *
     * @param force   If true, force-flushes everything accumulated in the {@link WriterSegmentProcessor}, regardless of
     *                the value returned by {@link #mustFlush()}.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a summary of the flush operation. If any errors
     * occurred during the flush, the Future will be completed with the appropriate exception.
     */
    CompletableFuture<WriterFlushResult> flush(boolean force, Duration timeout);

    /**
     * Flushes the contents of the Processor without forcing it. Equivalent to {@code flush(false, timeout)}.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a summary of the flush operation. If any errors
     * occurred during the flush, the Future will be completed with the appropriate exception.
     */
    default CompletableFuture<WriterFlushResult> flush(Duration timeout) {
        return flush(false, timeout);
    }
}

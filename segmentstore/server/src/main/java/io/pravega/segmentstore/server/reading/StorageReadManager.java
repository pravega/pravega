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
package io.pravega.segmentstore.server.reading;

import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.storage.ReadOnlyStorage;
import io.pravega.segmentstore.storage.SegmentHandle;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import lombok.extern.slf4j.Slf4j;

/**
 * Facilitates and Organizes the reads from Storage.
 */
@Slf4j
@ThreadSafe
public class StorageReadManager implements AutoCloseable {
    //region Members

    private final String traceObjectId;
    private final ReadOnlyStorage storage;
    private final Executor executor;
    private final String segmentName;
    @GuardedBy("lock")
    private final TreeMap<Long, Request> pendingRequests;
    @GuardedBy("lock")
    private CompletableFuture<SegmentHandle> handle;
    private final Object lock = new Object();
    @GuardedBy("lock")
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StorageReadManager class.
     *
     * @param segmentMetadata A SegmentMetadata to create the StorageReadManager for.
     * @param storage         A ReadOnlyStorage to use for data fetching.
     * @param executor        An Executor to use for running asynchronous tasks.
     */
    StorageReadManager(SegmentMetadata segmentMetadata, ReadOnlyStorage storage, Executor executor) {
        Preconditions.checkNotNull(storage, "storage");
        Preconditions.checkNotNull(executor, "executor");

        this.traceObjectId = String.format("StorageReader[%d-%d]", segmentMetadata.getContainerId(), segmentMetadata.getId());
        this.segmentName = segmentMetadata.getName();
        this.storage = storage;
        this.executor = executor;
        this.pendingRequests = new TreeMap<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        synchronized (this.lock) {
            if (this.closed) {
                return;
            }

            this.closed = true;

            // Cancel all pending reads and unregister them.
            ArrayList<Request> toClose = new ArrayList<>(this.pendingRequests.values());
            toClose.forEach(Request::cancel);
        }
    }

    //endregion

    //region Request Processing

    /**
     * Queues the given request. The Request will be checked against existing pending Requests. If necessary, this request
     * will be adjusted to take advantage of an existing request (i.e., if it overlaps with an existing request, no actual
     * Storage read will happen for this one, yet the result of the previous one will be used instead). The callbacks passed
     * to the request will be invoked with either the result of the read or with the exception that caused the read to fail.
     *
     * @param request The request to queue.
     */
    void execute(Request request) {
        log.debug("{}: StorageRead.Execute {}", this.traceObjectId, request);
        synchronized (this.lock) {
            Exceptions.checkNotClosed(this.closed, this);
            Request existingRequest = findOverlappingRequest(request);
            if (existingRequest != null) {
                // We found an overlapping request. Adjust the current request length.
                int newLength = (int) (existingRequest.getOffset() + existingRequest.getLength() - request.getOffset());
                if (newLength > 0 && newLength < request.getLength()) {
                    request.adjustLength(newLength);
                    existingRequest.addDependent(request);
                    return;
                }
            }

            this.pendingRequests.put(request.getOffset(), request);
        }

        // Initiate the Storage Read.
        executeStorageRead(request);
    }

    /**
     * Executes the Storage Read for the given request.
     *
     * @param request The request.
     */
    private void executeStorageRead(Request request) {
        try {
            byte[] buffer = new byte[request.length];
            getHandle()
                    .thenComposeAsync(handle -> this.storage.read(handle, request.offset, buffer, 0, buffer.length, request.getTimeout()), this.executor)
                    .thenAcceptAsync(bytesRead -> request.complete(new ByteArraySegment(buffer, 0, bytesRead)), this.executor)
                    .whenComplete((r, ex) -> {
                        if (ex != null) {
                            request.fail(ex);
                        }

                        // Unregister the Request after every request fulfillment.
                        finalizeRequest(request);
                    });
        } catch (Throwable ex) {
            if (Exceptions.mustRethrow(ex)) {
                throw ex;
            }

            request.fail(ex);
            finalizeRequest(request);
        }
    }

    /**
     * Ensures that the given request has been finalized (if not, it is failed), and unregisters it from the pending reads.
     *
     * @param request The request.
     */
    private void finalizeRequest(Request request) {
        // Check, one last time, if the request was finalized. Better fail it with an AssertionError rather than leave it
        // hanging forever.
        if (!request.isDone()) {
            request.fail(new AssertionError("Request finalized but not yet completed."));
        }

        // Unregister the request.
        synchronized (this.lock) {
            this.pendingRequests.remove(request.getOffset());
        }

        log.debug("{}: StorageRead.Finalize {}, Success = {}", this.traceObjectId, request, !request.resultFuture.isCompletedExceptionally());
    }

    /**
     * Finds a pending Request that overlaps with the given request, based on the given request's Offset.
     *
     * @param request The request.
     * @return The overlapping request, or null if no such request exists.
     */
    @GuardedBy("lock")
    private Request findOverlappingRequest(Request request) {
        Map.Entry<Long, Request> previousEntry = this.pendingRequests.floorEntry(request.getOffset());
        if (previousEntry != null && request.getOffset() < previousEntry.getValue().getEndOffset()) {
            // Found one.
            return previousEntry.getValue();
        }

        // Either no previous entry, or the highest previous entry does not overlap.
        return null;
    }

    private CompletableFuture<SegmentHandle> getHandle() {
        synchronized (this.lock) {
            if (this.handle == null) {
                this.handle = storage.openRead(this.segmentName)
                        .whenComplete((h, ex) -> {
                            if (ex != null) {
                                synchronized (this.lock) {
                                    log.debug("{}: storage.openRead failed for {}. Resetting handle. {}", this.traceObjectId, this.segmentName, ex);
                                    this.handle = null;
                                }
                            }
                        });
            }

            return this.handle;
        }
    }

    //endregion

    //region Result

    /**
     * Represents a Result for a StorageReaderOperation.
     */
    static class Result {
        private final ByteArraySegment data;
        private final boolean derived;

        private Result(ByteArraySegment data, boolean derived) {
            this.data = data;
            this.derived = derived;
        }

        /**
         * Gets a pointer to a ByteArraySegment that contains the data for this Result.
         */
        public ByteArraySegment getData() {
            return this.data;
        }

        /**
         * Gets a value indicating whether this Result is derived from another result that has already been completed.
         */
        public boolean isDerived() {
            return this.derived;
        }

        @Override
        public String toString() {
            return String.format("Length = %d, Derived = %s", this.data.getLength(), this.derived);
        }
    }

    //endregion

    //region Request

    /**
     * Represents a Request for a StorageReadManager operation.
     */
    static class Request {
        //region Members

        private final long offset;
        private int length;
        private final CompletableFuture<Result> resultFuture;
        private final Duration timeout;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the StorageReadManager.Request class.
         *
         * @param offset          The offset to read at.
         * @param length          The length of the read.
         * @param successCallback A Consumer that will be invoked in case of successful completion of this request.
         * @param failureCallback A Consumer that will be invoked in case this request failed to process.
         * @param timeout         Timeout for the request.
         */
        Request(long offset, int length, Consumer<Result> successCallback, Consumer<Throwable> failureCallback, Duration timeout) {
            Preconditions.checkArgument(offset >= 0, "offset must be a non-negative number.");
            Preconditions.checkArgument(length > 0, "length must be a positive integer.");

            this.offset = offset;
            this.length = length;
            this.timeout = timeout;
            this.resultFuture = new CompletableFuture<>();
            this.resultFuture.thenAccept(successCallback);
            Futures.exceptionListener(this.resultFuture, failureCallback);
        }

        //endregion

        //region Properties

        /**
         * Gets a value indicating the Starting Offset for this Request.
         */
        long getOffset() {
            return this.offset;
        }

        /**
         * Gets a value indicating the length of this Request.
         */
        int getLength() {
            return this.length;
        }

        /**
         * Gets a value indicating the last offset of this Request.
         */
        long getEndOffset() {
            return this.offset + this.length;
        }

        /**
         * Gets a value indicating whether this request is completed (whether successfully or failed).
         */
        boolean isDone() {
            return this.resultFuture.isDone();
        }

        /**
         * Gets a value indicating the timeout for this operation.
         */
        Duration getTimeout() {
            return this.timeout;
        }

        /**
         * Registers the given request as a dependent of this request. If this Request succeeds, the given Request will
         * be completed as well (with the appropriate result). If this Request fails, the given Request will fail as well.
         *
         * @param request The request to add as a dependent.
         */
        void addDependent(Request request) {
            Preconditions.checkArgument(isSubRequest(this, request), "Given Request does is not a sub-request of this one.");
            this.resultFuture.thenRun(() -> request.complete(this));
            Futures.exceptionListener(this.resultFuture, request::fail);
        }

        /**
         * Re-sets the offset and length to the given arguments.
         *
         * @param newLength The new Length.
         * @throws IllegalArgumentException If the new interval is outside of the original request interval.
         */
        private void adjustLength(int newLength) {
            Preconditions.checkArgument(newLength >= 0 && newLength <= this.length, "length is outside of the original request bounds.");
            this.length = newLength;
        }

        @Override
        public String toString() {
            return String.format("Offset = %d, Length = %d", this.offset, this.length);
        }

        //endregion

        // region Completion

        /**
         * Cancels this Request.
         */
        private void cancel() {
            fail(new CancellationException());
        }

        /**
         * Completes this Request with the given request as a source (this Request is a sub-interval of the given request).
         *
         * @param source The source Request to complete with.
         */
        private void complete(Request source) {
            Preconditions.checkState(!isDone(), "This Request is already completed.");
            Preconditions.checkArgument(source.isDone(), "Given request is not completed.");
            Preconditions.checkArgument(isSubRequest(source, this), "This Request is not a sub-request of the given one.");

            try {
                // Get the source Request's result, slice it and return the sub-segment that this request maps to.
                Result sourceResult = source.resultFuture.join();
                int offset = (int) (this.getOffset() - source.getOffset());
                this.resultFuture.complete(new Result(sourceResult.getData().slice(offset, getLength()), true));
            } catch (Throwable ex) {
                if (Exceptions.mustRethrow(ex)) {
                    throw ex;
                }

                fail(ex);
            }
        }

        /**
         * Completes this Request with the given result.
         *
         * @param data The result to complete with.
         */

        // NOTE: https://github.com/spotbugs/spotbugs/issues/811
        @SuppressFBWarnings
        private void complete(ByteArraySegment data) {
            Preconditions.checkState(!isDone(), "This Request is already completed.");
            this.resultFuture.complete(new Result(data, false));
        }

        /**
         * Fails this Request with the given Exception.
         *
         * @param exception The exception to fail with.
         */
        private void fail(Throwable exception) {
            if (isDone()) {
                // Nothing to do.
                return;
            }

            this.resultFuture.completeExceptionally(exception);
        }

        /**
         * Determines if the innerRequest is a sub-request of outerRequest.
         *
         * @param outerRequest The outer request.
         * @param innerRequest The inner request.
         * @return True if innerRequest can be fulfilled with the result from outerRequest, false otherwise.
         */
        private static boolean isSubRequest(Request outerRequest, Request innerRequest) {
            return innerRequest.getOffset() >= outerRequest.getOffset()
                    && innerRequest.getEndOffset() <= outerRequest.getEndOffset();
        }

        //endregion
    }

    //endregion
}

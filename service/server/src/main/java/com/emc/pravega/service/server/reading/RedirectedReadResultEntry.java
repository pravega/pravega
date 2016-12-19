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

package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.ObjectClosedException;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * A ReadResultEntry that wraps an inner Entry, but allows for offset adjustment. Useful for returning read results
 * that point to Transaction Read Indices.
 */
@Slf4j
class RedirectedReadResultEntry implements CompletableReadResultEntry {
    //region Members

    private static final Duration RETRY_TIMEOUT = Duration.ofSeconds(30); // TODO: these two should either be dynamic or configurable.
    private static final Duration EXCEPTION_DELAY = Duration.ofMillis(1000);
    private final AtomicReference<CompletableReadResultEntry> baseEntry;
    private final AtomicLong adjustedOffset;
    private final GetEntry retryGetEntry;
    private final AtomicBoolean firstAttempt;
    private final ScheduledExecutorService executorService;
    private final AtomicReference<CompletableFuture<ReadResultEntryContents>> result;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RedirectedReadResultEntry class.
     *
     * @param baseEntry        The CompletableReadResultEntry to wrap.
     * @param offsetAdjustment The amount to adjust the offset by.
     * @param retryGetEntry    A BiFunction to invoke when needing to retry an entry. First argument: offset, Second: length.
     * @param executorService  An executor service to execute background operations on.
     */
    RedirectedReadResultEntry(CompletableReadResultEntry baseEntry, long offsetAdjustment, GetEntry retryGetEntry, ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(baseEntry, "baseEntry");
        Preconditions.checkNotNull(retryGetEntry, "retryGetEntry");
        Preconditions.checkNotNull(executorService, "executorService");
        this.baseEntry = new AtomicReference<>(baseEntry);
        this.adjustedOffset = new AtomicLong(baseEntry.getStreamSegmentOffset() + offsetAdjustment);
        Preconditions.checkArgument(this.adjustedOffset.get() >= 0, "Given offset adjustment would result in a negative offset.");
        this.retryGetEntry = retryGetEntry;
        this.executorService = executorService;
        this.result = new AtomicReference<>();
        this.firstAttempt = new AtomicBoolean(true);
    }

    //endregion

    //region ReadResultEntry Implementation

    @Override
    public long getStreamSegmentOffset() {
        return this.adjustedOffset.get();
    }

    @Override
    public int getRequestedReadLength() {
        return this.baseEntry.get().getRequestedReadLength();
    }

    @Override
    public ReadResultEntryType getType() {
        return this.baseEntry.get().getType();
    }

    @Override
    public CompletableFuture<ReadResultEntryContents> getContent() {
        // We need to make sure we return the same CompletableFuture instance with every call to this method.
        if (FutureHelpers.isSuccessful(this.baseEntry.get().getContent())) {
            // Current base entry already has data available.
            if (this.result.get() == null) {
                return this.baseEntry.get().getContent();
            } else {
                // Someone else might be waiting on us.
                this.result.get().complete(this.baseEntry.get().getContent().join());
            }
        } else {
            // Current base entry is not yet done (or is, and is unsuccessful).
            if (this.result.get() == null) {
                this.result.set(new CompletableFuture<>());

                // Connect the current base entry to our result.
                linkBaseEntryToResult();
            }
        }

        return this.result.get();
    }

    @Override
    public void requestContent(Duration timeout) {
        try {
            this.baseEntry.get().requestContent(timeout);
        } catch (Throwable ex) {
            if (!handle(ex, timeout)) {
                // Unable to swap or ineligible exception; rethrow immediately.
                throw ex;
            }
        }
    }

    @Override
    public void setCompletionCallback(CompletionConsumer completionCallback) {
        this.baseEntry.get().setCompletionCallback(completionCallback);
    }

    @Override
    public CompletionConsumer getCompletionCallback() {
        return this.baseEntry.get().getCompletionCallback();
    }

    //endregion

    //region Helpers

    /**
     * Connects the current base entry to this ResultEntry's result (returned from getContent).
     */
    private void linkBaseEntryToResult() {
        if (this.result.get() == null) {
            return;
        }

        this.baseEntry
                .get().getContent()
                .thenAccept(this.result.get()::complete) // Current baseEntry finished up fine; complete the result.
                .exceptionally(ex -> {
                    // Attempt to handle & switch, and tie the outcome to the result we are returning.
                    // Some exceptions may require a delay before retrying (i.e. StreamSegmentNotFoundException).
                    // This is because, when transactions are merged, they are first deleted and then the ReadIndex
                    // is updated with the fact. If we were unlucky enough to request an entry right between these two
                    // events, then we need to wait until the ReadIndex is updated. The best way, using the data available
                    // is to retry after some time (the ReadIndex cannot be updated before the concat happens in Storage,
                    // nor can it be done at the same time).
                    FutureHelpers.delayedFuture(getExceptionDelay(ex), this.executorService)
                                 .thenAccept(v -> handleGetContentFailure(ex));
                    return null;
                });
    }

    protected Duration getExceptionDelay(Throwable ex) {
        boolean requiresDelay = this.firstAttempt.get() && ExceptionHelpers.getRealException(ex) instanceof StreamSegmentNotExistsException;
        return requiresDelay ? EXCEPTION_DELAY : Duration.ZERO;
    }

    /**
     * Handles an exception that was caught. If this is the first exception ever caught, and it is eligible for retries,
     * then this method will invoke the retryGetEntry that was passed through the constructor to get a new base entry.
     * If that succeeds, the existing base entry is replaced with the result (and internal state is updated).
     * <p>
     * The new entry will only be accepted if it is not a RedirectedReadResultEntry (since that is likely to get us in the
     * same situation again).
     *
     * @param ex      The exception to inspect.
     * @param timeout Timeout for the operation (a new call to requestContent will be made).
     * @return True if the exception was handled properly and the base entry swapped, false otherwise.
     */
    private boolean handle(Throwable ex, Duration timeout) {
        // TODO: in theory this could be outsourced to the Retry class (but it's complicated due to the use of requestContent and getContent).
        ex = ExceptionHelpers.getRealException(ex);
        if (this.firstAttempt.getAndSet(false) && isRetryable(ex)) {
            CompletableReadResultEntry oldEntry = this.baseEntry.get();
            CompletableReadResultEntry newEntry = this.retryGetEntry.apply(getStreamSegmentOffset(), oldEntry.getRequestedReadLength());
            if (!(newEntry instanceof RedirectedReadResultEntry)) {
                newEntry.requestContent(timeout);
                switchBase(newEntry);
                return true;
            }
        }

        return false;
    }

    private void handleGetContentFailure(Throwable ex) {
        ex = ExceptionHelpers.getRealException(ex);
        boolean success;
        try {
            success = handle(ex, RETRY_TIMEOUT);
        } catch (Throwable ex2) {
            ex.addSuppressed(ex2);
            success = false;
        }

        if (success) {
            // We were able to switch; tie the outcome of our result to the outcome of the new entry's getContent().
            linkBaseEntryToResult();
        } else {
            // Unable to switch.
            this.result.get().completeExceptionally(ex);
        }
    }

    /**
     * Switches the base entry with the given entry.
     *
     * @param newEntry The new entry to switch to.
     */
    private void switchBase(CompletableReadResultEntry newEntry) {
        // Validations.
        assert newEntry.getStreamSegmentOffset() == this.baseEntry.get().getStreamSegmentOffset() : "new entry does not have the same StreamSegmentOffset";
        assert newEntry.getRequestedReadLength() == this.baseEntry.get().getRequestedReadLength() : "new entry does not have the same RequestedReadLength";
        log.trace("Replaced {} with {} due to retryable exception being caught.", this.baseEntry.get(), newEntry);

        // Do the swap.
        newEntry.setCompletionCallback(this.baseEntry.get().getCompletionCallback());
        this.baseEntry.set(newEntry);
        this.adjustedOffset.set(newEntry.getStreamSegmentOffset());

        // Connect the new base entry to our result.
        linkBaseEntryToResult();
    }

    /**
     * Determines whether the given exception is retryable.
     *
     * @param ex The exception to inspect.
     */
    private boolean isRetryable(Throwable ex) {
        return ex instanceof ObjectClosedException // StorageReader was closed before execution began.
                || ex instanceof CancellationException // StorageReader was closed during execution (or queueing).
                || ex instanceof StreamSegmentNotExistsException; // Transaction Segment has already been deleted.
    }

    @Override
    public String toString() {
        return String.format("%s, AdjustedOffset = %s", this.baseEntry, this.adjustedOffset);
    }

    //endregion

    @FunctionalInterface
    public interface GetEntry extends BiFunction<Long, Integer, CompletableReadResultEntry> {
    }
}

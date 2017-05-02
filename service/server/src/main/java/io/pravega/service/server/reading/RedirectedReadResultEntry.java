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

package io.pravega.service.server.reading;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.service.contracts.ReadResultEntryContents;
import io.pravega.service.contracts.ReadResultEntryType;
import io.pravega.service.contracts.StreamSegmentNotExistsException;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

import lombok.extern.slf4j.Slf4j;

/**
 * A ReadResultEntry that wraps an inner Entry, but allows for offset adjustment. Useful for returning read results
 * that point to Transaction Read Indices.
 */
@Slf4j
class RedirectedReadResultEntry implements CompletableReadResultEntry {
    //region Members

    private static final Duration RETRY_TIMEOUT = Duration.ofSeconds(30); // TODO: these two should either be dynamic or configurable.
    private static final Duration EXCEPTION_DELAY = Duration.ofMillis(1000);
    private final CompletableReadResultEntry firstEntry;
    private final long adjustedOffset;
    private CompletableReadResultEntry secondEntry;
    private final CompletableFuture<ReadResultEntryContents> result;
    private final GetEntry retryGetEntry;
    private final ScheduledExecutorService executorService;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RedirectedReadResultEntry class.
     *
     * @param entry            The CompletableReadResultEntry to wrap.
     * @param offsetAdjustment The amount to adjust the offset by. This value, added to the entry's SegmentOffset, should
     *                         equal the offset in the Parent Segment where the read is thought to be at.
     * @param retryGetEntry    A BiFunction to invoke when needing to retry an entry. First argument: offset, Second: length.
     * @param executorService  An executor service to execute background operations on.
     */
    RedirectedReadResultEntry(CompletableReadResultEntry entry, long offsetAdjustment, GetEntry retryGetEntry, ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(entry, "entry");
        Preconditions.checkNotNull(retryGetEntry, "retryGetEntry");
        Preconditions.checkNotNull(executorService, "executorService");
        this.firstEntry = entry;
        this.adjustedOffset = entry.getStreamSegmentOffset() + offsetAdjustment;
        Preconditions.checkArgument(this.adjustedOffset >= 0, "Given offset adjustment would result in a negative offset.");
        this.retryGetEntry = retryGetEntry;
        this.executorService = executorService;
        if (FutureHelpers.isSuccessful(entry.getContent())) {
            this.result = entry.getContent();
        } else {
            this.result = new CompletableFuture<>();
            linkFirstEntryToResult();
        }
    }

    private void linkFirstEntryToResult() {
        this.firstEntry.getContent()
                       .thenAccept(this.result::complete)
                       .exceptionally(ex -> {
                           FutureHelpers.delayedFuture(getExceptionDelay(ex), this.executorService)
                                        .thenAccept(v -> handleGetContentFailure(ex));
                           return null;
                       });
    }

    //endregion

    //region ReadResultEntry Implementation

    @Override
    public long getStreamSegmentOffset() {
        return this.adjustedOffset;
    }

    @Override
    public int getRequestedReadLength() {
        return this.firstEntry.getRequestedReadLength();
    }

    @Override
    public ReadResultEntryType getType() {
        return this.firstEntry.getType();
    }

    @Override
    public CompletableFuture<ReadResultEntryContents> getContent() {
        return this.result;
    }

    @Override
    public void requestContent(Duration timeout) {
        try {
            this.firstEntry.requestContent(timeout);
        } catch (Throwable ex) {
            if (!handle(ex, timeout)) {
                // Unable to swap or ineligible exception; rethrow immediately.
                throw ex;
            }
        }
    }

    @Override
    public void setCompletionCallback(CompletionConsumer completionCallback) {
        getActiveEntry().setCompletionCallback(completionCallback);
    }

    @Override
    public CompletionConsumer getCompletionCallback() {
        return getActiveEntry().getCompletionCallback();
    }

    //endregion

    //region Helpers

    protected Duration getExceptionDelay(Throwable ex) {
        boolean requiresDelay = this.secondEntry == null && ExceptionHelpers.getRealException(ex) instanceof StreamSegmentNotExistsException;
        return requiresDelay ? EXCEPTION_DELAY : Duration.ZERO;
    }

    /**
     * Handles an exception that was caught. If this is the first exception ever caught, and it is eligible for retries,
     * then this method will invoke the retryGetEntry that was passed through the constructor to get a new entry.
     * If that succeeds, the new entry is then used to serve up the result.
     * <p>
     * The new entry will only be accepted if it is not a RedirectedReadResultEntry (since that is likely to get us in the
     * same situation again).
     *
     * @param ex      The exception to inspect.
     * @param timeout Timeout for the operation (a new call to requestContent will be made).
     * @return True if the exception was handled properly and the base entry swapped, false otherwise.
     */
    private boolean handle(Throwable ex, Duration timeout) {
        ex = ExceptionHelpers.getRealException(ex);
        if (this.secondEntry == null && isRetryable(ex)) {
            // This is the first attempt and we caught a retry-eligible exception; issue the query for the new entry.
            CompletableReadResultEntry newEntry = this.retryGetEntry.apply(getStreamSegmentOffset(), this.firstEntry.getRequestedReadLength());
            if (!(newEntry instanceof RedirectedReadResultEntry)) {
                // Request the content for the new entry (if that fails, we do not change any state).
                newEntry.requestContent(timeout);
                assert newEntry.getStreamSegmentOffset() == this.adjustedOffset : "new entry's StreamSegmentOffset does not match the adjusted offset of this entry";
                assert newEntry.getRequestedReadLength() == this.firstEntry.getRequestedReadLength() : "new entry does not have the same RequestedReadLength";

                // After all checks are done, update the internal state to use the new entry.
                newEntry.setCompletionCallback(this.firstEntry.getCompletionCallback());
                this.secondEntry = newEntry;
                setOutcomeAfterSecondEntry();
                return true;
            }
        }

        return false;
    }

    /**
     * Handles an exception that was caught in a callback from getContent. This does the same steps as handle(), but
     * it is invoked from an exceptionally() callback, so proper care has to be taken in order to guarantee that
     * the result future will complete one way or another.
     */
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
            setOutcomeAfterSecondEntry();
        } else {
            // Unable to switch.
            this.result.completeExceptionally(ex);
        }
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

    private CompletableReadResultEntry getActiveEntry() {
        return this.secondEntry != null ? this.secondEntry : this.firstEntry;
    }

    private void setOutcomeAfterSecondEntry() {
        CompletableFuture<ReadResultEntryContents> sourceFuture = this.secondEntry.getContent();
        sourceFuture.thenAccept(this.result::complete);
        FutureHelpers.exceptionListener(sourceFuture, this.result::completeExceptionally);
    }

    @Override
    public String toString() {
        return String.format("%s, AdjustedOffset = %s", getActiveEntry(), this.adjustedOffset);
    }

    //endregion

    @FunctionalInterface
    public interface GetEntry extends BiFunction<Long, Integer, CompletableReadResultEntry> {
    }
}

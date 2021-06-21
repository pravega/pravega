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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/**
 * A ReadResultEntry that wraps an inner Entry, but allows for offset adjustment. Useful for returning read results
 * that point to Transaction Read Indices.
 */
class RedirectedReadResultEntry implements CompletableReadResultEntry {
    //region Members

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private final CompletableReadResultEntry firstEntry;
    private final long adjustedOffset;
    private CompletableReadResultEntry secondEntry;
    private TimeoutTimer timer;
    private final CompletableFuture<BufferView> result;
    private final GetEntry retryGetEntry;
    private final long redirectedSegmentId;
    private volatile boolean failed = false;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RedirectedReadResultEntry class.
     *
     * @param entry               The CompletableReadResultEntry to wrap.
     * @param offsetAdjustment    The amount to adjust the offset by. This value, added to the entry's SegmentOffset, should
     *                            equal the offset in the Parent Segment where the read is thought to be at.
     * @param retryGetEntry       A BiFunction to invoke when needing to retry an entry. First argument: offset, Second: length.
     * @param redirectedSegmentId The Id of the redirected Segment's read index.
     */
    RedirectedReadResultEntry(CompletableReadResultEntry entry, long offsetAdjustment, GetEntry retryGetEntry, long redirectedSegmentId) {
        this.firstEntry = Preconditions.checkNotNull(entry, "entry");
        this.adjustedOffset = entry.getStreamSegmentOffset() + offsetAdjustment;
        Preconditions.checkArgument(this.adjustedOffset >= 0, "Given offset adjustment would result in a negative offset.");
        this.retryGetEntry = Preconditions.checkNotNull(retryGetEntry, "retryGetEntry");
        this.redirectedSegmentId = redirectedSegmentId;
        if (Futures.isSuccessful(entry.getContent())) {
            this.result = entry.getContent();
        } else {
            this.result = new CompletableFuture<>();
            linkFirstEntryToResult();
        }
    }

    private void linkFirstEntryToResult() {
        this.firstEntry.getContent()
                       .thenAccept(this.result::complete)
                       .exceptionally(this::handleGetContentFailure);
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
    public CompletableFuture<BufferView> getContent() {
        return this.result;
    }

    @Override
    public void requestContent(Duration timeout) {
        this.timer = new TimeoutTimer(timeout);
        try {
            this.firstEntry.requestContent(timer.getRemaining());
        } catch (Throwable ex) {
            if (!handle(ex)) {
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

    @Override
    public void fail(Throwable ex) {
        // This is usually invoked by the StreamSegmentReadResult - we must set a flag to prevent certain failures from
        // automatically re-triggering the query for the second entry.
        this.failed = true;
        this.result.completeExceptionally(ex);
        getActiveEntry().fail(ex);
    }

    //endregion

    //region Helpers

    /**
     * Handles an exception that was caught. If this is the first exception ever caught, and it is eligible for retries,
     * then this method will invoke the retryGetEntry that was passed through the constructor to get a new entry.
     * If that succeeds, the new entry is then used to serve up the result.
     * <p>
     * The new entry will only be accepted if it is not a RedirectedReadResultEntry (since that is likely to get us in the
     * same situation again).
     *
     * @param ex      The exception to inspect.
     * @return True if the exception was handled properly and the base entry swapped, false otherwise.
     */
    private boolean handle(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        if (!this.failed && this.secondEntry == null && isRetryable(ex)) {
            // This is the first attempt and we caught a retry-eligible exception; issue the query for the new entry.
            CompletableReadResultEntry newEntry = this.retryGetEntry.apply(getStreamSegmentOffset(), this.firstEntry.getRequestedReadLength(), this.redirectedSegmentId);
            if (!(newEntry instanceof RedirectedReadResultEntry)) {
                // Request the content for the new entry (if that fails, we do not change any state).
                newEntry.requestContent(this.timer == null ? DEFAULT_TIMEOUT : timer.getRemaining());
                assert newEntry.getStreamSegmentOffset() == this.adjustedOffset : "new entry's StreamSegmentOffset does not match the adjusted offset of this entry";

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
    private Void handleGetContentFailure(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        boolean success;
        try {
            success = handle(ex);
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

        return null;
    }

    /**
     * Determines whether the given exception is retryable.
     *
     * @param ex The exception to inspect.
     */
    private boolean isRetryable(Throwable ex) {
        return ex instanceof ObjectClosedException // StorageReadManager was closed before execution began.
                || ex instanceof CancellationException // StorageReadManager was closed during execution (or queueing).
                || ex instanceof StreamSegmentNotExistsException; // Transaction Segment has already been deleted.
    }

    private CompletableReadResultEntry getActiveEntry() {
        return this.secondEntry != null ? this.secondEntry : this.firstEntry;
    }

    private void setOutcomeAfterSecondEntry() {
        CompletableFuture<BufferView> sourceFuture = this.secondEntry.getContent();
        sourceFuture.thenAccept(this.result::complete);
        Futures.exceptionListener(sourceFuture, this.result::completeExceptionally);
    }

    @VisibleForTesting
    boolean hasSecondEntrySet() {
        return this.secondEntry != null;
    }

    @Override
    public String toString() {
        return String.format("%s, AdjustedOffset = %s", getActiveEntry(), this.adjustedOffset);
    }

    //endregion

    @FunctionalInterface
    public interface GetEntry {
        CompletableReadResultEntry apply(long readOffset, int requestedReadLength, long redirectedSegmentId);
    }
}

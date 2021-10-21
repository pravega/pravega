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
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import java.util.concurrent.CancellationException;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a Read Result from a Stream Segment. This is essentially an Iterator over smaller, continuous ReadResultEntries.
 */
@Slf4j
@ThreadSafe
@VisibleForTesting
public class StreamSegmentReadResult implements ReadResult {
    //region Members

    private final String traceObjectId;
    private final long streamSegmentStartOffset;
    private final int maxResultLength;
    private final NextEntrySupplier getNextItem;
    @GuardedBy("this")
    private CompletableReadResultEntry lastEntry;
    @GuardedBy("this")
    private int consumedLength;
    @GuardedBy("this")
    private boolean canRead;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private boolean copyOnRead;
    @GuardedBy("this")
    private int maxReadAtOnce;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentReadResult class.
     *
     * @param streamSegmentStartOffset The StreamSegment Offset where the ReadResult starts at.
     * @param maxResultLength          The maximum number of bytes to read.
     * @param getNextItem              A Bi-Function that returns the next ReadResultEntry to consume. The first argument
     *                                 is startOffset (long) and the second is remainingLength (int).
     * @param traceObjectId            Used for logging.
     * @throws NullPointerException     If getNextItem is null.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    public StreamSegmentReadResult(long streamSegmentStartOffset, int maxResultLength, @NonNull NextEntrySupplier getNextItem, String traceObjectId) {
        Exceptions.checkArgument(streamSegmentStartOffset >= 0, "streamSegmentStartOffset", "streamSegmentStartOffset must be a non-negative number.");
        Exceptions.checkArgument(maxResultLength >= 0, "maxResultLength", "maxResultLength must be a non-negative number.");
        this.traceObjectId = traceObjectId;
        this.streamSegmentStartOffset = streamSegmentStartOffset;
        this.maxResultLength = maxResultLength;
        this.maxReadAtOnce = this.maxResultLength;
        this.getNextItem = getNextItem;
        this.consumedLength = 0;
        this.canRead = true;

        // By default, all cache reads are to be copied into heap buffers before being served to calling code. This is
        // to avoid situations where there upstream code has pointers to evicted (and reallocated) cache blocks. If this
        // is not desired, then the upstream code should disable this and document WHY it is safe to do so.
        this.copyOnRead = true;
    }

    //endregion

    //region ReadResult Implementation

    @Override
    public long getStreamSegmentStartOffset() {
        return this.streamSegmentStartOffset;
    }

    @Override
    public int getMaxResultLength() {
        return this.maxResultLength;
    }

    @Override
    public synchronized int getConsumedLength() {
        return this.consumedLength;
    }

    @Override
    public synchronized boolean isCopyOnRead() {
        return this.copyOnRead;
    }

    @Override
    public synchronized void setCopyOnRead(boolean value) {
        this.copyOnRead = value;
    }

    @Override
    public synchronized int getMaxReadAtOnce() {
        return this.maxReadAtOnce;
    }

    @Override
    public synchronized void setMaxReadAtOnce(int value) {
        this.maxReadAtOnce = value <= 0 || value > this.maxResultLength ? this.maxResultLength : value;
    }

    @Override
    public synchronized boolean isClosed() {
        return this.closed || !hasNext();
    }

    @Override
    public String toString() {
        return String.format("Offset = %d, MaxLength = %d, Consumed = %d", this.streamSegmentStartOffset, this.maxResultLength, getConsumedLength());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        CompletableReadResultEntry lastEntry = null;
        synchronized (this) {
            if (!this.closed) {
                this.closed = true;
                lastEntry = this.lastEntry;
                this.lastEntry = null;
                log.trace("{}.ReadResult[{}]: Closed.", this.traceObjectId, this.streamSegmentStartOffset);
            }
        }

        // If we have already returned a result but it hasn't been consumed yet, cancel it, but make sure we do it
        // outside of the lock.
        if (lastEntry != null && !lastEntry.isDone()) {
            lastEntry.fail(new CancellationException(String.format("ReadResult[%s] closed.", this.traceObjectId)));
            log.trace("{}.ReadResult[{}]: Cancelled last entry '{}'.", this.traceObjectId, this.streamSegmentStartOffset, lastEntry);
        }
    }

    //endregion

    //region Iterator Implementation

    /**
     * Gets a value indicating whether we have further elements to process. All of the following must be true:
     * <ul>
     * <li> The StreamSegmentReadResult is not closed.
     * <li> We haven't reached the end of a sealed StreamSegment.
     * <li> We have at least one more byte to read.
     * </ul>
     */
    @Override
    public synchronized boolean hasNext() {
        return !this.closed && this.canRead && this.consumedLength < this.maxResultLength;
    }

    /**
     * Gets the next ReadResultEntry that exists in the ReadResult. This will return null if hasNext() indicates false
     * (as opposed from throwing a NoSuchElementException, like a general iterator).
     *
     * Notes:
     * <ul>
     * <li> Calls to next() will block until the ReadResultEntry.getContent() for the previous call to next() has been completed (normally or exceptionally).
     * <li> Due to a result from next() only being considered "consumed" after its getContent() is completed, it is possible
     * that hasNext() will report true, but a subsequent call to next() will return null - that's because next() will wait
     * for the previous entry to complete and then do more processing.
     * </ul>
     *
     * @throws IllegalStateException If we have more elements, but the last element returned hasn't finished processing.
     */
    @Override
    public synchronized ReadResultEntry next() {
        Exceptions.checkNotClosed(this.closed, this);

        // If the previous entry hasn't finished yet, we cannot proceed.
        Preconditions.checkState(this.lastEntry == null || this.lastEntry.isDone(),
                "Cannot request a new entry when the previous one hasn't completed retrieval yet.");

        // Only check for hasNext now, after we have waited for the previous entry to finish - since that updates
        // some fields that hasNext relies on.
        if (!hasNext()) {
            return null;
        }

        // Retrieve the next item.
        long startOffset = this.streamSegmentStartOffset + this.consumedLength;
        int remainingLength = Math.min(this.maxReadAtOnce, this.maxResultLength - this.consumedLength);
        CompletableReadResultEntry entry = this.getNextItem.apply(startOffset, remainingLength, this.copyOnRead);

        if (entry == null) {
            assert remainingLength <= 0 : String.format("No ReadResultEntry received when one was expected. Offset %d, MaxLen %d.", startOffset, remainingLength);
            this.lastEntry = null;
        } else {
            assert entry.getStreamSegmentOffset() == startOffset : String.format("Invalid ReadResultEntry. Expected offset %d, given %d.", startOffset, entry.getStreamSegmentOffset());
            if (entry.getType().isTerminal()) {
                // We encountered Terminal Entry, which means we cannot read anymore using this ReadResult.
                // This can be the case if either the StreamSegment has been truncated beyond the current ReadResult offset,
                // or if the StreamSegment is now sealed and we have requested an offset that is beyond the StreamSegment
                // length. We cannot continue reading; close the ReadResult and return the appropriate Result Entry.
                // If we don't close the ReadResult, hasNext() will erroneously return true and next() will have undefined behavior.
                this.lastEntry = null;
                this.canRead = false;
            } else {
                // After the previous entry is done, update the consumedLength value.
                entry.setCompletionCallback(length -> {
                    synchronized (StreamSegmentReadResult.this) {
                        this.consumedLength += length;
                    }
                });
                this.lastEntry = entry;

                // Check, again, if we are closed. It is possible that this Result was closed after the last check
                // and before we got the lastEntryFuture. If this happened, throw the exception and don't return anything.
                Exceptions.checkNotClosed(this.closed, this);
            }
        }

        log.trace("{}.ReadResult[{}]: Consumed = {}, MaxLength = {}, Entry = ({}).", this.traceObjectId, this.streamSegmentStartOffset, this.consumedLength, this.maxResultLength, entry);
        return entry;
    }

    //endregion

    //region NextEntrySupplier

    /**
     * Defines a Function that given a startOffset (long), remainingLength (int) and whether to make a copy of any returned
     * cached data, returns the next entry to be consumed (CompletableReadResultEntry).
     */
    @FunctionalInterface
    public interface NextEntrySupplier {
        CompletableReadResultEntry apply(Long startOffset, Integer remainingLength, Boolean makeCopy);
    }

    //endregion
}

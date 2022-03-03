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
package io.pravega.segmentstore.server.writer;

import com.google.common.base.Preconditions;
import io.pravega.common.AbstractTimer;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.logs.operations.Operation;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Holds the current state for the StorageWriter.
 */
class WriterState {
    //region Members

    private final AtomicLong lastReadSequenceNumber;
    private final AtomicLong lastTruncatedSequenceNumber;
    private final AtomicBoolean lastIterationError;
    private final AtomicReference<Duration> currentIterationStartTime;
    private final AtomicLong iterationId;
    private final AtomicReference<ForceFlushContext> forceFlushContext;
    private final AtomicReference<Queue<Operation>> lastRead;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WriterState class.
     */
    WriterState() {
        this.lastReadSequenceNumber = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
        this.lastTruncatedSequenceNumber = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
        this.lastIterationError = new AtomicBoolean(false);
        this.currentIterationStartTime = new AtomicReference<>();
        this.forceFlushContext = new AtomicReference<>();
        this.iterationId = new AtomicLong();
        this.lastRead = new AtomicReference<>(null);
    }

    //endregion

    //region Properties

    /**
     * Records the fact that an iteration started.
     *
     * @param timer The reference timer.
     */
    void recordIterationStarted(AbstractTimer timer) {
        this.iterationId.incrementAndGet();
        this.currentIterationStartTime.set(timer.getElapsed());
        this.lastIterationError.set(false);
    }

    /**
     * Calculates the amount of time elapsed since the current iteration started.
     *
     * @param timer The reference timer.
     */
    Duration getElapsedSinceIterationStart(AbstractTimer timer) {
        return timer.getElapsed().minus(this.currentIterationStartTime.get());
    }

    /**
     * Gets a value indicating the id of the current iteration.
     */
    long getIterationId() {
        return this.iterationId.get();
    }

    /**
     * Gets a value indicating whether the last iteration had an error or not.
     */
    boolean getLastIterationError() {
        return this.lastIterationError.get();
    }

    /**
     * Records the fact that the current iteration had an error.
     */
    void recordIterationError() {
        this.lastIterationError.set(true);
    }

    /**
     * Gets a value indicating the Sequence Number of the last Truncated Operation.
     */
    long getLastTruncatedSequenceNumber() {
        return this.lastTruncatedSequenceNumber.get();
    }

    /**
     * Sets the Sequence Number of the last Truncated Operation.
     *
     * @param value The Sequence Number to set.
     */
    void setLastTruncatedSequenceNumber(long value) {
        Preconditions.checkArgument(value >= this.lastTruncatedSequenceNumber.get(), "New LastTruncatedSequenceNumber cannot be smaller than the previous one.");
        this.lastTruncatedSequenceNumber.set(value);
    }

    /**
     * Gets a value indicating the Sequence Number of the last read Operation (from the Operation Log).
     */
    long getLastReadSequenceNumber() {
        return this.lastReadSequenceNumber.get();
    }

    /**
     * Sets the Sequence Number of the last read Operation.
     *
     * @param value The Sequence Number to set.
     */
    void setLastReadSequenceNumber(long value) {
        Preconditions.checkArgument(value >= this.lastReadSequenceNumber.get(), "New LastReadSequenceNumber cannot be smaller than the previous one.");
        this.lastReadSequenceNumber.set(value);
        recordReadComplete();
    }

    /**
     * Indicates the fact that the {@link StorageWriter} has completed reading.
     */
    void recordReadComplete() {
        val ffc = this.forceFlushContext.get();
        if (ffc != null) {
            ffc.setLastReadSequenceNumber(this.lastReadSequenceNumber.get());
        }
    }

    /**
     * Indicates the fact that the {@link StorageWriter} has completed a flush stage.
     *
     * @param result The {@link WriterFlushResult} summarizing the flush stage.
     */
    void recordFlushComplete(WriterFlushResult result) {
        val ffc = this.forceFlushContext.get();
        if (ffc != null && ffc.flushComplete(result)) {
            this.forceFlushContext.set(null);
            ffc.getCompletion().complete(ffc.isAnythingFlushed());
        }
    }

    /**
     * Configures a Force Flush for all Operations up to, and including, the given Sequence Number.
     *
     * @param upToSequenceNumber The Sequence Number to configure the Force Flush to.
     * @return A CompletableFuture, that, when completed, will indicate that all Operations up to, and including the
     * given Sequence Number have been flushed.
     */
    CompletableFuture<Boolean> setForceFlush(long upToSequenceNumber) {
        if (upToSequenceNumber <= getLastTruncatedSequenceNumber()) {
            // The desired seq no has already been acknowledged.
            return CompletableFuture.completedFuture(false);
        }

        val context = new ForceFlushContext(upToSequenceNumber);
        Preconditions.checkState(this.forceFlushContext.compareAndSet(null, context), "Another force-flush is in progress.");
        return context.getCompletion();
    }

    /**
     * Gets a value indicating whether a Forced Flush is pending.
     *
     * @return True if a Force Flush is pending.
     */
    boolean isForceFlush() {
        return this.forceFlushContext.get() != null;
    }

    /**
     * Gets the Last Read operations.
     *
     * @return A {@link Queue} containing the last read (and unprocessed) Operations, or null if there are no such Operations.
     */
    Queue<Operation> getLastRead() {
        Queue<Operation> result = this.lastRead.get();
        if (result != null && result.isEmpty()) {
            this.lastRead.compareAndSet(result, null);
            return null;
        }

        return result;
    }

    /**
     * Sets the Last Read operations.
     *
     * @param lastRead A {@link Queue} containing the last read Operations.
     * @return {@code lastRead}.
     */
    Queue<Operation> setLastRead(Queue<Operation> lastRead) {
        this.lastRead.set(lastRead);
        return lastRead;
    }

    @Override
    public String toString() {
        return String.format("LastRead=%s, LastTruncate=%s, Error=%s", this.lastReadSequenceNumber, this.lastTruncatedSequenceNumber, this.lastIterationError);
    }

    //endregion

    //region ForceFlushContext

    @RequiredArgsConstructor
    private static class ForceFlushContext {
        private final long upToSequenceNumber;
        @GuardedBy("this")
        private long lastReadSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
        @GuardedBy("this")
        private boolean anythingFlushed = false;
        @Getter
        private final CompletableFuture<Boolean> completion = new CompletableFuture<>();

        /**
         * Records the last read sequence number.
         *
         * @param lastReadSequenceNumber The last read sequence number.
         */
        synchronized void setLastReadSequenceNumber(long lastReadSequenceNumber) {
            this.lastReadSequenceNumber = lastReadSequenceNumber;
        }

        /**
         * Gets a value indicating whether anything got flushed.
         */
        synchronized boolean isAnythingFlushed() {
            return this.anythingFlushed;
        }

        /**
         * Indicates that a {@link StorageWriter} flush stage has completed.
         *
         * @param result The {@link WriterFlushResult} summarizing the flush stage.
         * @return True if a Force Flush has been completed as a result (irrespective of outcome), false otherwise.
         */
        synchronized boolean flushComplete(WriterFlushResult result) {
            if (this.lastReadSequenceNumber != Operation.NO_SEQUENCE_NUMBER && result.isAnythingFlushed()) {
                this.anythingFlushed = true;
            }

            return this.lastReadSequenceNumber >= this.upToSequenceNumber;
        }
    }

    //endregion
}
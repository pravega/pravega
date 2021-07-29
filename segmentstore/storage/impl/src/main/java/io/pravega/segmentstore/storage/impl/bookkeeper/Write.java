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
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.Timer;
import io.pravega.common.util.CompositeArrayView;
import io.pravega.segmentstore.storage.LogAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.val;

/**
 * A single Write in the BookKeeperLog Write Queue.
 */
class Write {
    //region Members

    @Getter
    private final ByteBuf data;
    @Getter
    private final int length;
    private final CompletableFuture<LogAddress> result;
    private final AtomicInteger attemptCount;
    private final AtomicReference<WriteLedger> writeLedger;
    private final AtomicLong entryId;
    private final AtomicReference<Timer> beginAttemptTimer;
    private final AtomicReference<Throwable> failureCause;
    @Getter
    @Setter
    private long queueAddedTimestamp;


    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Write class.
     *
     * @param data               An ArrayView representing the data to write.
     * @param initialWriteLedger The WriteLedger this write is initially assigned to.
     * @param result             A CompletableFuture that will be completed with the result (or failure cause) once this
     *                           Write is completed.
     */
    Write(@NonNull CompositeArrayView data, WriteLedger initialWriteLedger, CompletableFuture<LogAddress> result) {
        this.data = convertData(data);
        this.length = data.getLength();
        this.writeLedger = new AtomicReference<>(Preconditions.checkNotNull(initialWriteLedger, "initialWriteLedger"));
        this.result = Preconditions.checkNotNull(result, "result");
        this.attemptCount = new AtomicInteger();
        this.failureCause = new AtomicReference<>();
        this.entryId = new AtomicLong(Long.MIN_VALUE);
        this.beginAttemptTimer = new AtomicReference<>();
    }

    private ByteBuf convertData(CompositeArrayView data) {
        ByteBuf[] components = new ByteBuf[data.getComponentCount()];
        val index = new AtomicInteger();
        data.collect(bb -> components[index.getAndIncrement()] = Unpooled.wrappedBuffer(bb));
        return Unpooled.wrappedUnmodifiableBuffer(components);
    }

    //endregion

    //region Properties

    /**
     * Gets the WriteLedger associated with this write.
     *
     * @return The WriteLedger.
     */
    WriteLedger getWriteLedger() {
        return this.writeLedger.get();
    }

    /**
     * Sets the WriteLedger to be associated with this write.
     *
     * @param writeLedger The WriteLedger to associate.
     */
    void setWriteLedger(WriteLedger writeLedger) {
        this.writeLedger.set(writeLedger);
        this.entryId.set(Long.MIN_VALUE);
    }

    /**
     * Sets the assigned Ledger Entry Id. This should be set any time such information is available (regardless of whether
     * the Write failed or succeeded).
     *
     * @param value The value to assign.
     */
    void setEntryId(long value) {
        this.entryId.set(value);
    }

    /**
     * Gets the assigned Ledger Entry Id.
     *
     * @return The result.
     */
    long getEntryId() {
        return this.entryId.get();
    }

    /**
     * Records the fact that a new attempt to execute this write is begun.
     *
     * @return The current attempt number.
     */
    int beginAttempt() {
        Preconditions.checkState(this.beginAttemptTimer.compareAndSet(null, new Timer()), "Write already in progress. Cannot restart.");
        return this.attemptCount.incrementAndGet();
    }

    /**
     * Records the fact that an attempt to execute this write has ended.
     */
    private Timer endAttempt() {
        return this.beginAttemptTimer.getAndSet(null);
    }

    /**
     * Gets a value indicating whether an attempt to execute this write is in progress.
     *
     * @return True or false.
     */
    boolean isInProgress() {
        return this.beginAttemptTimer.get() != null;
    }

    /**
     * Gets a value indicating whether this write is completed (successfully or not).
     *
     * @return True or false.
     */
    boolean isDone() {
        return this.result.isDone();
    }

    /**
     * Gets the failure cause, if any.
     *
     * @return The failure cause.
     */
    Throwable getFailureCause() {
        return this.failureCause.get();
    }

    /**
     * Indicates that this write completed successfully. This will set the final result on the externalCompletion future.
     */
    Timer complete() {
        Preconditions.checkState(this.entryId.get() >= 0, "entryId not set; cannot complete Write.");
        this.failureCause.set(null);
        this.result.complete(new LedgerAddress(this.writeLedger.get().metadata, this.entryId.get()));
        return endAttempt();
    }

    /**
     * Indicates that this write failed.
     *
     * @param cause    The failure cause. If null, the previous failure cause is preserved.
     * @param complete If true, the externalCompletion will be immediately be completed with the current failure cause.
     *                 If false, no completion will be done.
     */
    void fail(Throwable cause, boolean complete) {
        if (cause != null) {
            this.failureCause.set(cause);
        }

        endAttempt();
        WriteLedger ledger = this.writeLedger.get();
        if (ledger != null && ledger.isRolledOver()) {
            // Rollovers aren't really failures (they're caused by us). In that case, do not count this failure as an attempt.
            this.attemptCount.updateAndGet(v -> Math.max(0, v - 1));
        }

        if (complete) {
            this.result.completeExceptionally(this.failureCause.get());
        }
    }

    @Override
    public String toString() {
        return String.format("LedgerId = %s, Length = %s, Attempts = %s, InProgress = %s, Done = %s, Failed %s",
                this.writeLedger.get().metadata.getLedgerId(), getLength(), this.attemptCount, isInProgress(),
                isDone(), this.failureCause.get() != null);
    }

    //endregion
}


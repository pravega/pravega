/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.storage.LogAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.Setter;
import org.apache.bookkeeper.client.LedgerHandle;

/**
 * A single Write in the BookKeeperLog Write Queue.
 */
class Write {
    //region Members

    final ArrayView data;
    private final CompletableFuture<LogAddress> result;
    private final AtomicInteger attemptCount;
    private final AtomicReference<WriteLedger> writeLedger;
    private final AtomicBoolean inProgress;
    private final AtomicReference<Throwable> failureCause;
    @Getter
    @Setter
    private long timestamp;

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
    Write(ArrayView data, WriteLedger initialWriteLedger, CompletableFuture<LogAddress> result) {
        this.data = Preconditions.checkNotNull(data, "data");
        this.writeLedger = new AtomicReference<>(Preconditions.checkNotNull(initialWriteLedger, "initialWriteLedger"));
        this.result = Preconditions.checkNotNull(result, "result");
        this.attemptCount = new AtomicInteger();
        this.failureCause = new AtomicReference<>();
        this.inProgress = new AtomicBoolean();
    }

    //endregion

    //region Properties

    /**
     * Gets the LedgerHandle associated with this write.
     */
    LedgerHandle getLedger() {
        return this.writeLedger.get().ledger;
    }

    /**
     * Gets the LedgerMetadata for the Ledger associated with this write.
     */
    LedgerMetadata getLedgerMetadata() {
        return this.writeLedger.get().metadata;
    }

    /**
     * Sets the WriteLedger to be associated with this write.
     *
     * @param writeLedger The WriteLedger to associate.
     */
    void setWriteLedger(WriteLedger writeLedger) {
        this.writeLedger.set(writeLedger);
    }

    /**
     * Records the fact that a new attempt to execute this write is begun.
     *
     * @return The current attempt number.
     */
    int beginAttempt() {
        Preconditions.checkState(this.inProgress.compareAndSet(false, true), "Write already in progress. Cannot restart.");
        return this.attemptCount.incrementAndGet();
    }

    /**
     * Records the fact that an attempt to execute this write has ended.
     *
     * @param rollback If true, it rolls back the attempt count.
     */
    void endAttempt(boolean rollback) {
        if (rollback && this.attemptCount.decrementAndGet() < 0) {
            this.attemptCount.set(0); // Make sure it doesn't become negative.
        }

        this.inProgress.set(false);
    }

    /**
     * Gets a value indicating whether an attempt to execute this write is in progress.
     *
     * @return True or false.
     */
    boolean isInProgress() {
        return this.inProgress.get();
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
     *
     * @param result The result to set.
     */
    void complete(LogAddress result) {
        this.failureCause.set(null);
        this.result.complete(result);
        endAttempt(false);
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
            Throwable e = this.failureCause.get();
            if (e != null && e != cause) {
                cause.addSuppressed(e);
            }

            this.failureCause.set(cause);
        }

        endAttempt(false);
        if (complete) {
            this.result.completeExceptionally(this.failureCause.get());
        }
    }

    @Override
    public String toString() {
        return String.format("LedgerId = %s, Length = %s, Attempts = %s, InProgress = %s, Done = %s, Failed %s",
                this.writeLedger.get().metadata.getLedgerId(), this.data.getLength(), this.attemptCount, this.inProgress,
                isDone(), this.failureCause.get() != null);
    }

    //endregion
}


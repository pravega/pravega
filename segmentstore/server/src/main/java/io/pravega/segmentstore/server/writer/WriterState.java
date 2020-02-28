/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.writer;

import com.google.common.base.Preconditions;
import io.pravega.common.AbstractTimer;
import io.pravega.segmentstore.server.logs.operations.Operation;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holds the current state for the StorageWriter.
 */
class WriterState {
    //region Members

    private final AtomicLong lastReadSequenceNumber;
    private final AtomicLong lastTruncatedSequenceNumber;
    private final AtomicBoolean lastIterationError;
    private final AtomicReference<TaskTracker> currentIterationTracker;
    private final AtomicLong iterationId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WriterState class.
     */
    WriterState() {
        this.lastReadSequenceNumber = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
        this.lastTruncatedSequenceNumber = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
        this.lastIterationError = new AtomicBoolean(false);
        this.currentIterationTracker = new AtomicReference<>();
        this.iterationId = new AtomicLong();
    }

    //endregion

    //region Properties

    /**
     * Records the fact that an iteration started.
     *
     * @param timer The reference timer.
     */
    void beginIteration(AbstractTimer timer) {
        this.iterationId.incrementAndGet();
        this.currentIterationTracker.set(new TaskTracker(timer));
        this.lastIterationError.set(false);
    }

    /**
     * Calculates the amount of time elapsed since the current iteration started.
     */
    Duration endIteration() {
        TaskTracker tracker = this.currentIterationTracker.getAndSet(null);
        return tracker == null ? Duration.ZERO : tracker.getElapsedSinceCreation();
    }

    /**
     * Gets a {@link TaskTracker} that is created every time {@link #beginIteration} is invoked and destroyed when
     * {@link #endIteration()}.
     *
     * @return A {@link TaskTracker}, or null if no iteration is active.
     */
    TaskTracker getIterationTracker() {
        return this.currentIterationTracker.get();
    }

    /**
     * Gets a {@link Duration} representing the highest amount of time for any pending task. If no tasks are pending or
     * there is no iteration active, this will return {@link Duration#ZERO}.
     * @return A {@link Duration}.
     */
    Duration getIterationIdleDuration() {
        TaskTracker t = getIterationTracker();
        return t == null ? Duration.ZERO : t.getLongestPendingDuration();
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
    }

    @Override
    public String toString() {
        return String.format("LastRead=%s, LastTruncate=%s, Error=%s", this.lastReadSequenceNumber, this.lastTruncatedSequenceNumber, this.lastIterationError);
    }

    //endregion
}
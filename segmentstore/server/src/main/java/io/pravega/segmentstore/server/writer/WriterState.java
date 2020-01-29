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

import io.pravega.common.AbstractTimer;
import io.pravega.segmentstore.server.logs.operations.Operation;
import com.google.common.base.Preconditions;

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
    private final AtomicReference<Duration> currentIterationStartTime;
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
        this.currentIterationStartTime = new AtomicReference<>();
        this.iterationId = new AtomicLong();
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
    }

    @Override
    public String toString() {
        return String.format("LastRead=%s, LastTruncate=%s, Error=%s", this.lastReadSequenceNumber, this.lastTruncatedSequenceNumber, this.lastIterationError);
    }

    //endregion
}
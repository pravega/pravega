/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import io.pravega.common.ExponentialMovingAverage;
import io.pravega.common.MathHelpers;
import io.pravega.common.util.ReusableLatch;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * See {@link AppendBatchSizeTracker}.
 * 
 * This implementation tracks three things:
 * 1. The time between appends
 * 2. The size of each append
 * 3. The number of unackedAppends there are outstanding
 * 
 * If the number of unacked appends is <= 1 batching is disabled. This improves latency for low volume and 
 * synchronous writers. Otherwise the batch size is set to the amount of data that will be written in the next
 * {@link #MAX_BATCH_TIME_MILLIS} or half the server round trip time (whichever is less)
 */
class AppendBatchSizeTrackerImpl implements AppendBatchSizeTracker {
    private static final int MAX_BATCH_TIME_MILLIS = 100;

    private final Supplier<Long> clock;
    private final AtomicLong lastAppendNumber;
    private final AtomicLong lastAppendTime;
    private final AtomicLong lastAckNumber;
    private final ExponentialMovingAverage eventSize = new ExponentialMovingAverage(1024, 0.1, true);
    private final ExponentialMovingAverage millisBetweenAppends = new ExponentialMovingAverage(10, 0.1, false);
    private final ExponentialMovingAverage appendsOutstanding = new ExponentialMovingAverage(2, 0.05, false);
    
    private static final long BACK_PREASURE_THREASHOLD = MAX_BATCH_SIZE;
    
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ReusableLatch appendLatch = new ReusableLatch(true);

    AppendBatchSizeTrackerImpl() {
        clock = System::currentTimeMillis;
        lastAppendTime = new AtomicLong(clock.get());
        lastAckNumber = new AtomicLong(0);
        lastAppendNumber = new AtomicLong(0);
    }

    @Override
    public void recordAppend(long eventNumber, int size) {
        long now = Math.max(lastAppendTime.get(), clock.get());
        long last = lastAppendTime.getAndSet(now);
        lastAppendNumber.set(eventNumber);
        millisBetweenAppends.addNewSample(now - last);
        long numOutstanding = eventNumber - lastAckNumber.get();
        appendsOutstanding.addNewSample(numOutstanding);
        eventSize.addNewSample(size);
        double dataOutstanding = eventSize.getCurrentValue() * numOutstanding;
        if (!closed.get() && dataOutstanding >= BACK_PREASURE_THREASHOLD) {
            appendLatch.reset();
        }
    }

    @Override
    public long recordAck(long eventNumber) {
        lastAckNumber.getAndSet(eventNumber);
        long outstandingAppendCount = lastAppendNumber.get() - eventNumber;
        appendsOutstanding.addNewSample(outstandingAppendCount);
        if (eventSize.getCurrentValue() * outstandingAppendCount < BACK_PREASURE_THREASHOLD) {
            appendLatch.release();
        }
        return outstandingAppendCount;
    }

    /**
     * Returns a block size that is an estimate of how much data will be written in the next
     * {@link #MAX_BATCH_TIME_MILLIS} or half the server round trip time (whichever is less).
     */
    @Override
    public int getAppendBlockSize() {
        long numInflight = lastAppendNumber.get() - lastAckNumber.get();
        if (numInflight <= 1) {
            return 0;
        }
        double appendsInMaxBatch = Math.max(1.0, MAX_BATCH_TIME_MILLIS / millisBetweenAppends.getCurrentValue());
        double targetAppendsOutstanding = MathHelpers.minMax(appendsOutstanding.getCurrentValue() * 0.5, 1.0,
                                                             appendsInMaxBatch);
        return (int) MathHelpers.minMax((long) (targetAppendsOutstanding * eventSize.getCurrentValue()), 0,
                                        MAX_BATCH_SIZE);
    }

    @Override
    public int getBatchTimeout() {
        return MAX_BATCH_TIME_MILLIS;
    }

    @Override
    public void waitForCapacity() {
        appendLatch.awaitUninterruptibly();
    }

    @Override
    public void close() {
        closed.set(true);
        appendLatch.release();
    }
}

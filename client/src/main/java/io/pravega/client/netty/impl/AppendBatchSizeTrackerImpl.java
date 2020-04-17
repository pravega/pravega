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
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
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
public class AppendBatchSizeTrackerImpl implements AppendBatchSizeTracker {
    private static final int MAX_BATCH_TIME_MILLIS = 100;
    private static final double NANOS_PER_MILLI = 1000000;
    
    private static final int BASE_TIME_NANOS = 660000;
    private static final int BASE_SIZE = 32 * 1024;
    private static final double OUTSTANDING_FRACTION = 0.66;
    
    private final Supplier<Long> clock;
    private final AtomicLong lastAppendNumber;
    private final AtomicLong lastAppendTime;
    private final AtomicLong lastAckNumber;
    private final ExponentialMovingAverage eventSize = new ExponentialMovingAverage(1024, 0.01, true);
    private final ExponentialMovingAverage nanosBetweenAppends = new ExponentialMovingAverage(10 * NANOS_PER_MILLI, 0.0001, false);
    private final ExponentialMovingAverage appendsOutstanding = new ExponentialMovingAverage(20, 0.0001, false);

    public AppendBatchSizeTrackerImpl() {
        clock = System::nanoTime;
        lastAppendTime = new AtomicLong(clock.get());
        lastAckNumber = new AtomicLong(0);
        lastAppendNumber = new AtomicLong(0);
    }

    @Override
    public void recordAppend(long eventNumber, int size) {
        long now = Math.max(lastAppendTime.get(), clock.get());
        long last = lastAppendTime.getAndSet(now);
        lastAppendNumber.set(eventNumber);
        nanosBetweenAppends.addNewSample(now - last);
        appendsOutstanding.addNewSample(eventNumber - lastAckNumber.get());
        eventSize.addNewSample(size);
    }

    @Override
    public long recordAck(long eventNumber) {
        lastAckNumber.getAndSet(eventNumber);
        long outstandingAppendCount = lastAppendNumber.get() - eventNumber;
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
        double appendsInTime = Math.max(1.0, BASE_TIME_NANOS / nanosBetweenAppends.getCurrentValue());
        double appendsInBatch = (appendsOutstanding.getCurrentValue() * OUTSTANDING_FRACTION + appendsInTime);
        int size = (int) (appendsInBatch * eventSize.getCurrentValue()) + BASE_SIZE;
        return MathHelpers.minMax(size, 0, MAX_BATCH_SIZE);
    }

    @Override
    public int getBatchTimeout() {
        return MAX_BATCH_TIME_MILLIS;
    }
}

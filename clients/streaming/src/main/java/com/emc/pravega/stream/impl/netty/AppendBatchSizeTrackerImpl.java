package com.emc.pravega.stream.impl.netty;

import com.emc.pravega.common.Clock;
import com.emc.pravega.common.ExponentialMovingAverage;
import com.emc.pravega.common.MathHelpers;
import com.emc.pravega.common.netty.AppendBatchSizeTracker;

import java.util.concurrent.atomic.AtomicLong;

class AppendBatchSizeTrackerImpl implements AppendBatchSizeTracker {
    private final int MAX_BATCH_SIZE = 32 * 1024;

    private final Clock clock;
    private final AtomicLong lastAppendTime;
    private final AtomicLong lastAckTime;
    private final ExponentialMovingAverage eventSize = new ExponentialMovingAverage(1024, 0.1, true);
    private final ExponentialMovingAverage millisBetweenAppends = new ExponentialMovingAverage(10, 0.1, true);
    private final ExponentialMovingAverage millisBetweenAcks = new ExponentialMovingAverage(10, 0.1, true);

    AppendBatchSizeTrackerImpl() {
        clock = new Clock();
        lastAppendTime = new AtomicLong(clock.get());
        lastAckTime = new AtomicLong(clock.get());
    }

    @Override
    public void noteAppend(int size) {
        Long now = clock.get();
        long last = lastAppendTime.getAndSet(now);
        millisBetweenAppends.addNewSample(now - last);
        eventSize.addNewSample(size);
    }

    @Override
    public void noteAck() {
        Long now = clock.get();
        long last = lastAckTime.getAndSet(now);
        millisBetweenAcks.addNewSample(now - last);
    }

    @Override
    public int getAppendBlockSize() {
        return (int) MathHelpers.minMax((long) ((millisBetweenAcks.getCurrentValue()
                / millisBetweenAppends.getCurrentValue() - 1) * eventSize.getCurrentValue()), 0, MAX_BATCH_SIZE);
    }
}

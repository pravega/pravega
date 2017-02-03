package com.emc.pravega.stream.impl.netty;

import com.emc.pravega.common.ExponentialMovingAverage;
import com.emc.pravega.common.MathHelpers;
import com.emc.pravega.common.netty.AppendBatchSizeTracker;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

class AppendBatchSizeTrackerImpl implements AppendBatchSizeTracker {
    private final int MAX_BATCH_TIME_MILLIS = 100;
    private final int TARGET_BATCH_TIME_MILLIS = 10;
    private final int MAX_BATCH_SIZE = 32 * 1024;

    private final Supplier<Long> clock;
    private final AtomicLong lastAppendNumber;
    private final AtomicLong lastAppendTime;
    private final AtomicLong lastAckNumber;
    private final ExponentialMovingAverage eventSize = new ExponentialMovingAverage(1024, 0.05, true);
    private final ExponentialMovingAverage millisBetweenAppends = new ExponentialMovingAverage(10, 0.1, false);

    AppendBatchSizeTrackerImpl() {
        clock = System::currentTimeMillis;
        lastAppendTime = new AtomicLong(clock.get());
        lastAckNumber = new AtomicLong(0);
        lastAppendNumber = new AtomicLong(0);
    }

    @Override
    public void noteAppend(long eventNumber, int size) {
        long now = Math.max(lastAppendTime.get(), clock.get());
        long last = lastAppendTime.getAndSet(now);
        lastAppendNumber.set(eventNumber);
        millisBetweenAppends.addNewSample(now - last);
        eventSize.addNewSample(size);
    }

    @Override
    public void noteAck(long eventNumber) {
        lastAckNumber.getAndSet(eventNumber);
    }

    @Override
    public int getAppendBlockSize() {
        long numInflight = lastAppendNumber.get() - lastAckNumber.get();
        if (numInflight <= 1) {
            return 0;
        }
        return (int) MathHelpers.minMax((long) (TARGET_BATCH_TIME_MILLIS / millisBetweenAppends.getCurrentValue()
                * eventSize.getCurrentValue()), 0, MAX_BATCH_SIZE);
    }

    @Override
    public int getBatchTimeout() {
        return MAX_BATCH_TIME_MILLIS;
    }
}

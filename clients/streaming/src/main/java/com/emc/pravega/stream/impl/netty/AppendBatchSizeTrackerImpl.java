/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.netty;

import com.emc.pravega.common.ExponentialMovingAverage;
import com.emc.pravega.common.MathHelpers;
import com.emc.pravega.common.netty.AppendBatchSizeTracker;

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
 * If the number of unacked appends is <= 1 batching is disabled. This improves latency for low volume and synchronus writers.
 * Otherwise the batch size is set to the amount of data that will be written in the next {@link #TARGET_BATCH_TIME_MILLIS}
 */
class AppendBatchSizeTrackerImpl implements AppendBatchSizeTracker {
    private static final int MAX_BATCH_TIME_MILLIS = 100;
    private static final int TARGET_BATCH_TIME_MILLIS = 10;
    private static final int MAX_BATCH_SIZE = 32 * 1024;

    private final Supplier<Long> clock;
    private final AtomicLong lastAppendNumber;
    private final AtomicLong lastAppendTime;
    private final AtomicLong lastAckNumber;
    private final ExponentialMovingAverage eventSize = new ExponentialMovingAverage(1024, 0.1, true);
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

    /**
     * Returns a block size that in an estimate of how much data will be written in the next {@link #TARGET_BATCH_TIME_MILLIS}
     */
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

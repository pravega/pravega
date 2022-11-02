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
package io.pravega.client.connection.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.AbstractTimer;
import io.pravega.common.ExponentialMovingAverage;
import io.pravega.common.MathHelpers;
import io.pravega.common.util.EnvVars;
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
 * If the number of unacked appends is &lt;= 1 batching is disabled. This improves latency for low volume and
 * synchronous writers. Otherwise the batch size is set to the amount of data that will be written in the next
 * {@link #MAX_BATCH_TIME_MILLIS} or half the server round trip time (whichever is less)
 */
public class AppendBatchSizeTrackerImpl implements AppendBatchSizeTracker {
    
    // This must be less than WireCommands.MAX_WIRECOMMAND_SIZE / 2;
    @VisibleForTesting
    static final int MAX_BATCH_SIZE = EnvVars.readIntegerFromEnvVar("PRAVEGA_MAX_BATCH_SIZE",
                                                                 2 * TcpClientConnection.TCP_BUFFER_SIZE - 1024);
    @VisibleForTesting
    static final int BASE_TIME_NANOS = EnvVars.readIntegerFromEnvVar("PRAVEGA_BATCH_BASE_TIME_NANOS", 0);
    @VisibleForTesting
    static final int BASE_SIZE = EnvVars.readIntegerFromEnvVar("PRAVEGA_BATCH_BASE_SIZE", 0);
    @VisibleForTesting
    static final double OUTSTANDING_FRACTION = 1.0 / EnvVars.readIntegerFromEnvVar("PRAVEGA_BATCH_OUTSTANDING_DENOMINATOR", 2);
    
    private static final double NANOS_PER_MILLI = AbstractTimer.NANOS_TO_MILLIS;
    
    private final Supplier<Long> clock;
    private final AtomicLong lastAppendNumber;
    private final AtomicLong lastAppendTime;
    private final AtomicLong lastAckNumber;
    private final ExponentialMovingAverage eventSize = new ExponentialMovingAverage(1024, 0.01, true);
    private final ExponentialMovingAverage nanosBetweenAppends = new ExponentialMovingAverage(10 * NANOS_PER_MILLI, 0.001, false);
    private final ExponentialMovingAverage appendsOutstanding = new ExponentialMovingAverage(20, 0.001, false);

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
        eventSize.addNewSample(size);
    }

    @Override
    public long recordAck(long eventNumber) {
        lastAckNumber.getAndSet(eventNumber);
        long outstandingAppendCount = lastAppendNumber.get() - eventNumber;
        appendsOutstanding.addNewSample(outstandingAppendCount);
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
        double nanosPerAppend = nanosBetweenAppends.getCurrentValue();
        double appendsInMaxBatchTime = Math.max(1.0, (MAX_BATCH_TIME_MILLIS * NANOS_PER_MILLI) / nanosPerAppend);
        double appendsInTime = Math.max(0.0, BASE_TIME_NANOS / nanosPerAppend);
        double appendsInBatch = MathHelpers.minMax(appendsOutstanding.getCurrentValue() * OUTSTANDING_FRACTION + appendsInTime, 1.0, appendsInMaxBatchTime);
        int size = (int) (appendsInBatch * eventSize.getCurrentValue()) + BASE_SIZE;
        return MathHelpers.minMax(size, 0, MAX_BATCH_SIZE);
    }

    @Override
    public int getBatchTimeout() {
        return MAX_BATCH_TIME_MILLIS;
    }
}

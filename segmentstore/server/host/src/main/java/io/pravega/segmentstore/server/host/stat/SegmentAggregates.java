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
package io.pravega.segmentstore.server.host.stat;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.shared.segment.ScaleType;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;

/**
 * This class is meant to compute and store aggregates per segment.
 * It have two entry points to receive traffic information - 1. upadte 2. updateTx.
 * Update method is called whenever normal traffic for a segment is received.
 * The method takes incoming traffic volume and adjusts four different rates over varied durations
 * using the new input.
 * The rates are Exponential Weighted moving averages. These averages include new values into the calculated rate
 * by applying an exponential weight. Each of four rates are over different durations and have different alpha factor
 * for exponential weighing.
 */
@ThreadSafe
abstract class SegmentAggregates {

    private static final int SECONDS_PER_MINUTE = 60;

    private static final int INTERVAL_IN_SECONDS = 5;
    private static final long TICK_INTERVAL = Duration.ofSeconds(5).toMillis();

    /**
     * Exponential weights.
     */
    private static final double M2_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 2);
    private static final double M5_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 5);
    private static final double M10_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 10);
    private static final double M20_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 20);

    private final AtomicLong lastReportedTime;

    @GuardedBy("this")
    private int targetRate;

    /**
     * Rates for Scale up.
     */
    @GuardedBy("this")
    private double twoMinuteRate;
    @GuardedBy("this")
    private double fiveMinuteRate;
    @GuardedBy("this")
    private double tenMinuteRate;

    /**
     * Rate for Scale down = 8 bytes.
     */
    @GuardedBy("this")
    private double twentyMinuteRate;

    /**
     * Start time and last ticked time.
     */
    @Getter
    private final long startTime;
    @GuardedBy("this")
    private long lastTick;
    @GuardedBy("this")
    private long currentCount;

    @VisibleForTesting
    SegmentAggregates(int targetRate) {
        this.targetRate = targetRate;
        this.startTime = getTimeMillis();
        this.lastReportedTime = new AtomicLong(this.startTime);
        this.lastTick = this.startTime;
        this.currentCount = 0;
        this.twoMinuteRate = 0.0;
        this.fiveMinuteRate = 0.0;
        this.tenMinuteRate = 0.0;
        this.twentyMinuteRate = 0.0;
    }

    static SegmentAggregates forPolicy(ScaleType scaleType, int targetRate) {
        switch (scaleType) {
            case EventRate:
                return new ByEventCount(targetRate);
            case Throughput:
                return new ByThroughput(targetRate);
            default:
                return new Fixed(targetRate);
        }
    }

    @VisibleForTesting
    protected long getTimeMillis() {
        return System.currentTimeMillis();
    }

    @VisibleForTesting
    synchronized long getCurrentCount() {
        return currentCount;
    }

    abstract ScaleType getScaleType();

    protected abstract long getUpdateCountDelta(long dataLength, int numOfEvents);

    protected abstract double getRate(double rate);

    boolean isScalingEnabled() {
        return true;
    }

    synchronized boolean update(long dataLength, int numOfEvents) {
        if (isScalingEnabled()) {
            currentCount += getUpdateCountDelta(dataLength, numOfEvents);
            final long newTick = getTimeMillis();
            final long age = newTick - lastTick;
            if (age > TICK_INTERVAL) {
                lastTick = newTick;
                final long count = currentCount;
                currentCount = 0;
                long iterations = age / TICK_INTERVAL;
                // If the age is greater than tick interval, then account for silent periods between last 
                // reported update and current update by calling the decay function for all silent tick intervals
                // with event count as 0 for them.
                for (long i = 0; i < iterations - 1; i++) {
                    computeDecay(0, TICK_INTERVAL / 1000.0);
                }
                double duration = (age - ((iterations - 1) * TICK_INTERVAL)) / 1000.0;
                computeDecay(count, duration);
            }

            return true;
        }

        return false;
    }

    boolean updateTx(long dataSize, int numOfEvents, long txnCreationTime) {
        long durationInMillis = getTimeMillis() - txnCreationTime;

        if (durationInMillis < TICK_INTERVAL) {
            // Not large enough lifespan for transaction. Include in regular traffic.
            return update(dataSize, numOfEvents);
        } else {
            assert durationInMillis > 0;
            int amortizedNumOfEvents = (int) (numOfEvents * TICK_INTERVAL) / (int) durationInMillis;
            return update((dataSize * TICK_INTERVAL) / durationInMillis, amortizedNumOfEvents);
        }
    }

    @GuardedBy("this")
    private void computeDecay(long count, double duration) {
        twoMinuteRate = decayingRate(count, twoMinuteRate, M2_ALPHA, duration);
        fiveMinuteRate = decayingRate(count, fiveMinuteRate, M5_ALPHA, duration);
        tenMinuteRate = decayingRate(count, tenMinuteRate, M10_ALPHA, duration);
        twentyMinuteRate = decayingRate(count, twentyMinuteRate, M20_ALPHA, duration);
    }

    private double decayingRate(long count, double rate, double alpha, double interval) {
        final double instantRate = count / interval;
        if (rate == 0) {
            return instantRate;
        } else {
            return rate + (alpha * (instantRate - rate));
        }
    }

    synchronized int getTargetRate() {
        return targetRate;
    }

    synchronized void setTargetRate(int value) {
        targetRate = value;
    }

    synchronized double getTwoMinuteRate() {
        return getRate(twoMinuteRate);
    }

    synchronized double getFiveMinuteRate() {
        return getRate(fiveMinuteRate);
    }

    synchronized double getTenMinuteRate() {
        return getRate(tenMinuteRate);
    }

    synchronized double getTwentyMinuteRate() {
        return getRate(twentyMinuteRate);
    }

    boolean reportIfNeeded(Duration reportingDuration) {
        long now = getTimeMillis();
        long newValue = lastReportedTime.updateAndGet(prev -> {
            if (now - prev > reportingDuration.toMillis()) {
                return now;
            }

            return prev;
        });
        return now == newValue;

    }

    //region Implementing classes

    static class ByThroughput extends SegmentAggregates {
        ByThroughput(int targetRate) {
            super(targetRate);
        }

        @Override
        public ScaleType getScaleType() {
            return ScaleType.Throughput;
        }

        @Override
        protected long getUpdateCountDelta(long dataLength, int numOfEvents) {
            return dataLength;
        }

        @Override
        protected double getRate(double rate) {
            return rate / 1024;
        }
    }

    static class ByEventCount extends SegmentAggregates {
        ByEventCount(int targetRate) {
            super(targetRate);
        }

        @Override
        public ScaleType getScaleType() {
            return ScaleType.EventRate;
        }

        @Override
        protected long getUpdateCountDelta(long dataLength, int numOfEvents) {
            return numOfEvents;
        }

        @Override
        protected double getRate(double rate) {
            return rate;
        }
    }

    static class Fixed extends SegmentAggregates {
        Fixed(int targetRate) {
            super(targetRate);
        }

        @Override
        public ScaleType getScaleType() {
            return ScaleType.NoScaling;
        }

        @Override
        public boolean isScalingEnabled() {
            return false;
        }

        @Override
        protected long getUpdateCountDelta(long dataLength, int numOfEvents) {
            return 0;
        }

        @Override
        protected double getRate(double rate) {
            return rate;
        }
    }

    //endregion
}

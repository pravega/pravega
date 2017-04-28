/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.server.host.stat;

import io.pravega.shared.protocol.netty.WireCommands;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

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
class SegmentAggregates {

    private static final int SECONDS_PER_MINUTE = 60;

    private static final int INTERVAL_IN_SECONDS = 5;
    private static final long TICK_INTERVAL = Duration.ofSeconds(5).toNanos();

    /**
     * Exponential weights.
     */
    private static final double M2_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 2);
    private static final double M5_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 5);
    private static final double M10_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 10);
    private static final double M20_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 20);

    // Amount of data stored in each aggregate object in memory = 77 bytes + object overhead

    /**
     * 8 bytes.
     */
    AtomicLong lastReportedTime;

    /**
     * Policy = 5 bytes.
     */
    @Getter
    private byte scaleType;

    @Setter
    @Getter
    private int targetRate;

    /**
     * Rates for Scale up = 24 bytes.
     */
    private AtomicLong twoMinuteRate;
    private AtomicLong fiveMinuteRate;
    private AtomicLong tenMinuteRate;

    /**
     * Rate for Scale down = 8 bytes.
     */
    private AtomicLong twentyMinuteRate;

    /**
     * Start time and last ticked time.
     * 16 bytes.
     */
    @Getter
    private long startTime;

    /**
     * 8 bytes.
     */
    private AtomicLong lastTick;

    /**
     * 8 bytes.
     */
    private AtomicLong currentCount;

    SegmentAggregates(byte scaleType, int targetRate) {
        this.targetRate = targetRate;
        this.scaleType = scaleType;
        this.startTime = System.currentTimeMillis();
        this.lastReportedTime = new AtomicLong(System.currentTimeMillis());
        this.lastTick = new AtomicLong(System.nanoTime());
        this.currentCount = new AtomicLong(Double.doubleToLongBits(0.0));
        this.twoMinuteRate = new AtomicLong(Double.doubleToLongBits(0.0));
        this.fiveMinuteRate = new AtomicLong(Double.doubleToLongBits(0.0));
        this.tenMinuteRate = new AtomicLong(Double.doubleToLongBits(0.0));
        this.twentyMinuteRate = new AtomicLong(Double.doubleToLongBits(0.0));
    }

    @VisibleForTesting
    AtomicLong getCurrentCount() {
        return currentCount;
    }

    void update(long dataLength, int numOfEvents) {
        if (scaleType == WireCommands.CreateSegment.IN_KBYTES_PER_SEC) {
            currentCount.addAndGet(dataLength / 1024); // convert to kbps
        } else if (scaleType == WireCommands.CreateSegment.IN_EVENTS_PER_SEC) {
            currentCount.addAndGet(numOfEvents);
        } else {
            return;
        }

        final long newTick = System.nanoTime();
        final long age = newTick - lastTick.get();
        if (age > TICK_INTERVAL) {
            lastTick.set(newTick);
            final long count = currentCount.getAndSet(0);
            computeDecay(count, Duration.ofNanos(age).toMillis() / 1000);
        }
    }

    void updateTx(long dataSize, int numOfEvents, long txnCreationTime) {

        long amortizedPerTick = 0;
        long durationInSeconds = (System.currentTimeMillis() - txnCreationTime) / 1000;
        long numOfTicks = (int) durationInSeconds / INTERVAL_IN_SECONDS;

        if (numOfTicks == 0) {
            // Not large enough lifespan for transaction. Include in regular traffic.
            update(dataSize, numOfEvents);
        } else {
            // Transaction lasted at least one tick internal (5 seconds) or more.
            // Amortize traffic over per tick and decay the rate accordingly.
            if (scaleType == WireCommands.CreateSegment.IN_KBYTES_PER_SEC) {
                amortizedPerTick = dataSize / numOfTicks;
            } else if (scaleType == WireCommands.CreateSegment.IN_EVENTS_PER_SEC) {
                amortizedPerTick = numOfEvents / numOfTicks;
            }

            for (int i = 0; i < numOfTicks; i++) {
                computeDecay(amortizedPerTick, INTERVAL_IN_SECONDS);
            }
        }
    }

    private void computeDecay(long count, long duration) {
        twoMinuteRate.getAndUpdate(prev -> Double.doubleToRawLongBits(decayingRate(count, Double.longBitsToDouble(prev), M2_ALPHA, duration)));
        fiveMinuteRate.getAndUpdate(prev -> Double.doubleToRawLongBits(decayingRate(count, Double.longBitsToDouble(prev), M5_ALPHA, duration)));
        tenMinuteRate.getAndUpdate(prev -> Double.doubleToRawLongBits(decayingRate(count, Double.longBitsToDouble(prev), M10_ALPHA, duration)));
        twentyMinuteRate.getAndUpdate(prev -> Double.doubleToRawLongBits(decayingRate(count, Double.longBitsToDouble(prev), M20_ALPHA, duration)));
    }

    private double decayingRate(long count, double rate, double alpha, long interval) {
        final double instantRate = (double) count / (double) interval;
        if (rate == 0) {
            return instantRate;
        } else {
            return rate + (alpha * (instantRate - rate));
        }
    }

    double getTwoMinuteRate() {
        return Double.longBitsToDouble(twoMinuteRate.get());
    }

    double getFiveMinuteRate() {
        return Double.longBitsToDouble(fiveMinuteRate.get());
    }

    double getTenMinuteRate() {
        return Double.longBitsToDouble(tenMinuteRate.get());
    }

    double getTwentyMinuteRate() {
        return Double.longBitsToDouble(twentyMinuteRate.get());
    }
}

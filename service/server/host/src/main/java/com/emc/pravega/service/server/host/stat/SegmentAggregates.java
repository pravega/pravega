/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host.stat;

import com.emc.pravega.common.netty.WireCommands;
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
 *
 * This class is not synchronization protected, so any computation is not thread safe.
 * This is done intentionally to not put performance overheads while doing these computations.
 * However, it has a drawback in loss of accuracy and we may compute rates with erring on lower side.
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
    @Getter
    private volatile double twoMinuteRate;
    @Getter
    private volatile double fiveMinuteRate;
    @Getter
    private volatile double tenMinuteRate;

    /**
     * Rate for Scale down = 8 bytes.
     */
    @Getter
    private volatile double twentyMinuteRate;

    /**
     * 16 bytes.
     */
    @Getter
    @Setter
    private long lastReportedTime;

    /**
     * Start time and last ticked time.
     * 16 bytes.
     */
    @Getter
    private long startTime;

    private AtomicLong lastTick;

    /**
     * 8 bytes.
     */
    // Note: we are not concurrency protecting this variable for performance reasons
    private AtomicLong currentCount;

    SegmentAggregates(byte scaleType, int targetRate) {
        this.targetRate = targetRate;
        this.scaleType = scaleType;
        startTime = System.currentTimeMillis();
        lastReportedTime = System.currentTimeMillis();
        this.lastTick = new AtomicLong(System.nanoTime());
        this.currentCount = new AtomicLong(0);
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

        if (scaleType == WireCommands.CreateSegment.IN_KBYTES_PER_SEC) {
            amortizedPerTick = dataSize / numOfTicks;
        } else if (scaleType == WireCommands.CreateSegment.IN_EVENTS_PER_SEC) {
            amortizedPerTick = numOfEvents / numOfTicks;
        }

        for (int i = 0; i < numOfEvents; i++) {
            computeDecay(amortizedPerTick, INTERVAL_IN_SECONDS);
        }
    }

    private void computeDecay(long count, long duration) {
        twoMinuteRate = decayingRate(count, twoMinuteRate, M2_ALPHA, duration);
        fiveMinuteRate = decayingRate(count, fiveMinuteRate, M5_ALPHA, duration);
        tenMinuteRate = decayingRate(count, tenMinuteRate, M10_ALPHA, duration);
        twentyMinuteRate = decayingRate(count, twentyMinuteRate, M20_ALPHA, duration);
    }

    private double decayingRate(long count, double rate, double alpha, long interval) {
        final double instantRate = (double) count / (double) interval;
        return rate + (alpha * (instantRate - rate));
    }
}

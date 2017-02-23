/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host.stat;

import com.emc.pravega.common.netty.WireCommands;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.time.Duration;

class SegmentAggregates implements Serializable {
    private static final int INTERVAL_IN_SECONDS = 5;

    private static final int SECONDS_PER_MINUTE = 60;

    private static final double M2_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 2);
    private static final double M5_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 5);
    private static final double M10_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 10);
    private static final double M20_ALPHA = 1 - StrictMath.exp((double) -INTERVAL_IN_SECONDS / (double) SECONDS_PER_MINUTE / 20);

    private static final long TICK_INTERVAL_IN_SECONDS = Duration.ofSeconds(5).getSeconds();
    private static final long TICK_INTERVAL = Duration.ofSeconds(5).toNanos();

    // Amount of data stored in each aggregate = 74 bytes.

    /**
     * Policy = 10 bytes.
     */
    @Setter
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
    private volatile long lastReportedTime;

    /**
     * Start time and last ticked time.
     * 16 bytes.
     */
    @Getter
    private long startTime;

    private volatile long lastTick;

    /**
     * 8 bytes.
     */
    // Note: we are not concurrency protecting this variable for performance reasons
    private volatile long currentCount;

    SegmentAggregates(byte scaleType, int targetRate) {
        this.targetRate = targetRate;
        this.scaleType = scaleType;
        startTime = System.currentTimeMillis();
        lastReportedTime = System.currentTimeMillis();
        lastTick = System.nanoTime();
    }

    void update(long dataLength, int numOfEvents) {
        if (scaleType == WireCommands.CreateSegment.IN_KBPS) {
            currentCount += dataLength / 1024; // convert to kbps
        } else if (scaleType == WireCommands.CreateSegment.IN_EVENTS_PER_SEC) {
            currentCount += numOfEvents;
        } else {
            return;
        }

        final long newTick = System.nanoTime();
        final long age = newTick - lastTick;
        if (age > TICK_INTERVAL) {
            lastTick = newTick;

            final long count = currentCount;
            currentCount = 0;

            computeDecay(count, age);
        }
    }

    void updateTx(long dataSize, int numOfEvents, long txnCreationTime) {
        if (scaleType == WireCommands.CreateSegment.IN_KBPS) {
            computeDecay(dataSize, (System.currentTimeMillis() - txnCreationTime) * 1000000);
        } else if (scaleType == WireCommands.CreateSegment.IN_EVENTS_PER_SEC) {
            computeDecay(numOfEvents, (System.currentTimeMillis() - txnCreationTime) * 1000000);
        }
    }

    private void computeDecay(long count, long duration) {
        // We have two options here --
        // currentCount data can be assumed to be evenly distributed over the tick period.
        // Or assume this was received in this instant and every other tick gets 0.
        // We will go with evenly distributed count as we are also dealing with txns in these updates
        // and they supply their durations whereas regular writes are more expensive.

        final long requiredTicks = Math.max(duration / TICK_INTERVAL, 1);

        final long perTickCount = count / requiredTicks;

        for (long i = 0; i < requiredTicks; i++) {
            twoMinuteRate = decayingRate(perTickCount, twoMinuteRate, M2_ALPHA, TICK_INTERVAL_IN_SECONDS);
            fiveMinuteRate = decayingRate(perTickCount, fiveMinuteRate, M5_ALPHA, TICK_INTERVAL_IN_SECONDS);
            tenMinuteRate = decayingRate(perTickCount, tenMinuteRate, M10_ALPHA, TICK_INTERVAL_IN_SECONDS);
            twentyMinuteRate = decayingRate(perTickCount, twentyMinuteRate, M20_ALPHA, TICK_INTERVAL_IN_SECONDS);
        }
    }

    private double decayingRate(long count, double rate, double alpha, long interval) {
        final double instantRate = (double) count / (double) interval;
        return rate + (alpha * (instantRate - rate));
    }

}

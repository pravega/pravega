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
package com.emc.pravega.service.server.stats;

import com.emc.pravega.common.netty.WireCommands;

import java.io.Serializable;
import java.time.Duration;

class SegmentAggregates implements Serializable {
    private static final int INTERVAL = 5;

    private static final int SECONDS_PER_MINUTE = 60;

    private static final double M2_ALPHA = 1 - StrictMath.exp((double) -INTERVAL / (double) SECONDS_PER_MINUTE / 2);
    private static final double M5_ALPHA = 1 - StrictMath.exp((double) -INTERVAL / (double) SECONDS_PER_MINUTE / 5);
    private static final double M10_ALPHA = 1 - StrictMath.exp((double) -INTERVAL / (double) SECONDS_PER_MINUTE / 10);
    private static final double M20_ALPHA = 1 - StrictMath.exp((double) -INTERVAL / (double) SECONDS_PER_MINUTE / 20);

    private static final long TWO_MINUTE = Duration.ofMinutes(2).toMinutes() * SECONDS_PER_MINUTE;
    private static final long FIVE_MINUTE = Duration.ofMinutes(5).toMinutes() * SECONDS_PER_MINUTE;
    private static final long TEN_MINUTE = Duration.ofMinutes(10).toMinutes() * SECONDS_PER_MINUTE;
    private static final long TWENTY_MINUTE = Duration.ofMinutes(20).toMinutes() * SECONDS_PER_MINUTE;

    private static final long TICK_INTERVAL = Duration.ofSeconds(5).toNanos();

    // Amount of data stored in each aggregate = 74 bytes.

    /**
     * Policy = 10 bytes.
     */
    private byte scaleType;

    private int targetRate;

    /**
     * Rates for Scale up = 24 bytes.
     */
    private volatile double twoMinuteRate;
    private volatile double fiveMinuteRate;
    private volatile double tenMinuteRate;

    /**
     * Rate for Scale down = 8 bytes.
     */
    private volatile double twentyMinuteRate;

    /**
     * 16 bytes.
     */
    private volatile long lastReportedTime;

    /**
     * Start time and last ticked time.
     * 16 bytes.
     */
    private long startTime;

    private volatile long lastTick;

    /**
     * 8 bytes.
     */
    // Note: we are not concurrency protecting this variable for performance reasons
    private volatile long currentCount;

    SegmentAggregates(int targetRate, byte scaleType) {
        this.targetRate = targetRate;
        this.scaleType = scaleType;
        startTime = System.currentTimeMillis();
        lastReportedTime = System.currentTimeMillis();
    }

    void setScaleType(byte scaleType) {
        this.scaleType = scaleType;
    }

    void setTargetRate(int targetRate) {
        this.targetRate = targetRate;
    }

    byte getScaleType() {
        return scaleType;
    }

    long getTargetRate() {
        return targetRate;
    }

    double getTwoMinuteRate() {
        return twoMinuteRate;
    }

    double getFiveMinuteRate() {
        return fiveMinuteRate;
    }

    double getTenMinuteRate() {
        return tenMinuteRate;
    }

    double getTwentyMinuteRate() {
        return twentyMinuteRate;
    }

    long getLastReportedTime() {
        return lastReportedTime;
    }

    long getStartTime() {
        return startTime;
    }

    void setLastReportedTime(long lastReportedTime) {
        this.lastReportedTime = lastReportedTime;
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


    private void computeDecay(long size, long duration) {
        // We have two options here --
        // currentCount data can be assumed to be evenly distributed over the tick period.
        // Or assume this was received in this instant and every other tick gets 0.
        // We will go with evenly distributed count as we are also dealing with txns in these updates
        // and they supply their durations whereas regular writes are more expensive.

        final long requiredTicks = Math.max(duration / TICK_INTERVAL, 1);

        final long count = size / requiredTicks;

        for (long i = 0; i < requiredTicks; i++) {
            twoMinuteRate = decayingRate(count, twoMinuteRate, M2_ALPHA, TWO_MINUTE);
            fiveMinuteRate = decayingRate(count, fiveMinuteRate, M5_ALPHA, FIVE_MINUTE);
            tenMinuteRate = decayingRate(count, tenMinuteRate, M10_ALPHA, TEN_MINUTE);
            twentyMinuteRate = decayingRate(count, twentyMinuteRate, M20_ALPHA, TWENTY_MINUTE);
        }
    }

    private double decayingRate(long count, double rate, double alpha, long interval) {
        final double instantRate = count / interval;
        return rate + (alpha * (instantRate - rate));
    }

}

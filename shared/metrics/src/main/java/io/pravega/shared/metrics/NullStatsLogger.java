/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import java.time.Duration;
import java.util.EnumMap;
import java.util.function.Supplier;

public class NullStatsLogger implements StatsLogger {

    public static final NullStatsLogger INSTANCE = new NullStatsLogger();
    static final NullCounter NULLCOUNTER = new NullCounter();
    static final NullGauge NULLGAUGE = new NullGauge();
    static final NullMeter NULLMETER = new NullMeter();
    static final NullOpStatsLogger NULLOPSTATSLOGGER = new NullOpStatsLogger();
    private static final String NULLNAME = "";

    @Override
    public OpStatsLogger createStats(String name) {
        return NULLOPSTATSLOGGER;
    }

    @Override
    public Counter createCounter(String name) {
        return NULLCOUNTER;
    }

    private static class NullGauge implements Gauge {
        @Override
        public String getName() {
            return NULLNAME;
        }

        @Override
        public void close() {
            // nop
        }
    }

    @Override
    public <T extends Number> Gauge registerGauge(String name, Supplier<T> value) {
        return NULLGAUGE;
    }

    @Override
    public Meter createMeter(String name) {
        return NULLMETER;
    }

    @Override
    public StatsLogger createScopeLogger(String name) {
        return this;
    }

    private static class NullOpStatsLogger implements OpStatsLogger {
        final OpStatsData nullOpStats = new OpStatsData(0, 0, 0, new EnumMap<OpStatsData.Percentile, Long>(OpStatsData.Percentile.class));

        @Override
        public void reportFailEvent(Duration duration) {
            // nop
        }

        @Override
        public void reportSuccessEvent(Duration duration) {
            // nop
        }

        @Override
        public void reportSuccessValue(long value) {
            // nop
        }

        @Override
        public void reportFailValue(long value) {
            // nop
        }

        @Override
        public OpStatsData toOpStatsData() {
            return nullOpStats;
        }

        @Override
        public void clear() {
            // nop
        }

        @Override
        public String getName() {
            return NULLNAME;
        }

        @Override
        public void close() {
            // nop
        }
    }

    private static class NullCounter implements Counter {
        @Override
        public void clear() {
            // nop
        }

        @Override
        public void inc() {
            // nop
        }

        @Override
        public void dec() {
            // nop
        }

        @Override
        public void add(long delta) {
            // nop
        }

        @Override
        public long get() {
            return 0L;
        }

        @Override
        public String getName() {
            return NULLNAME;
        }

        @Override
        public void close() {
            // nop
        }
    }

    private static class NullMeter implements Meter {
        @Override
        public void recordEvent() {
            // nop
        }

        @Override
        public void recordEvents(long n) {
            // nop
        }

        @Override
        public long getCount() {
            return 0L;
        }

        @Override
        public String getName() {
            return NULLNAME;
        }

        @Override
        public void close() {
            // nop(DONE)
        }
    }
}

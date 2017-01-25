/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.common.metrics;

import java.time.Duration;
import java.util.EnumMap;
import java.util.function.Supplier;

public class NullStatsLogger implements StatsLogger {

    public static final NullStatsLogger INSTANCE = new NullStatsLogger();

    static class NullOpStatsLogger implements OpStatsLogger {
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
    }

    private static NullOpStatsLogger nullOpStatsLogger = new NullOpStatsLogger();

    static class NullCounter implements Counter {
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
    }

    private static NullCounter nullCounter = new NullCounter();

    static class NullMeter implements Meter {
        @Override
        public void mark() {
            // nop
        }

        @Override
        public void mark(long n) {
            // nop
        }

        @Override
        public long getCount() {
            return 0L;
        }

        @Override
        public double getFifteenMinuteRate() {
            return 0;
        }

        @Override
        public double getFiveMinuteRate() {
            return 0;
        }

        @Override
        public double getMeanRate() {
            return 0;
        }

        @Override
        public double getOneMinuteRate() {
            return 0;
        }
    }

    private static NullMeter nullMeter = new NullMeter();

    @Override
    public OpStatsLogger createStats(String name) {
        return nullOpStatsLogger;
    }

    @Override
    public Counter createCounter(String name) {
        return nullCounter;
    }

    @Override
    public Meter createMeter(String name) {
        return nullMeter;
    }

    @Override
    public <T extends Number> void registerGauge(String name, Supplier<T> value){
        // nop
    }

    @Override
    public StatsLogger createScopeLogger(String name) {
        return this;
    }

}

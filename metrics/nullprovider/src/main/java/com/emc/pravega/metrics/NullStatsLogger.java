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
package com.emc.pravega.metrics;

import java.util.concurrent.TimeUnit;

public class NullStatsLogger implements StatsLogger {

    public static final NullStatsLogger INSTANCE = new NullStatsLogger();

    static class NullOpStatsLogger implements OpStatsLogger {
        final OpStatsData nullOpStats = new OpStatsData(0, 0, 0, new long[6]);

        @Override
        public void registerFailedEvent(long eventLatency, TimeUnit unit) {
            // nop
        }

        @Override
        public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
            // nop
        }

        @Override
        public void reportSuccess(long eventLatency){
            // nop
        }

        @Override
        public void reportFailure(long eventLatency){
            // nop
        }

        @Override
        public void registerSuccessfulValue(long value) {
            // nop
        }

        @Override
        public void registerFailedValue(long value) {
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

    static NullOpStatsLogger nullOpStatsLogger = new NullOpStatsLogger();

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
        public Long get() {
            return 0L;
        }
    }

    static NullCounter nullCounter = new NullCounter();

    @Override
    public OpStatsLogger getStats(String name) {
        return nullOpStatsLogger;
    }

    @Override
    public Counter getCounter(String name) {
        return nullCounter;
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
        // nop
    }

    @Override
    public void registerGauge(final String statName, final Long value) {
        // nop
    }

    @Override
    public void registerGauge(String name, final Double value) {
        // nop
    }

    @Override
    public StatsLogger scope(String name) {
        return this;
    }

}

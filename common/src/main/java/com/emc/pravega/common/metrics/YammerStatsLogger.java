/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import java.util.function.Supplier;


import static com.codahale.metrics.MetricRegistry.name;

public class YammerStatsLogger implements StatsLogger {
    protected final String basename;
    private final MetricRegistry metrics;

    YammerStatsLogger(MetricRegistry metrics, String basename) {
        Preconditions.checkNotNull(metrics, "metrics");
        this.metrics = metrics;
        this.basename = basename;
    }

    @Override
    public OpStatsLogger createStats(String statName) {
        Timer success = metrics.timer(name(basename, statName));
        Timer failure = metrics.timer(name(basename, statName+"-fail"));
        return new YammerOpStatsLogger(success, failure);
    }

    @Override
    public Counter createCounter(String statName) {
        final com.codahale.metrics.Counter c = metrics.counter(name(basename, statName));
        return new CounterImpl(c, name(basename, statName));
    }

    @Override
    public <T extends Number> Gauge registerGauge(final String statName, Supplier<T> value) {
        String metricName = name(basename, statName);
        metrics.remove(metricName);
        com.codahale.metrics.Gauge gauge = new com.codahale.metrics.Gauge<T>() {
            @Override
            public T getValue() {
                return value.get();
            }
        };
        metrics.register(metricName, gauge);
        return new GaugeImpl(gauge, metricName);
    }

    @Override
    public Meter createMeter(String statName) {
        final com.codahale.metrics.Meter meter = metrics.meter(name(basename, statName));
        return new MeterImpl(meter, name(basename, statName));
    }

    @Override
    public StatsLogger createScopeLogger(String scope) {
        String scopeName;
        if (0 == basename.length()) {
            scopeName = scope;
        } else {
            scopeName = name(basename, scope);
        }
        return new YammerStatsLogger(metrics, scopeName);
    }

    private static class CounterImpl implements Counter {
        private final com.codahale.metrics.Counter counter;
        private final String name;

        CounterImpl(com.codahale.metrics.Counter c, String name) {
            counter = c;
            this.name = name;
        }

        @Override
        public synchronized void clear() {
            long cur = counter.getCount();
            counter.dec(cur);
        }

        @Override
        public long get() {
            return counter.getCount();
        }

        @Override
        public void inc() {
            counter.inc();
        }

        @Override
        public void dec() {
            counter.dec();
        }

        @Override
        public void add(long delta) {
            counter.inc(delta);
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private static class GaugeImpl implements Gauge {
        private final com.codahale.metrics.Gauge gauge;
        private final String name;

        GaugeImpl(com.codahale.metrics.Gauge gauge, String name) {
            this.gauge = gauge;
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }

    private static class MeterImpl implements Meter {
        private final com.codahale.metrics.Meter meter;
        private final String name;

        MeterImpl(com.codahale.metrics.Meter meter, String name) {
            this.meter = meter;
            this.name = name;
        }

        @Override
        public void recordEvent() {
            meter.mark();
        }

        @Override
        public void recordEvents(long n) {
            meter.mark(n);
        }

        @Override
        public long getCount() {
            return meter.getCount();
        }

        @Override
        public String getName() {
            return name;
        }
    }
}

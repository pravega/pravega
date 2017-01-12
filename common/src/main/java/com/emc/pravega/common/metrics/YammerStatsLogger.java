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

import com.codahale.metrics.Timer;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;

import static com.codahale.metrics.MetricRegistry.name;
import java.util.function.Supplier;

public class YammerStatsLogger implements StatsLogger {
    protected final String basename;
    final MetricRegistry metrics;

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

    private static class CounterImpl implements Counter {
        private final com.codahale.metrics.Counter counter;

        CounterImpl(com.codahale.metrics.Counter c) {
             counter = c;
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
    }

    @Override
    public Counter createCounter(String statName) {
        final com.codahale.metrics.Counter c = metrics.counter(name(basename, statName));
        return new CounterImpl(c);
    }

    @Override
    public <T extends Number> void registerGauge(final String statName, Supplier<T> value) {
        String metricName = name(basename, statName);
        metrics.remove(metricName);
        Gauge gauge = new Gauge<T>() {
            @Override
            public T getValue() {
                return value.get();
            }
        };
        metrics.register(metricName, gauge);
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
}

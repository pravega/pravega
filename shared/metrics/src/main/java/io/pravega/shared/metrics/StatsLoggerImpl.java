/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import com.google.common.base.Preconditions;

import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Meter.Id;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.metrics.NullStatsLogger.NULLCOUNTER;
import static io.pravega.shared.metrics.NullStatsLogger.NULLGAUGE;
import static io.pravega.shared.metrics.NullStatsLogger.NULLMETER;
import static io.pravega.shared.metrics.NullStatsLogger.NULLOPSTATSLOGGER;

@Slf4j
public class StatsLoggerImpl implements StatsLogger {
    private final String basename;
    private final MeterRegistry metrics;

    StatsLoggerImpl(MeterRegistry metrics, String basename) {
        this.metrics = Preconditions.checkNotNull(metrics, "metrics");
        this.basename = basename;
    }

    @Override
    public OpStatsLogger createStats(String statName, String... tags) {
        try {
            return new OpStatsLoggerImpl(metrics, basename, statName, tags);
        } catch (Exception e) {
            log.warn("createStats failure: {}", statName, e);
            return NULLOPSTATSLOGGER;
        }
    }

    @Override
    public Counter createCounter(String statName, String... tags) {
        try {
            return new CounterImpl(statName, tags);
        } catch (Exception e) {
            log.warn("createCounter failure: {}", statName, e);
            return NULLCOUNTER;
        }
    }

    @Override
    public <T extends Number> Gauge registerGauge(final String statName, Supplier<T> valueSupplier, String... tags) {
        try {
            return new GaugeImpl<>(statName, valueSupplier, tags);
        } catch (Exception e) {
            log.warn("registerGauge failure: {}", statName, e);
            return NULLGAUGE;
        }
    }

    @Override
    public Meter createMeter(String statName, String... tags) {
        try {
            return new MeterImpl(statName, tags);
        } catch (Exception e) {
            log.warn("createMeter failure: {}", statName, e);
            return NULLMETER;
        }
    }

    @Override
    public StatsLogger createScopeLogger(String scope) {
        String scopeName;
        if (0 == basename.length()) {
            scopeName = scope;
        } else {
            scopeName = basename + "." + scope;
        }
        return new StatsLoggerImpl(metrics, scopeName);
    }

    private class CounterImpl implements Counter {
        private io.micrometer.core.instrument.Counter counter; //TODO: mutability issue to be discussed
        private final io.micrometer.core.instrument.Tags tags;
        @Getter
        private final Id id;
        private final String name;

        CounterImpl(String statName, String... tagPairs) {
            this.tags = io.micrometer.core.instrument.Tags.of(tagPairs);
            this.name = basename + "." + statName;
            this.counter = metrics.counter(name, this.tags);
            this.id = counter.getId();
        }

        @Override
        public synchronized void close() {
            metrics.remove(counter);
            counter = null;
        }

        @Override
        public synchronized void clear() {
            metrics.remove(counter.getId());
            this.counter = metrics.counter(name, tags);
        }

        @Override
        public long get() {
            return (long) counter.count();
        }

        @Override
        public void inc() {
            counter.increment();
        }

        @Override
        public void add(long delta) {
            counter.increment(delta);
        }
    }

    private class GaugeImpl<T extends Number> implements Gauge {
        @Getter
        private final Id id;

        GaugeImpl(String statName, Supplier<T> value, String... tagPairs) {
            String name = basename + "." + statName;
            io.micrometer.core.instrument.Tags tags = io.micrometer.core.instrument.Tags.of(tagPairs);
            this.id = new Id(name, tags, null, null,
                    io.micrometer.core.instrument.Meter.Type.GAUGE);
            metrics.remove(this.id);
            metrics.gauge(name, tags, value.get());
        }

        @Override
        public void close() {
            metrics.remove(this.id);
        }

    }

    private class MeterImpl implements Meter {
        private io.micrometer.core.instrument.DistributionSummary summary; //TODO: mutable, strong reference
        @Getter
        private final Id id;

        MeterImpl(String statName, String... tagPairs) {
            String name = basename + "." + statName;
            io.micrometer.core.instrument.Tags tags = io.micrometer.core.instrument.Tags.of(tagPairs);
            this.summary = metrics.summary(name, tags);
            this.id = summary.getId();
        }

        @Override
        public void close() {
            metrics.remove(summary);
            summary = null;
        }

        @Override
        public void recordEvent() {
            summary.record(1);
        }

        @Override
        public void recordEvents(long n) {
            summary.record(n);
        }

        @Override
        public long getCount() {
            return (long) summary.count();
        }
    }
}

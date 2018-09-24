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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static com.codahale.metrics.MetricRegistry.name;
import static io.pravega.shared.metrics.NullStatsLogger.NULLCOUNTER;
import static io.pravega.shared.metrics.NullStatsLogger.NULLGAUGE;
import static io.pravega.shared.metrics.NullStatsLogger.NULLMETER;
import static io.pravega.shared.metrics.NullStatsLogger.NULLOPSTATSLOGGER;

@Slf4j
public class StatsLoggerImpl implements StatsLogger {
    private final String basename;
    private final MetricRegistry metrics;

    StatsLoggerImpl(MetricRegistry metrics, String basename) {
        this.metrics = Preconditions.checkNotNull(metrics, "metrics");
        this.basename = basename;
    }

    @Override
    public OpStatsLogger createStats(String statName) {
        try {
            return new OpStatsLoggerImpl(metrics, basename, statName);
        } catch (Exception e) {
            log.warn("createStats failure: {}", statName, e);
            return NULLOPSTATSLOGGER;
        }
    }

    @Override
    public Counter createCounter(String statName) {
        try {
            return new CounterImpl(statName);
        } catch (Exception e) {
            log.warn("createCounter failure: {}", statName, e);
            return NULLCOUNTER;
        }
    }

    @Override
    public <T extends Number> Gauge registerGauge(final String statName, Supplier<T> valueSupplier) {
        try {
            return new GaugeImpl<>(statName, valueSupplier);
        } catch (Exception e) {
            log.warn("registerGauge failure: {}", statName, e);
            return NULLGAUGE;
        }
    }

    @Override
    public Meter createMeter(String statName) {
        try {
            return new MeterImpl(statName);
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
            scopeName = name(basename, scope);
        }
        return new StatsLoggerImpl(metrics, scopeName);
    }

    private class CounterImpl implements Counter {
        private final com.codahale.metrics.Counter counter;
        @Getter
        private final String name;

        CounterImpl(String statName) {
            this.name = name(basename, statName);
            this.counter = metrics.counter(name);
        }

        @Override
        public void close() {
            metrics.remove(this.name);
        }

        @Override
        @SuppressWarnings("deprecation") // deprecated since Java 10
        protected void finalize() {
            close();
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

    private class GaugeImpl<T> implements Gauge {
        private final com.codahale.metrics.Gauge<T> gauge;
        @Getter
        private final String name;

        GaugeImpl(String statName, Supplier<T> value) {
            this.name = name(basename, statName);
            this.gauge = value::get;
            metrics.remove(name);
            metrics.register(name, gauge);
        }

        @Override
        public void close() {
            metrics.remove(this.name);
        }

        @Override
        @SuppressWarnings("deprecation") // deprecated since Java 10
        protected void finalize() {
            close();
        }
    }

    private class MeterImpl implements Meter {
        private final com.codahale.metrics.Meter meter;
        @Getter
        private final String name;

        MeterImpl(String statName) {
            this.name = name(basename, statName);
            this.meter = metrics.meter(this.name);
        }

        @Override
        public void close() {
            metrics.remove(this.name);
        }

        @Override
        @SuppressWarnings("deprecation") // deprecated since Java 10
        protected void finalize() {
            close();
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
    }
}

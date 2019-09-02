/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import java.util.concurrent.atomic.AtomicReference;

public class DynamicLoggerProxy implements DynamicLogger {
    private final AtomicReference<DynamicLogger> instance = new AtomicReference<>();

    DynamicLoggerProxy(DynamicLogger logger) {
        this.instance.set(logger);
    }

    void setLogger(DynamicLogger logger) {
        this.instance.set(logger);
    }

    @Override
    public void incCounterValue(String name, long delta, String... tags) {
        this.instance.get().incCounterValue(name, delta, tags);
    }

    @Override
    public void updateCounterValue(String name, long value, String... tags) {
        this.instance.get().updateCounterValue(name, value, tags);
    }

    @Override
    public void freezeCounter(String name, String... tags) {
        this.instance.get().freezeCounter(name, tags);
    }

    @Override
    public <T extends Number> void reportGaugeValue(String name, T value, String... tags) {
        this.instance.get().reportGaugeValue(name, value, tags);
    }

    @Override
    public void freezeGaugeValue(String name, String... tags) {
        this.instance.get().freezeGaugeValue(name, tags);
    }

    @Override
    public void recordMeterEvents(String name, long number, String... tags) {
        this.instance.get().recordMeterEvents(name, number, tags);
    }
}

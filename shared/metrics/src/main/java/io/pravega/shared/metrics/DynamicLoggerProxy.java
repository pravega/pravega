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
    public void incCounterValue(String name, long delta) {
        this.instance.get().incCounterValue(name, delta);
    }

    @Override
    public void updateCounterValue(String name, long value) {
        this.instance.get().updateCounterValue(name, value);
    }

    @Override
    public void freezeCounter(String name) {
        this.instance.get().freezeCounter(name);
    }

    @Override
    public <T extends Number> void reportGaugeValue(String name, T value) {
        this.instance.get().reportGaugeValue(name, value);
    }

    @Override
    public void freezeGaugeValue(String name) {
        this.instance.get().freezeGaugeValue(name);
    }

    @Override
    public void recordMeterEvents(String name, long number) {
        this.instance.get().recordMeterEvents(name, number);
    }
}

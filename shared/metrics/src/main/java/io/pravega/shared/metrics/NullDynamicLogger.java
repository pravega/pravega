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

public class NullDynamicLogger implements DynamicLogger {
    public static final NullDynamicLogger INSTANCE = new NullDynamicLogger();

    @Override
    public void incCounterValue(String name, long delta, String... tags) {
        // nop
    }

    @Override
    public void updateCounterValue(String name, long value, String... tags) {
        // nop
    }

    @Override
    public void freezeCounter(String name, String... tags) {
        // nop
    }

    @Override
    public <T extends Number> void reportGaugeValue(String name, T value, String... tags) {
        // nop
    }

    @Override
    public void freezeGaugeValue(String name, String... tags) {
        // nop
    }

    @Override
    public void recordMeterEvents(String name, long number, String... tags) {
        // nop
    }
}

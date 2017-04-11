/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.shared.metrics;

public class NullDynamicLogger implements DynamicLogger {
    public static final NullDynamicLogger INSTANCE = new NullDynamicLogger();

    @Override
    public void incCounterValue(String name, long delta) {
        // nop
    }

    @Override
    public <T extends Number> void reportGaugeValue(String name, T value) {
        // nop
    }

    @Override
    public void recordMeterEvents(String name, long number) {
        // nop
    }
}

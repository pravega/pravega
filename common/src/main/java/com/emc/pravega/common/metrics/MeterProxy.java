/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.common.metrics;

import java.util.concurrent.atomic.AtomicReference;

public class MeterProxy implements Meter {
    private final AtomicReference<Meter> instance = new AtomicReference<>();

    MeterProxy(Meter meter) {
        instance.set(meter);
    }

    void setMeter(Meter meter) {
        instance.set(meter);
    }

    @Override
    public void recordEvent() {
        instance.get().recordEvent();
    }

    @Override
    public void recordEvents(long n) {
        instance.get().recordEvents(n);
    }

    @Override
    public long getCount() {
        return instance.get().getCount();
    }

    @Override
    public String getName() {
        return instance.get().getName();
    }
}

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

/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.metrics;

import java.util.concurrent.atomic.AtomicReference;

public class GaugeProxy implements Gauge {
    private final AtomicReference<Gauge> instance = new AtomicReference<>();

    GaugeProxy(Gauge gauge) {
        instance.set(gauge);
    }

    void setGauge(Gauge gauge) {
        instance.set(gauge);
    }

    @Override
    public String getName() {
        return instance.get().getName();
    }
}

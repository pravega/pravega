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

import java.util.function.Consumer;
import java.util.function.Supplier;

class GaugeProxy extends MetricProxy<Gauge> implements Gauge {

    GaugeProxy(Gauge gauge, String proxyName, Consumer<String> closeCallback) {
        super(gauge, proxyName, closeCallback);
    }

    @Override
    public void setSupplier(Supplier<Number> supplier) {
        getInstance().setSupplier(supplier);
    }

    @Override
    public Supplier<Number> getSupplier() {
        return getInstance().getSupplier();
    }
}

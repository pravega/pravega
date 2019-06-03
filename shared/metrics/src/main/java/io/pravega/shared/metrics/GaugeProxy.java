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

import com.google.common.base.Preconditions;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Getter;

class GaugeProxy extends MetricProxy<Gauge> implements Gauge {
    @Getter
    private final Supplier<? extends Number> valueSupplier;

    GaugeProxy(Gauge gauge, String proxyName, Supplier<? extends Number> valueSupplier, Consumer<String> closeCallback) {
        super(gauge, proxyName, closeCallback);
        this.valueSupplier = Preconditions.checkNotNull(valueSupplier, "valueSupplier");
    }
}

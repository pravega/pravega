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

public class MeterProxy extends MetricProxy<Meter> implements Meter {
    MeterProxy(Meter meter, String proxyName, Consumer<String> closeCallback) {
        super(meter, proxyName, closeCallback);
    }

    @Override
    public void recordEvent() {
        getInstance().recordEvent();
    }

    @Override
    public void recordEvents(long n) {
        getInstance().recordEvents(n);
    }

    @Override
    public long getCount() {
        return getInstance().getCount();
    }
}

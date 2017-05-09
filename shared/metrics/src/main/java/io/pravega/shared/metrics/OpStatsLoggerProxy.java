/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

public class OpStatsLoggerProxy implements OpStatsLogger {
    private final AtomicReference<OpStatsLogger> instance = new AtomicReference<>();

    OpStatsLoggerProxy(OpStatsLogger logger) {
        instance.set(logger);
    }

    void setLogger(OpStatsLogger logger) {
        instance.set(logger);
    }

    @Override
    public void reportSuccessEvent(Duration duration) {
        instance.get().reportSuccessEvent(duration);
    }

    @Override
    public void reportFailEvent(Duration duration) {
        instance.get().reportFailEvent(duration);
    }

    @Override
    public void reportSuccessValue(long value) {
        instance.get().reportSuccessValue(value);
    }

    @Override
    public void reportFailValue(long value) {
        instance.get().reportFailValue(value);
    }

    @Override
    public OpStatsData toOpStatsData() {
        return instance.get().toOpStatsData();
    }

    @Override
    public void clear() {
        instance.get().clear();
    }
}

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

import java.time.Duration;
import java.util.function.Consumer;

public class OpStatsLoggerProxy extends MetricProxy<OpStatsLogger> implements OpStatsLogger {

    OpStatsLoggerProxy(OpStatsLogger logger, String proxyName, Consumer<String> closeCallback) {
        super(logger, proxyName, closeCallback);
    }

    @Override
    public void reportSuccessEvent(Duration duration) {
        getInstance().reportSuccessEvent(duration);
    }

    @Override
    public void reportFailEvent(Duration duration) {
        getInstance().reportFailEvent(duration);
    }

    @Override
    public void reportSuccessValue(long value) {
        getInstance().reportSuccessValue(value);
    }

    @Override
    public void reportFailValue(long value) {
        getInstance().reportFailValue(value);
    }

    @Override
    public OpStatsData toOpStatsData() {
        return getInstance().toOpStatsData();
    }

    @Override
    public void clear() {
        getInstance().clear();
    }
}

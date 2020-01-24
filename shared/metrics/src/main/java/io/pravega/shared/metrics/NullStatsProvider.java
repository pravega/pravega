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

public class NullStatsProvider implements StatsProvider {

    private final StatsLogger nullStatsLogger = new NullStatsLogger();
    private final DynamicLogger nullDynamicLogger = new NullDynamicLogger();

    @Override
    public void start() {
        // no-op
    }

    @Override
    public void startWithoutExporting() {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public StatsLogger createStatsLogger(String scope) {
        return nullStatsLogger;
    }

    @Override
    public DynamicLogger createDynamicLogger() {
        return nullDynamicLogger;
    }
}

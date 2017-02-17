/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

public class NullStatsProvider implements StatsProvider {

    private final StatsLogger nullStatsLogger = new NullStatsLogger();
    private final DynamicLogger nullDynamicLogger = new NullDynamicLogger();

    @Override
    public void start(MetricsConfig metricsConfig ) {
        // nop
    }

    @Override
    public void close() {
        // nop
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

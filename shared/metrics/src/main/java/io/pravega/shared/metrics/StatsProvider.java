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

/**
 * Provider of StatsLogger instances depending on scope.
 * An implementation of this interface possibly returns a separate instance per Pravega scope.
 */
public interface StatsProvider extends AutoCloseable {
    /**
     * Initialize the stats provider.
     */
    void start();

    /**
     * Initialize the stats provider with SimpleMeterRegistry only which is memory based without exporting.
     * Note different Micrometer registry may behave inconsistently from storage perspective (e.g. same metric
     * may return different value from different registry).
     * To keep things consistent, particularly for unit tests, only SimpleMeterRegistry is bound here.
     */
    void startWithoutExporting();

    /**
     * Close the stats provider.
     */
    @Override
    void close();

    /**
     * Return the StatsLogger instance associated with the given <i>scope</i>.
     *
     * @param scope Scope for the given stats.
     * @return stats logger for the given <i>scope</i>.
     */
    StatsLogger createStatsLogger(String scope);

    /**
     * Create a dynamic logger.
     */
    DynamicLogger createDynamicLogger();
}

/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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

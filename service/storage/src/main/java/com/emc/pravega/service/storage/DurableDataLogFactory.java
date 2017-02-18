/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.storage;

/**
 * Defines a Factory for DataFrameLogs.
 */
public interface DurableDataLogFactory extends AutoCloseable {
    /**
     * Creates a new instance of a DurableDataLog class.
     *
     * @param containerId The Id of the StreamSegmentContainer for the DurableDataLog.
     */
    DurableDataLog createDurableDataLog(int containerId);

    @Override
    void close();
}

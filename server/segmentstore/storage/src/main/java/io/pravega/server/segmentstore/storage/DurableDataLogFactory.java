/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.storage;

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

    /**
     * Initializes the DurableDataLogFactory.
     *
     * @throws DurableDataLogException If an exception occurred. The causing exception is usually wrapped in this one.
     */
    void initialize() throws DurableDataLogException;

    @Override
    void close();
}

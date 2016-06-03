package com.emc.logservice.storageabstraction;

/**
 * Defines a Factory for DataFrameLogs.
 */
public interface DurableDataLogFactory extends AutoCloseable{
    /**
     * Creates a new instance of a DurableDataLog class.
     *
     * @return The result.
     */
    DurableDataLog createDurableDataLog(String containerId);

    @Override
    void close();
}

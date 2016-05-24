package com.emc.logservice.storageabstraction;

/**
 * Defines a Factory for DataFrameLogs.
 */
public interface DurableDataLogFactory {
    /**
     * Creates a new instance of a DurableDataLog class.
     * TODO: append configuration.
     *
     * @return The result.
     */
    DurableDataLog createDurableDataLog(String containerId);
}

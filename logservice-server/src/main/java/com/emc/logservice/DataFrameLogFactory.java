package com.emc.logservice;

import com.emc.logservice.logs.DataFrameLog;

/**
 * Defines a Factory for DataFrameLogs.
 */
public interface DataFrameLogFactory {
    /**
     * Creates a new instance of a DataFrameLog class.
     * TODO: add configuration.
     *
     * @return The result.
     */
    DataFrameLog createDataFrameLog(String containerId);
}

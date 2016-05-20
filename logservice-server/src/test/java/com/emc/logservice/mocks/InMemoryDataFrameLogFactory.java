package com.emc.logservice.mocks;

import com.emc.logservice.DataFrameLogFactory;
import com.emc.logservice.logs.DataFrameLog;

import java.util.HashMap;

/**
 * Created by padura on 5/18/16.
 */
public class InMemoryDataFrameLogFactory implements DataFrameLogFactory {
    private final HashMap<String, InMemoryDataFrameLog> logs = new HashMap<>();

    @Override
    public DataFrameLog createDataFrameLog(String containerId) {
        synchronized (this.logs) {
            InMemoryDataFrameLog result = this.logs.getOrDefault(containerId, null);
            if (result == null) {
                result = new InMemoryDataFrameLog();
                this.logs.put(containerId, result);
            }

            return result;
        }
    }
}

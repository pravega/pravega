package com.emc.logservice.storageabstraction.mocks;

import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;

import java.util.HashMap;

/**
 * In-memory mock for DurableDataLogFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemoryDurableDataLogFactory implements DurableDataLogFactory {
    private final HashMap<String, InMemoryDurableDataLog> logs = new HashMap<>();

    @Override
    public DurableDataLog createDurableDataLog(String containerId) {
        synchronized (this.logs) {
            InMemoryDurableDataLog result = this.logs.getOrDefault(containerId, null);
            if (result == null) {
                result = new InMemoryDurableDataLog();
                this.logs.put(containerId, result);
            }

            return result;
        }
    }
}

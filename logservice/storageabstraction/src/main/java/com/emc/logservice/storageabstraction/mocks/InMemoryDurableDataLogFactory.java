package com.emc.logservice.storageabstraction.mocks;

import com.emc.logservice.common.ObjectClosedException;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;

import java.util.HashMap;

/**
 * In-memory mock for DurableDataLogFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemoryDurableDataLogFactory implements DurableDataLogFactory {
    private final HashMap<String, InMemoryDurableDataLog.EntryCollection> persistedData = new HashMap<>();
    private boolean closed;

    @Override
    public DurableDataLog createDurableDataLog(String containerId) {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        InMemoryDurableDataLog.EntryCollection entries;
        synchronized (this.persistedData) {
            entries = this.persistedData.getOrDefault(containerId, null);
            if (entries == null) {
                entries = new InMemoryDurableDataLog.EntryCollection();
                this.persistedData.put(containerId, entries);
            }
        }

        return new InMemoryDurableDataLog(entries);
    }

    @Override
    public void close() {
        this.closed = true;
    }
}

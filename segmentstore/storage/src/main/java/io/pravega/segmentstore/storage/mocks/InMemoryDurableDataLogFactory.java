/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import lombok.Setter;

/**
 * In-memory mock for DurableDataLogFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemoryDurableDataLogFactory implements DurableDataLogFactory {
    private final HashMap<Integer, InMemoryDurableDataLog.EntryCollection> persistedData = new HashMap<>();
    private final int maxAppendSize;
    private final ScheduledExecutorService executorService;
    @Setter
    private Supplier<Duration> appendDelayProvider = InMemoryDurableDataLog.DEFAULT_APPEND_DELAY_PROVIDER;
    private boolean closed;

    public InMemoryDurableDataLogFactory(ScheduledExecutorService executorService) {
        this(-1, executorService);
    }

    public InMemoryDurableDataLogFactory(int maxAppendSize, ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(executorService, "executorService");
        this.maxAppendSize = maxAppendSize;
        this.executorService = executorService;
    }

    @Override
    public DurableDataLog createDurableDataLog(int containerId) {
        Exceptions.checkNotClosed(this.closed, this);

        InMemoryDurableDataLog.EntryCollection entries;
        synchronized (this.persistedData) {
            entries = this.persistedData.getOrDefault(containerId, null);
            if (entries == null) {
                entries = this.maxAppendSize < 0 ? new InMemoryDurableDataLog.EntryCollection() : new InMemoryDurableDataLog.EntryCollection(this.maxAppendSize);
                this.persistedData.put(containerId, entries);
            }
        }

        return new InMemoryDurableDataLog(entries, this.appendDelayProvider, this.executorService);
    }

    @Override
    public void initialize() throws DurableDataLogException {
        // Nothing to do.
    }

    @Override
    public void close() {
        this.closed = true;
    }
}

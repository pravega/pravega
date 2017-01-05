/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogFactory;
import com.google.common.base.Preconditions;
import lombok.Setter;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

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
    public void close() {
        this.closed = true;
    }
}

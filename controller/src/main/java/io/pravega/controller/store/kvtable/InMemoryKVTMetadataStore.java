/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.kvtable;

import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.AtomicInt96;
import io.pravega.controller.store.InMemoryScope;
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.index.InMemoryHostIndex;
import io.pravega.controller.store.stream.StoreException;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * In-memory stream store.
 */
@Slf4j
public class InMemoryKVTMetadataStore extends AbstractKVTableMetadataStore {

    @GuardedBy("$lock")
    private final Map<String, InMemoryKVTable> kvTables = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, Integer> deletedKVTables = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, InMemoryScope> scopes = new HashMap<>();

    private final AtomicInt96 counter;

    private final Executor executor;

    public InMemoryKVTMetadataStore(Executor executor) {
        super(new InMemoryHostIndex());
        this.executor = executor;
        this.counter = new AtomicInt96();
    }

    @Override
    @Synchronized
    KeyValueTable newKeyValueTable(String scope, String name) {
        if (kvTables.containsKey(scopedKVTName(scope, name))) {
            return kvTables.get(scopedKVTName(scope, name));
        } else {
            return new InMemoryKVTable(scope, name);
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> checkScopeExists(String scope) {
        return CompletableFuture.completedFuture(scopes.containsKey(scope));
    }

    @Override
    @Synchronized
    Scope newScope(final String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return scopes.get(scopeName);
        } else {
            return new InMemoryScope(scopeName);
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<CreateKVTableResponse> createKeyValueTable(final String scopeName, final String kvtName,
                                                                final KeyValueTableConfiguration configuration,
                                                                final long timeStamp,
                                                                final KVTOperationContext context,
                                                                final Executor executor) {
        if (scopes.containsKey(scopeName)) {
            InMemoryKVTable kvt = (InMemoryKVTable) getKVTable(scopeName, kvtName, context);
            return getSafeStartingSegmentNumberFor(scopeName, kvtName)
                    .thenCompose(startingSegmentNumber -> kvt.create(configuration, timeStamp, startingSegmentNumber)
                    .thenCompose(status -> {
                        kvTables.put(scopedKVTName(scopeName, kvtName), kvt);
                        return scopes.get(scopeName).addStreamToScope(kvtName).thenApply(v -> status);
                    }));
        } else {
            return Futures.
                    failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName) {
        final Integer safeStartingSegmentNumber = deletedKVTables.get(scopedKVTName(scopeName, streamName));
        return CompletableFuture.completedFuture((safeStartingSegmentNumber != null) ? safeStartingSegmentNumber + 1 : 0);
    }

    @Override
    public void close() throws IOException {
    }

    public CompletableFuture<Void> createEntryForKVTable(final String scopeName,
                                                         final String kvtName,
                                                         final Executor executor) {
        return Futures.completeOn(((InMemoryScope) getScope(scopeName)).addKVTableToScope(kvtName), executor);
    }

    private String scopedKVTName(final String scopeName, final String streamName) {
        return new StringBuilder(scopeName).append("/").append(streamName).toString();
    }
}

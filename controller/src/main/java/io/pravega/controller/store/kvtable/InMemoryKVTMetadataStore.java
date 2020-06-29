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

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.AtomicInt96;
import io.pravega.controller.store.InMemoryScope;
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.index.InMemoryHostIndex;
import lombok.Setter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * In-memory stream store.
 */
@Slf4j
public class InMemoryKVTMetadataStore extends AbstractKVTableMetadataStore {

    @GuardedBy("$lock")
    private final Map<String, Integer> deletedKVTables = new HashMap<>();

    @Setter
    @GuardedBy("$lock")
    private Map<String, InMemoryScope> scopes = new HashMap<>();

    private final AtomicInt96 counter;

    private final Executor executor;

    public InMemoryKVTMetadataStore(Executor executor) {
        super(new InMemoryHostIndex());
        this.executor = executor;
        this.counter = new AtomicInt96();
    }

    @Override
    KeyValueTable newKeyValueTable(String scope, String kvTableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyValueTable getKVTable(String scope, final String name, KVTOperationContext context) {
        KeyValueTable kvt;
        if (context != null) {
            kvt = context.getKvTable();
            assert kvt.getScopeName().equals(scope);
            assert kvt.getName().equals(name);
        } else {
            if (!scopes.containsKey(scope)) {
                return new InMemoryKVTable(scope, name);
            }
            InMemoryScope kvtScope = scopes.get(scope);
            Optional<InMemoryKVTable> kvTable = kvtScope.getKVTableFromScope(name);
            kvt = kvTable.orElse(new InMemoryKVTable(scope, name));
        }
        return kvt;
    }

    @Override
    public CompletableFuture<Void> deleteFromScope(final String scope,
                                                   final String name,
                                                   final KVTOperationContext context,
                                                   final Executor executor) {
        return Futures.completeOn(((InMemoryScope) getScope(scope)).removeKVTableFromScope(name),
                executor);
    }

    @Override
    CompletableFuture<Void> recordLastKVTableSegment(String scope, String kvtable, int lastActiveSegment, KVTOperationContext context, Executor executor) {
        Integer oldLastActiveSegment = deletedKVTables.put(getScopedKVTName(scope, kvtable), lastActiveSegment);
        Preconditions.checkArgument(oldLastActiveSegment == null || lastActiveSegment >= oldLastActiveSegment);
        log.debug("Recording last segment {} for kvtable {}/{} on deletion.", lastActiveSegment, scope, kvtable);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> checkScopeExists(String scope) {
        return CompletableFuture.completedFuture(scopes.containsKey(scope));
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> checkTableExists(String scopeName, String kvt) {
        return CompletableFuture.completedFuture((InMemoryScope) getScope(scopeName)).thenApply(scope -> scope.checkTableExists(kvt));
    }

    @Override
    @Synchronized
    public Scope newScope(final String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return scopes.get(scopeName);
        } else {
            return new InMemoryScope(scopeName);
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String kvtName) {
        final Integer safeStartingSegmentNumber = deletedKVTables.get(scopedKVTName(scopeName, kvtName));
        return CompletableFuture.completedFuture((safeStartingSegmentNumber != null) ? safeStartingSegmentNumber + 1 : 0);
    }

    @Override
    public void close() throws IOException {
    }

    public CompletableFuture<Void> createEntryForKVTable(final String scopeName,
                                                         final String kvtName,
                                                         final byte[] id,
                                                         final Executor executor) {
        return Futures.completeOn(scopes.get(scopeName).addKVTableToScope(kvtName, id), executor);
    }

    private String scopedKVTName(final String scopeName, final String streamName) {
        return new StringBuilder(scopeName).append("/").append(streamName).toString();
    }
}

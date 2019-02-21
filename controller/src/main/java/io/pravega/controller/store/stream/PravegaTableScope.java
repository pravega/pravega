/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.common.concurrent.Futures;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SCOPES_TABLE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SYSTEM_SCOPE;

public class PravegaTableScope implements Scope {
    private static final String STREAMS_IN_SCOPE_TABLE_FORMAT = "streamsInScope-%s";
    private final String streamsInScopeTable;
    private final String scopeName;
    private final PravegaTablesStoreHelper storeHelper;
    private final AtomicBoolean scopesTableCreated;
    private final Executor executor;
    PravegaTableScope(final String scopeName, PravegaTablesStoreHelper storeHelper, Executor executor) {
        this.scopeName = scopeName;
        this.storeHelper = storeHelper;
        this.streamsInScopeTable = String.format(STREAMS_IN_SCOPE_TABLE_FORMAT, scopeName);
        this.executor = executor;
        this.scopesTableCreated = new AtomicBoolean(false);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Void> createScope() {
        CompletableFuture<Void> future;
        if (!scopesTableCreated.get()) {
            future = storeHelper.createTable(SYSTEM_SCOPE, SCOPES_TABLE)
                    .thenAccept(x -> scopesTableCreated.set(true));
        } else {
            future = CompletableFuture.completedFuture(null);
        }
        // add entry to scopes table followed by creating scope specific table
        return future.thenCompose(tableCreated -> storeHelper.addNewEntryIfAbsent(SYSTEM_SCOPE, SCOPES_TABLE, scopeName, new byte[0]))
                .thenCompose(entryAdded -> storeHelper.createTable(scopeName, streamsInScopeTable));
    }

    @Override
    public CompletableFuture<Void> deleteScope() {
        return Futures.toVoid(storeHelper.deleteTable(scopeName, streamsInScopeTable, true))
                .thenCompose(deleted -> storeHelper.removeEntry(SYSTEM_SCOPE, SCOPES_TABLE, scopeName));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreamsInScope(int limit, String continuationToken, Executor executor) {
        return storeHelper.getKeysPaginated(scopeName, streamsInScopeTable);
    }

    @Override
    public CompletableFuture<List<String>> listStreamsInScope() {
        List<String> result = new LinkedList<>();
        return storeHelper.getAllKeys(scopeName, streamsInScopeTable).forEachRemaining(result::add, executor)
                .thenApply(v -> result);
    }

    @Override
    public void refresh() {
    }

    public CompletableFuture<Void> addStreamToScope(String stream) {
        return Futures.toVoid(storeHelper.addNewEntryIfAbsent(scopeName, streamsInScopeTable, stream, new byte[0]));
    }

    public CompletableFuture<Void> removeStreamFromScope(String stream) {
        return Futures.toVoid(storeHelper.removeEntry(scopeName, streamsInScopeTable, stream));
    }
}

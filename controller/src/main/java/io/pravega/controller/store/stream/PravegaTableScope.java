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

import io.netty.buffer.Unpooled;
import io.pravega.common.concurrent.Futures;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Base64;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SCOPES_TABLE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SYSTEM_SCOPE;

public class PravegaTableScope implements Scope {
    private static final String STREAMS_IN_SCOPE_TABLE_FORMAT = "streamsInScope-%s";
    private final String streamsInScopeTable;
    private final String scopeName;
    private final PravegaTablesStoreHelper storeHelper;
    private final Executor executor;
    PravegaTableScope(final String scopeName, PravegaTablesStoreHelper storeHelper, Executor executor) {
        this.scopeName = scopeName;
        this.storeHelper = storeHelper;
        this.streamsInScopeTable = String.format(STREAMS_IN_SCOPE_TABLE_FORMAT, scopeName);
        this.executor = executor;
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Void> createScope() {
        // add entry to scopes table followed by creating scope specific table
        return Futures.exceptionallyComposeExpecting(storeHelper.addNewEntryIfAbsent(SYSTEM_SCOPE, SCOPES_TABLE, scopeName, new byte[0]),
                DATA_NOT_FOUND_PREDICATE, () -> storeHelper.createTable(SYSTEM_SCOPE, SCOPES_TABLE).thenCompose(v -> storeHelper.addNewEntryIfAbsent(SYSTEM_SCOPE, SCOPES_TABLE, scopeName, new byte[0])))
                      .thenCompose(entryAdded -> Futures.toVoid(storeHelper.createTable(scopeName, streamsInScopeTable)));
    }

    @Override
    public CompletableFuture<Void> deleteScope() {
        return storeHelper.deleteTable(scopeName, streamsInScopeTable, true)
                .thenCompose(deleted -> storeHelper.removeEntry(SYSTEM_SCOPE, SCOPES_TABLE, scopeName));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreamsInScope(int limit, String continuationToken, Executor executor) {
        List<String> taken = new LinkedList<>();
        AtomicReference<String> token = new AtomicReference<>(continuationToken);
        AtomicBoolean canContinue = new AtomicBoolean(true);
        return Futures.loop(() -> taken.size() < limit && canContinue.get(), 
                () -> storeHelper.getKeysPaginated(scopeName, streamsInScopeTable, 
                        Unpooled.wrappedBuffer(Base64.getDecoder().decode(token.get())), limit - taken.size())
                .thenAccept(result -> {
                    if (result.getValue().isEmpty()) {
                        canContinue.set(false);
                    } else {
                        taken.addAll(result.getValue());
                    }
                    token.set(Base64.getEncoder().encodeToString(result.getKey().array()));
                }), executor)
                .thenApply(v -> {
                    List<String> result = taken.size() > limit ? taken.subList(0, limit) : taken;
                    return new ImmutablePair<>(result, token.get());
                });
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

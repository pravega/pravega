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
import io.pravega.shared.NameUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Base64;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SCOPES_TABLE;

public class PravegaTableScope implements Scope {
    static final String STREAMS_IN_SCOPE_TABLE_FORMAT = "Table.#.streamsInScope.#.%s";
    private final String streamsInScopeTable;
    private final String scopeName;
    private final PravegaTablesStoreHelper storeHelper;
    
    PravegaTableScope(final String scopeName, PravegaTablesStoreHelper storeHelper, Executor executor) {
        this.scopeName = scopeName;
        this.storeHelper = storeHelper;
        this.streamsInScopeTable = String.format(STREAMS_IN_SCOPE_TABLE_FORMAT, scopeName);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Void> createScope() {
        // add entry to scopes table followed by creating scope specific table
        return Futures.exceptionallyComposeExpecting(storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE, scopeName, newId()),
                DATA_NOT_FOUND_PREDICATE, () -> storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE)
                                                           .thenCompose(v -> storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE, scopeName, newId())))
                      .thenCompose(v -> getStreamsInScopeTableName())
                      .thenCompose(tableName -> Futures.toVoid(storeHelper.createTable(scopeName, tableName)));
    }

    private byte[] newId() {
        return UUID.randomUUID().toString().getBytes();
    }

    CompletableFuture<String> getStreamsInScopeTableName() {
        return storeHelper.getEntry(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE, scopeName)
                .thenApply(entry -> {
                    UUID id = UUID.fromString(new String(entry.getData()));
                    return streamsInScopeTable + id;
                });
    }

    @Override
    public CompletableFuture<Void> deleteScope() {
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> storeHelper.deleteTable(scopeName, tableName, true))
                .thenCompose(deleted -> storeHelper.removeEntry(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE, scopeName));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreamsInScope(int limit, String continuationToken, Executor executor) {
        List<String> taken = new ArrayList<>();
        AtomicReference<String> token = new AtomicReference<>(continuationToken);
        AtomicBoolean canContinue = new AtomicBoolean(true);
        return getStreamsInScopeTableName()
            .thenCompose(entry -> Futures.loop(() -> taken.size() < limit && canContinue.get(), 
                () -> storeHelper.getKeysPaginated(scopeName, entry, 
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
                }));
    }

    @Override
    public CompletableFuture<List<String>> listStreamsInScope() {
        List<String> result = new ArrayList<>();
        return getStreamsInScopeTableName()
            .thenCompose(tableName -> storeHelper.getAllKeys(scopeName, tableName).collectRemaining(result::add)
                .thenApply(v -> result));
    }

    @Override
    public void refresh() {
    }

    CompletableFuture<Void> addStreamToScope(String stream) {
        return getStreamsInScopeTableName()
            .thenCompose(tableName -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(scopeName, tableName, stream, newId())));
    }
    
    CompletableFuture<Void> removeStreamFromScope(String stream) {
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> Futures.toVoid(storeHelper.removeEntry(scopeName, tableName, stream)));
    }
}

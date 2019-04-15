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
import io.pravega.common.util.BitConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DATA_NOT_FOUND_PREDICATE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SCOPES_TABLE;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.SEPARATOR;
import static io.pravega.shared.NameUtils.INTERNAL_SCOPE_NAME;
import static io.pravega.shared.segment.StreamSegmentNameUtils.getQualifiedTableName;

@Slf4j
/**
 * Pravega Tables based scope metadata.
 * At top level, there is a common scopes table at _system. This has a list of all scopes in the cluster. 
 * Then there are per scopes table called `scope`/streamsInScope.
 * Each such scope table is protected against recreation of scope by attaching a unique id to the scope when it is created. 
 */
public class PravegaTablesScope implements Scope {
    private static final String STREAMS_IN_SCOPE_TABLE_FORMAT = "streamsInScope" + SEPARATOR + "%s";
    private final String scopeName;
    private final PravegaTablesStoreHelper storeHelper;
    private final AtomicReference<UUID> idRef;

    PravegaTablesScope(final String scopeName, PravegaTablesStoreHelper storeHelper) {
        this.scopeName = scopeName;
        this.storeHelper = storeHelper;
        this.idRef = new AtomicReference<>(null);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Void> createScope() {
        // We will first attempt to create the entry for the scope in scopes table. 
        // If scopes table does not exist, we create the scopes table (idempotent)
        // followed by creating a new entry for this scope with a new unique id. 
        // We then retrive id from the store (in case someone concurrently created the entry or entry already existed.
        // This unique id is used to create scope specific table with unique id 
        return Futures.exceptionallyComposeExpecting(storeHelper.addNewEntry(
                SCOPES_TABLE, scopeName, newId()),
                DATA_NOT_FOUND_PREDICATE,
                () -> storeHelper.createTable(SCOPES_TABLE)
                                 .thenCompose(v -> {
                                     log.debug("table created {}", SCOPES_TABLE);
                                     return storeHelper.addNewEntryIfAbsent(SCOPES_TABLE, scopeName, newId());
                                 }))
                      .thenCompose(v -> getStreamsInScopeTableName())
                      .thenCompose(tableName -> storeHelper.createTable(tableName)
                                                           .thenAccept(v -> log.debug("table created {}", tableName)));
    }

    private byte[] newId() {
        byte[] b = new byte[2 * Long.BYTES];
        BitConverter.writeUUID(b, 0, UUID.randomUUID());
        return b;
    }

    CompletableFuture<String> getStreamsInScopeTableName() {
        return getId().thenApply(id -> 
                getQualifiedTableName(INTERNAL_SCOPE_NAME, scopeName, String.format(STREAMS_IN_SCOPE_TABLE_FORMAT, id.toString())));
    }

    CompletableFuture<UUID> getId() {
        UUID id = idRef.get();
        if (Objects.isNull(id)) {
            return storeHelper.getEntry(
                    SCOPES_TABLE, 
                    scopeName, x -> BitConverter.readUUID(x, 0))
                              .thenCompose(entry -> {
                                  UUID uuid = entry.getObject();
                                  idRef.compareAndSet(null, uuid);
                                  return getId();
                              });
        } else {
            return CompletableFuture.completedFuture(id);
        }
    }

    @Override
    public CompletableFuture<Void> deleteScope() {
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> storeHelper.deleteTable(tableName, true)
                                                     .thenAccept(v -> log.debug("table deleted {}", tableName)))
                .thenCompose(deleted -> storeHelper.removeEntry(SCOPES_TABLE, scopeName));
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listStreams(int limit, String continuationToken, Executor executor) {
        List<String> taken = new ArrayList<>();
        AtomicReference<String> token = new AtomicReference<>(continuationToken);
        AtomicBoolean canContinue = new AtomicBoolean(true);
        return getStreamsInScopeTableName()
                .thenCompose(streamsInScopeTable -> storeHelper.getKeysPaginated(streamsInScopeTable,
                        Unpooled.wrappedBuffer(Base64.getDecoder().decode(token.get())), limit)
                                                 .thenApply(result -> {
                                                     if (result.getValue().isEmpty()) {
                                                         canContinue.set(false);
                                                     } else {
                                                         taken.addAll(result.getValue());
                                                     }
                                                     token.set(Base64.getEncoder().encodeToString(result.getKey().array()));
                                                     return new ImmutablePair<>(taken, token.get());
                                                 }));
    }

    @Override
    public CompletableFuture<List<String>> listStreamsInScope() {
        List<String> result = new ArrayList<>();
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> storeHelper.getAllKeys(tableName).collectRemaining(result::add)
                                                     .thenApply(v -> result));
    }

    @Override
    public void refresh() {
        idRef.set(null);
    }

    CompletableFuture<Void> addStreamToScope(String stream) {
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> Futures.toVoid(storeHelper.addNewEntryIfAbsent(tableName, stream, newId())));
    }

    CompletableFuture<Void> removeStreamFromScope(String stream) {
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> Futures.toVoid(storeHelper.removeEntry(tableName, stream)));
    }

    CompletableFuture<Boolean> checkStreamExistsInScope(String stream) {
        return getStreamsInScopeTableName()
                .thenCompose(tableName -> storeHelper.expectingDataNotFound(
                        storeHelper.getEntry(tableName, stream, x -> x).thenApply(v -> true), false));
    }
}

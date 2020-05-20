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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.PravegaTablesScope;
import io.pravega.common.Exceptions;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.store.stream.*;
import io.pravega.controller.util.Config;
import io.pravega.shared.NameUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * Pravega Tables stream metadata store.
 */
@Slf4j
public class PravegaTablesKVTableMetadataStore extends AbstractKVTableMetadataStore {
    static final String SEPARATOR = ".#.";
    static final String SCOPES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "scopes");
    static final String DELETED_STREAMS_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "deletedStreams");

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final PravegaTablesStoreHelper storeHelper;
    private final ScheduledExecutorService executor;

    @VisibleForTesting
    PravegaTablesKVTableMetadataStore(SegmentHelper segmentHelper, CuratorFramework client, ScheduledExecutorService executor, GrpcAuthHelper authHelper) {
        this(segmentHelper, client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS), authHelper);
    }

    @VisibleForTesting
    PravegaTablesKVTableMetadataStore(SegmentHelper segmentHelper, CuratorFramework curatorClient, ScheduledExecutorService executor, Duration gcPeriod, GrpcAuthHelper authHelper) {
        super(new ZKHostIndex(curatorClient, "/hostRequestIndex", executor));
        this.storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
        this.executor = executor;
    }

    @Override
    PravegaTablesKeyValueTable newKeyValueTable(final String scope, final String name) {
        return new PravegaTablesKeyValueTable(scope, name, storeHelper,
                () -> ((PravegaTablesScope) getScope(scope)).getStreamsInScopeTableName(), executor);
    }

    @Override
    PravegaTablesScope newScope(final String scopeName) {
        return new PravegaTablesScope(scopeName, storeHelper);
    }
/*
    @Override
    CompletableFuture<Int96> getNextCounter() {
        return withCompletion(counter.getNextCounter(), executor);
    }
*/

    @Override
    CompletableFuture<Boolean> checkScopeExists(String scope) {
        return withCompletion(storeHelper.expectingDataNotFound(
                storeHelper.getEntry(SCOPES_TABLE, scope, x -> x).thenApply(v -> true),
                false), executor);
    }

    @Override
    public CompletableFuture<CreateKVTableResponse> createKeyValueTable(final String scope,
                                                                final String name,
                                                                final KeyValueTableConfiguration configuration,
                                                                final long createTimestamp,
                                                                final KVTOperationContext context,
                                                                final Executor executor) {
        return withCompletion(
                ((PravegaTablesScope) getScope(scope))
                        .addKVTableToScope(name)
                        .thenCompose(id -> super.createKeyValueTable(scope, name, configuration, createTimestamp, context, executor)),
                executor);
    }

    @Override
    public CompletableFuture<Boolean> checkKeyValueTableExists(final String scopeName,
                                                               final String streamName) {
        return withCompletion(((PravegaTablesScope) getScope(scopeName)).checkKVTableExistsInScope(streamName), executor);
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String kvtName) {
        return withCompletion(storeHelper.getEntry(DELETED_STREAMS_TABLE, getScopedKVTName(scopeName, kvtName),
                x -> BitConverter.readInt(x, 0))
                .handle((data, ex) -> {
                    if (ex == null) {
                        return data.getObject() + 1;
                    } else if (Exceptions.unwrap(ex) instanceof StoreException.DataNotFoundException) {
                        return 0;
                    } else {
                        log.error("Problem found while getting a safe starting segment number for {}.",
                                getScopedKVTName(scopeName, kvtName), ex);
                        throw new CompletionException(ex);
                    }
                }), executor);
    }



    @Override
    public void close() {
        // do nothing
    }
/*
    @Override
    public CompletableFuture<Void> deleteStream(final String scope,
                                                final String name,
                                                final OperationContext context,
                                                final Executor executor) {
        return withCompletion(super.deleteStream(scope, name, context, executor)
                    .thenCompose(status -> ((PravegaTablesScope) getScope(scope)).removeStreamFromScope(name).thenApply(v -> status)),
                executor);
    }
*/
    /*
    @Override
    Version getEmptyVersion() {
        return Version.LongVersion.EMPTY;
    }

    @Override
    Version parseVersionData(byte[] data) {
        return Version.IntVersion.fromBytes(data);
    }

    @Override
    public CompletableFuture<String> getScopeConfiguration(final String scopeName) {
        return withCompletion(storeHelper.getEntry(SCOPES_TABLE, scopeName, x -> x)
                          .thenApply(x -> scopeName), executor);
    }

    @Override
    public CompletableFuture<List<String>> listScopes() {
        List<String> scopes = new ArrayList<>();
        return withCompletion(Futures.exceptionallyComposeExpecting(storeHelper.getAllKeys(SCOPES_TABLE)
                                                                .collectRemaining(scopes::add)
                                                                .thenApply(v -> scopes), DATA_NOT_FOUND_PREDICATE,
                () -> storeHelper.createTable(SCOPES_TABLE).thenApply(v -> Collections.emptyList())),
                executor);
    }
*/

}

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
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.PravegaTablesScope;
import static io.pravega.controller.store.stream.PravegaTablesStreamMetadataStore.DELETED_STREAMS_TABLE;
import io.pravega.common.Exceptions;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.store.stream.StoreException;
import static io.pravega.shared.NameUtils.getQualifiedTableName;
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

/**
 * Pravega Tables stream metadata store.
 */
@Slf4j
public class PravegaTablesKVTMetadataStore extends AbstractKVTableMetadataStore {
    static final String SCOPES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "scopes");

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final PravegaTablesStoreHelper storeHelper;
    private final ScheduledExecutorService executor;

    @VisibleForTesting
    PravegaTablesKVTMetadataStore(SegmentHelper segmentHelper, CuratorFramework client, ScheduledExecutorService executor, GrpcAuthHelper authHelper) {
        this(segmentHelper, client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS), authHelper);
    }

    @VisibleForTesting
    PravegaTablesKVTMetadataStore(SegmentHelper segmentHelper, CuratorFramework curatorClient, ScheduledExecutorService executor, Duration gcPeriod, GrpcAuthHelper authHelper) {
        super(new ZKHostIndex(curatorClient, "/hostRequestIndex", executor));
        this.storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
        this.executor = executor;
    }

    @Override
    PravegaTablesKVTable newKeyValueTable(final String scope, final String name) {
        log.info("Fetching KV Table from pravega tables store {}/{}", scope, name);
        return new PravegaTablesKVTable(scope, name, storeHelper,
                () -> ((PravegaTablesScope) getScope(scope)).getKVTablesInScopeTableName(), executor);
    }

    @Override
    public PravegaTablesScope newScope(final String scopeName) {
        return new PravegaTablesScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<Boolean> checkScopeExists(String scope) {
        return Futures.completeOn(storeHelper.expectingDataNotFound(
                storeHelper.getEntry(SCOPES_TABLE, scope, x -> x).thenApply(v -> true),
                false), executor);
    }

    public CompletableFuture<Void> createEntryForKVTable(final String scopeName,
                                                         final String kvtName,
                                                         final byte[] id,
                                                         final Executor executor) {
        return Futures.completeOn(((PravegaTablesScope) getScope(scopeName)).addKVTableToScope(kvtName, id), executor);
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String kvtName) {
        return Futures.completeOn(storeHelper.getEntry(DELETED_STREAMS_TABLE, getScopedKVTName(scopeName, kvtName),
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
}

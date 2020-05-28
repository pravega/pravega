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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.OperationContext;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.Artifact;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ControllerEventSerializer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public abstract class AbstractTableMetadataStore implements TableMetadataStore {
    private final LoadingCache<String, Scope> scopeCache;
    private final LoadingCache<Pair<String, String>, KeyValueTable> cache;
    private final HostIndex hostTaskIndex;
    private final ControllerEventSerializer controllerEventSerializer;

    protected AbstractTableMetadataStore(HostIndex hostTaskIndex) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<Pair<String, String>, KeyValueTable>() {
                            @Override
                            @ParametersAreNonnullByDefault
                            public KeyValueTable load(Pair<String, String> input) {
                                try {
                                    return newKeyValueTable(input.getKey(), input.getValue());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

        scopeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, Scope>() {
                            @Override
                            @ParametersAreNonnullByDefault
                            public Scope load(String scopeName) {
                                try {
                                    return newScope(scopeName);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
        this.hostTaskIndex = hostTaskIndex;
        this.controllerEventSerializer = new ControllerEventSerializer();
    }

    protected Scope getScope(final String scopeName) {
        Scope scope = scopeCache.getUnchecked(scopeName);
        scope.refresh();
        return scope;
    }

    /**
     * Returns a Scope object from scope identifier.
     *
     * @param scopeName scope identifier is scopeName.
     * @return Scope object.
     */
    abstract Scope newScope(final String scopeName);

    abstract KeyValueTable newKeyValueTable(final String scope, final String kvTableName);

    @Override
    public OperationContext createContext(String scope, String name) {
        return new KVTOperationContext(getKVTable(scope, name, null));
    }

    public Artifact getArtifact(String scope, String stream, OperationContext context) {
        return getKVTable(scope, stream, context);
    }

    protected KeyValueTable getKVTable(String scope, final String name, OperationContext context) {
        KeyValueTable kvt;
        if (context != null) {
            kvt = (KeyValueTable) context.getObject();
            assert kvt.getScope().equals(scope);
            assert kvt.getName().equals(name);
        } else {
            kvt = cache.getUnchecked(new ImmutablePair<>(scope, name));
            kvt.refresh();
        }
        return kvt;
    }

    @Override
    public CompletableFuture<CreateKVTableResponse> createKeyValueTable(final String scope,
                                                                final String name,
                                                                final KeyValueTableConfiguration configuration,
                                                                final long createTimestamp,
                                                                final OperationContext context,
                                                                final Executor executor) {
        return getSafeStartingSegmentNumberFor(scope, name)
                .thenCompose(startingSegmentNumber ->
                        Futures.withCompletion(checkScopeExists(scope)
                                .thenCompose(exists -> {
                                    if (exists) {
                                        // Create kvtable may fail if scope is deleted as we attempt to create the table under scope.
                                        return getKVTable(scope, name, context)
                                                .create(configuration, createTimestamp, startingSegmentNumber);
                                    } else {
                                        return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "scope does not exist"));
                                    }
                                }), executor));
    }

    String getScopedKVTName(String scope, String name) {
        return String.format("%s/%s", scope, name);
    }

    @Override
    public CompletableFuture<Long> getCreationTime(final String scope,
                                                   final String name,
                                                   final OperationContext context,
                                                   final Executor executor) {
        return Futures.withCompletion(getKVTable(scope, name, context).getCreationTime(), executor);
    }

    @Override
    public CompletableFuture<Void> setState(final String scope, final String name,
                                            final KVTableState state, final OperationContext context,
                                            final Executor executor) {
        return Futures.withCompletion(getKVTable(scope, name, context).updateState(state), executor);
    }

    @Override
    public CompletableFuture<KVTableState> getState(final String scope, final String name,
                                             final boolean ignoreCached,
                                             final OperationContext context,
                                             final Executor executor) {
        return Futures.withCompletion(getKVTable(scope, name, context).getState(ignoreCached), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<KVTableState>> updateVersionedState(final String scope, final String name,
                                                                            final KVTableState state, final VersionedMetadata<KVTableState> previous,
                                                                            final OperationContext context,
                                                                            final Executor executor) {
        return Futures.withCompletion(getKVTable(scope, name, context).updateVersionedState(previous, state), executor);
    }

    @Override
    public CompletableFuture<VersionedMetadata<KVTableState>> getVersionedState(final String scope, final String name,
                                                                         final OperationContext context,
                                                                         final Executor executor) {
        return Futures.withCompletion(getKVTable(scope, name, context).getVersionedState(), executor);
    }

    @Override
    public CompletableFuture<Void> addRequestToIndex(String hostId, String id, ControllerEvent task) {
        return hostTaskIndex.addEntity(hostId, id, controllerEventSerializer.toByteBuffer(task).array());
    }

    @Override
    public CompletableFuture<Void> removeTaskFromIndex(String hostId, String id) {
        return hostTaskIndex.removeEntity(hostId, id, true);
    }

    /**
     * This method retrieves a safe base segment number from which a stream's segment ids may start. In the case of a
     * new stream, this method will return 0 as a starting segment number (default). In the case that a stream with the
     * same name has been recently deleted, this method will provide as a safe starting segment number the last segment
     * number of the previously deleted stream + 1. This will avoid potential segment naming collisions with segments
     * being asynchronously deleted from the segment store.
     *
     * @param scopeName scope
     * @param kvtName KeyValueTable name
     * @return CompletableFuture with a safe starting segment number for this stream.
     */
    abstract CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String kvtName);

    public abstract CompletableFuture<Boolean> checkScopeExists(String scope);

    public abstract CompletableFuture<UUID> createEntryForKVTable(final String scopeName,
                                                                  final String kvtName,
                                                                  final Executor executor);
}

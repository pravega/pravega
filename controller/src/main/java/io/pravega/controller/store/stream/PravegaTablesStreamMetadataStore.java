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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.Int96;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.index.PravegaTablesHostIndex;
import io.pravega.controller.util.Config;
import io.pravega.shared.NameUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Pravega Tables stream metadata store.
 */
@Slf4j
public class PravegaTablesStreamMetadataStore extends AbstractStreamMetadataStore {
    public static final String SEPARATOR = ".#.";
    static final String SCOPES_TABLE = "Table" + SEPARATOR + "scopes";
    static final String DELETED_STREAMS_TABLE = "Table" + SEPARATOR + "deletedStreams";
    static final String COMPLETED_TRANSACTIONS_BATCHES_TABLE = "Table" + SEPARATOR + "completedTransactionsBatches";
    static final String COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT = "Table" + SEPARATOR + "completedTransactionsBatch-%d";

    private static final String COMPLETED_TXN_GC_NAME = "completedTxnGC";

    private final ZkInt96Counter counter;
    private final ZKGarbageCollector completedTxnGC;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final PravegaTablesStoreHelper storeHelper;
    private final ScheduledExecutorService executor;

    @VisibleForTesting
    PravegaTablesStreamMetadataStore(SegmentHelper segmentHelper, CuratorFramework client, ScheduledExecutorService executor) {
        this(segmentHelper, client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS));
    }

    @VisibleForTesting
    PravegaTablesStreamMetadataStore(SegmentHelper segmentHelper, CuratorFramework curatorClient, ScheduledExecutorService executor, Duration gcPeriod) {
        super(new PravegaTablesHostIndex("hostTxnIndex", new PravegaTablesStoreHelper(segmentHelper, executor)));
        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(curatorClient, executor);
        this.completedTxnGC = new ZKGarbageCollector(COMPLETED_TXN_GC_NAME, zkStoreHelper, this::gcCompletedTxn, gcPeriod);
        this.completedTxnGC.startAsync();
        this.completedTxnGC.awaitRunning();
        this.counter = new ZkInt96Counter(zkStoreHelper);
        this.storeHelper = new PravegaTablesStoreHelper(segmentHelper, executor);
        this.executor = executor;
    }

    private CompletableFuture<Void> gcCompletedTxn() {
        List<String> batches = new ArrayList<>();
        return Futures.exceptionallyExpecting(storeHelper.getAllKeys(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                                                         .collectRemaining(batches::add)
                                                         .thenApply(v -> {
                                                                     // retain latest two and delete remainder.
                                                                     if (batches.size() > 2) {
                                                                         return batches.subList(0, batches.size() - 2);
                                                                     } else {
                                                                         return new ArrayList<String>();
                                                                     }
                                                                 }
                                                         )
                                                         .thenCompose(toDeleteList -> {
                                                             log.debug("deleting batches {} on new scheme", toDeleteList);

                                                             // delete all those marked for toDelete.
                                                             return Futures.allOf(
                                                                     toDeleteList.stream()
                                                                                 .map(toDelete -> {
                                                                                     String table = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, Long.parseLong(toDelete));
                                                                                     return storeHelper.deleteTable(NameUtils.INTERNAL_SCOPE_NAME, table, false);
                                                                                 })
                                                                                 .collect(Collectors.toList()))
                                                                           .thenCompose(v -> storeHelper.removeEntries(NameUtils.INTERNAL_SCOPE_NAME, COMPLETED_TRANSACTIONS_BATCHES_TABLE, toDeleteList));
                                                         }), DATA_NOT_FOUND_PREDICATE, null);
    }

    @Override
    PravegaTablesStream newStream(final String scope, final String name) {
        return new PravegaTablesStream(scope, name, storeHelper, completedTxnGC::getLatestBatch,
                () -> ((PravegaTableScope) getScope(scope)).getStreamsInScopeTableName());
    }

    @Override
    CompletableFuture<Int96> getNextCounter() {
        return counter.getNextCounter();
    }

    @Override
    CompletableFuture<Boolean> checkScopeExists(String scope) {
        return Futures.exceptionallyExpecting(storeHelper.getEntry(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE, scope).thenApply(v -> true),
                DATA_NOT_FOUND_PREDICATE, false);
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(final String scope,
                                                                final String name,
                                                                final StreamConfiguration configuration,
                                                                final long createTimestamp,
                                                                final OperationContext context,
                                                                final Executor executor) {
        return ((PravegaTableScope) getScope(scope)).addStreamToScope(name)
                                                    .thenCompose(id -> super.createStream(scope, name, configuration, createTimestamp, context, executor));
    }

    @Override
    public CompletableFuture<Void> deleteStream(final String scope,
                                                final String name,
                                                final OperationContext context,
                                                final Executor executor) {
        return super.deleteStream(scope, name, context, executor)
                    .thenCompose(status -> ((PravegaTableScope) getScope(scope)).removeStreamFromScope(name).thenApply(v -> status));
    }

    @Override
    Version getEmptyVersion() {
        return Version.LongVersion.EMPTY;
    }

    @Override
    Version parseVersionData(byte[] data) {
        return Version.IntVersion.fromBytes(data);
    }

    @Override
    PravegaTableScope newScope(final String scopeName) {
        return new PravegaTableScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<String> getScopeConfiguration(final String scopeName) {
        return storeHelper.getEntry(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE, scopeName)
                          .thenApply(x -> scopeName);
    }

    @Override
    public CompletableFuture<List<String>> listScopes() {
        List<String> scopes = new ArrayList<>();
        return Futures.exceptionallyComposeExpecting(storeHelper.getAllKeys(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE)
                                                                .collectRemaining(scopes::add)
                                                                .thenApply(v -> scopes), DATA_NOT_FOUND_PREDICATE,
                () -> storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, SCOPES_TABLE).thenApply(v -> Collections.emptyList()));
    }

    @Override
    public CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                        final String streamName) {
        return Futures.exceptionallyExpecting(getStream(scopeName, streamName, null)
                .getState(true).thenApply(v -> true), DATA_NOT_FOUND_PREDICATE, false);
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName) {
        return storeHelper.getEntry(NameUtils.INTERNAL_SCOPE_NAME, DELETED_STREAMS_TABLE, getScopedStreamName(scopeName, streamName))
                          .handle((data, ex) -> {
                              if (ex == null) {
                                  return BitConverter.readInt(data.getData(), 0) + 1;
                              } else if (Exceptions.unwrap(ex) instanceof StoreException.DataNotFoundException) {
                                  return 0;
                              } else {
                                  log.error("Problem found while getting a safe starting segment number for {}.",
                                          getScopedStreamName(scopeName, streamName), ex);
                                  throw new CompletionException(ex);
                              }
                          });
    }

    @Override
    CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, final int lastActiveSegment,
                                                    OperationContext context, final Executor executor) {
        final String key = getScopedStreamName(scope, stream);
        byte[] maxSegmentNumberBytes = new byte[Integer.BYTES];
        BitConverter.writeInt(maxSegmentNumberBytes, 0, lastActiveSegment);
        return storeHelper.createTable(NameUtils.INTERNAL_SCOPE_NAME, DELETED_STREAMS_TABLE)
                          .thenCompose(created -> {
                              return Futures.exceptionallyExpecting(storeHelper.getEntry(NameUtils.INTERNAL_SCOPE_NAME, DELETED_STREAMS_TABLE, key),
                                      DATA_NOT_FOUND_PREDICATE, null)
                                            .thenCompose(existing -> {
                                                log.debug("Recording last segment {} for stream {}/{} on deletion.", lastActiveSegment, scope, stream);
                                                if (existing != null) {
                                                    final int oldLastActiveSegment = BitConverter.readInt(existing.getData(), 0);
                                                    Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                                            "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                                            oldLastActiveSegment, scope, stream, lastActiveSegment);
                                                    return Futures.toVoid(storeHelper.updateEntry(NameUtils.INTERNAL_SCOPE_NAME, DELETED_STREAMS_TABLE,
                                                            key, new Data(maxSegmentNumberBytes, existing.getVersion())));
                                                } else {
                                                    return Futures.toVoid(storeHelper.addNewEntryIfAbsent(NameUtils.INTERNAL_SCOPE_NAME, DELETED_STREAMS_TABLE,
                                                            key, maxSegmentNumberBytes));
                                                }
                                            });
                          });
    }

    @Override
    public void close() {
        completedTxnGC.stopAsync();
        completedTxnGC.awaitTerminated();
    }
}

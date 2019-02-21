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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * ZK stream metadata store.
 */
@Slf4j
class PravegaTablesStreamMetadataStore extends AbstractStreamMetadataStore {
    static final String SYSTEM_SCOPE = "_system";
    static final String SCOPES_TABLE = "scopes";
    static final String DELETED_STREAMS_TABLE = "deletedStreams";
    static final String COMPLETED_TRANSACTIONS_BATCHES_TABLE = "completedTransactionsBatches";
    static final String COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT = "completedTransactionsBatch-%d";
    static final Predicate<Throwable> DATA_NOT_FOUND_PREDICATE = e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException;

    private static final String COMPLETED_TXN_GC_NAME = "completedTxnGC";

    private final ZkInt96Counter counter;
    private final ZKGarbageCollector completedTxnGC;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final PravegaTablesStoreHelper storeHelper;
    private final Executor executor;

    @VisibleForTesting
    PravegaTablesStreamMetadataStore(SegmentHelper segmentHelper, CuratorFramework client, Executor executor) {
        this(segmentHelper, client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS));
    }
    
    @VisibleForTesting
    PravegaTablesStreamMetadataStore(SegmentHelper segmentHelper, CuratorFramework curatorClient, Executor executor, Duration gcPeriod) {
        super(new PravegaTablesHostIndex(segmentHelper, "hostTxnIndex", executor));
        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(curatorClient, executor);
        this.completedTxnGC = new ZKGarbageCollector(COMPLETED_TXN_GC_NAME, zkStoreHelper, this::gcCompletedTxn, gcPeriod);
        this.completedTxnGC.startAsync();
        this.completedTxnGC.awaitRunning();
        this.counter = new ZkInt96Counter(zkStoreHelper);
        this.storeHelper = new PravegaTablesStoreHelper(segmentHelper, executor);
        this.executor = executor;
    }

    private CompletableFuture<Void> gcCompletedTxn() {
        List<String> batches = new LinkedList<>();
        return Futures.exceptionallyExpecting(storeHelper.getAllKeys(SYSTEM_SCOPE, COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                          .forEachRemaining(batches::add, executor)
                          .thenApply(v -> {
                                      // retain latest two and delete remainder.
                                      if (batches.size() > 2) {
                                          return batches.subList(0, batches.size() - 2);
                                      } else {
                                          return new ArrayList<String>();
                                      }
                                  }
                          ), DATA_NOT_FOUND_PREDICATE, new ArrayList<String>())
                          .thenCompose(toDeleteList -> {
                              log.debug("deleting batches {} on new scheme", toDeleteList);

                              // delete all those marked for toDelete.
                              return Futures.allOf(
                                      toDeleteList.stream()
                                                  .map(toDelete -> {
                                                      String table = String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, Long.parseLong(toDelete));
                                                      return storeHelper.deleteTable(SYSTEM_SCOPE, table, false);
                                                  })
                                                  .collect(Collectors.toList()))
                                      .thenCompose(v -> storeHelper.removeEntries(SYSTEM_SCOPE, COMPLETED_TRANSACTIONS_BATCHES_TABLE, toDeleteList));
                          });
    }

    @Override
    PravegaTablesStream newStream(final String scope, final String name) {
        return new PravegaTablesStream(scope, name, storeHelper, completedTxnGC::getLatestBatch, executor);
    }

    @Override
    CompletableFuture<Int96> getNextCounter() {
        return counter.getNextCounter();
    }

    @Override
    CompletableFuture<Boolean> checkScopeExists(String scope) {
        return Futures.exceptionallyExpecting(storeHelper.getEntry(SYSTEM_SCOPE, SCOPES_TABLE, scope).thenApply(v -> true), 
                DATA_NOT_FOUND_PREDICATE, false);
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(final String scope,
                                                                final String name,
                                                                final StreamConfiguration configuration,
                                                                final long createTimestamp,
                                                                final OperationContext context,
                                                                final Executor executor) {
        return super.createStream(scope, name, configuration, createTimestamp, context, executor)
                    .thenCompose(status -> ((PravegaTableScope) getScope(scope)).addStreamToScope(name).thenApply(v -> status));
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
        return new PravegaTableScope(scopeName, storeHelper, executor);
    }

    @Override
    public CompletableFuture<String> getScopeConfiguration(final String scopeName) {
        return storeHelper.getEntry(SYSTEM_SCOPE, SCOPES_TABLE, scopeName)
                .thenApply(x -> scopeName);
    }

    @Override
    public CompletableFuture<List<String>> listScopes() {
        List<String> scopes = new LinkedList<>();
        return storeHelper.getAllKeys(SYSTEM_SCOPE, SCOPES_TABLE).forEachRemaining(scopes::add, executor)
                          .thenApply(v -> scopes);
    }

    @Override
    public CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                        final String streamName) {
        return Futures.exceptionallyExpecting(getStream(scopeName, streamName, null)
                .getState(true).thenApply(v -> true), DATA_NOT_FOUND_PREDICATE, false);
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName) {
        return storeHelper.getEntry(SYSTEM_SCOPE, DELETED_STREAMS_TABLE, getScopedStreamName(scopeName, streamName))
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
        return storeHelper.createTable(SYSTEM_SCOPE, DELETED_STREAMS_TABLE)
                .thenCompose(created -> {
                    return Futures.exceptionallyExpecting(storeHelper.getEntry(SYSTEM_SCOPE, DELETED_STREAMS_TABLE, key),
                            DATA_NOT_FOUND_PREDICATE, null)
                            .thenCompose(existing -> {
                                log.debug("Recording last segment {} for stream {}/{} on deletion.", lastActiveSegment, scope, stream);
                                if (existing != null) {
                                    final int oldLastActiveSegment = BitConverter.readInt(existing.getData(), 0);
                                    Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                            "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                            oldLastActiveSegment, scope, stream, lastActiveSegment);
                                    return Futures.toVoid(storeHelper.updateEntry(SYSTEM_SCOPE, DELETED_STREAMS_TABLE, 
                                            key, new Data(maxSegmentNumberBytes, existing.getVersion())));
                                } else {
                                    return Futures.toVoid(storeHelper.addNewEntryIfAbsent(SYSTEM_SCOPE, DELETED_STREAMS_TABLE, 
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

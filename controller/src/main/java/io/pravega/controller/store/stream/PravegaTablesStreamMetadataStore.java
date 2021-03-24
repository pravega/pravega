/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.Int96;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.PravegaTablesScope;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.ZKStoreHelper;
import io.pravega.controller.store.index.ZKHostIndex;
import io.pravega.controller.util.Config;
import io.pravega.shared.NameUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;

import static io.pravega.shared.NameUtils.getQualifiedTableName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Pravega Tables stream metadata store.
 */
@Slf4j
public class PravegaTablesStreamMetadataStore extends AbstractStreamMetadataStore {
    public static final String SEPARATOR = ".#.";
    public static final String SCOPES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "scopes");
    static final String DELETED_STREAMS_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "deletedStreams");
    static final String COMPLETED_TRANSACTIONS_BATCHES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, 
            "completedTransactionsBatches");
    static final String COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT = "completedTransactionsBatch-%d";

    private static final String COMPLETED_TXN_GC_NAME = "completedTxnGC";

    private final ZkInt96Counter counter;
    private final AtomicReference<ZKGarbageCollector> completedTxnGCRef;
    private final ZKGarbageCollector completedTxnGC;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final PravegaTablesStoreHelper storeHelper;
    private final ZkOrderedStore orderer;

    private final ScheduledExecutorService executor;
    @VisibleForTesting
    PravegaTablesStreamMetadataStore(SegmentHelper segmentHelper, CuratorFramework client, ScheduledExecutorService executor, GrpcAuthHelper authHelper) {
        this(segmentHelper, client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS), authHelper);
    }

    @VisibleForTesting
    PravegaTablesStreamMetadataStore(SegmentHelper segmentHelper, CuratorFramework curatorClient, ScheduledExecutorService executor, Duration gcPeriod, GrpcAuthHelper authHelper) {
        super(new ZKHostIndex(curatorClient, "/hostTxnIndex", executor), new ZKHostIndex(curatorClient, "/hostRequestIndex", executor));
        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(curatorClient, executor);
        this.orderer = new ZkOrderedStore("txnCommitOrderer", zkStoreHelper, executor);
        this.completedTxnGC = new ZKGarbageCollector(COMPLETED_TXN_GC_NAME, zkStoreHelper, this::gcCompletedTxn, gcPeriod);
        this.completedTxnGC.startAsync();
        this.completedTxnGC.awaitRunning();
        this.completedTxnGCRef = new AtomicReference<>(completedTxnGC);
        this.counter = new ZkInt96Counter(zkStoreHelper);
        this.storeHelper = new PravegaTablesStoreHelper(segmentHelper, authHelper, executor);
        this.executor = executor;
    }

    @VisibleForTesting 
    CompletableFuture<Void> gcCompletedTxn() {
        List<String> batches = new ArrayList<>();
        return Futures.completeOn(storeHelper.expectingDataNotFound(storeHelper.getAllKeys(COMPLETED_TRANSACTIONS_BATCHES_TABLE)
                                                         .collectRemaining(batches::add)
                                                         .thenApply(v -> findStaleBatches(batches))
                                                         .thenCompose(toDeleteList -> {
                                                             log.debug("deleting batches {} on new scheme", toDeleteList);

                                                             // delete all those marked for toDelete.
                                                             return Futures.allOf(
                                                                     toDeleteList.stream()
                                                                                 .map(toDelete -> {
                                                                                     String table = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, 
                                                                                             String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, Long.parseLong(toDelete)));
                                                                                     return storeHelper.deleteTable(table, false);
                                                                                 })
                                                                                 .collect(Collectors.toList()))
                                                                           .thenCompose(v -> storeHelper.removeEntries(COMPLETED_TRANSACTIONS_BATCHES_TABLE, toDeleteList));
                                                         }), null), executor);
    }

    @VisibleForTesting
    List<String> findStaleBatches(List<String> batches) {
        // exclude latest two batches and return remainder.
        if (batches.size() > 2) {
            int biggestIndex = Integer.MIN_VALUE;
            int secondIndex = Integer.MIN_VALUE;
            long biggest = Long.MIN_VALUE;
            long second = Long.MIN_VALUE;
            for (int i = 0; i < batches.size(); i++) {
                long element = Long.parseLong(batches.get(i));
                if (element > biggest) {
                    secondIndex = biggestIndex;
                    second = biggest;
                    biggest = element;
                    biggestIndex = i;
                } else if (element > second) {
                    secondIndex = i;
                    second = element;
                }
            }

            List<String> list = new ArrayList<>(batches);

            list.remove(biggestIndex);
            if (biggestIndex < secondIndex) {
                list.remove(secondIndex - 1);
            } else {
                list.remove(secondIndex);
            }
            return list;
        } else {
            return new ArrayList<>();
        }
    }

    @VisibleForTesting
    void setCompletedTxnGCRef(ZKGarbageCollector garbageCollector) {
        completedTxnGCRef.set(garbageCollector);
    }

    @Override
    PravegaTablesStream newStream(final String scope, final String name) {
        return new PravegaTablesStream(scope, name, storeHelper, orderer, completedTxnGCRef.get()::getLatestBatch,
                () -> ((PravegaTablesScope) getScope(scope)).getStreamsInScopeTableName(), executor);
    }

    @Override
    CompletableFuture<Int96> getNextCounter() {
        return Futures.completeOn(counter.getNextCounter(), executor);
    }

    @Override
    public CompletableFuture<Boolean> checkScopeExists(String scope) {
        return Futures.completeOn(storeHelper.expectingDataNotFound(
                storeHelper.getEntry(SCOPES_TABLE, scope, x -> x).thenApply(v -> true),
                false), executor);
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(final String scope,
                                                                final String name,
                                                                final StreamConfiguration configuration,
                                                                final long createTimestamp,
                                                                final OperationContext context,
                                                                final Executor executor) {
        return Futures.completeOn(
                ((PravegaTablesScope) getScope(scope))
                        .addStreamToScope(name)
                        .thenCompose(id -> super.createStream(scope, name, configuration, createTimestamp, context, executor)), 
                executor);
    }

    @Override
    public CompletableFuture<Void> deleteStream(final String scope,
                                                final String name,
                                                final OperationContext context,
                                                final Executor executor) {
        return Futures.completeOn(super.deleteStream(scope, name, context, executor)
                    .thenCompose(status -> ((PravegaTablesScope) getScope(scope)).removeStreamFromScope(name).thenApply(v -> status)),
                executor);
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
    PravegaTablesScope newScope(final String scopeName) {
        return new PravegaTablesScope(scopeName, storeHelper);
    }

    @Override
    public CompletableFuture<String> getScopeConfiguration(final String scopeName) {
        return Futures.completeOn(storeHelper.getEntry(SCOPES_TABLE, scopeName, x -> x)
                          .thenApply(x -> scopeName), executor);
    }

    @Override
    public CompletableFuture<List<String>> listScopes() {
        List<String> scopes = new ArrayList<>();
        return Futures.completeOn(Futures.exceptionallyComposeExpecting(storeHelper.getAllKeys(SCOPES_TABLE)
                                                                .collectRemaining(scopes::add)
                                                                .thenApply(v -> scopes), DATA_NOT_FOUND_PREDICATE,
                () -> storeHelper.createTable(SCOPES_TABLE).thenApply(v -> Collections.emptyList())),
                executor);
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listScopes(String continuationToken,
                                                                    int limit, Executor executor) {
        return Futures.completeOn(Futures.exceptionallyComposeExpecting(storeHelper.getKeysPaginated(SCOPES_TABLE, Unpooled.wrappedBuffer(Base64.getDecoder().decode(continuationToken)), limit)
                          .thenApply(result -> new ImmutablePair<>(result.getValue(),
                                  Base64.getEncoder().encodeToString(result.getKey().array()))), DATA_NOT_FOUND_PREDICATE,
                () -> storeHelper.createTable(SCOPES_TABLE).thenApply(v -> ImmutablePair.of(Collections.emptyList(), continuationToken))),
                executor);
    }


    @Override
    public CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                        final String streamName) {
        return Futures.completeOn(((PravegaTablesScope) getScope(scopeName)).checkStreamExistsInScope(streamName), executor);
    }

    @Override
    public CompletableFuture<Boolean> checkReaderGroupExists(final String scopeName,
                                                        final String rgName) {
        return Futures.completeOn(((PravegaTablesScope) getScope(scopeName)).checkReaderGroupExistsInScope(rgName), executor);
    }


    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName) {
        return Futures.completeOn(storeHelper.getEntry(DELETED_STREAMS_TABLE, getScopedStreamName(scopeName, streamName),
                x -> BitConverter.readInt(x, 0))
                          .handle((data, ex) -> {
                              if (ex == null) {
                                  return data.getObject() + 1;
                              } else if (Exceptions.unwrap(ex) instanceof StoreException.DataNotFoundException) {
                                  return 0;
                              } else {
                                  log.error("Problem found while getting a safe starting segment number for {}.",
                                          getScopedStreamName(scopeName, streamName), ex);
                                  throw new CompletionException(ex);
                              }
                          }), executor);
    }

    @Override
    CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, final int lastActiveSegment,
                                                    OperationContext context, final Executor executor) {
        final String key = getScopedStreamName(scope, stream);
        byte[] maxSegmentNumberBytes = new byte[Integer.BYTES];
        BitConverter.writeInt(maxSegmentNumberBytes, 0, lastActiveSegment);
        return Futures.completeOn(storeHelper.createTable(DELETED_STREAMS_TABLE)
                          .thenCompose(created -> {
                              return storeHelper.expectingDataNotFound(storeHelper.getEntry(
                                      DELETED_STREAMS_TABLE, key, x -> BitConverter.readInt(x, 0)),
                                      null)
                                            .thenCompose(existing -> {
                                                log.debug("Recording last segment {} for stream {}/{} on deletion.", lastActiveSegment, scope, stream);
                                                if (existing != null) {
                                                    final int oldLastActiveSegment = existing.getObject();
                                                    Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                                            "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                                            oldLastActiveSegment, scope, stream, lastActiveSegment);
                                                    return Futures.toVoid(storeHelper.updateEntry(DELETED_STREAMS_TABLE,
                                                            key, maxSegmentNumberBytes, existing.getVersion()));
                                                } else {
                                                    return Futures.toVoid(storeHelper.addNewEntryIfAbsent(DELETED_STREAMS_TABLE,
                                                            key, maxSegmentNumberBytes));
                                                }
                                            });
                          }), executor);
    }

    @Override
    public void close() {
        completedTxnGC.stopAsync();
        completedTxnGC.awaitTerminated();
    }

    // region Reader Group
    @Override
    PravegaTablesReaderGroup newReaderGroup(final String scope, final String rgName) {
        return new PravegaTablesReaderGroup(scope, rgName, storeHelper,
                () -> ((PravegaTablesScope) getScope(scope)).getReaderGroupsInScopeTableName(), executor);
    }

    @Override
    public CompletableFuture<Void> addReaderGroupToScope(final String scope,
                                                         final String name, final UUID readerGroupId) {
        return Futures.completeOn(((PravegaTablesScope) getScope(scope))
                .addReaderGroupToScope(name, readerGroupId), executor);
    }

    @Override
    public CompletableFuture<Void> deleteReaderGroup(final String scope, final String name,
                                                    final RGOperationContext context, final Executor executor) {
        return Futures.completeOn(super.deleteReaderGroup(scope, name, context, executor)
                        .thenCompose(status -> ((PravegaTablesScope) getScope(scope))
                                .removeReaderGroupFromScope(name).thenApply(v -> status)),
                executor);
    }
    //endregion
}

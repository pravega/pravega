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
import io.pravega.common.tracing.TagLogger;
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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.pravega.controller.store.PravegaTablesStoreHelper.BYTES_TO_INTEGER_FUNCTION;
import static io.pravega.controller.store.PravegaTablesStoreHelper.INTEGER_TO_BYTES_FUNCTION;
import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCHES_TABLE;
import static io.pravega.shared.NameUtils.COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT;
import static io.pravega.shared.NameUtils.DELETED_STREAMS_TABLE;
import static io.pravega.controller.store.PravegaTablesScope.DELETING_SCOPES_TABLE;
import static io.pravega.shared.NameUtils.getQualifiedTableName;

/**
 * Pravega Tables stream metadata store.
 */
public class PravegaTablesStreamMetadataStore extends AbstractStreamMetadataStore {
    public static final String SCOPES_TABLE = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, "scopes");

    private static final String COMPLETED_TXN_GC_NAME = "completedTxnGC";
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PravegaTablesStreamMetadataStore.class));

    private final ZkInt96Counter counter;
    private final AtomicReference<ZKGarbageCollector> completedTxnGCRef;
    private final ZKGarbageCollector completedTxnGC;
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final PravegaTablesStoreHelper storeHelper;
    private final ZkOrderedStore orderer;

    private final ScheduledExecutorService executor;
    @VisibleForTesting
    PravegaTablesStreamMetadataStore(SegmentHelper segmentHelper, CuratorFramework client, 
                                     ScheduledExecutorService executor, GrpcAuthHelper authHelper) {
        this(segmentHelper, client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS), authHelper);
    }

    @VisibleForTesting
    PravegaTablesStreamMetadataStore(SegmentHelper segmentHelper, CuratorFramework curatorClient,
                                     ScheduledExecutorService executor, Duration gcPeriod, GrpcAuthHelper authHelper) {
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
    public PravegaTablesStreamMetadataStore(CuratorFramework curatorClient, ScheduledExecutorService executor,
                                     Duration gcPeriod, PravegaTablesStoreHelper helper) {
        super(new ZKHostIndex(curatorClient, "/hostTxnIndex", executor), new ZKHostIndex(curatorClient, "/hostRequestIndex", executor));
        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(curatorClient, executor);
        this.orderer = new ZkOrderedStore("txnCommitOrderer", zkStoreHelper, executor);
        this.completedTxnGC = new ZKGarbageCollector(COMPLETED_TXN_GC_NAME, zkStoreHelper, this::gcCompletedTxn, gcPeriod);
        this.completedTxnGC.startAsync();
        this.completedTxnGC.awaitRunning();
        this.completedTxnGCRef = new AtomicReference<>(completedTxnGC);
        this.counter = new ZkInt96Counter(zkStoreHelper);
        this.storeHelper = helper;
        this.executor = executor;
    }

    @Override
    public OperationContext createScopeContext(String scopeName, long requestId) {
        PravegaTablesScope scope = newScope(scopeName);
        return new ScopeOperationContext(scope, requestId);
    }

    @Override
    public OperationContext createStreamContext(String scopeName, String streamName, long requestId) {
        PravegaTablesScope scope = newScope(scopeName);
        Stream stream = new PravegaTablesStream(scopeName, streamName, storeHelper, orderer,
                completedTxnGCRef.get()::getLatestBatch, scope::getStreamsInScopeTableName, executor);

        return new StreamOperationContext(scope, stream, requestId);
    }

    @Override
    @VisibleForTesting
    public CompletableFuture<Void> sealScope(String scope, OperationContext context, ScheduledExecutorService executor) {
        log.debug("Add entry to _deletingScopesTable called for scope {}", scope);
        return ((PravegaTablesScope) getScope(scope, context)).sealScope(scope, context);
    }

    @VisibleForTesting 
    CompletableFuture<Void> gcCompletedTxn() {
        List<String> batches = new ArrayList<>();
        OperationContext context = getOperationContext(null);
        return Futures.completeOn(storeHelper.expectingDataNotFound(storeHelper.getAllKeys(COMPLETED_TRANSACTIONS_BATCHES_TABLE, context.getRequestId())
                 .collectRemaining(batches::add)
                 .thenApply(v -> findStaleBatches(batches))
                 .thenCompose(toDeleteList -> {
                     log.debug(context.getRequestId(), "deleting batches {} on new scheme", toDeleteList);

                     // delete all those marked for toDelete.
                     return Futures.allOf(
                             toDeleteList.stream()
                                         .map(toDelete -> {
                                             String table = getQualifiedTableName(NameUtils.INTERNAL_SCOPE_NAME, 
                                                     String.format(COMPLETED_TRANSACTIONS_BATCH_TABLE_FORMAT, Long.parseLong(toDelete)));
                                             return storeHelper.deleteTable(table, false, context.getRequestId());
                                         })
                                         .collect(Collectors.toList()))
                                   .thenCompose(v -> storeHelper.removeEntries(COMPLETED_TRANSACTIONS_BATCHES_TABLE, toDeleteList,
                                           context.getRequestId()));
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
                (x, y) -> ((PravegaTablesScope) getScope(scope, y)).getStreamsInScopeTableName(x, y), executor);
    }

    @Override
    CompletableFuture<Int96> getNextCounter() {
        return Futures.completeOn(counter.getNextCounter(), executor);
    }

    @Override
    public CompletableFuture<Boolean> checkScopeExists(String scope,  OperationContext context, Executor executor) {
        long requestId = getOperationContext(context).getRequestId();
        return Futures.completeOn(storeHelper.expectingDataNotFound(
                storeHelper.getEntry(SCOPES_TABLE, scope, x -> x, requestId).thenApply(v -> true),
                false), executor);
    }

    @Override
    public CompletableFuture<Boolean> isScopeSealed(String scope, OperationContext context, Executor executor) {
        long requestId = getOperationContext(context).getRequestId();
        return Futures.completeOn(storeHelper.expectingDataNotFound(
                storeHelper.getEntry(DELETING_SCOPES_TABLE, scope, x -> x, requestId).thenApply(v -> true),
                false), executor);
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(final String scope,
                                                                final String name,
                                                                final StreamConfiguration configuration,
                                                                final long createTimestamp,
                                                                final OperationContext ctx,
                                                                final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(
                ((PravegaTablesScope) getScope(scope, context))
                        .addStreamToScope(name, context)
                        .thenCompose(id -> super.createStream(scope, name, configuration, createTimestamp, context, executor)),
                executor);
    }

    @Override
    public CompletableFuture<Void> deleteStream(final String scope,
                                                final String name,
                                                final  OperationContext ctx,
                                                final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(super.deleteStream(scope, name, context, executor)
                    .thenCompose(status -> ((PravegaTablesScope) getScope(scope, context)).removeStreamFromScope(name, context)
                                                                                          .thenApply(v -> status)),
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
    public CompletableFuture<String> getScopeConfiguration(final String scopeName,  OperationContext context, Executor executor) {
        long requestId = getOperationContext(context).getRequestId();

        return Futures.completeOn(storeHelper.getEntry(SCOPES_TABLE, scopeName, x -> x, requestId)
                          .thenApply(x -> scopeName), executor);
    }

    @Override
    public CompletableFuture<List<String>> listScopes(Executor executor, long requestId) {
        List<String> scopes = new ArrayList<>();
        return Futures.completeOn(Futures.exceptionallyComposeExpecting(storeHelper.getAllKeys(SCOPES_TABLE, requestId)
                                                                .collectRemaining(scopes::add)
                                                                .thenApply(v -> scopes), DATA_NOT_FOUND_PREDICATE,
                () -> storeHelper.createTable(SCOPES_TABLE, requestId).thenApply(v -> Collections.emptyList())),
                executor);
    }

    @Override
    public CompletableFuture<Pair<List<String>, String>> listScopes(String continuationToken,
                                                                    int limit, Executor executor, long requestId) {
        
        return Futures.completeOn(Futures.exceptionallyComposeExpecting(storeHelper.getKeysPaginated(SCOPES_TABLE, 
                Unpooled.wrappedBuffer(Base64.getDecoder().decode(continuationToken)), limit, requestId)
                          .thenApply(result -> new ImmutablePair<>(result.getValue(),
                                  Base64.getEncoder().encodeToString(result.getKey().array()))), DATA_NOT_FOUND_PREDICATE,
                () -> storeHelper.createTable(SCOPES_TABLE, requestId).thenApply(v -> ImmutablePair.of(Collections.emptyList(), continuationToken))),
                executor);
    }

    @Override
    public CompletableFuture<Void> addStreamTagsToIndex(String scope, String streamName, StreamConfiguration config,
                                                        OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        if (streamName.startsWith(NameUtils.INTERNAL_NAME_PREFIX) || config.getTags().isEmpty()) {
            // Tags are not allowed on internal streams.
            return CompletableFuture.completedFuture(null);
        } else {
            return Futures.completeOn(((PravegaTablesScope) getScope(scope, context))
                                              .addTagsUnderScope(streamName, config.getTags(), context), executor);
        }
    }

    @Override
    public CompletableFuture<Void> removeTagsFromIndex(String scope, String streamName, Set<String> tagsRemoved,
                                                       OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        if (streamName.startsWith(NameUtils.INTERNAL_NAME_PREFIX) || tagsRemoved.isEmpty()) {
            // Tags are not allowed on internal streams.
            return CompletableFuture.completedFuture(null);
        } else {
            return Futures.completeOn(((PravegaTablesScope) getScope(scope, context))
                                              .removeTagsUnderScope(streamName, tagsRemoved, context), executor);
        }
    }

    @Override
    public CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                        final String streamName,
                                                        final  OperationContext ctx, 
                                                        final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(((PravegaTablesScope) getScope(scopeName, context))
                .checkStreamExistsInScope(streamName, context), executor);
    }

    @Override
    public CompletableFuture<Boolean> checkReaderGroupExists(final String scopeName,
                                                             final String rgName,  OperationContext ctx, Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(((PravegaTablesScope) getScope(scopeName, context))
                .checkReaderGroupExistsInScope(rgName, context), executor);
    }

    @Override
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName, 
                                                                      final  OperationContext context, final Executor executor) {
        long requestId = getOperationContext(context).getRequestId();
        return Futures.completeOn(storeHelper.getEntry(DELETED_STREAMS_TABLE, getScopedStreamName(scopeName, streamName),
                BYTES_TO_INTEGER_FUNCTION, requestId)
                          .handle((data, ex) -> {
                              if (ex == null) {
                                  return data.getObject() + 1;
                              } else if (Exceptions.unwrap(ex) instanceof StoreException.DataNotFoundException) {
                                  return 0;
                              } else {
                                  log.warn(context.getRequestId(), 
                                          "Problem found while getting a safe starting segment number for {}.",
                                          getScopedStreamName(scopeName, streamName), ex);
                                  throw new CompletionException(ex);
                              }
                          }), executor);
    }

    @Override
    CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, final int lastActiveSegment,
                                                     OperationContext ctx, final Executor executor) {
        final String key = getScopedStreamName(scope, stream);
        OperationContext context = getOperationContext(ctx);
        long requestId = getOperationContext(context).getRequestId();

        return Futures.completeOn(
                Futures.handleCompose(storeHelper.getEntry(DELETED_STREAMS_TABLE, key, 
                        BYTES_TO_INTEGER_FUNCTION, requestId), 
                        (r, e) -> {
                            if (e != null) {
                                Throwable unwrap = Exceptions.unwrap(e);
                                if (unwrap instanceof StoreException.DataContainerNotFoundException) {
                                    return storeHelper.createTable(DELETED_STREAMS_TABLE, requestId).thenApply(v -> null);
                                } else if (unwrap instanceof StoreException.DataNotFoundException) {
                                    return CompletableFuture.completedFuture(null);
                                } else {
                                    throw new CompletionException(unwrap);
                                }
                            } else {
                                return CompletableFuture.completedFuture(r);
                            }
                        })
                       .thenCompose(existing -> {
                           log.debug(context.getRequestId(), 
                                   "Recording last segment {} for stream {}/{} on deletion.", lastActiveSegment, scope, stream);
                           if (existing != null) {
                               final int oldLastActiveSegment = existing.getObject();
                               Preconditions.checkArgument(lastActiveSegment >= oldLastActiveSegment,
                                       "Old last active segment ({}) for {}/{} is higher than current one {}.",
                                       oldLastActiveSegment, scope, stream, lastActiveSegment);
                               return Futures.toVoid(storeHelper.updateEntry(DELETED_STREAMS_TABLE,
                                       key, lastActiveSegment, INTEGER_TO_BYTES_FUNCTION, existing.getVersion(), requestId));
                           } else {
                               return Futures.toVoid(storeHelper.addNewEntryIfAbsent(DELETED_STREAMS_TABLE,
                                       key, lastActiveSegment, INTEGER_TO_BYTES_FUNCTION, requestId));
                           }
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
                (x, y) -> ((PravegaTablesScope) getScope(scope, null)).getReaderGroupsInScopeTableName(x, y), executor);
    }

    @Override
    public CompletableFuture<Void> addReaderGroupToScope(final String scope,
                                                         final String name, final UUID readerGroupId, final  OperationContext ctx,
                                                         final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(((PravegaTablesScope) getScope(scope, context))
                .addReaderGroupToScope(name, readerGroupId, context), executor);
    }

    @Override
    public CompletableFuture<Void> deleteReaderGroup(final String scope, final String name,
                                                    final  OperationContext ctx, final Executor executor) {
        OperationContext context = getOperationContext(ctx);
        return Futures.completeOn(super.deleteReaderGroup(scope, name, context, executor)
                        .thenCompose(status -> ((PravegaTablesScope) getScope(scope, context))
                                .removeReaderGroupFromScope(name, context).thenApply(v -> status)),
                executor);
    }
    //endregion
}

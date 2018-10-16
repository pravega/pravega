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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.lang.Int96;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamConfigurationRecord;
import io.pravega.controller.store.stream.tables.StreamCutRecord;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.pravega.shared.MetricsNames.ABORT_TRANSACTION;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION;
import static io.pravega.shared.MetricsNames.OPEN_TRANSACTIONS;
import static io.pravega.shared.MetricsNames.SEGMENTS_COUNT;
import static io.pravega.shared.MetricsNames.SEGMENTS_MERGES;
import static io.pravega.shared.MetricsNames.SEGMENTS_SPLITS;
import static io.pravega.shared.MetricsNames.nameFromStream;

/**
 * Abstract Stream metadata store. It implements various read queries using the Stream interface.
 * Implementation of create and update queries are delegated to the specific implementations of this abstract class.
 */
@Slf4j
public abstract class AbstractStreamMetadataStore implements StreamMetadataStore {

    protected static final StatsProvider METRICS_PROVIDER = MetricsProvider.getMetricsProvider();
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final StatsLogger STATS_LOGGER = METRICS_PROVIDER.createStatsLogger("controller");
    private static final OpStatsLogger CREATE_STREAM = STATS_LOGGER.createStats(MetricsNames.CREATE_STREAM);
    private static final OpStatsLogger SEAL_STREAM = STATS_LOGGER.createStats(MetricsNames.SEAL_STREAM);
    private static final OpStatsLogger DELETE_STREAM = STATS_LOGGER.createStats(MetricsNames.DELETE_STREAM);
    private final static String RESOURCE_PART_SEPARATOR = "_%_";

    protected final int bucketCount;

    private final LoadingCache<String, Scope> scopeCache;
    private final LoadingCache<Pair<String, String>, Stream> cache;
    private final HostIndex hostIndex;

    protected AbstractStreamMetadataStore(HostIndex hostIndex, int bucketCount) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<Pair<String, String>, Stream>() {
                            @Override
                            @ParametersAreNonnullByDefault
                            public Stream load(Pair<String, String> input) {
                                try {
                                    return newStream(input.getKey(), input.getValue());
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

        this.hostIndex = hostIndex;
        this.bucketCount = bucketCount;
    }

    /**
     * Returns a Scope object from scope identifier.
     *
     * @param scopeName scope identifier is scopeName.
     * @return Scope object.
     */
    abstract Scope newScope(final String scopeName);

    @Override
    public OperationContext createContext(String scope, String name) {
        return new OperationContextImpl(getStream(scope, name, null));
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(final String scope,
                                                   final String name,
                                                   final StreamConfiguration configuration,
                                                   final long createTimestamp,
                                                   final OperationContext context,
                                                   final Executor executor) {
        return getSafeStartingSegmentNumberFor(scope, name)
                .thenCompose(startingSegmentNumber ->
                    withCompletion(getStream(scope, name, context).create(configuration, createTimestamp, startingSegmentNumber), executor)
                    .thenApply(result -> {
                        if (result.getStatus().equals(CreateStreamResponse.CreateStatus.NEW)) {
                            CREATE_STREAM.reportSuccessValue(1);
                            DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, name), 0);
                            DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(SEGMENTS_COUNT, scope, name),
                                    configuration.getScalingPolicy().getMinNumSegments());
                            DYNAMIC_LOGGER.incCounterValue(nameFromStream(SEGMENTS_SPLITS, scope, name), 0);
                            DYNAMIC_LOGGER.incCounterValue(nameFromStream(SEGMENTS_MERGES, scope, name), 0);
                        }

                        return result;
                    }));
    }

    @Override
    public CompletableFuture<Void> deleteStream(final String scope,
                                                final String name,
                                                final OperationContext context,
                                                final Executor executor) {
        Stream s = getStream(scope, name, context);
        return s.getActiveSegments()
                .thenApply(activeSegments -> activeSegments.stream().map(StreamSegmentNameUtils::getSegmentNumber)
                                                                    .reduce(Integer::max).get())
                .thenCompose(lastActiveSegment -> recordLastStreamSegment(scope, name, lastActiveSegment, context, executor))
                .thenCompose(v -> withCompletion(s.delete(), executor))
                .thenAccept(v -> cache.invalidate(new ImmutablePair<>(scope, name)))
                .thenApply(result -> {
                    DELETE_STREAM.reportSuccessValue(1);
                    DYNAMIC_LOGGER.freezeCounter(nameFromStream(COMMIT_TRANSACTION, scope, name));
                    DYNAMIC_LOGGER.freezeGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, name));
                    DYNAMIC_LOGGER.freezeCounter(nameFromStream(SEGMENTS_SPLITS, scope, name));
                    DYNAMIC_LOGGER.freezeCounter(nameFromStream(SEGMENTS_MERGES, scope, name));
                    return result;
                });
    }

    @Override
    public CompletableFuture<Boolean> setState(final String scope, final String name,
                                               final State state, final OperationContext context,
                                               final Executor executor) {
        return withCompletion(getStream(scope, name, context).updateState(state), executor);
    }

    @Override
    public CompletableFuture<State> getState(final String scope, final String name,
                                             final boolean ignoreCached,
                                             final OperationContext context,
                                             final Executor executor) {
        return withCompletion(getStream(scope, name, context).getState(ignoreCached), executor);
    }

    @Override
    public CompletableFuture<Void> resetStateConditionally(String scope, String name, State state, OperationContext context, Executor executor) {
        return withCompletion(getStream(scope, name, context).resetStateConditionally(state), executor);
    }

    /**
     * Create a scope with given name.
     *
     * @param scopeName Name of scope to created.
     * @return CreateScopeStatus future.
     */
    @Override
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName) {
        return getScope(scopeName).createScope().handle((result, ex) -> {
            if (ex == null) {
                return CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SUCCESS).build();
            }
            if (ex instanceof StoreException.DataExistsException ||
                    ex.getCause() instanceof StoreException.DataExistsException) {
                return CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SCOPE_EXISTS).build();
            } else {
                log.debug("Create scope failed due to ", ex);
                return CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.FAILURE).build();
            }
        });
    }

    /**
     * Delete a scope with given name.
     *
     * @param scopeName Name of scope to be deleted
     * @return DeleteScopeStatus future.
     */
    @Override
    public CompletableFuture<DeleteScopeStatus> deleteScope(final String scopeName) {
        return getScope(scopeName).deleteScope().handle((result, ex) -> {
            if (ex == null) {
                return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SUCCESS).build();
            }
            if (ex.getCause() instanceof StoreException.DataNotFoundException
                    || ex instanceof StoreException.DataNotFoundException) {
                return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build();
            } else if (ex.getCause() instanceof StoreException.DataNotEmptyException
                    || ex instanceof StoreException.DataNotEmptyException) {
                return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY).build();
            } else {
                log.debug("DeleteScope failed due to {} ", ex);
                return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.FAILURE).build();
            }
        });
    }

    /**
     * List the streams in scope.
     *
     * @param scopeName Name of scope
     * @return List of streams in scope
     */
    @Override
    public CompletableFuture<List<StreamConfiguration>> listStreamsInScope(final String scopeName) {
        return getScope(scopeName).listStreamsInScope().thenCompose(streams -> {
            return Futures.allOfWithResults(
                    streams.stream()
                            .map(s -> getStream(scopeName, s, null).getConfiguration())
                            .collect(Collectors.toList()));
        });
    }


    @Override
    public CompletableFuture<Void> startTruncation(final String scope,
                                                   final String name,
                                                   final Map<Long, Long> streamCut,
                                                   final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).startTruncation(streamCut), executor);
    }

    @Override
    public CompletableFuture<Void> completeTruncation(final String scope, final String name,
                                                      final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).completeTruncation(), executor);
    }

    @Override
    public CompletableFuture<StreamTruncationRecord> getTruncationRecord(final String scope,
                                                                         final String name,
                                                                         final boolean ignoreCached,
                                                                         final OperationContext context,
                                                                         final Executor executor) {
        return withCompletion(getStream(scope, name, context).getTruncationRecord(ignoreCached), executor);
    }

    @Override
    public CompletableFuture<Void> startUpdateConfiguration(final String scope,
                                                            final String name,
                                                            final StreamConfiguration configuration,
                                                            final OperationContext context,
                                                            final Executor executor) {
        return withCompletion(getStream(scope, name, context).startUpdateConfiguration(configuration), executor);
    }

    @Override
    public CompletableFuture<Void> completeUpdateConfiguration(final String scope, final String name,
                                                               final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).completeUpdateConfiguration(), executor);
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration(final String scope,
                                                                   final String name,
                                                                   final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).getConfiguration(), executor);
    }

    @Override
    public CompletableFuture<StreamConfigurationRecord> getConfigurationRecord(final String scope,
                                                                               final String name,
                                                                               final boolean ignoreCached,
                                                                               final OperationContext context,
                                                                               final Executor executor) {
        return withCompletion(getStream(scope, name, context).getConfigurationRecord(ignoreCached), executor);
    }

    @Override
    public CompletableFuture<Boolean> isSealed(final String scope, final String name, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).getState(true).thenApply(state -> state.equals(State.SEALED)), executor);
    }

    @Override
    public CompletableFuture<Boolean> setSealed(final String scope, final String name, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).updateState(State.SEALED), executor).thenApply(result -> {
            SEAL_STREAM.reportSuccessValue(1);
            DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, name), 0);
            return result;
        });
    }


    @Override
    public CompletableFuture<Segment> getSegment(final String scope, final String name, final long segmentId, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).getSegment(segmentId), executor);
    }

    @Override
    public CompletableFuture<List<Segment>> getActiveSegments(final String scope, final String name, final OperationContext context, final Executor executor) {
        final Stream stream = getStream(scope, name, context);
        return withCompletion(stream.getState(true)
                        .thenComposeAsync(state -> {
                            if (State.SEALED.equals(state)) {
                                return CompletableFuture.completedFuture(Collections.emptyList());
                            } else {
                                return stream.getActiveSegments();
                            }
                        }, executor)
                        .thenComposeAsync(currentSegments ->
                                Futures.allOfWithResults(currentSegments.stream().map(stream::getSegment)
                                                                        .collect(Collectors.toList())), executor),
                executor);
    }


    @Override
    public CompletableFuture<Map<Long, Long>> getActiveSegments(final String scope, final String name, final long timestamp, final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, name, context);
        return withCompletion(stream.getActiveSegments(timestamp), executor);
    }

    @Override
    public CompletableFuture<List<Segment>> getActiveSegments(final String scope,
                                                              final String stream,
                                                              final int epoch,
                                                              final OperationContext context,
                                                              final Executor executor) {
        final Stream streamObj = getStream(scope, stream, context);
        return withCompletion(streamObj.getActiveSegments(epoch).thenComposeAsync(segments -> {
            return Futures.allOfWithResults(segments
                    .stream()
                    .map(streamObj::getSegment)
                    .collect(Collectors.toList()));
        }, executor), executor);
    }

    @Override
    public CompletableFuture<List<Long>> getActiveSegmentIds(final String scope,
                                                                          final String stream,
                                                                          final int epoch,
                                                                          final OperationContext context,
                                                                          final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getActiveSegments(epoch), executor);
    }

    @Override
    public CompletableFuture<Map<Long, List<Long>>> getSuccessors(final String scope, final String streamName,
                                                                  final long segmentId, final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        return withCompletion(stream.getSuccessorsWithPredecessors(segmentId), executor);
    }

    @Override
    public CompletableFuture<List<Segment>> getSegmentsBetweenStreamCuts(final String scope, final String streamName, final Map<Long, Long> from,
                                                                         final Map<Long, Long> to, final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        return withCompletion(stream.getSegmentsBetweenStreamCuts(from, to), executor);
    }

    @Override
    public CompletableFuture<Boolean> isStreamCutValid(final String scope,
                                                final String streamName,
                                                final Map<Long, Long> streamCut,
                                                final OperationContext context,
                                                final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        return withCompletion(stream.isStreamCutValid(streamCut), executor);
    }


    @Override
    public CompletableFuture<EpochTransitionRecord> startScale(final String scope,
                                                               final String name,
                                                               final List<Long> sealedSegments,
                                                               final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                               final long scaleTimestamp,
                                                               final boolean runOnlyIfStarted,
                                                               final OperationContext context,
                                                               final Executor executor) {
        return withCompletion(getStream(scope, name, context)
                .startScale(sealedSegments, newRanges, scaleTimestamp, runOnlyIfStarted), executor);
    }

    @Override
    public CompletableFuture<Void> scaleCreateNewSegments(final String scope,
                                                          final String name,
                                                          final boolean isManualScale,
                                                          final OperationContext context,
                                                          final Executor executor) {
        return withCompletion(getStream(scope, name, context).scaleCreateNewSegments(isManualScale), executor);
    }

    @Override
    public CompletableFuture<Void> scaleNewSegmentsCreated(final String scope,
                                                           final String name,
                                                           final OperationContext context,
                                                           final Executor executor) {
        return withCompletion(getStream(scope, name, context).scaleNewSegmentsCreated(), executor);
    }

    @Override
    public CompletableFuture<Void> scaleSegmentsSealed(final String scope,
                                                       final String name,
                                                       final Map<Long, Long> sealedSegmentSizes,
                                                       final OperationContext context,
                                                       final Executor executor) {
        CompletableFuture<Void> future = withCompletion(getStream(scope, name, context).scaleOldSegmentsSealed(sealedSegmentSizes), executor);

        future.thenCompose(result -> CompletableFuture.allOf(
                getActiveSegments(scope, name, System.currentTimeMillis(), null, executor).thenAccept(list ->
                        DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(SEGMENTS_COUNT, scope, name), list.size())),
                findNumSplitsMerges(scope, name, executor).thenAccept(simpleEntry -> {
                    DYNAMIC_LOGGER.updateCounterValue(nameFromStream(SEGMENTS_SPLITS, scope, name), simpleEntry.getKey());
                    DYNAMIC_LOGGER.updateCounterValue(nameFromStream(SEGMENTS_MERGES, scope, name), simpleEntry.getValue());
                })));

        return future;
    }

    @Override
    public CompletableFuture<Void> rollingTxnNewSegmentsCreated(String scope, String name, Map<Long, Long> sealedTxnEpochSegments,
                                                                int txnEpoch, long time, OperationContext context, Executor executor) {
        return withCompletion(getStream(scope, name, context).rollingTxnNewSegmentsCreated(sealedTxnEpochSegments, txnEpoch, time), executor);
    }

    @Override
    public CompletableFuture<Void> rollingTxnActiveEpochSealed(String scope, String name, Map<Long, Long> sealedActiveEpochSegments,
                                                               int activeEpoch, long time, OperationContext context, Executor executor) {
        return withCompletion(getStream(scope, name, context).rollingTxnActiveEpochSealed(sealedActiveEpochSegments, activeEpoch, time), executor);
    }

    @Override
    public CompletableFuture<Void> addStreamCutToRetentionSet(final String scope, final String name, final StreamCutRecord streamCut,
                                                              final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, name, context);
        return withCompletion(stream.addStreamCutToRetentionSet(streamCut), executor);
    }

    @Override
    public CompletableFuture<List<StreamCutRecord>> getStreamCutsFromRetentionSet(final String scope, final String name,
                                                                                  final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, name, context);
        return withCompletion(stream.getRetentionStreamCuts(), executor);
    }

    @Override
    public CompletableFuture<Void> deleteStreamCutBefore(final String scope, final String name, final StreamCutRecord streamCut,
                                                         final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, name, context);
        return withCompletion(stream.deleteStreamCutBefore(streamCut), executor);
    }

    @Override
    public CompletableFuture<Long> getSizeTillStreamCut(final String scope, final String name, final Map<Long, Long> streamCut,
                                                        final OperationContext context, final ScheduledExecutorService executor) {
        Stream stream = getStream(scope, name, context);
        return withCompletion(stream.getSizeTillStreamCut(streamCut), executor);
    }

    @Override
    public CompletableFuture<UUID> generateTransactionId(final String scopeName, final String streamName,
                                                             final OperationContext context,
                                                             final Executor executor) {
        Stream stream = getStream(scopeName, streamName, context);

        // This can throw write conflict exception
        CompletableFuture<Int96> nextFuture = getNextCounter();
        return withCompletion(nextFuture.thenCompose(next -> stream.generateNewTxnId(next.getMsb(), next.getLsb())), executor);
    }

    @Override
    public CompletableFuture<VersionedTransactionData> createTransaction(final String scopeName,
                                                                         final String streamName,
                                                                         final UUID txnId,
                                                                         final long lease,
                                                                         final long maxExecutionTime,
                                                                         final OperationContext context,
                                                                         final Executor executor) {
        Stream stream = getStream(scopeName, streamName, context);
        return withCompletion(stream.createTransaction(txnId, lease, maxExecutionTime), executor)
                .thenApply(result -> {
                    stream.getNumberOfOngoingTransactions().thenAccept(count -> {
                        DYNAMIC_LOGGER.incCounterValue(nameFromStream(CREATE_TRANSACTION, scopeName, streamName), 1);
                        DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scopeName, streamName), count);
                    });
                    return result;
                });
    }

    @Override
    public CompletableFuture<VersionedTransactionData> pingTransaction(final String scopeName, final String streamName,
                                                                       final VersionedTransactionData txData,
                                                                       final long lease,
                                                                       final OperationContext context,
                                                                       final Executor executor) {
        return withCompletion(getStream(scopeName, streamName, context).pingTransaction(txData, lease), executor);
    }

    @Override
    public CompletableFuture<VersionedTransactionData> getTransactionData(final String scopeName,
                                                                          final String streamName,
                                                                          final UUID txId,
                                                                          final OperationContext context,
                                                                          final Executor executor) {
        return withCompletion(getStream(scopeName, streamName, context).getTransactionData(txId), executor);
    }

    @Override
    public CompletableFuture<TxnStatus> transactionStatus(final String scopeName, final String streamName,
                                                          final UUID txId, final OperationContext context,
                                                          final Executor executor) {
        return withCompletion(getStream(scopeName, streamName, context).checkTransactionStatus(txId), executor);
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String streamName,
                                                          final UUID txId, final OperationContext context,
                                                          final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        CompletableFuture<TxnStatus> future = withCompletion(stream.commitTransaction(txId), executor);

        future.thenCompose(result -> {
            return stream.getNumberOfOngoingTransactions().thenAccept(count -> {
                DYNAMIC_LOGGER.incCounterValue(nameFromStream(COMMIT_TRANSACTION, scope, streamName), 1);
                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, streamName), count);
            });
        });

        return future;
    }

    @Override
    public CompletableFuture<SimpleEntry<TxnStatus, Integer>> sealTransaction(final String scopeName,
                                                                              final String streamName,
                                                                              final UUID txId,
                                                                              final boolean commit,
                                                                              final Optional<Integer> version,
                                                                              final OperationContext context,
                                                                              final Executor executor) {
        return withCompletion(getStream(scopeName, streamName, context)
                .sealTransaction(txId, commit, version), executor);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final String scope, final String streamName,
                                                         final UUID txId, final OperationContext context,
                                                         final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        CompletableFuture<TxnStatus> future = withCompletion(stream.abortTransaction(txId), executor);
        future.thenApply(result -> {
            stream.getNumberOfOngoingTransactions().thenAccept(count -> {
                DYNAMIC_LOGGER.incCounterValue(nameFromStream(ABORT_TRANSACTION, scope, streamName), 1);
                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, streamName), count);
            });
            return result;
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> addTxnToIndex(String hostId, TxnResource txn, int version) {
        return hostIndex.addEntity(hostId, getTxnResourceString(txn), ByteBuffer.allocate(Integer.BYTES).putInt(version).array());
    }

    @Override
    public CompletableFuture<Void> removeTxnFromIndex(String hostId, TxnResource txn, boolean deleteEmptyParent) {
        return hostIndex.removeEntity(hostId, getTxnResourceString(txn), deleteEmptyParent);
    }

    @Override
    public CompletableFuture<Optional<TxnResource>> getRandomTxnFromIndex(final String hostId) {
        return hostIndex.getEntities(hostId).thenApply(list -> list != null && list.size() > 0 ?
                Optional.of(this.getTxnResource(list.get(RandomFactory.create().nextInt(list.size())))) : Optional.empty());
    }

    @Override
    public CompletableFuture<Integer> getTxnVersionFromIndex(final String hostId, final TxnResource resource) {
        return hostIndex.getEntityData(hostId, getTxnResourceString(resource)).thenApply(data ->
            data != null ? ByteBuffer.wrap(data).getInt() : null);
    }

    @Override
    public CompletableFuture<Void> removeHostFromIndex(String hostId) {
        return hostIndex.removeHost(hostId);
    }

    @Override
    public CompletableFuture<Set<String>> listHostsOwningTxn() {
        return hostIndex.getHosts();
    }

    @Override
    public CompletableFuture<Void> markCold(final String scope, final String stream, final long segmentId, final long timestamp,
                                            final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).setColdMarker(segmentId, timestamp), executor);
    }

    @Override
    public CompletableFuture<Boolean> isCold(final String scope, final String stream, final long segmentId,
                                             final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getColdMarker(segmentId)
                .thenApply(marker -> marker != null && marker > System.currentTimeMillis()), executor);
    }

    @Override
    public CompletableFuture<Void> removeMarker(final String scope, final String stream, final long segmentId,
                                                final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).removeColdMarker(segmentId), executor);
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns(final String scope, final String stream,
                                                                       final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getActiveTxns(), executor);
    }

    @Override
    public CompletableFuture<List<ScaleMetadata>> getScaleMetadata(final String scope, final String name,
                                                                   final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).getScaleMetadata(), executor);
    }

    @Override
    public CompletableFuture<HistoryRecord>  getActiveEpoch(final String scope,
                                                            final String stream,
                                                            final OperationContext context,
                                                            final boolean ignoreCached,
                                                            final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getActiveEpoch(ignoreCached), executor);
    }

    @Override
    public CompletableFuture<HistoryRecord> getEpoch(final String scope,
                                                     final String stream,
                                                     final int epoch,
                                                     final OperationContext context,
                                                     final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getEpochRecord(epoch), executor);
    }

    @Override
    public CompletableFuture<Void> createCommittingTransactionsRecord(String scope, String stream, int epoch, List<UUID> txnsToCommit,
                                                                      OperationContext context, ScheduledExecutorService executor) {
        return withCompletion(getStream(scope, stream, context).createCommittingTransactionsRecord(epoch, txnsToCommit), executor);
    }

    @Override
    public CompletableFuture<CommittingTransactionsRecord> getCommittingTransactionsRecord(String scope, String stream, OperationContext context,
                                                                                           ScheduledExecutorService executor) {
        return withCompletion(getStream(scope, stream, context).getCommittingTransactionsRecord(), executor);
    }

    @Override
    public CompletableFuture<Void> deleteCommittingTransactionsRecord(String scope, String stream, OperationContext context, ScheduledExecutorService executor) {
        return withCompletion(getStream(scope, stream, context).deleteCommittingTransactionsRecord(), executor);
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getTransactionsInEpoch(String scope, String stream, int epoch,
                                                                                OperationContext context, ScheduledExecutorService executor) {
        return withCompletion(getStream(scope, stream, context).getTransactionsInEpoch(epoch), executor);
    }

    @Override
    public CompletableFuture<Void> createWaitingRequestIfAbsent(String scope, String stream, String processorName,
                                                                OperationContext context, ScheduledExecutorService executor) {
        return withCompletion(getStream(scope, stream, context).createWaitingRequestIfAbsent(processorName), executor);
    }

    @Override
    public CompletableFuture<String> getWaitingRequestProcessor(String scope, String stream, OperationContext context, ScheduledExecutorService executor) {
        return withCompletion(getStream(scope, stream, context).getWaitingRequestProcessor(), executor);
    }

    @Override
    public CompletableFuture<Void> deleteWaitingRequestConditionally(String scope, String stream, String processorName,
                                                                     OperationContext context, ScheduledExecutorService executor) {
        return withCompletion(getStream(scope, stream, context).deleteWaitingRequestConditionally(processorName), executor);
    }

    protected Stream getStream(String scope, final String name, OperationContext context) {
        Stream stream;
        if (context != null) {
            stream = context.getStream();
            assert stream.getScope().equals(scope);
            assert stream.getName().equals(name);
        } else {
            stream = cache.getUnchecked(new ImmutablePair<>(scope, name));
            stream.refresh();
        }
        return stream;
    }

    @VisibleForTesting
    void setStream(Stream stream) {
        cache.put(new ImmutablePair<>(stream.getScope(), stream.getName()), stream);
    }

    private Scope getScope(final String scopeName) {
        Scope scope = scopeCache.getUnchecked(scopeName);
        scope.refresh();
        return scope;
    }

    private <T> CompletableFuture<T> withCompletion(CompletableFuture<T> future, final Executor executor) {

        // Following makes sure that the result future given out to caller is actually completed on
        // caller's executor. So any chaining, if done without specifying an executor, will either happen on
        // caller's executor or fork join pool but never on someone else's executor.

        CompletableFuture<T> result = new CompletableFuture<>();

        future.whenCompleteAsync((r, e) -> {
            if (e != null) {
                result.completeExceptionally(e);
            } else {
                result.complete(r);
            }
        }, executor);

        return result;
    }

    private CompletableFuture<SimpleEntry<Long, Long>> findNumSplitsMerges(String scopeName, String streamName, Executor executor) {
        return getScaleMetadata(scopeName, streamName, null, executor).thenApply(scaleMetadataList -> {
            AtomicLong totalNumSplits = new AtomicLong(0L);
            AtomicLong totalNumMerges = new AtomicLong(0L);
            scaleMetadataList.forEach(x -> {
                totalNumMerges.addAndGet(x.getMerges());
                totalNumSplits.addAndGet(x.getSplits());
            });

            return new SimpleEntry<>(totalNumSplits.get(), totalNumMerges.get());
        });
    }

    /**
     * This method retrieves a safe base segment number from which a stream's segment ids may start. In the case of a
     * new stream, this method will return 0 as a starting segment number (default). In the case that a stream with the
     * same name has been recently deleted, this method will provide as a safe starting segment number the last segment
     * number of the previously deleted stream + 1. This will avoid potential segment naming collisions with segments
     * being asynchronously deleted from the segment store.
     *
     * @param scopeName scope
     * @param streamName stream
     * @return CompletableFuture with a safe starting segment number for this stream.
     */
    abstract CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName);

    /**
     * This method stores the last active segment for a stream upon its deletion. Persistently storing this value is
     * necessary in the case of a stream re-creation for picking an appropriate starting segment number.
     *
     * @param scope scope
     * @param stream stream
     * @param lastActiveSegment segment with highest number for a stream
     * @param context context
     * @param executor executor
     * @return CompletableFuture which indicates the task completion related to persistently store lastActiveSegment.
     */
    abstract CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, int lastActiveSegment,
                                                             OperationContext context, final Executor executor);

    abstract Stream newStream(final String scope, final String name);

    abstract CompletableFuture<Int96> getNextCounter();

    private String getTxnResourceString(TxnResource txn) {
        return txn.toString(RESOURCE_PART_SEPARATOR);
    }

    private TxnResource getTxnResource(String str) {
        return TxnResource.parse(str, RESOURCE_PART_SEPARATOR);
    }

    int getBucket(String scope, String stream) {
        String scopedStreamName = getScopedStreamName(scope, stream);
        return scopedStreamName.hashCode() % bucketCount;
    }

    String getScopedStreamName(String scope, String stream) {
        return String.format("%s/%s", scope, stream);
    }
}


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

import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.task.TxnResource;
import io.pravega.shared.MetricsNames;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.client.stream.StreamConfiguration;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.ParametersAreNonnullByDefault;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

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

    private final LoadingCache<String, Scope> scopeCache;
    private final LoadingCache<Pair<String, String>, Stream> cache;
    private final HostIndex hostIndex;

    protected AbstractStreamMetadataStore(HostIndex hostIndex) {
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
        return withCompletion(getStream(scope, name, context).create(configuration, createTimestamp), executor)
                .thenApply(result -> {
                    CREATE_STREAM.reportSuccessValue(1);
                    DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, name), 0);
                    DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(SEGMENTS_COUNT, scope, name),
                            configuration.getScalingPolicy().getMinNumSegments());
                    DYNAMIC_LOGGER.incCounterValue(nameFromStream(SEGMENTS_SPLITS, scope, name), 0);
                    DYNAMIC_LOGGER.incCounterValue(nameFromStream(SEGMENTS_MERGES, scope, name), 0);

                    return result;
                });
    }

    @Override
    public CompletableFuture<Void> deleteStream(final String scope,
                                                final String name,
                                                final OperationContext context,
                                                final Executor executor) {
        return withCompletion(getStream(scope, name, context).delete(), executor)
                .thenApply(result -> {
                    DELETE_STREAM.reportSuccessValue(1);
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
                                             final OperationContext context,
                                             final Executor executor) {
        return withCompletion(getStream(scope, name, context).getState(), executor);
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
            return FutureHelpers.allOfWithResults(
                    streams.stream()
                            .map(s -> getStream(scopeName, s, null).getConfiguration())
                            .collect(Collectors.toList()));
        });
    }

    @Override
    public CompletableFuture<Boolean> updateConfiguration(final String scope,
                                                          final String name,
                                                          final StreamConfiguration configuration,
                                                          final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).updateConfiguration(configuration), executor);
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration(final String scope,
                                                                   final String name,
                                                                   final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).getConfiguration(), executor);
    }

    @Override
    public CompletableFuture<Boolean> isSealed(final String scope, final String name, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).getState().thenApply(state -> state.equals(State.SEALED)), executor);
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
    public CompletableFuture<Segment> getSegment(final String scope, final String name, final int number, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).getSegment(number), executor);
    }

    @Override
    public CompletableFuture<Integer> getSegmentCount(final String scope, final String name, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).getSegmentCount(), executor);
    }

    @Override
    public CompletableFuture<List<Segment>> getActiveSegments(final String scope, final String name, final OperationContext context, final Executor executor) {
        final Stream stream = getStream(scope, name, context);
        return withCompletion(stream.getState()
                        .thenComposeAsync(state -> {
                            if (State.SEALED.equals(state)) {
                                return CompletableFuture.completedFuture(Collections.<Integer>emptyList());
                            } else {
                                return stream.getActiveSegments();
                            }
                        }, executor)
                        .thenComposeAsync(currentSegments ->
                                FutureHelpers.allOfWithResults(currentSegments.stream().map(stream::getSegment)
                                        .collect(Collectors.toList())), executor),
                executor);
    }


    @Override
    public CompletableFuture<List<Integer>> getActiveSegments(final String scope, final String name, final long timestamp, final OperationContext context, final Executor executor) {
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
            return FutureHelpers.allOfWithResults(segments
                    .stream()
                    .map(streamObj::getSegment)
                    .collect(Collectors.toList()));
        }, executor), executor);
    }

    @Override
    public CompletableFuture<List<Integer>> getActiveSegmentIds(final String scope,
                                                                final String stream,
                                                                final int epoch,
                                                                final OperationContext context,
                                                                final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getActiveSegments(epoch), executor);
    }

    @Override
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String scope, final String streamName,
                                                                        final int segmentNumber, final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        return withCompletion(stream.getSuccessorsWithPredecessors(segmentNumber), executor);
    }

    @Override
    public CompletableFuture<StartScaleResponse> startScale(final String scope,
                                                            final String name,
                                                            final List<Integer> sealedSegments,
                                                            final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                            final long scaleTimestamp,
                                                            final boolean runOnlyIfStarted,
                                                            final OperationContext context,
                                                            final Executor executor) {
        return withCompletion(getStream(scope, name, context)
                .startScale(sealedSegments, newRanges, scaleTimestamp, runOnlyIfStarted), executor);
    }

    @Override
    public CompletableFuture<Void> scaleNewSegmentsCreated(final String scope, final String name,
                                                           final List<Integer> sealedSegments,
                                                           final List<Segment> newSegments,
                                                           final int activeEpoch,
                                                           final long scaleTimestamp,
                                                           final OperationContext context,
                                                           final Executor executor) {
        List<Integer> newSegmentNumbers = newSegments.stream().map(Segment::getNumber).collect(Collectors.toList());
        return withCompletion(getStream(scope, name, context)
                .scaleNewSegmentsCreated(sealedSegments, newSegmentNumbers, activeEpoch, scaleTimestamp), executor);
    }

    @Override
    public CompletableFuture<Void> scaleSegmentsSealed(final String scope,
                                                       final String name,
                                                       final List<Integer> sealedSegments,
                                                       final List<Segment> newSegments,
                                                       final int activeEpoch,
                                                       final long scaleTimestamp,
                                                       final OperationContext context,
                                                       final Executor executor) {
        List<Integer> newSegmentNumbers = newSegments.stream().map(Segment::getNumber).collect(Collectors.toList());
        CompletableFuture<Void> future = withCompletion(getStream(scope, name, context)
                .scaleOldSegmentsSealed(sealedSegments, newSegmentNumbers, activeEpoch, scaleTimestamp), executor);
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = newSegments.stream().map(x ->
                new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())).collect(Collectors.toList());

        future.thenCompose(result -> CompletableFuture.allOf(
                getActiveSegments(scope, name, System.currentTimeMillis(), null, executor).thenAccept(list ->
                        DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(SEGMENTS_COUNT, scope, name), list.size())),
                findNumSplits(scope, name, executor).thenAccept(numSplits ->
                        DYNAMIC_LOGGER.updateCounterValue(nameFromStream(SEGMENTS_SPLITS, scope, name), numSplits)),
                findNumMerges(scope, name, executor).thenAccept( numMerges ->
                        DYNAMIC_LOGGER.updateCounterValue(nameFromStream(SEGMENTS_MERGES, scope, name), numMerges))));

        return future;
    }

    @Override
    public CompletableFuture<DeleteEpochResponse> tryDeleteEpochIfScaling(final String scope,
                                                                          final String name,
                                                                          final int epoch,
                                                                          final OperationContext context,
                                                                          final Executor executor) {
        Stream stream = getStream(scope, name, context);
        return withCompletion(stream.scaleTryDeleteEpoch(epoch), executor)
                .thenCompose(deleted -> {
                    if (deleted) {
                        return stream.latestScaleData()
                                .thenCompose(pair -> {
                                    List<Integer> segmentsSealed = pair.getLeft();
                                    return FutureHelpers.allOfWithResults(pair.getRight().stream().map(stream::getSegment)
                                            .collect(Collectors.toList()))
                                            .thenApply(segmentsCreated ->
                                                    new DeleteEpochResponse(true, segmentsSealed, segmentsCreated));
                                });
                    } else {
                        return CompletableFuture.completedFuture(
                                new DeleteEpochResponse(false, null, null));
                    }
                });
    }

    @Override
    public CompletableFuture<VersionedTransactionData> createTransaction(final String scopeName,
                                                                         final String streamName,
                                                                         final UUID txnId,
                                                                         final long lease,
                                                                         final long maxExecutionTime,
                                                                         final long scaleGracePeriod,
                                                                         final OperationContext context,
                                                                         final Executor executor) {
        Stream stream = getStream(scopeName, streamName, context);
        return withCompletion(stream.createTransaction(txnId, lease, maxExecutionTime, scaleGracePeriod), executor)
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
    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String streamName, final int epoch,
                                                          final UUID txId, final OperationContext context,
                                                          final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        CompletableFuture<TxnStatus> future = withCompletion(stream.commitTransaction(epoch, txId), executor);

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
    public CompletableFuture<TxnStatus> abortTransaction(final String scope, final String streamName, final int epoch,
                                                         final UUID txId, final OperationContext context,
                                                         final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        CompletableFuture<TxnStatus> future = withCompletion(stream.abortTransaction(epoch, txId), executor);
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
    public CompletableFuture<Boolean> isTransactionOngoing(final String scope, final String stream, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getNumberOfOngoingTransactions(), executor)
                .thenApply(num -> num > 0);
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
                Optional.of(this.getTxnResource(list.get(new Random().nextInt(list.size())))) : Optional.empty());
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
    public CompletableFuture<Void> markCold(final String scope, final String stream, final int segmentNumber, final long timestamp,
                                            final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).setColdMarker(segmentNumber, timestamp), executor);
    }

    @Override
    public CompletableFuture<Boolean> isCold(final String scope, final String stream, final int number,
                                             final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getColdMarker(number)
                .thenApply(marker -> marker != null && marker > System.currentTimeMillis()), executor);
    }

    @Override
    public CompletableFuture<Void> removeMarker(final String scope, final String stream, final int number,
                                                final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).removeColdMarker(number), executor);
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
    public CompletableFuture<Pair<Integer, List<Integer>>>  getActiveEpoch(final String scope,
                                              final String stream,
                                              final OperationContext context,
                                              final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getActiveEpoch(), executor);
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
                log.error("AbstractStreamMetadataStore exception: {}", e);
                result.completeExceptionally(e);
            } else {
                result.complete(r);
            }
        }, executor);

        return result;
    }

    private CompletableFuture<List<AbstractMap.SimpleEntry<Double, Double>>> getSealedRanges(final String scopeName,
                                                                                             final String streamName,
                                                                                             final List<Integer> sealedSegments,
                                                                                             final OperationContext context,
                                                                                             final Executor executor) {
        return FutureHelpers.allOfWithResults(
                sealedSegments.stream()
                        .map((Integer value) ->
                                getSegment(scopeName, streamName, value, context, executor).thenApply(segment ->
                                        new AbstractMap.SimpleEntry<>(
                                                segment.getKeyStart(),
                                                segment.getKeyEnd())))
                        .collect(Collectors.toList()));
    }

    private CompletableFuture<Long> findNumSplits(String scopeName, String streamName, Executor executor) {
        return getScaleMetadata(scopeName, streamName, null, executor).thenApply(scaleMetadataList -> {
            long size = scaleMetadataList.size();
            long totalNumSplits = 0;

            for (int i = 0, j = 1; j < size; j++) {
                long countSplitsEventI = scaleMetadataList.get(i).getSegments().size();
                long countSplitsEventJ = scaleMetadataList.get(j).getSegments().size();
                totalNumSplits = (countSplitsEventI < countSplitsEventJ) ? (countSplitsEventJ - countSplitsEventI) : 0;
            }

            return totalNumSplits;
        });
    }

    private CompletableFuture<Long> findNumMerges(String scopeName, String streamName, Executor executor) {
        return getScaleMetadata(scopeName, streamName, null, executor).thenApply(scaleMetadataList -> {
            long size = scaleMetadataList.size();
            long totalNumMerges = 0;

            for (int i = 0, j = 1; j < size; j++) {
                long countMergesEventI = scaleMetadataList.get(i).getSegments().size();
                long countMergesEventJ = scaleMetadataList.get(j).getSegments().size();
                totalNumMerges = (countMergesEventI > countMergesEventJ) ? (countMergesEventI - countMergesEventJ) : 0;
            }

            return totalNumMerges;
        });
    }

    abstract Stream newStream(final String scope, final String name);

    private String getTxnResourceString(TxnResource txn) {
        return txn.toString(RESOURCE_PART_SEPARATOR);
    }

    private TxnResource getTxnResource(String str) {
        return TxnResource.parse(str, RESOURCE_PART_SEPARATOR);
    }
}


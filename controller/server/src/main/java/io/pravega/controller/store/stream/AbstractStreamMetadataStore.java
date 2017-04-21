/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.store.stream;

import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
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
import io.pravega.stream.StreamConfiguration;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import static io.pravega.controller.store.stream.StoreException.Type.NODE_EXISTS;
import static io.pravega.controller.store.stream.StoreException.Type.NODE_NOT_EMPTY;
import static io.pravega.controller.store.stream.StoreException.Type.NODE_NOT_FOUND;

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

    private final LoadingCache<String, Scope> scopeCache;
    private final LoadingCache<Pair<String, String>, Stream> cache;

    protected AbstractStreamMetadataStore() {
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
    public CompletableFuture<Boolean> createStream(final String scope,
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
            if (ex.getCause() instanceof StoreException && ((StoreException) ex.getCause()).getType() == NODE_EXISTS) {
                return CreateScopeStatus.newBuilder().setStatus(CreateScopeStatus.Status.SCOPE_EXISTS).build();
            } else if (ex instanceof StoreException && ((StoreException) ex).getType() == NODE_EXISTS) {
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
            if ((ex.getCause() instanceof StoreException
                    && ((StoreException) ex.getCause()).getType() == NODE_NOT_FOUND)
                    || (ex instanceof StoreException && (((StoreException) ex).getType() == NODE_NOT_FOUND))) {
                return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build();
            } else if (ex.getCause() instanceof StoreException
                    && ((StoreException) ex.getCause()).getType() == NODE_NOT_EMPTY
                    || (ex instanceof StoreException && (((StoreException) ex).getType() == NODE_NOT_EMPTY))) {
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
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String scope, final String streamName,
                                                                        final int segmentNumber, final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        return withCompletion(stream.getSuccessorsWithPredecessors(segmentNumber), executor);
    }

    @Override
    public CompletableFuture<List<Segment>> startScale(final String scope, final String name,
                                                       final List<Integer> sealedSegments,
                                                       final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                       final long scaleTimestamp,
                                                       final OperationContext context,
                                                       final Executor executor) {
        return withCompletion(getStream(scope, name, context)
                .startScale(sealedSegments, newRanges, scaleTimestamp), executor);
    }

    @Override
    public CompletableFuture<Void> scaleNewSegmentsCreated(final String scope, final String name,
                                                           final List<Integer> sealedSegments,
                                                           final List<Segment> newSegments,
                                                           final long scaleTimestamp,
                                                           final OperationContext context,
                                                           final Executor executor) {
        List<Integer> collect = newSegments.stream().map(Segment::getNumber).collect(Collectors.toList());
        return withCompletion(getStream(scope, name, context)
                .scaleNewSegmentsCreated(sealedSegments, collect, scaleTimestamp), executor);
    }

    @Override
    public CompletableFuture<Void> scaleSegmentsSealed(final String scope, final String name,
                                                       final List<Integer> sealedSegments,
                                                       final List<Segment> newSegments,
                                                       final long scaleTimestamp,
                                                       final OperationContext context,
                                                       final Executor executor) {
        List<Integer> collect = newSegments.stream().map(Segment::getNumber).collect(Collectors.toList());
        CompletableFuture<Void> future = withCompletion(getStream(scope, name, context)
                .scaleOldSegmentsSealed(sealedSegments, collect, scaleTimestamp), executor);
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = newSegments.stream().map(x ->
                new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())).collect(Collectors.toList());

        future.thenAccept(result -> {
            DYNAMIC_LOGGER.incCounterValue(nameFromStream(SEGMENTS_COUNT, scope, name),
                    newSegments.size() - sealedSegments.size());
            getSealedRanges(scope, name, sealedSegments, context, executor)
                    .thenAccept(sealedRanges -> {
                        DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(SEGMENTS_SPLITS, scope, name),
                                findSplits(sealedRanges, newRanges));
                        DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(SEGMENTS_MERGES, scope, name),
                                findSplits(newRanges, sealedRanges));
                    });
        });

        return future;
    }

    @Override
    public CompletableFuture<VersionedTransactionData> createTransaction(final String scopeName,
                                                                         final String streamName,
                                                                         final long lease,
                                                                         final long maxExecutionTime,
                                                                         final long scaleGracePeriod,
                                                                         final OperationContext context,
                                                                         final Executor executor) {
        Stream stream = getStream(scopeName, streamName, context);
        return withCompletion(stream.createTransaction(lease, maxExecutionTime, scaleGracePeriod), executor)
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
                                                                       final UUID txId, final long lease,
                                                                       final OperationContext context,
                                                                       final Executor executor) {
        return withCompletion(getStream(scopeName, streamName, context).pingTransaction(txId, lease), executor);
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
    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String streamName, final UUID txId, final OperationContext context, final Executor executor) {
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
    public CompletableFuture<TxnStatus> sealTransaction(final String scopeName, final String streamName,
                                                        final UUID txId, final boolean commit,
                                                        final Optional<Integer> version,
                                                        final OperationContext context,
                                                        final Executor executor) {
        return withCompletion(getStream(scopeName, streamName, context)
                .sealTransaction(txId, commit, version), executor);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final String scope, final String streamName, final UUID txId, final OperationContext context, final Executor executor) {
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
    public CompletableFuture<Boolean> isTransactionOngoing(final String scope, final String stream, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getNumberOfOngoingTransactions(), executor)
                .thenApply(num -> num > 0);
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

    private Stream getStream(String scope, final String name, OperationContext context) {
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

    private int findSplits(final List<AbstractMap.SimpleEntry<Double, Double>> sealedRanges,
                           final List<AbstractMap.SimpleEntry<Double, Double>> newRanges) {
        int splits = 0;
        for (AbstractMap.SimpleEntry<Double, Double> sealedRange : sealedRanges) {
            int overlaps = 0;
            for (AbstractMap.SimpleEntry<Double, Double> newRange : newRanges) {
                if (Segment.overlaps(sealedRange, newRange)) {
                    overlaps++;
                }
                if (overlaps > 1) {
                    splits++;
                    break;
                }
            }
        }
        return splits;
    }

    abstract Stream newStream(final String scope, final String name);
}


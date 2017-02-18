/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.metrics.DynamicLogger;
import com.emc.pravega.common.metrics.MetricsProvider;
import com.emc.pravega.common.metrics.OpStatsLogger;
import com.emc.pravega.common.metrics.StatsLogger;
import com.emc.pravega.common.metrics.StatsProvider;
import com.emc.pravega.controller.server.MetricNames;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.emc.pravega.controller.server.MetricNames.ABORT_TRANSACTION;
import static com.emc.pravega.controller.server.MetricNames.COMMIT_TRANSACTION;
import static com.emc.pravega.controller.server.MetricNames.CREATE_TRANSACTION;
import static com.emc.pravega.controller.server.MetricNames.OPEN_TRANSACTIONS;
import static com.emc.pravega.controller.server.MetricNames.nameFromStream;

/**
 * Abstract Stream metadata store. It implements various read queries using the Stream interface.
 * Implementation of create and update queries are delegated to the specific implementations of this abstract class.
 */
public abstract class AbstractStreamMetadataStore implements StreamMetadataStore {

    protected static final StatsProvider METRICS_PROVIDER = MetricsProvider.getMetricsProvider();
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final StatsLogger STATS_LOGGER = METRICS_PROVIDER.createStatsLogger("Controller");
    private static final OpStatsLogger CREATE_STREAM = STATS_LOGGER.createStats(MetricNames.CREATE_STREAM);
    private static final OpStatsLogger SEAL_STREAM = STATS_LOGGER.createStats(MetricNames.SEAL_STREAM);
    private final LoadingCache<Pair<String, String>, Stream> cache;

    protected AbstractStreamMetadataStore() {
        cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<Pair<String, String>, Stream>() {
                            @ParametersAreNonnullByDefault
                            public Stream load(Pair<String, String> input) {
                                try {
                                    return newStream(input.getKey(), input.getValue());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
    }

    abstract Stream newStream(final String scope, final String name);

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
                    DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, "", name), 0);
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
    public CompletableFuture<Boolean> setSealed(final String scope, final String name, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).updateState(State.SEALED), executor).thenApply(result -> {
            SEAL_STREAM.reportSuccessValue(1);
            DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, "", name), 0);
            return result;
        });
    }

    public CompletableFuture<Boolean> isSealed(final String scope, final String name, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).getState().thenApply(state -> state.equals(State.SEALED)), executor);
    }

    @Override
    public CompletableFuture<Segment> getSegment(final String scope, final String name, final int number, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).getSegment(number), executor);
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
    public CompletableFuture<SegmentFutures> getActiveSegments(final String scope, final String name, final long timestamp, final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, name, context);
        return withCompletion(stream.getActiveSegments(timestamp)
                        .thenComposeAsync(activeSegments -> constructSegmentFutures(stream, activeSegments, executor), executor),
                executor);
    }

    @Override
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String scope, final String streamName,
                                                                        final int segmentNumber, final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        return withCompletion(stream.getSuccessorsWithPredecessors(segmentNumber), executor);
    }

    @Override
    public CompletableFuture<List<Segment>> scale(final String scope, final String name,
                                                  final List<Integer> sealedSegments,
                                                  final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                  final long scaleTimestamp, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, name, context).scale(sealedSegments, newRanges, scaleTimestamp), executor);
    }

    @Override
    public CompletableFuture<UUID> createTransaction(final String scope, final String streamName, final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        return withCompletion(stream.createTransaction(), executor).thenApply(result -> {
            stream.getNumberOfOngoingTransactions().thenAccept(count -> {
                DYNAMIC_LOGGER.recordMeterEvents(nameFromStream(CREATE_TRANSACTION, scope, streamName), 1);
                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, streamName), count);
            });
            return result;
        });
    }

    @Override
    public CompletableFuture<TxnStatus> transactionStatus(final String scope, final String stream, final UUID txId, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).checkTransactionStatus(txId), executor);
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String streamName, final UUID txId, final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        return withCompletion(stream.commitTransaction(txId), executor).thenApply(result -> {
            stream.getNumberOfOngoingTransactions().thenAccept(count -> {
                DYNAMIC_LOGGER.recordMeterEvents(nameFromStream(COMMIT_TRANSACTION, scope, streamName), 1);
                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, streamName), count);
            });
            return result;
        });
    }

    @Override
    public CompletableFuture<TxnStatus> sealTransaction(final String scope, final String stream, final UUID txId, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).sealTransaction(txId), executor);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final String scope, final String streamName, final UUID txId, final OperationContext context, final Executor executor) {
        Stream stream = getStream(scope, streamName, context);
        return withCompletion(stream.abortTransaction(txId), executor).thenApply(result -> {
            stream.getNumberOfOngoingTransactions().thenAccept(count -> {
                DYNAMIC_LOGGER.recordMeterEvents(nameFromStream(ABORT_TRANSACTION, scope, streamName), 1);
                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, streamName), count);
            });
            return result;
        });
    }

    @Override
    public CompletableFuture<Boolean> isTransactionOngoing(final String scope, final String stream, final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getNumberOfOngoingTransactions(), executor).thenApply(num -> num > 0);
    }

    @Override
    public CompletableFuture<Void> setMarker(final String scope, final String stream, final int segmentNumber, final long timestamp,
                                             final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).setMarker(segmentNumber, timestamp), executor);
    }

    @Override
    public CompletableFuture<Optional<Long>> getMarker(final String scope, final String stream, final int number,
                                                       final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getMarker(number), executor);
    }

    @Override
    public CompletableFuture<Void> removeMarker(final String scope, final String stream, final int number,
                                                final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).removeMarker(number), executor);
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxRecord>> getActiveTxns(final String scope, final String stream,
                                                                      final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).getActiveTxns(), executor);
    }

    @Override
    public CompletableFuture<Void> blockTransactions(final String scope, final String stream,
                                                     final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).blockTransactions(), executor);
    }

    @Override
    public CompletableFuture<Void> unblockTransactions(final String scope, final String stream,
                                                       final OperationContext context, final Executor executor) {
        return withCompletion(getStream(scope, stream, context).unblockTransactions(), executor);
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

    private CompletableFuture<SegmentFutures> constructSegmentFutures(final Stream stream, final List<Integer> activeSegments, final Executor executor) {
        Map<Integer, Integer> futureSegments = new HashMap<>();
        List<CompletableFuture<List<Integer>>> list =
                activeSegments.stream().map(number -> getDefaultFutures(stream, number, executor)).collect(Collectors.toList());

        CompletableFuture<List<List<Integer>>> futureDefaultFutures = FutureHelpers.allOfWithResults(list);
        return futureDefaultFutures
                .thenApplyAsync(futureList -> {
                    for (int i = 0; i < futureList.size(); i++) {
                        for (Integer future : futureList.get(i)) {
                            futureSegments.put(future, activeSegments.get(i));
                        }
                    }
                    return new SegmentFutures(activeSegments, futureSegments);
                }, executor);
    }

    /**
     * Finds all successors of a given segment, that have exactly one predecessor,
     * and hence can be included in the futures of the given segment.
     *
     * @param stream input stream
     * @param number segment number for which default futures are sought.
     * @return the list of successors of specified segment who have only one predecessor.
     * <p>
     * return stream.getSuccessors(number).stream()
     * .filter(x -> stream.getPredecessors(x).size() == 1)
     * .*                collect(Collectors.toList());
     */
    private CompletableFuture<List<Integer>> getDefaultFutures(final Stream stream, final int number, final Executor executor) {
        CompletableFuture<List<Integer>> futureSuccessors = stream.getSuccessors(number);
        return futureSuccessors.thenComposeAsync(
                list -> FutureHelpers.filter(list, elem -> stream.getPredecessors(elem).thenApply(x -> x.size() == 1)), executor);
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

}


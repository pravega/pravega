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
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.DeleteScopeStatus;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.ParametersAreNonnullByDefault;

import static com.emc.pravega.controller.server.MetricNames.ABORT_TRANSACTION;
import static com.emc.pravega.controller.server.MetricNames.COMMIT_TRANSACTION;
import static com.emc.pravega.controller.server.MetricNames.CREATE_TRANSACTION;
import static com.emc.pravega.controller.server.MetricNames.MERGES;
import static com.emc.pravega.controller.server.MetricNames.NUMBER_OF_SEGMENTS;
import static com.emc.pravega.controller.server.MetricNames.OPEN_TRANSACTIONS;
import static com.emc.pravega.controller.server.MetricNames.SPLITS;
import static com.emc.pravega.controller.server.MetricNames.nameFromStream;
import static com.emc.pravega.controller.store.stream.StoreException.Type.NODE_EXISTS;
import static com.emc.pravega.controller.store.stream.StoreException.Type.NODE_NOT_EMPTY;
import static com.emc.pravega.controller.store.stream.StoreException.Type.NODE_NOT_FOUND;

/**
 * Abstract Stream metadata store. It implements various read queries using the Stream interface.
 * Implementation of create and update queries are delegated to the specific implementations of this abstract class.
 */
@Slf4j
public abstract class AbstractStreamMetadataStore implements StreamMetadataStore {

    protected static final StatsProvider METRICS_PROVIDER = MetricsProvider.getMetricsProvider();
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final StatsLogger STATS_LOGGER = METRICS_PROVIDER.createStatsLogger("Controller");
    private static final OpStatsLogger CREATE_STREAM = STATS_LOGGER.createStats(MetricNames.CREATE_STREAM);
    private static final OpStatsLogger SEAL_STREAM = STATS_LOGGER.createStats(MetricNames.SEAL_STREAM);
    private final LoadingCache<String, Scope> scopeCache;
    private final LoadingCache<String, Stream> streamCache;

    protected AbstractStreamMetadataStore() {
        streamCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, Stream>() {
                            @ParametersAreNonnullByDefault
                            public Stream load(String name) {
                                try {
                                    return newStream(name);
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
     * Returns a Stream object from stream identifier.
     *
     * @param scopedStreamName scopedStreamName is stream identifier  (scopeName/streamName)
     * @return Stream Object.
     */
    abstract Stream newStream(final String scopedStreamName);

    /**
     * Returns a Scope object from scope identifier.
     *
     * @param scopeName scope identifier is scopeName.
     * @return Scope object.
     */
    abstract Scope newScope(final String scopeName);

    @Override
    public CompletableFuture<Boolean> createStream(final String scopeName,
                                                   final String streamName,
                                                   final StreamConfiguration configuration,
                                                   final long createTimestamp) {
        Stream stream = getStream(scopeName, streamName);
        return stream.create(configuration, createTimestamp).thenApply(result -> {
            CREATE_STREAM.reportSuccessValue(1);
            DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scopeName, streamName), 0);
            DYNAMIC_LOGGER.incCounterValue(nameFromStream(NUMBER_OF_SEGMENTS, scopeName, streamName),
                    configuration.getScalingPolicy().getMinNumSegments());
            DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(SPLITS, scopeName, streamName), 0);
            DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(MERGES, scopeName, streamName), 0);
            return result;
        });
    }

    /**
     * Create a scope with given name.
     *
     * @param scopeName Name of scope to created.
     * @return CreateScopeStatus future.
     */
    @Override
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName) {
        if (!validateName(scopeName)) {
            log.error("Create scope failed due to invalid scope name {}", scopeName);
            return CompletableFuture.completedFuture(CreateScopeStatus.INVALID_SCOPE_NAME);
        } else {
            return getScope(scopeName).createScope()
                    .handle((result, ex) -> {
                        if (ex != null) {
                            if (ex.getCause() instanceof StoreException &&
                                    ((StoreException) ex.getCause()).getType() == NODE_EXISTS) {
                                return CreateScopeStatus.SCOPE_EXISTS;
                            } else if (ex instanceof StoreException &&
                                    ((StoreException) ex).getType() == NODE_EXISTS) {
                                return CreateScopeStatus.SCOPE_EXISTS;
                            } else {
                                log.debug("Create scope failed due to ", ex);
                                return CreateScopeStatus.FAILURE;
                            }
                        } else {
                            return CreateScopeStatus.SUCCESS;
                        }
                    });
        }
    }

    /**
     * Delete a scope with given name.
     *
     * @param scopeName Name of scope to be deleted
     * @return DeleteScopeStatus future.
     */
    @Override
    public CompletableFuture<DeleteScopeStatus> deleteScope(final String scopeName) {
        return getScope(scopeName).deleteScope()
                .handle((result, ex) -> {
                    if (ex != null) {
                        if ((ex.getCause() instanceof StoreException &&
                                ((StoreException) ex.getCause()).getType() == NODE_NOT_FOUND) ||
                                (ex instanceof StoreException && (((StoreException) ex).getType() == NODE_NOT_FOUND))) {
                            return DeleteScopeStatus.SCOPE_NOT_FOUND;
                        } else if (ex.getCause() instanceof StoreException &&
                                ((StoreException) ex.getCause()).getType() == NODE_NOT_EMPTY ||
                                (ex instanceof StoreException && (((StoreException) ex).getType() == NODE_NOT_EMPTY))) {
                            return DeleteScopeStatus.SCOPE_NOT_EMPTY;
                        } else {
                            log.debug("DeleteScope failed due to {} ", ex);
                            return DeleteScopeStatus.FAILURE;
                        }
                    } else {
                        return DeleteScopeStatus.SUCCESS;
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
    public CompletableFuture<List<String>> listStreamsInScope(final String scopeName) {
        return getScope(scopeName).listStreamsInScope();
    }

    @Override
    public CompletableFuture<Boolean> updateConfiguration(final String scopeName,
                                                          final String streamName,
                                                          final StreamConfiguration configuration) {
        return getStream(scopeName, streamName).updateConfiguration(configuration);
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration(final String scopeName, final String streamName) {
        return getStream(scopeName, streamName).getConfiguration();
    }

    @Override
    public CompletableFuture<Boolean> isSealed(final String scopeName, final String streamName) {
        return getStream(scopeName, streamName).getState().thenApply(state -> state.equals(State.SEALED));
    }

    @Override
    public CompletableFuture<Boolean> setSealed(final String scopeName, final String streamName) {
        Stream stream = getStream(scopeName, streamName);
        return stream.updateState(State.SEALED).thenApply(result -> {
            SEAL_STREAM.reportSuccessValue(1);
            DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scopeName, streamName), 0);
            return result;
        });
    }

    @Override
    public CompletableFuture<Segment> getSegment(final String scopeName, final String streamName, final int number) {
        return getStream(scopeName, streamName).getSegment(number);
    }

    @Override
    public CompletableFuture<List<Segment>> getActiveSegments(final String scopeName, final String streamName) {
        final Stream stream = getStream(scopeName, streamName);
        return stream.getState().thenCompose(state -> {
            if (State.SEALED.equals(state)) {
                return CompletableFuture.completedFuture(Collections.<Integer>emptyList());
            } else {
                return stream.getActiveSegments();
            }
        }).thenCompose(currentSegments -> FutureHelpers.allOfWithResults(currentSegments.stream().map(stream::getSegment).collect(Collectors.toList())));
    }

    @Override
    public CompletableFuture<SegmentFutures> getActiveSegments(final String scopeName, final String streamName,
                                                               final long timestamp) {
        Stream stream = getStream(scopeName, streamName);
        CompletableFuture<List<Integer>> futureActiveSegments = stream.getActiveSegments(timestamp);
        return futureActiveSegments.thenCompose(activeSegments -> constructSegmentFutures(stream, activeSegments));
    }

    @Override
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String scopeName, final String streamName,
                                                                        final int segmentNumber) {
        Stream stream = getStream(scopeName, streamName);
        return stream.getSuccessorsWithPredecessors(segmentNumber);
    }

    @Override
    public CompletableFuture<List<Segment>> scale(final String scopeName, final String streamName,
                                                  final List<Integer> sealedSegments,
                                                  final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                  final long scaleTimestamp) {
        return getStream(scopeName, streamName).scale(sealedSegments, newRanges, scaleTimestamp)
                .thenApply(result -> {
                    DYNAMIC_LOGGER.incCounterValue(nameFromStream(NUMBER_OF_SEGMENTS, scopeName, streamName),
                            newRanges.size() - sealedSegments.size());

                    getSealedRanges(scopeName, streamName, sealedSegments)
                            .thenAccept(sealedRanges -> {

                                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(SPLITS, scopeName, streamName),
                                        findSplits(sealedRanges, newRanges));

                                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(MERGES, scopeName, streamName),
                                        findSplits(newRanges, sealedRanges));
                            });

                    return result;
                });
    }

    private CompletableFuture<List<AbstractMap.SimpleEntry<Double, Double>>> getSealedRanges(final String scopeName,
                                                                                             final String streamName,
                                                                                             final List<Integer>
                                                                                                     sealedSegments) {
        return FutureHelpers.allOfWithResults(
                sealedSegments.stream()
                        .map(value ->
                                getSegment(scopeName, streamName, value).thenApply(segment ->
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

    @Override
    public CompletableFuture<UUID> createTransaction(final String scopeName, final String streamName) {
        Stream stream = getStream(scopeName, streamName);
        return stream.createTransaction().thenApply(result -> {
            stream.getNumberOfOngoingTransactions().thenAccept(count -> {
                DYNAMIC_LOGGER.recordMeterEvents(nameFromStream(CREATE_TRANSACTION, scopeName, streamName), 1);
                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scopeName, streamName), count);
            });
            return result;
        });
    }

    @Override
    public CompletableFuture<TxnStatus> transactionStatus(final String scopeName, final String streamName, final UUID txId) {
        return getStream(scopeName, streamName).checkTransactionStatus(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final String scopeName, final String streamName, final UUID txId) {
        Stream stream = getStream(scopeName, streamName);
        return stream.commitTransaction(txId).thenApply(result -> {
            stream.getNumberOfOngoingTransactions().thenAccept(count -> {
                DYNAMIC_LOGGER.recordMeterEvents(nameFromStream(COMMIT_TRANSACTION, scopeName, streamName), 1);
                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scopeName, streamName), count);
            });
            return result;
        });
    }

    @Override
    public CompletableFuture<TxnStatus> sealTransaction(final String scopeName, final String streamName, final UUID txId,
                                                        final boolean commit) {
        return getStream(scopeName, streamName).sealTransaction(txId, commit);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final String scopeName, final String streamName, final UUID txId) {
        Stream stream = getStream(scopeName, streamName);
        return stream.abortTransaction(txId).thenApply(result -> {
            stream.getNumberOfOngoingTransactions().thenAccept(count -> {
                DYNAMIC_LOGGER.recordMeterEvents(nameFromStream(ABORT_TRANSACTION, scopeName, streamName), 1);
                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scopeName, streamName), count);
            });
            return result;
        });
    }

    @Override
    public CompletableFuture<Boolean> isTransactionOngoing(final String scopeName, final String streamName) {
        Stream stream = getStream(scopeName, streamName);
        return stream.getNumberOfOngoingTransactions().thenApply(num -> num > 0);
    }

    private Stream getStream(final String scopeName, final String streamName) {

        Stream stream = streamCache.getUnchecked(getScopedStreamName(scopeName, streamName));
        stream.refresh();
        return stream;
    }

    private Scope getScope(final String scopeName) {
        Scope scope = scopeCache.getUnchecked(scopeName);
        scope.refresh();
        return scope;
    }

    /**
     * ScopedStreamName is scopeName/streamName
     *
     * @param scopeName  Scope name.
     * @param streamName Stream name.
     * @return String scopeName/streamName.
     */
    private String getScopedStreamName(final String scopeName, final String streamName) {
        return new StringBuilder(scopeName).append("/").append(streamName).toString();
    }

    private CompletableFuture<SegmentFutures> constructSegmentFutures(final Stream stream, final List<Integer> activeSegments) {
        Map<Integer, Integer> futureSegments = new HashMap<>();
        List<CompletableFuture<List<Integer>>> list =
                activeSegments.stream().map(number -> getDefaultFutures(stream, number)).collect(Collectors.toList());

        CompletableFuture<List<List<Integer>>> futureDefaultFutures = FutureHelpers.allOfWithResults(list);
        return futureDefaultFutures
                .thenApply(futureList -> {
                            for (int i = 0; i < futureList.size(); i++) {
                                for (Integer future : futureList.get(i)) {
                                    futureSegments.put(future, activeSegments.get(i));
                                }
                            }
                            return new SegmentFutures(activeSegments, futureSegments);
                        }
                );
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
    private CompletableFuture<List<Integer>> getDefaultFutures(final Stream stream, final int number) {
        CompletableFuture<List<Integer>> futureSuccessors = stream.getSuccessors(number);
        return futureSuccessors.thenCompose(
                list -> FutureHelpers.filter(list, elem -> stream.getPredecessors(elem).thenApply(x -> x.size() == 1)));
    }

    static boolean validateName(final String path) {
        return (path.indexOf('\\') >= 0 || path.indexOf('/') >= 0) ? false : true;
    }
}

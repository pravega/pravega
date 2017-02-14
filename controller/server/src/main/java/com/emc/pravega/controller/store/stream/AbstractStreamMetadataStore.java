/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

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
    private final LoadingCache<String, Stream> cache;

    protected AbstractStreamMetadataStore() {
        cache = CacheBuilder.newBuilder()
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
    }

    abstract Stream newStream(final String name);

    @Override
    public CompletableFuture<Boolean> createStream(final String name,
                                                   final StreamConfiguration configuration,
                                                   final long createTimestamp) {
        Stream stream = getStream(name);
        return stream.create(configuration, createTimestamp).thenApply(result -> {
            CREATE_STREAM.reportSuccessValue(1);
            DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, "", name), 0);
            return result;
        });
    }

    @Override
    public CompletableFuture<Boolean> updateConfiguration(final String name,
                                                          final StreamConfiguration configuration) {
        return getStream(name).updateConfiguration(configuration);
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration(final String name) {
        return getStream(name).getConfiguration();
    }

    @Override
    public CompletableFuture<Boolean> isSealed(final String name) {
        return getStream(name).getState().thenApply(state -> state.equals(State.SEALED));
    }

    @Override
    public CompletableFuture<Boolean> setSealed(final String name) {
        Stream stream = getStream(name);
        return stream.updateState(State.SEALED).thenApply(result -> {
            SEAL_STREAM.reportSuccessValue(1);
            DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, "", name), 0);
            return result;
        });
    }

    @Override
    public CompletableFuture<Segment> getSegment(final String name, final int number) {
        return getStream(name).getSegment(number);
    }

    @Override
    public CompletableFuture<List<Segment>> getActiveSegments(final String name) {
        final Stream stream = getStream(name);
        return stream.getState().thenCompose(state -> {
            if (State.SEALED.equals(state)) {
                return CompletableFuture.completedFuture(Collections.<Integer>emptyList());
            } else {
                return stream.getActiveSegments();
            }
        }).thenCompose(currentSegments -> FutureHelpers.allOfWithResults(currentSegments.stream().map(stream::getSegment).collect(Collectors.toList())));
    }

    @Override
    public CompletableFuture<SegmentFutures> getActiveSegments(final String name, final long timestamp) {
        Stream stream = getStream(name);
        CompletableFuture<List<Integer>> futureActiveSegments = stream.getActiveSegments(timestamp);
        return futureActiveSegments.thenCompose(activeSegments -> constructSegmentFutures(stream, activeSegments));
    }

    @Override
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String streamName,
                                                                        final int segmentNumber) {
        Stream stream = getStream(streamName);
        return stream.getSuccessorsWithPredecessors(segmentNumber);
    }

    @Override
    public CompletableFuture<List<Segment>> scale(final String name,
                                                  final List<Integer> sealedSegments,
                                                  final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                  final long scaleTimestamp) {
        return getStream(name).scale(sealedSegments, newRanges, scaleTimestamp);
    }

    @Override
    public CompletableFuture<UUID> createTransaction(final String scope, final String streamName) {
        Stream stream = getStream(streamName);
        return stream.createTransaction().thenApply(result -> {
           stream.getNumberOfOngoingTransactions().thenAccept(count -> {
               DYNAMIC_LOGGER.recordMeterEvents(nameFromStream(CREATE_TRANSACTION, scope, streamName), 1);
               DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, streamName), count);
           });
           return result; 
        });
    }

    @Override
    public CompletableFuture<TxnStatus> transactionStatus(final String scope, final String stream, final UUID txId) {
        return getStream(stream).checkTransactionStatus(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String streamName, final UUID txId) {
        Stream stream = getStream(streamName);
        return stream.commitTransaction(txId).thenApply(result -> {
            stream.getNumberOfOngoingTransactions().thenAccept(count -> {
                DYNAMIC_LOGGER.recordMeterEvents(nameFromStream(COMMIT_TRANSACTION, scope, streamName), 1);
                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, streamName), count);
            });
            return result; 
         });
    }

    @Override
    public CompletableFuture<TxnStatus> sealTransaction(final String scope, final String stream, final UUID txId) {
        return getStream(stream).sealTransaction(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final String scope, final String streamName, final UUID txId) {
        Stream stream = getStream(streamName);
        return stream.abortTransaction(txId).thenApply(result -> {
            stream.getNumberOfOngoingTransactions().thenAccept(count -> {
                DYNAMIC_LOGGER.recordMeterEvents(nameFromStream(ABORT_TRANSACTION, scope, streamName), 1);
                DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, streamName), count);
            });
            return result; 
         });
    }

    @Override
    public CompletableFuture<Boolean> isTransactionOngoing(final String scope, final String streamName) {
        Stream stream = getStream(streamName);
        return stream.getNumberOfOngoingTransactions().thenApply(num -> num > 0);
    }

    private Stream getStream(final String name) {
        Stream stream = cache.getUnchecked(name);
        stream.refresh();
        return stream;
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
}

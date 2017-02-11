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

import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.common.concurrent.FutureHelpers;
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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Abstract Stream metadata store. It implements various read queries using the Stream interface.
 * Implementation of create and update queries are delegated to the specific implementations of this abstract class.
 */
public abstract class AbstractStreamMetadataStore implements StreamMetadataStore {

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
        return getStream(scope, name, null);
    }

    @Override
    public CompletableFuture<Boolean> createStream(final String scope,
                                                   final String name,
                                                   final StreamConfiguration configuration,
                                                   final long createTimestamp,
                                                   final OperationContext context) {
        return getStream(scope, name, context).create(configuration, createTimestamp);
    }

    @Override
    public CompletableFuture<Boolean> updateConfiguration(final String scope,
                                                          final String name,
                                                          final StreamConfiguration configuration,
                                                          final OperationContext context) {
        return getStream(scope, name, context).updateConfiguration(configuration);
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration(final String scope,
                                                                   final String name,
                                                                   final OperationContext context) {
        return getStream(scope, name, context).getConfiguration();
    }

    @Override
    public CompletableFuture<Boolean> isSealed(final String scope, final String name, final OperationContext context) {
        return getStream(scope, name, context).getState().thenApply(state -> state.equals(State.SEALED));
    }

    @Override
    public CompletableFuture<Boolean> setSealed(final String scope, final String name, final OperationContext context) {
        return getStream(scope, name, context).updateState(State.SEALED);
    }

    @Override
    public CompletableFuture<Segment> getSegment(final String scope, final String name, final int number, final OperationContext context) {
        return getStream(scope, name, context).getSegment(number);
    }

    @Override
    public CompletableFuture<List<Segment>> getActiveSegments(final String scope, final String name, final OperationContext context) {
        final Stream stream = getStream(scope, name, context);
        return stream.getState().thenCompose(state -> {
            if (State.SEALED.equals(state)) {
                return CompletableFuture.completedFuture(Collections.<Integer>emptyList());
            } else {
                return stream.getActiveSegments();
            }
        }).thenCompose(currentSegments -> FutureHelpers.allOfWithResults(currentSegments.stream().map(stream::getSegment).collect(Collectors.toList())));
    }

    @Override
    public CompletableFuture<SegmentFutures> getActiveSegments(final String scope, final String name, final long timestamp, final OperationContext context) {
        Stream stream = getStream(scope, name, context);
        CompletableFuture<List<Integer>> futureActiveSegments = stream.getActiveSegments(timestamp);
        return futureActiveSegments.thenCompose(activeSegments -> constructSegmentFutures(stream, activeSegments));
    }

    @Override
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessors(final String scope, final String streamName,
                                                                        final int segmentNumber, final OperationContext context) {
        Stream stream = getStream(scope, streamName, context);
        return stream.getSuccessorsWithPredecessors(segmentNumber);
    }

    @Override
    public CompletableFuture<List<Segment>> scale(final String scope, final String name,
                                                  final List<Integer> sealedSegments,
                                                  final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                  final long scaleTimestamp, final OperationContext context) {
        return getStream(scope, name, context).scale(sealedSegments, newRanges, scaleTimestamp);
    }

    @Override
    public CompletableFuture<UUID> createTransaction(final String scope, final String stream, final OperationContext context) {
        return getStream(scope, stream, context).createTransaction();
    }

    @Override
    public CompletableFuture<TxnStatus> transactionStatus(final String scope, final String stream, final UUID txId, final OperationContext context) {
        return getStream(scope, stream, context).checkTransactionStatus(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final UUID txId, final OperationContext context) {
        return getStream(scope, stream, context).commitTransaction(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> sealTransaction(final String scope, final String stream, final UUID txId, final OperationContext context) {
        return getStream(scope, stream, context).sealTransaction(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final String scope, final String stream, final UUID txId, final OperationContext context) {
        return getStream(scope, stream, context).abortTransaction(txId);
    }

    @Override
    public CompletableFuture<Boolean> isTransactionOngoing(final String scope, final String stream, final OperationContext context) {
        return getStream(scope, stream, context).isTransactionOngoing();
    }

    @Override
    public CompletableFuture<Void> setMarker(String scope, String stream, int segmentNumber, long timestamp, OperationContext context) {
        return getStream(scope, stream, context).setMarker(segmentNumber, timestamp);
    }

    @Override
    public CompletableFuture<Optional<Long>> getMarker(String scope, String stream, int number, OperationContext context) {
        return getStream(scope, stream, context).getMarker(number);
    }

    @Override
    public CompletableFuture<Void> removeMarker(String scope, String stream, int number, OperationContext context) {
        return getStream(scope, stream, context).removeMarker(number);
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxRecord>> getActiveTxns(String scope, String stream, OperationContext context) {
        return getStream(scope, stream, context).getActiveTxns();
    }

    @Override
    public CompletableFuture<Void> blockTransactions(String scope, String stream, OperationContext context) {
        return getStream(scope, stream, context).blockTransactions();
    }

    @Override
    public CompletableFuture<Void> unblockTransactions(String scope, String stream, OperationContext context) {
        return getStream(scope, stream, context).unblockTransactions();
    }

    private Stream getStream(String scope, final String name, OperationContext context) {
        Stream stream;
        if (context != null) {
            stream = (Stream) context;
            assert stream.getScope().equals(scope);
            assert stream.getName().equals(name);
        } else {
            stream = cache.getUnchecked(new ImmutablePair<>(scope, name));
            stream.refresh();
        }
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


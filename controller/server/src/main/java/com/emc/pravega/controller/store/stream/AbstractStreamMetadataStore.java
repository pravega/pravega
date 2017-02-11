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

/**
 * Abstract Stream metadata store. It implements various read queries using the Stream interface.
 * Implementation of create and update queries are delegated to the specific implementations of this abstract class.
 */
public abstract class AbstractStreamMetadataStore implements StreamMetadataStore {

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

    abstract Stream newStream(final String streamName);

    abstract Scope newScope(final String scopeName);

    @Override
    public CompletableFuture<Boolean> createStream(final String scopeName,
                                                   final String streamName,
                                                   final StreamConfiguration configuration,
                                                   final long createTimestamp) {
        return getStream(scopeName, streamName).create(configuration, createTimestamp);
    }

    @Override
    public CompletableFuture<Boolean> createScope(final String scopeName) {
        return getScope(scopeName).createScope(scopeName);
    }

    @Override
    public CompletableFuture<Boolean> deleteScope(final String scopeName) {
        return null;
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
        return getStream(scopeName, streamName).updateState(State.SEALED);
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
        return getStream(scopeName, streamName).scale(sealedSegments, newRanges, scaleTimestamp);
    }

    @Override
    public CompletableFuture<UUID> createTransaction(final String scopeName, final String streamName) {
        return getStream(scopeName, streamName).createTransaction();
    }

    @Override
    public CompletableFuture<TxnStatus> transactionStatus(final String scopeName, final String streamName, final UUID txId) {
        return getStream(scopeName, streamName).checkTransactionStatus(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final String scopeName, final String streamName, final UUID txId) {
        return getStream(scopeName, streamName).commitTransaction(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> sealTransaction(final String scopeName, final String streamName, final UUID txId) {
        return getStream(scopeName, streamName).sealTransaction(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(final String scopeName, final String streamName, final UUID txId) {
        return getStream(scopeName, streamName).abortTransaction(txId);
    }

    @Override
    public CompletableFuture<Boolean> isTransactionOngoing(final String scopeName, final String streamName) {
        return getStream(scopeName, streamName).isTransactionOngoing();
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

    private String getScopedStreamName(final String scopeName, final String streamName) {
        return new StringBuffer(scopeName).append("/").append(streamName).toString();
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

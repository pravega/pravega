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

import com.emc.pravega.controller.RetryableException;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecord;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.emc.pravega.common.concurrent.FutureCollectionHelper.filter;
import static com.emc.pravega.common.concurrent.FutureCollectionHelper.sequence;

/**
 * Abstract Stream metadata store. It implements various read queries using the Stream interface.
 * Implementation of create and update queries are delegated to the specific implementations of this abstract class.
 */
public abstract class AbstractStreamMetadataStore implements StreamMetadataStore {

    private final LoadingCache<String, Stream> cache;

    protected AbstractStreamMetadataStore() {
        cache = CacheBuilder.newBuilder()
                .maximumSize(100000)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, Stream>() {
                            @ParametersAreNonnullByDefault
                            public Stream load(String name) {
                                try {
                                    // TODO: add scope
                                    return newStream(null, name);
                                } catch (Exception e) {
                                    if (RetryableException.isRetryable(e)) {
                                        throw (RetryableException) e;
                                    }
                                    throw new RuntimeException(e);
                                }
                            }
                        });
    }

    abstract Stream newStream(final String scope, final String name);


    @Override
    public StreamContext createContext(final String scope, final String stream) {
        return getStream(scope, stream, null);
    }

    @Override
    public CompletableFuture<Boolean> createStream(final String scope, final String name,
                                                   final StreamConfiguration configuration,
                                                   final long createTimestamp, final StreamContext context) {
        return getStream(scope, name, context).create(configuration, createTimestamp);
    }

    @Override
    public CompletableFuture<Boolean> updateConfiguration(final String scope, final String name,
                                                          final StreamConfiguration configuration, final StreamContext context) {
        return getStream(scope, name, context).updateConfiguration(configuration);
    }

    @Override
    public CompletableFuture<StreamConfiguration> getConfiguration(final String scope, final String name, final StreamContext context) {
        return getStream(scope, name, context).getConfiguration();
    }

    @Override
    public CompletableFuture<Boolean> isSealed(final String scope, final String name, final StreamContext context) {
        return getStream(scope, name, context).getState().thenApply(state -> state.equals(State.SEALED));
    }

    @Override
    public CompletableFuture<Boolean> setSealed(final String scope, final String name, final StreamContext context) {
        return getStream(scope, name, context).updateState(State.SEALED);
    }

    @Override
    public CompletableFuture<Segment> getSegment(final String scope, final String name, final int number, final StreamContext context) {
        return getStream(scope, name, context).getSegment(number);
    }

    @Override
    public CompletableFuture<List<Segment>> getActiveSegments(final String scope, final String name, final StreamContext context) {
        final Stream stream = getStream(scope, name, context);
        return stream.getState()
                .thenCompose(state ->
                        State.SEALED.equals(state) ? CompletableFuture.completedFuture(Collections.emptyList()) : stream
                                .getActiveSegments())
                .thenCompose(currentSegments -> sequence(currentSegments.stream().map(stream::getSegment)
                        .collect(Collectors.toList())));
    }

    @Override
    public CompletableFuture<SegmentFutures> getActiveSegments(final String scope, final String name, final long timestamp, final StreamContext context) {
        Stream stream = getStream(scope, name, context);
        CompletableFuture<List<Integer>> futureActiveSegments = stream.getActiveSegments(timestamp);
        return futureActiveSegments.thenCompose(activeSegments -> constructSegmentFutures(stream, activeSegments));
    }

    @Override
    public CompletableFuture<List<SegmentFutures>> getNextSegments(final String scope, final String name,
                                                                   final Set<Integer> completedSegments,
                                                                   final List<SegmentFutures> positions, final StreamContext context) {
        Preconditions.checkNotNull(positions);
        Preconditions.checkArgument(positions.size() > 0);

        Stream stream = getStream(scope, name, context);

        Set<Integer> current = new HashSet<>();
        positions.forEach(position ->
                position.getCurrent().forEach(current::add));

        CompletableFuture<Set<Integer>> implicitCompletedSegments =
                getImplicitCompletedSegments(stream, completedSegments, current);

        CompletableFuture<Set<Integer>> successors =
                implicitCompletedSegments.thenCompose(x -> getSuccessors(stream, x));

        CompletableFuture<List<Integer>> newCurrents =
                implicitCompletedSegments.thenCompose(x -> successors.thenCompose(y -> getNewCurrents(stream, y, x, positions)));

        CompletableFuture<Map<Integer, List<Integer>>> newFutures =
                implicitCompletedSegments.thenCompose(x -> successors.thenCompose(y -> getNewFutures(stream, y, x)));

        return newCurrents.thenCompose(x -> newFutures.thenCompose(y -> divideSegments(stream, x, y, positions)));
    }

    @Override
    public CompletableFuture<List<Segment>> scale(final String scope, final String name,
                                                  final List<Integer> sealedSegments,
                                                  final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                  final long scaleTimestamp, final StreamContext context) {
        return getStream(scope, name, context).scale(sealedSegments, newRanges, scaleTimestamp);
    }

    @Override
    public CompletableFuture<UUID> createTransaction(final String scope, final String stream, final StreamContext context) {
        return getStream(scope, stream, context).createTransaction();
    }

    @Override
    public CompletableFuture<TxnStatus> transactionStatus(final String scope, final String stream, final UUID txId, final StreamContext context) {
        return getStream(scope, stream, context).checkTransactionStatus(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final UUID txId, final StreamContext context) {
        return getStream(scope, stream, context).commitTransaction(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> sealTransaction(final String scope, final String stream, final UUID txId, final StreamContext context) {
        return getStream(scope, stream, context).sealTransaction(txId);
    }

    @Override
    public CompletableFuture<TxnStatus> dropTransaction(final String scope, final String stream, final UUID txId, final StreamContext context) {
        return getStream(scope, stream, context).abortTransaction(txId);
    }

    @Override
    public CompletableFuture<Boolean> isTransactionOngoing(final String scope, final String stream, final StreamContext context) {
        return getStream(scope, stream, context).isTransactionOngoing();
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxRecord>> getActiveTxns(String scope, String stream, StreamContext context) {
        return getStream(scope, stream, context).getActiveTxns();
    }

    @Override
    public CompletableFuture<Void> blockTransactions(final String scope, final String stream, final StreamContext context) {
        return getStream(scope, stream, context).blockTransactions();
    }

    @Override
    public CompletableFuture<Void> unblockTransactions(final String scope, final String stream, final StreamContext context) {
        return getStream(scope, stream, context).unblockTransactions();
    }

    @Override
    public CompletableFuture<Void> setMarker(final String scope, final String stream, final int segmentNumber, final long timestamp, final StreamContext context) {
        return getStream(scope, stream, context).setMarker(segmentNumber, timestamp);
    }

    @Override
    public CompletableFuture<Optional<Long>> getMarker(final String scope, final String stream, final int number, final StreamContext context) {
        return getStream(scope, stream, context).getMarker(number);
    }

    @Override
    public CompletableFuture<Void> removeMarker(final String scope, final String stream, final int number, final StreamContext context) {
        return getStream(scope, stream, context).removeMarker(number);
    }

    private Stream getStream(final String scope, final String name, final StreamContext context) {
        Stream stream;
        if (context != null) {
            stream = (Stream) context;
            assert (stream.getScope().equals(scope));
            assert (stream.getName().equals(name));
        } else {
            stream = cache.getUnchecked(name);
            stream.refresh();
        }
        return stream;
    }

    private CompletableFuture<SegmentFutures> constructSegmentFutures(final Stream stream, final List<Integer> activeSegments) {
        Map<Integer, Integer> futureSegments = new HashMap<>();
        List<CompletableFuture<List<Integer>>> list =
                activeSegments.stream().map(number -> getDefaultFutures(stream, number)).collect(Collectors.toList());

        CompletableFuture<List<List<Integer>>> futureDefaultFutures = sequence(list);
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
                list -> filter(list, elem -> stream.getPredecessors(elem).thenApply(x -> x.size() == 1)));
    }

    private CompletableFuture<Set<Integer>> getImplicitCompletedSegments(final Stream stream,
                                                                         final Set<Integer> completedSegments,
                                                                         final Set<Integer> current) {
        List<CompletableFuture<Set<Integer>>> futures =
                current
                        .stream()
                        .map(x -> stream.getPredecessors(x).thenApply(list -> list.stream().collect(Collectors.toSet())))
                        .collect(Collectors.toList());

        return sequence(futures)
                .thenApply(list -> Sets.union(completedSegments, list.stream().reduce(Collections.emptySet(), Sets::union)));
    }

    private CompletableFuture<Set<Integer>> getSuccessors(final Stream stream, final Set<Integer> completedSegments) {
        // successors of completed segments are interesting, which means
        // some of them may become current, and
        // some of them may become future
        //Set<Integer> successors = completedSegments.stream().flatMap(x -> stream.getSuccessors(x).stream()).collect(Collectors.toSet());
        List<CompletableFuture<Set<Integer>>> futures =
                completedSegments
                        .stream()
                        .map(x -> stream.getSuccessors(x).thenApply(list -> list.stream().collect(Collectors.toSet())))
                        .collect(Collectors.toList());

        return sequence(futures)
                .thenApply(list -> list.stream().reduce(Collections.emptySet(), Sets::union));
    }

    private CompletableFuture<List<Integer>> getNewCurrents(final Stream stream,
                                                            final Set<Integer> successors,
                                                            final Set<Integer> completedSegments,
                                                            final List<SegmentFutures> positions) {
        // a successor that has
        // 1. it is not completed yet, and
        // 2. it is not current in any of the positions, and
        // 3. all its predecessors completed.
        // shall become current and be added to some position
        List<Integer> newCurrents = successors.stream().filter(x ->
                        // 2. it is not completed yet, and
                        !completedSegments.contains(x)
                                // 3. it is not current in any of the positions
                                && positions.stream().allMatch(z -> !z.getCurrent().contains(x))
        ).collect(Collectors.toList());

        // 3. all its predecessors completed, and
        Function<List<Integer>, Boolean> predicate = list -> list.stream().allMatch(completedSegments::contains);
        return filter(
                newCurrents,
                (Integer x) -> stream.getPredecessors(x).thenApply(predicate));
    }

    private CompletableFuture<Map<Integer, List<Integer>>> getNewFutures(final Stream stream,
                                                                         final Set<Integer> successors,
                                                                         final Set<Integer> completedSegments) {
        List<Integer> subset = successors.stream().filter(x -> !completedSegments.contains(x)).collect(Collectors.toList());

        List<CompletableFuture<List<Integer>>> predecessors = new ArrayList<>();
        for (Integer number : subset) {
            predecessors.add(stream.getPredecessors(number)
                            .thenApply(preds -> preds.stream().filter(y -> !completedSegments.contains(y)).collect(Collectors.toList()))
            );
        }

        return sequence(predecessors).thenApply((List<List<Integer>> preds) -> {
            Map<Integer, List<Integer>> map = new HashMap<>();
            for (int i = 0; i < preds.size(); i++) {
                List<Integer> filtered = preds.get(i);
                if (filtered.size() == 1) {
                    Integer pendingPredecessor = filtered.get(0);
                    if (map.containsKey(pendingPredecessor)) {
                        map.get(pendingPredecessor).add(subset.get(i));
                    } else {
                        List<Integer> list = new ArrayList<>();
                        list.add(subset.get(i));
                        map.put(pendingPredecessor, list);
                    }
                }
            }
            return map;
        });
    }

    /**
     * Divides the set of new current segments among existing positions and returns the updated positions
     *
     * @param stream      stream
     * @param newCurrents new set of current segments
     * @param newFutures  new set of future segments
     * @param positions   positions to be updated
     * @return the updated sequence of positions
     */
    private CompletableFuture<List<SegmentFutures>> divideSegments(final Stream stream,
                                                                   final List<Integer> newCurrents,
                                                                   final Map<Integer, List<Integer>> newFutures,
                                                                   final List<SegmentFutures> positions) {
        List<CompletableFuture<SegmentFutures>> newPositions = new ArrayList<>(positions.size());

        int quotient = newCurrents.size() / positions.size();
        int remainder = newCurrents.size() % positions.size();
        int counter = 0;
        for (int i = 0; i < positions.size(); i++) {
            SegmentFutures position = positions.get(i);

            // add the new current segments
            List<Integer> newCurrent = new ArrayList<>(position.getCurrent());
            int portion = (i < remainder) ? quotient + 1 : quotient;
            for (int j = 0; j < portion; j++, counter++) {
                newCurrent.add(newCurrents.get(counter));
            }
            Map<Integer, Integer> newFuture = new HashMap<>(position.getFutures());
            // add new futures if any
            position.getCurrent().forEach(
                    current -> {
                        if (newFutures.containsKey(current)) {
                            newFutures.get(current).stream().forEach(x -> newFuture.put(x, current));
                        }
                    }
            );
            // add default futures for new and old current segments, if any
            List<CompletableFuture<List<Integer>>> defaultFutures =
                    newCurrent.stream().map(x -> getDefaultFutures(stream, x)).collect(Collectors.toList());

            CompletableFuture<SegmentFutures> segmentFuture =
                    sequence(defaultFutures).thenApply((List<List<Integer>> list) -> {
                        for (int k = 0; k < list.size(); k++) {
                            Integer x = newCurrent.get(k);
                            list.get(k).stream().forEach(
                                    y -> {
                                        if (!newFuture.containsKey(y)) {
                                            newFuture.put(y, x);
                                        }
                                    }
                            );
                        }
                        return new SegmentFutures(newCurrent, newFuture);
                    });
            newPositions.add(segmentFuture);
        }
        return sequence(newPositions);
    }
}

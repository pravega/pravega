/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.store.stream.tables.ActiveTxnRecord;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.client.stream.StreamConfiguration;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import org.apache.commons.lang.NotImplementedException;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Stream properties
 * <p>
 * This class is no longer consistent and mostly not Implemented. Deprecating it.
 */
class InMemoryStream implements Stream {
    private final String streamName;
    private final String scopeName;
    private StreamConfiguration configuration;
    private State state;

    /**
     * Stores all segments in the stream, ordered by number, which implies that
     * these segments are also ordered in the increaing order of their start times.
     * Segment number is the index of that segment in this list.
     */
    private final List<InMemorySegment> segments = new ArrayList<>();

    /**
     * Stores segment numbers of currently active segments in the stream.
     * It enables efficient access to current segments needed by producers and tailing consumers.
     */
    private final List<Integer> currentSegments = new ArrayList<>();

    @AllArgsConstructor
    static class NonExistentStream implements Stream {

        private String scope;
        private String stream;

        @Override
        public String getScope() {
            return scope;
        }

        @Override
        public String getName() {
            return stream;
        }

        @Override
        public String getScopeName() {
            return scope;
        }

        @Override
        public CompletableFuture<Boolean> create(final StreamConfiguration configuration, final long createTimestamp) {
            return FutureHelpers.failedFuture(new IllegalStateException("Cannot create non-existent stream"));
        }

        @Override
        public CompletableFuture<Void> delete() {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Boolean> updateConfiguration(final StreamConfiguration configuration) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<StreamConfiguration> getConfiguration() {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Boolean> updateState(final State state) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<State> getState() {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Segment> getSegment(final int number) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Integer> getSegmentCount() {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<List<Integer>> getSuccessors(final int number) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Map<Integer, List<Integer>>> getSuccessorsWithPredecessors(final int number) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<List<Integer>> getPredecessors(final int number) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<List<Integer>> getActiveSegments() {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<List<Integer>> getActiveSegments(final long timestamp) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<List<Segment>> startScale(List<Integer> sealedSegments, List<SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Void> scaleNewSegmentsCreated(List<Integer> sealedSegments, List<Integer> newSegments, long scaleTimestamp) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Void> scaleOldSegmentsSealed(List<Integer> sealedSegments, List<Integer> newSegments, long ts) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Void> setColdMarker(final int segmentNumber, final long timestamp) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Long> getColdMarker(final int segmentNumber) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Void> removeColdMarker(final int segmentNumber) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<VersionedTransactionData> createTransaction(final long lease,
                                                                             final long maxExecutionTime,
                                                                             final long scaleGracePeriod) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<VersionedTransactionData> pingTransaction(final UUID txId,
                                                                           final long lease) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<VersionedTransactionData> getTransactionData(final UUID txId) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<TxnStatus> sealTransaction(final UUID txId,
                                                            final boolean commit,
                                                            final Optional<Integer> version) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<TxnStatus> checkTransactionStatus(final UUID txId) {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<TxnStatus> commitTransaction(final UUID txId) throws OperationOnTxNotAllowedException {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<TxnStatus> abortTransaction(final UUID txId) throws OperationOnTxNotAllowedException {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns() {
            return FutureHelpers.failedFuture(new DataNotFoundException(stream));
        }

        @Override
        public void refresh() {
        }
    }

    InMemoryStream(final String scopeName, final String streamName) {
        this.scopeName = scopeName;
        this.streamName = streamName;
    }

    @Override
    public synchronized CompletableFuture<Boolean> create(StreamConfiguration configuration, long timestamp) {
        this.configuration = configuration;
        this.state = State.ACTIVE;
        int numSegments = configuration.getScalingPolicy().getMinNumSegments();
        double keyRange = 1.0 / numSegments;
        IntStream.range(0, numSegments)
                .forEach(
                        x -> {
                            InMemorySegment segment = new InMemorySegment(x, 0, Long.MAX_VALUE, x * keyRange, (x + 1) * keyRange);
                            segments.add(segment);
                            currentSegments.add(x);
                        }
                );
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Void> delete() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String getScope() {
        return this.scopeName;
    }

    @Override
    public String getName() {
        return this.streamName;
    }

    @Override
    public String getScopeName() {
        return this.scopeName;
    }

    @Override
    public synchronized CompletableFuture<Boolean> updateConfiguration(StreamConfiguration configuration) {
        this.configuration = configuration;
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public synchronized CompletableFuture<StreamConfiguration> getConfiguration() {
        return CompletableFuture.completedFuture(this.configuration);
    }

    @Override
    public CompletableFuture<Boolean> updateState(State state) {
        this.state = state;
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<State> getState() {
        return CompletableFuture.completedFuture(state);
    }

    @Override
    public synchronized CompletableFuture<Segment> getSegment(int number) {
        return CompletableFuture.completedFuture(segments.get(number));
    }

    @Override
    public synchronized CompletableFuture<Integer> getSegmentCount() {
        return CompletableFuture.completedFuture(segments.size());
    }

    @Override
    public CompletableFuture<List<Integer>> getSuccessors(int number) {
        return CompletableFuture.completedFuture(segments.get(number).getSuccessors());
    }

    @Override
    public CompletableFuture<Map<Integer, List<Integer>>> getSuccessorsWithPredecessors(final int number) {
        Map<Integer, List<Integer>> result = new HashMap<>();
        for (Integer successor : segments.get(number).getSuccessors()) {
            result.put(successor, segments.get(successor).getPredecessors());
        }
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<List<Integer>> getPredecessors(int number) {
        return CompletableFuture.completedFuture(segments.get(number).getPredecessors());
    }

    /**
     * @return the list of currently active segments
     */
    @Override
    public synchronized CompletableFuture<List<Integer>> getActiveSegments() {
        return CompletableFuture.completedFuture(Collections.unmodifiableList(currentSegments));
    }

    /**
     * @return the list of segments active at a given timestamp.
     * GetActiveSegments runs in O(n), where n is the total number of segments.
     * It can be improved to O(k + logn), where k is the number of active segments at specified timestamp,
     * using augmented interval tree or segment index..
     * TODO: maintain a augmented interval tree or segment tree index
     */
    @Override
    public synchronized CompletableFuture<List<Integer>> getActiveSegments(long timestamp) {
        List<Integer> currentSegments = new ArrayList<>();
        int i = 0;
        while (i < segments.size() && timestamp >= segments.get(i).getStart()) {
            if (segments.get(i).getEnd() >= timestamp) {
                InMemorySegment segment = segments.get(i);
                currentSegments.add(segment.getNumber());
            }
            i++;
        }
        return CompletableFuture.completedFuture(currentSegments);
    }

    @Override
    public CompletableFuture<List<Segment>> startScale(List<Integer> sealedSegments, List<SimpleEntry<Double, Double>> keyRanges, long scaleTimestamp) {
        Preconditions.checkNotNull(keyRanges);
        Preconditions.checkArgument(keyRanges.size() > 0);

        List<List<Integer>> predecessors = new ArrayList<>();
        for (int i = 0; i < keyRanges.size(); i++) {
            predecessors.add(new ArrayList<>());
        }

        int start = segments.size();

        List<Segment> newSegments = new ArrayList<>();
        // assign start times, numbers to new segments. Add them to segments list and current list.
        for (int i = 0; i < keyRanges.size(); i++) {
            int number = start + i;
            InMemorySegment segment = new InMemorySegment(number, scaleTimestamp, Long.MAX_VALUE, keyRanges.get(i).getKey(), keyRanges.get(i).getValue(), InMemorySegment.Status.Active, new ArrayList<>(), predecessors.get(i));
            newSegments.add(segment);
            segments.add(segment);
            currentSegments.add(number);
        }

        return CompletableFuture.completedFuture(newSegments);
    }

    @Override
    public CompletableFuture<Void> scaleNewSegmentsCreated(List<Integer> sealedSegments, List<Integer> newSegments, long timestamp) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> scaleOldSegmentsSealed(List<Integer> sealedSegments, List<Integer> newSegments, long timestamp) {
        Preconditions.checkNotNull(sealedSegments);
        Preconditions.checkArgument(sealedSegments.size() > 0);

        List<List<Integer>> predecessors = new ArrayList<>();
        List<SimpleEntry<Double, Double>> keyRanges = newSegments.stream().map(x -> {
            Segment segment = FutureHelpers.getAndHandleExceptions(getSegment(x), RuntimeException::new);
            return new SimpleEntry<>(segment.getKeyStart(), segment.getKeyEnd());
        }).collect(Collectors.toList());
        for (int i = 0; i < keyRanges.size(); i++) {
            predecessors.add(new ArrayList<>());
        }

        int start = segments.size();
        // assign status, end times, and successors to sealed segments.
        // assign predecessors to new segments
        for (Integer sealed : sealedSegments) {
            InMemorySegment segment = segments.get(sealed);
            List<Integer> successors = new ArrayList<>();

            for (int i = 0; i < keyRanges.size(); i++) {
                if (segment.overlaps(keyRanges.get(i).getKey(), keyRanges.get(i).getValue())) {
                    successors.add(start + i);
                    predecessors.get(i).add(sealed);
                }
            }
            InMemorySegment sealedSegment = new InMemorySegment(sealed, segment.getStart(),
                    timestamp,
                    segment.getKeyStart(), segment.getKeyEnd(),
                    InMemorySegment.Status.Sealed, successors, segment.getPredecessors());
            segments.set(sealed, sealedSegment);
            currentSegments.remove(sealed);
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> setColdMarker(int segmentNumber, long timestamp) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Long> getColdMarker(int segmentNumber) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Void> removeColdMarker(int segmentNumber) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<VersionedTransactionData> createTransaction(final long lease, final long maxExecutionTime,
                                                                         final long scaleGracePeriod) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<VersionedTransactionData> pingTransaction(UUID txId, long lease) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<VersionedTransactionData> getTransactionData(UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<TxnStatus> sealTransaction(UUID txId, boolean commit, Optional<Integer> version) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<TxnStatus> checkTransactionStatus(UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<TxnStatus> commitTransaction(UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<TxnStatus> abortTransaction(UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Integer> getNumberOfOngoingTransactions() {
        return CompletableFuture.completedFuture(0); //Transactions are not supported in this implementation.
    }

    @Override
    public CompletableFuture<Map<UUID, ActiveTxnRecord>> getActiveTxns() {
        return null;
    }

    @Override
    public void refresh() {

    }

    public String toString() {
        return String.format("Current Segments:%s%nSegments:%s%n", currentSegments.toString(), segments.toString());
    }
}

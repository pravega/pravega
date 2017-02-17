/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.TxnStatus;
import com.google.common.base.Preconditions;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.apache.commons.lang.NotImplementedException;

/**
 * Stream properties
 */
class InMemoryStream implements Stream {
    private final String name;
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

    InMemoryStream(String name) {
        this.name = name;
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
    public String getName() {
        return this.name;
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

    /**
     * Seals a set of segments, and adds a new set of segments as current segments.
     * It sets appropriate endtime and successors of sealed segment.
     *
     * @param sealedSegments segments to be sealed
     * @param keyRanges      new segments to be added as active segments
     * @param scaleTimestamp scaling timestamp. This will be the end time of sealed segments and start time of new segments.
     * @return the list of new segments.
     */
    @Override
    public synchronized CompletableFuture<List<Segment>> scale(List<Integer> sealedSegments, List<SimpleEntry<Double, Double>> keyRanges, long scaleTimestamp) {
        Preconditions.checkNotNull(sealedSegments);
        Preconditions.checkNotNull(keyRanges);
        Preconditions.checkArgument(sealedSegments.size() > 0);
        Preconditions.checkArgument(keyRanges.size() > 0);

        List<List<Integer>> predecessors = new ArrayList<>();
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
            InMemorySegment sealedSegment = new InMemorySegment(sealed, segment.getStart(), scaleTimestamp, segment.getKeyStart(), segment.getKeyEnd(), InMemorySegment.Status.Sealed, successors, segment.getPredecessors());
            segments.set(sealed, sealedSegment);
            currentSegments.remove(sealed);
        }

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
    public CompletableFuture<UUID> createTransaction() {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<TxnStatus> sealTransaction(UUID txId, boolean commit) {
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
    public void refresh() {

    }

    public String toString() {
        return String.format("Current Segments:%s%nSegments:%s%n", currentSegments.toString(), segments.toString());
    }
}

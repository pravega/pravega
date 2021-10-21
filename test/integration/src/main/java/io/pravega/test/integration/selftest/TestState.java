/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents the current State of the SelfTest.
 */
public class TestState {
    //region Members

    static final OperationType[] SUMMARY_OPERATION_TYPES = {
            ProducerOperationType.STREAM_APPEND,
            ProducerOperationType.CREATE_STREAM_TRANSACTION,
            ProducerOperationType.MERGE_STREAM_TRANSACTION,
            ProducerOperationType.ABORT_STREAM_TRANSACTION,
            ProducerOperationType.STREAM_SEAL,
            ProducerOperationType.TABLE_REMOVE,
            ProducerOperationType.TABLE_UPDATE,
            ConsumerOperationType.END_TO_END,
            ConsumerOperationType.CATCHUP_READ,
            ConsumerOperationType.TABLE_GET,
            ConsumerOperationType.TABLE_ITERATOR,
            ConsumerOperationType.TABLE_ITERATOR_STEP};
    private static final double NANOS_PER_SECOND = 1000 * 1000 * 1000.0;
    private static final int MAX_LATENCY_MILLIS = 30000;

    private final AtomicBoolean warmup;
    private final AtomicInteger generatedOperationCount;
    private final AtomicInteger successfulOperationCount;
    private final AtomicLong producedLength;
    private final AtomicLong verifiedTailLength;
    private final AtomicLong verifiedCatchupLength;
    private final AtomicLong verifiedStorageLength;
    private final AtomicInteger tableUpdateCount;
    private final AtomicInteger tableGetCount;
    private final AtomicInteger tableGetTotalKeyCount;
    private final ConcurrentHashMap<String, StreamInfo> allStreams;
    @GuardedBy("allStreamNames")
    private final ArrayList<String> allStreamNames;
    private final Map<OperationType, LatencyCollection> durations;
    private final AtomicLong startTimeNanos;
    private final AtomicLong lastAppendTime;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TestState class.
     */
    TestState() {
        this.warmup = new AtomicBoolean(false);
        this.generatedOperationCount = new AtomicInteger();
        this.successfulOperationCount = new AtomicInteger();
        this.allStreams = new ConcurrentHashMap<>();
        this.allStreamNames = new ArrayList<>();
        this.producedLength = new AtomicLong();
        this.verifiedTailLength = new AtomicLong();
        this.verifiedCatchupLength = new AtomicLong();
        this.verifiedStorageLength = new AtomicLong();
        this.tableUpdateCount = new AtomicInteger();
        this.tableGetCount = new AtomicInteger();
        this.tableGetTotalKeyCount = new AtomicInteger();
        this.startTimeNanos = new AtomicLong();
        this.lastAppendTime = new AtomicLong();
        this.durations = Collections.unmodifiableMap(
                Arrays.stream(SUMMARY_OPERATION_TYPES)
                      .collect(Collectors.toMap(ot -> ot, ot -> new LatencyCollection(MAX_LATENCY_MILLIS))));
        reset();
    }

    //endregion

    //region Properties

    /**
     * Gets the throughput measured in Operations per Second.
     */
    double getOperationsPerSecond() {
        return this.successfulOperationCount.get() / getElapsedSeconds();
    }

    /**
     * Gets the throughput since the last time reset() was called in Bytes/Second.
     */
    double getThroughput() {
        return this.producedLength.get() / getElapsedSeconds();
    }

    /**
     * Gets a value indicating the total number of operations that were successfully completed.
     */
    int getSuccessfulOperationCount() {
        return this.successfulOperationCount.get();
    }

    /**
     * Gets a value indicating the total number of bytes produced (that were accepted by the Store).
     */
    long getProducedLength() {
        return this.producedLength.get();
    }

    /**
     * Gets a value indicating the total number of bytes verified by tail reads.
     */
    long getVerifiedTailLength() {
        return this.verifiedTailLength.get();
    }

    /**
     * Gets a value indicating the total number of bytes verified via catchup reads.
     */
    long getVerifiedCatchupLength() {
        return this.verifiedCatchupLength.get();
    }

    /**
     * Gets a value indicating the total number of bytes verified via direct storage reads (bypass the Store).
     */
    long getVerifiedStorageLength() {
        return this.verifiedStorageLength.get();
    }

    /**
     * Gets a value indicating the number of Table Updates & Removes performed.
     */
    int getTableUpdateCount() {
        return this.tableUpdateCount.get();
    }

    /**
     * Gets a value indicating the number of Table Get invocations.
     */
    int getTableGetCount() {
        return this.tableGetCount.get();
    }

    /**
     * Gets a value indicating the total number of Keys retrieved via Table Get invocations (each invocation may request
     * more than one key).
     */
    int getTableGetTotalKeyCount() {
        return this.tableGetTotalKeyCount.get();
    }

    /**
     * Gets an immutable collection containing information about all Segments and Transactions.
     */
    Collection<StreamInfo> getAllStreams() {
        return this.allStreams.values();
    }

    /**
     * Gets an immutable collection containing the names of all the Streams and Transactions.
     * The resulting object is a copy of the internal state, and will not reflect future modifications to this object.
     */
    Collection<String> getAllStreamNames() {
        synchronized (this.allStreamNames) {
            return new ArrayList<>(this.allStreamNames);
        }
    }

    /**
     * Gets an immutable collection containing the names of all the Transactions.
     * The resulting object is a copy of the internal state, and will not reflect future modifications to this object.
     */
    Collection<String> getTransactionNames() {
        return this.allStreams.values().stream()
                              .filter(StreamInfo::isTransaction)
                              .map(s -> s.name)
                              .collect(Collectors.toList());
    }

    /**
     * Gets information about a particular Stream.
     *
     * @param name The name of the Segment to inquire about.
     */
    StreamInfo getStream(String name) {
        return this.allStreams.get(name);
    }

    /**
     * Gets a StreamInfo that matches the given filter.
     *
     * @param filter The filter to use.
     * @return A StreamInfo (in no particular order) that matches the given filter, or null if none match.
     */
    StreamInfo getStream(Function<StreamInfo, Boolean> filter) {
        for (StreamInfo si : this.allStreams.values()) {
            if (filter.apply(si)) {
                return si;
            }
        }

        return null;
    }

    /**
     * Gets a StreamInfo of an arbitrary Stream/Transaction that is registered. Note that calling this method with the same
     * value for the argument may not necessarily produce the same result if done repeatedly.
     *
     * @param hint A hint to use to get the Stream name.
     */
    StreamInfo getStreamOrTransaction(int hint) {
        synchronized (this.allStreamNames) {
            return this.allStreams.get(this.allStreamNames.get(hint % this.allStreamNames.size()));
        }
    }

    /**
     * Gets the name of an arbitrary Stream (non-transaction) that is registered. Note that calling this method with the
     * same value for the argument may not necessarily produce the same result if done repeatedly.
     *
     * @param hint A hint to use to get the Stream name.
     */
    String getNonTransactionStreamName(int hint) {
        synchronized (this.allStreamNames) {
            int retry = 0;
            while (retry < this.allStreamNames.size()) {
                String currentName = this.allStreamNames.get((hint + retry) % this.allStreamNames.size());
                StreamInfo currentSegment = getStream(currentName);
                if (currentSegment != null && !currentSegment.isTransaction()) {
                    return currentName;
                }

                retry++;
            }

            throw new IllegalStateException("Unable to find at least one Non-Transaction Stream out of " + this.allStreamNames.size() + " total Streams.");
        }
    }

    /**
     * Gets a LatencyCollection for the given OperationType.
     *
     * @param operationType The OperationType to get Durations for.
     * @return A pointer to the list of durations.
     */
    LatencyCollection getDurations(OperationType operationType) {
        return this.durations.get(operationType);
    }

    private double getElapsedSeconds() {
        return (this.lastAppendTime.get() - this.startTimeNanos.get()) / NANOS_PER_SECOND;
    }

    //endregion

    //region Operations

    /**
     * Resets all statistics.
     */
    private void reset() {
        this.startTimeNanos.set(System.nanoTime());
        this.lastAppendTime.set(this.startTimeNanos.get());
        this.generatedOperationCount.set(0);
        this.successfulOperationCount.set(0);
        this.producedLength.set(0);
        this.verifiedTailLength.set(0);
        this.verifiedCatchupLength.set(0);
        this.verifiedStorageLength.set(0);
        this.tableUpdateCount.set(0);
        this.tableGetCount.set(0);
        this.durations.values().forEach(LatencyCollection::reset);
    }

    /**
     * Sets whether the test is in a warm-up or not. If the new value is different from the current one, all TestState
     * will be reset.
     *
     * @param value True if warm-up, false otherwise.
     */
    void setWarmup(boolean value) {
        if (this.warmup.compareAndSet(!value, value)) {
            reset();
        }
    }

    /**
     * Gets a value indicating whether the test is in a warp-up period.
     *
     * @return True if warmup, false otherwise.
     */
    boolean isWarmup() {
        return this.warmup.get();
    }

    /**
     * Records the creation (but not the completion) of a new Operation.
     *
     * @return The Operation Id (or numeric index).
     */
    int newOperation() {
        return this.generatedOperationCount.incrementAndGet();
    }

    /**
     * Records the fact that an operation completed for the given Stream.
     *
     * @param segmentName The name of the Stream for which the operation completed.
     * @return The number of total successful operations so far.
     */
    int operationCompleted(String segmentName) {
        StreamInfo si = this.getStream(segmentName);
        if (si != null) {
            si.operationCompleted();
        }

        return this.successfulOperationCount.incrementAndGet();
    }

    /**
     * Records the fact that an operation failed for the given Stream.
     *
     * @param segmentName The name of the Stream for which the operation failed.
     */
    void operationFailed(String segmentName) {
        StreamInfo si = this.getStream(segmentName);
        if (si != null) {
            si.operationCompleted();
        }

        this.generatedOperationCount.decrementAndGet();
    }

    /**
     * Records the duration of a single operation of the given type.
     *
     * @param operationType The type of the operation.
     * @param elapsedMillis The elapsed time of the operation, in millis.
     */
    void recordDuration(OperationType operationType, long elapsedMillis) {
        if (elapsedMillis < 0) {
            return;
        }

        LatencyCollection l = this.durations.getOrDefault(operationType, null);
        if (l != null) {
            l.record((int) elapsedMillis);
        }
    }

    /**
     * Records the fact that an append with the given length has been processed.
     *
     * @param length The length of the append.
     */
    void recordDataWritten(int length) {
        this.producedLength.addAndGet(length);
        this.lastAppendTime.set(System.nanoTime());
    }

    void recordTailRead(int length) {
        this.verifiedTailLength.addAndGet(length);
    }

    void recordCatchupRead(int length) {
        this.verifiedCatchupLength.addAndGet(length);
    }

    void recordStorageRead(int length) {
        this.verifiedStorageLength.addAndGet(length);
    }

    void recordTableModification(int updateCount) {
        this.tableUpdateCount.addAndGet(updateCount);
    }

    void recordTableGet(int keyCount) {
        this.tableGetCount.incrementAndGet();
        this.tableGetTotalKeyCount.addAndGet(keyCount);
    }

    /**
     * Records the fact that a new Stream (not a Transaction) was created.
     *
     * @param name The name of the Segment.
     */
    void recordNewStreamName(String name) {
        synchronized (this.allStreamNames) {
            Preconditions.checkArgument(!this.allStreams.containsKey(name), "Given Stream already exists");
            this.allStreamNames.add(name);
        }

        this.allStreams.put(name, new StreamInfo(name, false));
    }

    /**
     * Records the fact that a new Transaction was created.
     *
     * @param name The name of the Transaction.
     */
    void recordNewTransaction(String name) {
        synchronized (this.allStreamNames) {
            Preconditions.checkArgument(!this.allStreams.containsKey(name), "Given Stream already exists");
            this.allStreamNames.add(name);
        }

        this.allStreams.put(name, new StreamInfo(name, true));
    }

    /**
     * Records the fact that a Stream or a Transaction was deleted.
     *
     * @param name The name of the Segment/Transaction.
     */
    void recordDeletedStream(String name) {
        this.allStreams.remove(name);
        synchronized (this.allStreamNames) {
            this.allStreamNames.remove(name);
        }
    }

    //endregion

    //region StreamInfo

    /**
     * Represents information about a particular Segment.
     */
    @RequiredArgsConstructor
    static class StreamInfo {
        @Getter
        private final String name;
        @Getter
        private final boolean transaction;
        @GuardedBy("this")
        private boolean closed;
        @GuardedBy("this")
        private int startedOperationCount;
        @GuardedBy("this")
        private int completedOperationCount;
        @GuardedBy("this")
        private CompletableFuture<Void> closeCallback;

        /**
         * Indicates that a new operation regarding this Stream has Started.
         */
        synchronized void operationStarted() {
            this.startedOperationCount++;
        }

        /**
         * Indicates that an operation completed successfully for this Segment.
         */
        void operationCompleted() {
            CompletableFuture<Void> f = null;
            synchronized (this) {
                this.completedOperationCount++;
                if (this.completedOperationCount >= this.startedOperationCount) {
                    f = this.closeCallback;
                }
            }

            if (f != null) {
                f.complete(null);
            }
        }

        /**
         * Gets the number of completed operations so far.
         */
        synchronized int getCompletedOperationCount() {
            return this.completedOperationCount;
        }

        /**
         * Gets a value indicating whether this Stream is closed or not.
         */
        synchronized boolean isClosed() {
            return this.closed;
        }

        /**
         * Marks this Stream as closed (Sealed).
         *
         * @return A CompletableFuture that will be completed when the number of completed operations is at least the
         * number of Started operations.
         */
        CompletableFuture<Void> close() {
            CompletableFuture<Void> f;
            boolean isSatisfied;
            synchronized (this) {
                if (!this.closed) {
                    this.closeCallback = new CompletableFuture<>();
                    this.closed = true;
                }

                f = this.closeCallback;
                isSatisfied = this.completedOperationCount >= this.startedOperationCount;
            }

            if (isSatisfied) {
                f.complete(null);
            }

            return f;
        }

        @Override
        public String toString() {
            return String.format("%s, OpCount = %s, Transaction = %s", this.name, getCompletedOperationCount(), this.transaction);
        }
    }

    //endregion

    /**
     * Collects latencies and calculates statistics on them.
     */
    @ThreadSafe
    static class LatencyCollection {
        @GuardedBy("this")
        private final int[] latencyCounts;
        @GuardedBy("this")
        private int size;

        private LatencyCollection(int maxLatencyMillis) {
            this.latencyCounts = new int[maxLatencyMillis + 1];
        }

        /**
         * Resets this object to the initial state.
         */
        synchronized void reset() {
            Arrays.fill(this.latencyCounts, 0);
            this.size = 0;
        }

        /**
         * Records a new latency.
         *
         * @param durationMillis The latency (millis) to record, capped at the value of maxLatencyMillis.
         */
        synchronized void record(int durationMillis) {
            durationMillis = Math.min(durationMillis, this.latencyCounts.length - 1);
            this.latencyCounts[durationMillis]++;
            this.size++;
        }

        /**
         * Gets a value indicating the number of latencies recorded.
         */
        synchronized int count() {
            return this.size;
        }

        /**
         * Gets a value indicating the overall latency.
         */
        synchronized double sum() {
            long sum = 0;
            for (int i = 0; i < this.latencyCounts.length; i++) {
                sum += (long) this.latencyCounts[i] * i;
            }

            return sum;
        }

        /**
         * Gets a value indicating the average latency.
         */
        synchronized double average() {
            return sum() / this.size;
        }

        /**
         * Calculates the given percentiles.
         *
         * @param percentiles An array of sorted doubles indicating the desired percentiles.
         * @return An array of int[] with the same size as percentiles containing the desired results, in the same order
         * as percentiles.
         */
        synchronized int[] percentiles(double... percentiles) {
            int[] result = new int[percentiles.length];
            int itemIndex = 0; // Index for sought latency across all latencies.
            int currentLatency = 0; // Index within latencyCounts where itemIndex resides.
            long sumSoFar = 0; // Sum of all latencyCounts up to (excluding) currentLatency.
            for (int i = 0; i < percentiles.length; i++) {
                // Find the index for sought latency.
                double percentile = percentiles[i];
                Preconditions.checkState(percentile >= 0 && percentile <= 1, "Invalid percentile. Must be in interval [0,1].");
                int newItemIndex = (int) (percentiles[i] * (this.size-1));
                Preconditions.checkArgument(newItemIndex >= itemIndex, "percentiles not sorted at index %s", i);
                itemIndex = newItemIndex;

                // Find the latency we're looking for, starting from where we left off.
                while (sumSoFar + this.latencyCounts[currentLatency] <= itemIndex) {
                    sumSoFar += this.latencyCounts[currentLatency];
                    currentLatency++;
                }

                // Found it.
                result[i] = currentLatency;
            }

            return result;
        }
    }
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents the current State of the SelfTest.
 */
class TestState {
    //region Members

    static final OperationType[] SUMMARY_OPERATION_TYPES = {
            ProducerOperationType.APPEND,
            ProducerOperationType.CREATE_TRANSACTION,
            ProducerOperationType.MERGE_TRANSACTION,
            ProducerOperationType.SEAL,
            ConsumerOperationType.END_TO_END,
            ConsumerOperationType.CATCHUP_READ};
    private static final double NANOS_PER_SECOND = 1000 * 1000 * 1000.0;

    private final AtomicInteger generatedOperationCount;
    private final AtomicInteger successfulOperationCount;
    private final AtomicLong producedLength;
    private final AtomicLong verifiedTailLength;
    private final AtomicLong verifiedCatchupLength;
    private final AtomicLong verifiedStorageLength;
    private final ConcurrentHashMap<String, StreamInfo> allStreams;
    @GuardedBy("allStreamNames")
    private final ArrayList<String> allStreamNames;
    @GuardedBy("durations")
    private final HashMap<OperationType, List<Integer>> durations;
    private final AtomicLong startTimeNanos;
    private final AtomicLong lastAppendTime;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TestState class.
     */
    TestState() {
        this.generatedOperationCount = new AtomicInteger();
        this.successfulOperationCount = new AtomicInteger();
        this.allStreams = new ConcurrentHashMap<>();
        this.allStreamNames = new ArrayList<>();
        this.producedLength = new AtomicLong();
        this.verifiedTailLength = new AtomicLong();
        this.verifiedCatchupLength = new AtomicLong();
        this.verifiedStorageLength = new AtomicLong();
        this.durations = new HashMap<>();
        this.startTimeNanos = new AtomicLong();
        this.lastAppendTime = new AtomicLong();
        resetClock();

        synchronized (this.durations) {
            for (OperationType ot : SUMMARY_OPERATION_TYPES) {
                this.durations.put(ot, new ArrayList<>());
            }
        }
    }

    //endregion

    //region Properties

    /**
     * Gets the throughput since the last time resetClock() was called in Bytes/Second.
     */
    double getThroughput() {
        double durationSeconds = (this.lastAppendTime.get() - this.startTimeNanos.get()) / NANOS_PER_SECOND;
        return this.producedLength.get() / durationSeconds;
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
     * Sorts and returns a pointer to the list of durations for the given operation type.
     * For performance considerations, this method should only be called after the test is over.
     *
     * @param operationType The OperationType to get Durations for.
     * @return A pointer to the list of durations.
     */
    List<Integer> getSortedDurations(OperationType operationType) {
        synchronized (this.durations) {
            List<Integer> operationTypeDurations = this.durations.getOrDefault(operationType, null);
            if (operationTypeDurations != null) {
                operationTypeDurations.sort(Integer::compare);
            }

            return operationTypeDurations;
        }
    }

    //endregion

    //region Operations

    /**
     * Resets the clock used for estimating Producing Throughput.
     */
    void resetClock() {
        this.startTimeNanos.set(System.nanoTime());
        this.lastAppendTime.set(this.startTimeNanos.get());
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

        synchronized (this.durations) {
            List<Integer> operationTypeDurations = this.durations.getOrDefault(operationType, null);
            if (operationTypeDurations != null) {
                operationTypeDurations.add((int) elapsedMillis);
            }
        }
    }

    /**
     * Records the fact that an append with the given length has been processed.
     *
     * @param length The length of the append.
     */
    void recordAppend(int length) {
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
}

/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.selftest;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.concurrent.GuardedBy;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private final AtomicInteger generatedOperationCount;
    private final AtomicInteger successfulOperationCount;
    private final AtomicLong producedLength;
    private final AtomicLong verifiedTailLength;
    private final AtomicLong verifiedCatchupLength;
    private final AtomicLong verifiedStorageLength;
    private final ConcurrentHashMap<String, SegmentInfo> allSegments;
    @GuardedBy("allSegmentNames")
    private final ArrayList<String> allSegmentNames;
    @GuardedBy("durations")
    private final AbstractMap<OperationType, List<Integer>> durations;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TestState class.
     */
    TestState() {
        this.generatedOperationCount = new AtomicInteger();
        this.successfulOperationCount = new AtomicInteger();
        this.allSegments = new ConcurrentHashMap<>();
        this.allSegmentNames = new ArrayList<>();
        this.producedLength = new AtomicLong();
        this.verifiedTailLength = new AtomicLong();
        this.verifiedCatchupLength = new AtomicLong();
        this.verifiedStorageLength = new AtomicLong();
        this.durations = new HashMap<>();

        synchronized (this.durations) {
            for (OperationType ot : SUMMARY_OPERATION_TYPES) {
                this.durations.put(ot, new ArrayList<>());
            }
        }
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the total number of operations that were generated.
     */
    int getGeneratedOperationCount() {
        return this.generatedOperationCount.get();
    }

    /**
     * Gets a value indicating the total number of operations that were successfully completed..
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
    Collection<SegmentInfo> getAllSegments() {
        return this.allSegments.values();
    }

    /**
     * Gets an immutable collection containing the names of all the Segments and Transactions.
     * The resulting object is a copy of the internal state, and will not reflect future modifications to this object.
     */
    Collection<String> getAllSegmentNames() {
        synchronized (this.allSegmentNames) {
            return new ArrayList<>(this.allSegmentNames);
        }
    }

    /**
     * Gets an immutable collection containing the names of all the Transactions.
     * The resulting object is a copy of the internal state, and will not reflect future modifications to this object.
     */
    Collection<String> getTransactionNames() {
        return this.allSegments.values().stream()
                               .filter(SegmentInfo::isTransaction)
                               .map(s -> s.name)
                               .collect(Collectors.toList());
    }

    /**
     * Gets information about a particular Segment.
     *
     * @param name The name of the Segment to inquire about.
     */
    SegmentInfo getSegment(String name) {
        return this.allSegments.get(name);
    }

    /**
     * Gets a SegmentInfo that matches the given filter.
     *
     * @param filter The filter to use.
     * @return A SegmentInfo (in no particular order) that matches the given filter, or null if none match.
     */
    SegmentInfo getSegment(Function<SegmentInfo, Boolean> filter) {
        for (SegmentInfo si : this.allSegments.values()) {
            if (filter.apply(si)) {
                return si;
            }
        }

        return null;
    }

    /**
     * Gets the name of an arbitrary Segment/Transaction that is registered. Note that calling this method with the same
     * value for the argument may not necessarily produce the same result if done repeatedly.
     *
     * @param hint A hint to use to get the Segment name.
     */
    String getSegmentOrTransactionName(int hint) {
        synchronized (this.allSegmentNames) {
            return this.allSegmentNames.get(hint % this.allSegmentNames.size());
        }
    }

    /**
     * Gets the name of an arbitrary Segment (non-transaction) that is registered. Note that calling this method with the
     * same value for the argument may not necessarily produce the same result if done repeatedly.
     *
     * @param hint A hint to use to get the Segment name.
     */
    String getNonTransactionSegmentName(int hint) {
        synchronized (this.allSegmentNames) {
            int retry = 0;
            while (retry < this.allSegmentNames.size()) {
                String currentName = this.allSegmentNames.get((hint + retry) % this.allSegmentNames.size());
                SegmentInfo currentSegment = getSegment(currentName);
                if (currentSegment != null && !currentSegment.isTransaction()) {
                    return currentName;
                }

                retry++;
            }

            throw new IllegalStateException("Unable to find at least one Non-Transaction Segment out of " + this.allSegmentNames.size() + " total segments.");
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
     * Records the creation (but not the completion) of a new Operation.
     *
     * @return The Operation Id (or numeric index).
     */
    int newOperation() {
        return this.generatedOperationCount.incrementAndGet();
    }

    /**
     * Records the fact that an operation completed for the given Segment.
     *
     * @param segmentName The name of the Segment for which the operation completed.
     * @return The number of total successful operations so far.
     */
    int operationCompleted(String segmentName) {
        SegmentInfo si = this.getSegment(segmentName);
        if (si != null) {
            si.operationCompleted();
        }

        return this.successfulOperationCount.incrementAndGet();
    }

    /**
     * Records the fact that an operation failed for the given Segment.
     *
     * @param segmentName The name of the Segment for which the operation failed.
     */
    void operationFailed(String segmentName) {
        this.generatedOperationCount.decrementAndGet();
    }

    /**
     * Records the duration of a single operation of the given type.
     *
     * @param operationType The type of the operation.
     * @param duration The duration to record.
     */
    void recordDuration(OperationType operationType, Duration duration) {
        synchronized (this.durations) {
            List<Integer> operationTypeDurations = this.durations.getOrDefault(operationType, null);
            if (operationTypeDurations != null) {
                operationTypeDurations.add((int) duration.toMillis());
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
     * Records the fact that a new Segment (not a Transaction) was created.
     *
     * @param name The name of the Segment.
     */
    void recordNewSegmentName(String name) {
        synchronized (this.allSegmentNames) {
            Preconditions.checkArgument(!this.allSegments.containsKey(name), "Given segment already exists");
            this.allSegmentNames.add(name);
        }

        this.allSegments.put(name, new SegmentInfo(name, false));
    }

    /**
     * Records the fact that a new Transaction was created.
     *
     * @param name The name of the Transaction.
     */
    void recordNewTransaction(String name) {
        synchronized (this.allSegmentNames) {
            Preconditions.checkArgument(!this.allSegments.containsKey(name), "Given segment already exists");
            this.allSegmentNames.add(name);
        }

        this.allSegments.put(name, new SegmentInfo(name, true));
    }

    /**
     * Records the fact that a Segment or a Transaction was deleted.
     *
     * @param name The name of the Segment/Transaction.
     */
    void recordDeletedSegment(String name) {
        this.allSegments.remove(name);
        synchronized (this.allSegmentNames) {
            this.allSegmentNames.remove(name);
        }
    }

    //endregion

    //region SegmentInfo

    /**
     * Represents information about a particular Segment.
     */
    static class SegmentInfo {
        @Getter
        private final String name;
        @Getter
        private final boolean transaction;
        @Getter
        @Setter
        private boolean closed;
        private final AtomicInteger operationCount;

        private SegmentInfo(String name, boolean isTransaction) {
            this.name = name;
            this.transaction = isTransaction;
            this.operationCount = new AtomicInteger();
        }

        /**
         * Indicates that an operation completed successfully for this Segment.
         *
         * @return The number of completed operations so far.
         */
        int operationCompleted() {
            return this.operationCount.incrementAndGet();
        }

        /**
         * Gets the number of completed operations so far.
         */
        int getOperationCount() {
            return this.operationCount.get();
        }

        @Override
        public String toString() {
            return String.format("%s, OpCount = %s, Transaction = %s", this.name, this.operationCompleted(), this.transaction);
        }
    }

    //endregion
}

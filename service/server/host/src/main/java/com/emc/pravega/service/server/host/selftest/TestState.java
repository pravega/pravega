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

package com.emc.pravega.service.server.host.selftest;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the current State of the SelfTest.
 */
class TestState {
    //region Members

    private final AtomicInteger generatedOperationCount;
    private final AtomicInteger successfulOperationCount;
    private final ConcurrentHashMap<String, SegmentInfo> allSegments;
    private final ArrayList<String> allSegmentNames;

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
        return this.allSegments.values().stream().filter(s -> !s.isTransaction()).map(s -> s.name).collect(
                Collectors.toList());
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
     * Gets the name of an arbitrary Segment (non-transaction) that is registered. Note that calling this method with
     * the same value for the argument may not necessarily produce the same result if done repeatedly.
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

            throw new IllegalStateException(
                    "Unable to find at least one Non-Transaction Segment out of " + this.allSegmentNames.size() + " " +
                            "total segments.");
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
            return String.format("%s, OpCount = %s, Transaction = %s", this.name, this.operationCompleted(),
                    this.transaction);
        }
    }

    //endregion
}

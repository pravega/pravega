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

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a Data Source for all Producers of the SelfTest.
 */
class ProducerDataSource {
    //region Members

    private static final String LOG_ID = "DataSource";
    private final TestConfig config;
    private final TestState state;
    private final StoreAdapter store;
    private final HashMap<String, AppendContentGenerator> appendGenerators;
    private final Random appendSizeGenerator;
    private int lastCreatedTransaction;
    private final Object lock = new Object();

    //endregion

    //region Constructor

    ProducerDataSource(TestConfig config, TestState state, StoreAdapter store) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(state, "state");
        Preconditions.checkNotNull(store, "store");
        this.config = config;
        this.state = state;
        this.store = store;
        this.appendGenerators = new HashMap<>();
        this.appendSizeGenerator = new Random();
        this.lastCreatedTransaction = 0;
    }

    //endregion

    //region Operation Generation

    /**
     * Generates the next ProducerOperation to be executed. Based on the current TestState, this operation can be:
     * * CreateTransaction: If no CreateTransaction happened for a while (based on the TestConfig).
     * * MergeTransaction: If a transaction has exceeded its maximum number of appends (based on the TestConfig).
     * * Append: If any of the above are not met (this represents the majority of cases).
     *
     * @return The next ProducerOperation, or null if not such operation can be generated (we reached the end of the test).
     */
    ProducerOperation nextOperation() {
        int operationIndex = this.state.newOperation();
        if (operationIndex > this.config.getOperationCount()) {
            // Reached the end of the test. Nothing more to do.
            return null;
        }

        // Determine operation Type.
        ProducerOperation result = null;
        synchronized (this.lock) {
            if (operationIndex - this.lastCreatedTransaction >= this.config.getTransactionFrequency()) {
                // We have exceeded the number of operations since we last created a transaction.
                result = new ProducerOperation(OperationType.CreateTransaction, this.state.getNonTransactionSegmentName(operationIndex));
                this.lastCreatedTransaction = operationIndex;
            } else {
                // If any transaction has already exceeded the max number of appends, then merge it.
                for (TestState.SegmentInfo si : this.state.getAllSegments()) {
                    if (si != null
                            && si.isTransaction()
                            && !si.isMergeInProgress()
                            && si.getOperationCount() >= this.config.getMaxTransactionAppendCount()) {
                        result = new ProducerOperation(OperationType.MergeTransaction, si.getName());
                        si.setMergeInProgress(true);
                        break;
                    }
                }
            }
            // Otherwise append to random segment.
            if (result == null) {
                result = new ProducerOperation(OperationType.Append, this.state.getSegmentOrTransactionName(operationIndex));
            }
        }

        // Attach operation completion callbacks (both for success and for failure).
        result.setCompletionCallback(op -> {
            // Record the operation as completed in the State.
            this.state.operationCompleted(op.getTarget());

            // OperationType-specific updates.
            if (op.getType() == OperationType.MergeTransaction) {
                this.appendGenerators.remove(op.getTarget());
                this.state.recordDeletedSegment(op.getTarget());
            } else if (op.getType() == OperationType.CreateTransaction) {
                Object r = op.getResult();
                if (r == null || !(r instanceof String)) {
                    TestLogger.log(LOG_ID, "Operation %s completed but has result of wrong type.", op);
                    throw new IllegalArgumentException("Completed CreateTransaction operation has result of wrong type.");
                }

                postSegmentDeletion((String) r);
            }
        });

        result.setFailureCallback((op, ex) -> {
            // Record the operation as failed in the State.
            this.state.operationFailed(op.getTarget());

            // OperationType-specific cleanup.
            if (op.getType() == OperationType.MergeTransaction) {
                // Make sure we clear the 'MergeInProgress' flag if the operation failed.
                TestState.SegmentInfo si = this.state.getSegment(op.getTarget());
                if (si != null) {
                    si.setMergeInProgress(false);
                }
            }
        });

        return result;
    }

    /**
     * Generates a byte array representing the contents of an append (which follows a deterministic pattern).
     */
    byte[] generateAppendContent(String segmentName) {
        AppendContentGenerator generator = this.appendGenerators.getOrDefault(segmentName, null);
        Preconditions.checkArgument(generator != null, "No such segment was created using this DataSource");
        int maxSize = this.config.getMaxAppendSize();
        int minSize = this.config.getMinAppendSize();
        int size = maxSize;
        if (maxSize != minSize) {
            size = this.appendSizeGenerator.nextInt(maxSize - minSize) + minSize;
        }

        return generator.newAppend(size);
    }

    //endregion

    //region Segment Management

    /**
     * Creates all the segments required for this test.
     *
     * @return A CompletableFuture that will be completed when all segments are created.
     */
    CompletableFuture<Void> createSegments() {
        Preconditions.checkArgument(this.state.getAllSegments().size() == 0, "Cannot call createSegments more than once.");
        ArrayList<CompletableFuture<Void>> segmentFutures = new ArrayList<>();

        for (int i = 0; i < this.config.getSegmentCount(); i++) {
            final int segmentId = i;
            String name = String.format("Segment_%s", segmentId);
            segmentFutures.add(
                    this.store.createStreamSegment(name, this.config.getTimeout())
                              .thenRun(() -> {
                                  this.state.recordNewSegmentName(name);
                                  this.appendGenerators.put(name, new AppendContentGenerator(segmentId));
                              }));
        }

        return FutureHelpers.allOf(segmentFutures);
    }

    /**
     * Deletes all the segments required for this test.
     */
    CompletableFuture<Void> deleteAllSegments() {
        return deleteSegments(this.state.getTransactionNames())
                .thenCompose(v -> deleteSegments(this.state.getAllSegmentNames()));
    }

    private CompletableFuture<Void> deleteSegments(Collection<String> segmentNames) {
        ArrayList<CompletableFuture<Void>> deletionFutures = new ArrayList<>();
        for (String segmentName : segmentNames) {
            deletionFutures.add(deleteSegment(segmentName));
        }

        return FutureHelpers.allOf(deletionFutures);
    }

    private CompletableFuture<Void> deleteSegment(String name) {
        return this.store.deleteStreamSegment(name, this.config.getTimeout())
                         .thenRun(() -> postSegmentDeletion(name));
    }

    private void postSegmentDeletion(String name) {
        this.appendGenerators.remove(name);
        this.state.recordDeletedSegment(name);
    }

    //endregion
}

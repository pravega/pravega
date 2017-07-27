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
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import lombok.val;

/**
 * Represents a Data Source for all Producers of the SelfTest.
 */
class ProducerDataSource {
    //region Members

    private static final String LOG_ID = "DataSource";
    private final TestConfig config;
    private final TestState state;
    private final StoreAdapter store;
    private final ConcurrentHashMap<String, EventGenerator> eventGenerators;
    private final Random appendSizeGenerator;
    private final boolean sealSupported;
    private final boolean transactionsSupported;
    private final boolean appendSupported;
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
        this.eventGenerators = new ConcurrentHashMap<>();
        this.appendSizeGenerator = new Random();
        this.lastCreatedTransaction = 0;
        this.appendSupported = this.store.isFeatureSupported(StoreAdapter.Feature.Append);
        this.transactionsSupported = this.store.isFeatureSupported(StoreAdapter.Feature.Transaction);
        this.sealSupported = this.store.isFeatureSupported(StoreAdapter.Feature.Seal);
    }

    //endregion

    //region Operation Generation

    /**
     * Generates the next ProducerOperation to be executed. Based on the current TestState, this operation can be:
     * * CreateTransaction: If no CreateTransaction happened for a while (based on the TestConfig).
     * * MergeTransaction: If a transaction has exceeded its maximum number of appends (based on the TestConfig).
     * * Event: If any of the above are not met (this represents the majority of cases).
     *
     * @return The next ProducerOperation, or null if not such operation can be generated (we reached the end of the test).
     */
    ProducerOperation nextOperation() {
        int operationIndex = this.state.newOperation();
        ProducerOperation result = null;
        synchronized (this.lock) {
            if (operationIndex > this.config.getOperationCount()) {
                // We have reached the end of the test. We need to seal all segments before we stop.
                val si = this.state.getSegment(s -> !s.isClosed());
                if (si != null && this.sealSupported) {
                    // Seal the next segment that is on the list.
                    result = new ProducerOperation(ProducerOperationType.SEAL, si.getName());
                    si.setClosed(true);
                } else {
                    // We have reached the end of the test and no more segments are left for sealing.
                    return null;
                }
            } else if (this.transactionsSupported) {
                if (operationIndex - this.lastCreatedTransaction >= this.config.getTransactionFrequency()) {
                    // We have exceeded the number of operations since we last created a transaction.
                    result = new ProducerOperation(ProducerOperationType.CREATE_TRANSACTION, this.state.getNonTransactionSegmentName(operationIndex));
                    this.lastCreatedTransaction = operationIndex;
                } else {
                    // If any transaction has already exceeded the max number of appends, then merge it.
                    val si = this.state.getSegment(s -> s.isTransaction() && !s.isClosed() && s.getOperationCount() >= this.config.getMaxTransactionAppendCount());
                    if (si != null) {
                        result = new ProducerOperation(ProducerOperationType.MERGE_TRANSACTION, si.getName());
                        si.setClosed(true);
                    }
                }
            }

            // Otherwise append to random segment.
            if (result == null) {
                if (!this.appendSupported) {
                    return null;
                }

                result = new ProducerOperation(ProducerOperationType.APPEND, this.state.getSegmentOrTransactionName(operationIndex));
            }
        }

        // Attach operation completion callbacks (both for success and for failure).
        result.setCompletionCallback(this::operationCompletionCallback);
        result.setFailureCallback(this::operationFailureCallback);
        return result;
    }

    /**
     * Generates a new Event (which follows a deterministic pattern and has a routing key).
     */
    Event nextEvent(String streamName, int producerId) {
        EventGenerator generator = this.eventGenerators.getOrDefault(streamName, null);
        if (generator == null) {
            // If the argument is indeed correct, this segment was deleted between the time the operation got generated
            // and when this method was invoked.
            throw new UnknownStreamException(streamName);
        }

        int maxSize = this.config.getMaxAppendSize();
        int minSize = this.config.getMinAppendSize();
        int size = maxSize;
        if (maxSize != minSize) {
            size = this.appendSizeGenerator.nextInt(maxSize - minSize) + minSize;
        }

        return generator.newEvent(size, producerId);
    }

    /**
     * Determines if the Segment with given name is closed for appends or not.
     */
    boolean isClosed(String segmentName) {
        synchronized (this.lock) {
            val si = this.state.getSegment(segmentName);
            return si == null || si.isClosed();
        }
    }

    private void operationCompletionCallback(ProducerOperation op) {
        // Record the operation as completed in the State.
        this.state.operationCompleted(op.getTarget());
        this.state.recordDuration(op.getType(), op.getDuration());

        // OperationType-specific updates.
        if (op.getType() == ProducerOperationType.MERGE_TRANSACTION) {
            postStreamDeletion(op.getTarget());
        } else if (op.getType() == ProducerOperationType.CREATE_TRANSACTION) {
            Object r = op.getResult();
            if (r == null || !(r instanceof String)) {
                TestLogger.log(LOG_ID, "Operation %s completed but has result of wrong type.", op);
                throw new IllegalArgumentException("Completed CreateTransaction operation has result of wrong type.");
            }

            String transactionName = (String) r;
            this.state.recordNewTransaction(transactionName);
            int id = -this.lastCreatedTransaction;
            this.eventGenerators.put(transactionName, new EventGenerator(id, false));
        } else if (op.getType() == ProducerOperationType.APPEND) {
            this.state.recordAppend(op.getLength());
        }
    }

    private void operationFailureCallback(ProducerOperation op, Throwable ex) {
        // Record the operation as failed in the State.
        this.state.operationFailed(op.getTarget());

        // OperationType-specific cleanup.
        if (op.getType() == ProducerOperationType.MERGE_TRANSACTION || op.getType() == ProducerOperationType.SEAL) {
            // Make sure we clear the 'Closed' flag if the Seal/Merge operation failed.
            TestState.SegmentInfo si = this.state.getSegment(op.getTarget());
            if (si != null) {
                si.setClosed(false);
            }
        }
    }

    //endregion

    //region Segment Management

    /**
     * Creates all the Streams/Segments required for this test.
     *
     * @return A CompletableFuture that will be completed when all Streams/Segments are created.
     */
    CompletableFuture<Void> createStreams() {
        Preconditions.checkArgument(this.state.getAllSegments().size() == 0, "Cannot call createStreams more than once.");
        ArrayList<CompletableFuture<Void>> creationFutures = new ArrayList<>();

        TestLogger.log(LOG_ID, "Creating Streams.");
        StoreAdapter.Feature.Create.ensureSupported(this.store, "create streams");
        for (int i = 0; i < this.config.getStreamCount(); i++) {
            final int streamId = i;
            String name = String.format("Stream%s", streamId);
            creationFutures.add(this.store.createStream(name, this.config.getTimeout())
                    .thenRun(() -> {
                        this.state.recordNewSegmentName(name);
                        this.eventGenerators.put(name, new EventGenerator(streamId, true));
                    }));
        }

        return FutureHelpers.allOf(creationFutures);
    }

    /**
     * Deletes all the Streams/Segments required for this test.
     */
    CompletableFuture<Void> deleteAllStreams() {
        if (!this.store.isFeatureSupported(StoreAdapter.Feature.Delete)) {
            TestLogger.log(LOG_ID, "Not deleting Streams because the store adapter does not support it.");
            return CompletableFuture.completedFuture(null);
        }

        TestLogger.log(LOG_ID, "Deleting Streams.");
        return deleteSegments(this.state.getTransactionNames())
                .thenCompose(v -> deleteSegments(this.state.getAllSegmentNames()));
    }

    private CompletableFuture<Void> deleteSegments(Collection<String> segmentNames) {
        ArrayList<CompletableFuture<Void>> deletionFutures = new ArrayList<>();
        for (String segmentName : segmentNames) {
            deletionFutures.add(deleteStream(segmentName));
        }

        return FutureHelpers.allOf(deletionFutures);
    }

    private CompletableFuture<Void> deleteStream(String name) {
        return this.store.delete(name, this.config.getTimeout())
                         .exceptionally(ex -> {
                             ex = ExceptionHelpers.getRealException(ex);
                             if (!(ex instanceof StreamSegmentNotExistsException)) {
                                 throw new CompletionException(ex);
                             }

                             return null;
                         })
                .thenRun(() -> postStreamDeletion(name));
    }

    private void postStreamDeletion(String name) {
        this.eventGenerators.remove(name);
        this.state.recordDeletedSegment(name);
    }

    //endregion

    /**
     * Exception that is thrown whenever an unknown Stream/Segment name is passed to this data source (one that was not
     * created using it).
     */
    static class UnknownStreamException extends RuntimeException {
        private UnknownStreamException(String segmentName) {
            super(String.format("No such Stream/Segment was created using this DataSource: %s.", segmentName));
        }
    }
}

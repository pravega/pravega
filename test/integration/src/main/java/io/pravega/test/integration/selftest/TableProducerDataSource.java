/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.val;

public class TableProducerDataSource extends ProducerDataSource<TableUpdate> {
    private final ConcurrentHashMap<String, UpdateGenerator> updateGenerators;
    @GuardedBy("lock")
    private final Random random;
    private final Object lock = new Object();

    public TableProducerDataSource(TestConfig config, TestState state, StoreAdapter store) {
        super(config, state, store);
        Preconditions.checkArgument(store.isFeatureSupported(StoreAdapter.Feature.Tables), "StoreAdapter must support Tables.");
        this.updateGenerators = new ConcurrentHashMap<>();
        this.random = new Random();
    }

    @Override
    ProducerOperation<TableUpdate> nextOperation(int producerId) {
        int operationIndex = this.state.newOperation();
        ProducerOperation<TableUpdate> result;
        boolean removal;
        synchronized (this.lock) {
            if (operationIndex > this.config.getOperationCount()) {
                // End of the test.
                return null;
            }

            // Decide whether to remove.
            removal = decide(this.config.getTableRemovePercentage());

            // Choose operation type based on Test Configuration. For simplicity we don't mix conditional and unconditional
            // updates in this type of test.
            val opType = removal ? ProducerOperationType.TABLE_REMOVE : ProducerOperationType.TABLE_UPDATE;

            // Choose a Table to update.
            val si = this.state.getStreamOrTransaction(operationIndex);
            si.operationStarted();
            result = new ProducerOperation<>(opType, si.getName());
        }

        // Generate the update and link callbacks.
        result.setUpdate(generateUpdate(result.getTarget(), removal, producerId));
        result.setCompletionCallback(this::operationCompletionCallback);
        result.setFailureCallback(this::operationFailureCallback);
        return result;
    }

    @Override
    CompletableFuture<Void> createAll() {
        Preconditions.checkArgument(this.state.getAllStreams().size() == 0, "Cannot call createAll more than once.");
        ArrayList<CompletableFuture<Void>> creationFutures = new ArrayList<>();

        TestLogger.log(LOG_ID, "Creating Tables.");
        for (int i = 0; i < this.config.getStreamCount(); i++) {
            // Table Names are of the form: Table<TestId><TableId> - to avoid clashes between different tests.
            final int tableId = i;
            String name = String.format("Table%s%s", this.config.getTestId(), tableId);
            creationFutures.add(this.store.createTable(name, this.config.getTimeout())
                    .thenRun(() -> {
                        this.state.recordNewStreamName(name);
                        this.updateGenerators.put(name, new UpdateGenerator(tableId));
                    }));
        }

        return Futures.allOf(creationFutures);
    }

    @Override
    CompletableFuture<Void> deleteAll() {
        TestLogger.log(LOG_ID, "Deleting Tables.");
        ArrayList<CompletableFuture<Void>> deletionFutures = new ArrayList<>();
        for (String segmentName : this.state.getAllStreamNames()) {
            try {
                deletionFutures.add(deleteStream(segmentName));
            } catch (Throwable ex) {
                if (Exceptions.mustRethrow(ex) || !(Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException)) {
                    throw ex;
                }
            }
        }

        return Futures.allOf(deletionFutures);
    }

    private CompletableFuture<Void> deleteStream(String name) {
        return Futures.exceptionallyExpecting(this.store.deleteStream(name, this.config.getTimeout()),
                ex -> ex instanceof StreamSegmentNotExistsException,
                null)
                .thenRun(() -> postStreamDeletion(name));
    }

    private void postStreamDeletion(String name) {
        this.updateGenerators.remove(name);
        this.state.recordDeletedStream(name);
    }

    private TableUpdate generateUpdate(String target, boolean removal, int producerId) {
        // Choose whether to pick existing key or not (using configuration).
        // If conditional, pick a key that has a version defined. If none available, create new key.

        // Keys are generated based on a sequence number. Every new key will get a new sequence number.
        // Values are generated as a function of their key, producer and current timestamp.
        // TODO
        throw new UnsupportedOperationException("implement me");
    }

    private void operationCompletionCallback(ProducerOperation<TableUpdate> op) {
        this.state.operationCompleted(op.getTarget());
        this.state.recordDuration(op.getType(), op.getElapsedMillis());
        this.state.recordAppend(op.getLength());
        if (op.getResult() != null) {
            Long version = (Long) op.getResult();
            // TODO: record latest version.
        }
    }

    private void operationFailureCallback(ProducerOperation<?> op, Throwable ex) {
        this.state.operationFailed(op.getTarget());
    }

    @GuardedBy("lock")
    private boolean decide(int probabilityPercentage) {
        return random.nextInt(101) <= probabilityPercentage;
    }

    @RequiredArgsConstructor
    private class UpdateGenerator {
        private final int tableId;

    }
}

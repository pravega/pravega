/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.FutureHelpers;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;

/**
 * Represents an Operation Producer for the Self Tester.
 */
class Producer extends Actor {
    //region Members

    private final String logId;
    private final AtomicInteger iterationCount;
    private final AtomicBoolean canContinue;
    private final UUID clientId = UUID.randomUUID();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Producer class.
     *
     * @param id         The Id of the producer.
     * @param config     Test Configuration.
     * @param dataSource DataSource for the Producer.
     * @param store      A StoreAdapter to execute operations on.
     * @param executor   An Executor to use for async operations.
     */
    Producer(String id, TestConfig config, ProducerDataSource dataSource, StoreAdapter store, ScheduledExecutorService executor) {
        super(config, dataSource, store, executor);

        this.logId = String.format("Producer[%s]", id);
        this.iterationCount = new AtomicInteger();
        this.canContinue = new AtomicBoolean(true);
    }

    //endregion

    //region Actor Implementation

    @Override
    protected CompletableFuture<Void> run() {
        this.canContinue.set(true);
        return FutureHelpers.loop(
                this::canLoop,
                this::runOneIteration,
                this.executorService);
    }

    @Override
    protected String getLogId() {
        return this.logId;
    }

    //endregion

    //region Producer Implementation

    /**
     * Executes one iteration of the Producer.
     * 1. Requests a new ProducerOperation from the DataSource.
     * 2. Executes it.
     * 3. Completes the ProducerOperation with either success or failure based on the outcome step #2.
     */
    private CompletableFuture<Void> runOneIteration() {
        ProducerOperation op = this.dataSource.nextOperation();
        if (op == null) {
            // Nothing more to do.
            this.canContinue.set(false);
            return CompletableFuture.completedFuture(null);
        }

        this.iterationCount.incrementAndGet();
        final Timer timer = new Timer();
        CompletableFuture<Void> result;
        try {
            result = executeOperation(op);
        } catch (Throwable ex) {
            // Catch and handle sync errors.
            op.completed(timer.getElapsed());
            if (handleOperationError(ex, op)) {
                // Exception handled; skip this iteration since there's nothing more we can do.
                return CompletableFuture.completedFuture(null);
            } else {
                throw ex;
            }
        }

        return result.whenCompleteAsync((r, ex) -> {
            op.completed(timer.getElapsed());

            // Catch and handle async errors.
            if (ex != null && !handleOperationError(ex, op)) {
                throw new CompletionException(ex);
            }
        }, this.executorService);
    }

    private boolean handleOperationError(Throwable ex, ProducerOperation op) {
        // Log & throw every exception.
        ex = ExceptionHelpers.getRealException(ex);
        if (ex instanceof ProducerDataSource.UnknownSegmentException) {
            // This is OK: some other producer deleted the segment after we requested the operation and until we
            // tried to apply it.
            return true;
        }

        TestLogger.log(getLogId(), "Iteration %s FAILED with %s.", this.iterationCount, ex);
        this.canContinue.set(false);
        op.failed(ex);
        return false;
    }

    /**
     * Determines whether the Producer can loop to another iteration.
     */
    private boolean canLoop() {
        return isRunning() && this.canContinue.get();
    }

    /**
     * Executes the given operation.
     */
    private CompletableFuture<Void> executeOperation(ProducerOperation operation) {
        if (operation.getType() == ProducerOperationType.CREATE_TRANSACTION) {
            // Create the Transaction, then record it's name in the operation's result.
            StoreAdapter.Feature.Transaction.ensureSupported(this.store, "create transaction");
            return this.store.createTransaction(operation.getTarget(), this.config.getTimeout())
                             .thenAccept(operation::setResult);
        } else if (operation.getType() == ProducerOperationType.MERGE_TRANSACTION) {
            // Merge the Transaction.
            StoreAdapter.Feature.Transaction.ensureSupported(this.store, "merge transaction");
            return this.store.mergeTransaction(operation.getTarget(), this.config.getTimeout());
        } else if (operation.getType() == ProducerOperationType.APPEND) {
            // Generate some random data, then append it.
            StoreAdapter.Feature.Append.ensureSupported(this.store, "append");
            Event event = this.dataSource.nextEvent(operation.getTarget());
            operation.setLength(event.getSerialization().getLength());
            return this.store.append(operation.getTarget(), event, this.config.getTimeout())
                    .exceptionally(ex -> attemptReconcile(ex, operation));
        } else if (operation.getType() == ProducerOperationType.SEAL) {
            // Seal the target.
            StoreAdapter.Feature.Seal.ensureSupported(this.store, "seal");
            return this.store.seal(operation.getTarget(), this.config.getTimeout());
        } else {
            throw new IllegalArgumentException("Unsupported Operation Type: " + operation.getType());
        }
    }

    @SneakyThrows
    private Void attemptReconcile(Throwable ex, ProducerOperation operation) {
        ex = ExceptionHelpers.getRealException(ex);
        if (this.dataSource.isClosed(operation.getTarget())) {
            return null;
        } else {
            throw ex;
        }
    }

    //endregion
}

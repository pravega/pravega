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

import io.pravega.common.AbstractTimer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.lang.NotImplementedException;

/**
 * Represents an Operation Producer for the Self Tester.
 */
class Producer extends Actor {
    //region Members

    private static final Supplier<Long> TIME_PROVIDER = System::nanoTime;
    @Getter
    private final String logId;
    private final AtomicInteger iterationCount;
    private final AtomicBoolean canContinue;
    private final int id;
    private final ProducerDataSource dataSource;

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
    Producer(int id, TestConfig config, ProducerDataSource dataSource, StoreAdapter store, ScheduledExecutorService executor) {
        super(config, store, executor);

        this.id = id;
        this.logId = String.format("Producer[%s]", id);
        this.iterationCount = new AtomicInteger();
        this.dataSource = dataSource;
        this.canContinue = new AtomicBoolean(true);
    }

    //endregion

    //region Actor Implementation

    @Override
    protected CompletableFuture<Void> run() {
        this.canContinue.set(true);
        return Futures.loop(
                this::canLoop,
                this::runOneIteration,
                this.executorService);
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
        this.iterationCount.incrementAndGet();

        val futures = new ArrayList<CompletableFuture<Void>>();
        for (int i = 0; i < this.config.getProducerParallelism(); i++) {
            ProducerOperation op = this.dataSource.nextOperation();
            if (op == null) {
                // Nothing more to do.
                this.canContinue.set(false);
                break;
            }

            CompletableFuture<Void> result;
            try {
                CompletableFuture<Void> waitOn = op.getWaitOn();
                if (waitOn != null) {
                    result = waitOn
                            .exceptionally(ex -> null)
                            .thenComposeAsync(v -> executeOperation(op), this.executorService);
                } else {
                    result = executeOperation(op);
                }
            } catch (Throwable ex) {
                // Catch and handle sync errors.
                op.completed(-1);
                if (handleOperationError(ex, op)) {
                    // Exception handled; skip this iteration since there's nothing more we can do.
                    continue;
                } else {
                    result = Futures.failedFuture(ex);
                }
            }

            futures.add(result.exceptionally(ex -> {
                // Catch and handle async errors.
                if (handleOperationError(ex, op)) {
                    return null;
                }

                throw new CompletionException(ex);
            }));
        }

        return Futures.allOf(futures);
    }


    private boolean handleOperationError(Throwable ex, ProducerOperation op) {
        // Log & throw every exception.
        ex = Exceptions.unwrap(ex);
        if (ex instanceof ProducerDataSource.UnknownStreamException) {
            // This is OK: some other producer deleted the segment after we requested the operation and until we
            // tried to apply it.
            return true;
        } else if (ex instanceof NotImplementedException || ex instanceof UnsupportedOperationException) {
            // Operation is not supported. No need to fail the test; just log it.
            TestLogger.log(getLogId(), "Operation '%s' cannot be executed on '%s' because it is not supported.", op.getType(), op.getTarget());
            return true;
        }

        TestLogger.log(getLogId(), "Iteration %s FAILED for Op '%s' with %s.", this.iterationCount, op, ex);
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
        CompletableFuture<Void> result;
        final AtomicLong startTime = new AtomicLong(TIME_PROVIDER.get());
        if (operation.getType() == ProducerOperationType.CREATE_TRANSACTION) {
            // Create the Transaction, then record it's name in the operation's result.
            StoreAdapter.Feature.Transaction.ensureSupported(this.store, "create transaction");
            startTime.set(TIME_PROVIDER.get());
            result = this.store.createTransaction(operation.getTarget(), this.config.getTimeout())
                               .thenAccept(operation::setResult);
        } else if (operation.getType() == ProducerOperationType.MERGE_TRANSACTION) {
            // Merge the Transaction.
            StoreAdapter.Feature.Transaction.ensureSupported(this.store, "merge transaction");
            startTime.set(TIME_PROVIDER.get());
            result = this.store.mergeTransaction(operation.getTarget(), this.config.getTimeout());
        } else if (operation.getType() == ProducerOperationType.ABORT_TRANSACTION) {
            // Abort the Transaction.
            StoreAdapter.Feature.Transaction.ensureSupported(this.store, "abort transaction");
            startTime.set(TIME_PROVIDER.get());
            result = this.store.abortTransaction(operation.getTarget(), this.config.getTimeout());
        } else if (operation.getType() == ProducerOperationType.APPEND) {
            // Generate some random data, then append it.
            StoreAdapter.Feature.Append.ensureSupported(this.store, "append");
            Event event = this.dataSource.nextEvent(operation.getTarget(), this.id);
            operation.setLength(event.getSerialization().getLength());
            startTime.set(TIME_PROVIDER.get());
            result = this.store.append(operation.getTarget(), event, this.config.getTimeout());
        } else if (operation.getType() == ProducerOperationType.SEAL) {
            // Seal the target.
            StoreAdapter.Feature.SealStream.ensureSupported(this.store, "seal");
            startTime.set(TIME_PROVIDER.get());
            result = this.store.sealStream(operation.getTarget(), this.config.getTimeout());
        } else {
            throw new IllegalArgumentException("Unsupported Operation Type: " + operation.getType());
        }

        return result
                .exceptionally(ex -> attemptReconcile(ex, operation))
                .thenRun(() -> operation.completed((TIME_PROVIDER.get() - startTime.get()) / AbstractTimer.NANOS_TO_MILLIS));
    }

    @SneakyThrows
    private Void attemptReconcile(Throwable ex, ProducerOperation operation) {
        ex = Exceptions.unwrap(ex);
        if (this.dataSource.isClosed(operation.getTarget())) {
            return null;
        } else {
            throw ex;
        }
    }

    //endregion
}

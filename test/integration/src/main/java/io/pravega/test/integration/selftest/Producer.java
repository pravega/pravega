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
import com.google.common.collect.ImmutableMap;
import io.pravega.common.AbstractTimer;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.ArrayList;
import java.util.Map;
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

/**
 * Represents an Operation Producer for the Self Tester. The Producer is used by both Streaming and Table tests.
 */
class Producer<T extends ProducerUpdate> extends Actor {
    //region Members

    private static final Supplier<Long> TIME_PROVIDER = System::nanoTime;
    @Getter
    private final String logId;
    private final AtomicInteger iterationCount;
    private final AtomicBoolean canContinue;
    private final int id;
    private final ProducerDataSource<T> dataSource;
    private final Map<ProducerOperationType, OperationExecutor> executors;

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
    Producer(int id, TestConfig config, ProducerDataSource<T> dataSource, StoreAdapter store, ScheduledExecutorService executor) {
        super(config, store, executor);

        this.id = id;
        this.logId = String.format("Producer[%s]", id);
        this.iterationCount = new AtomicInteger();
        this.dataSource = dataSource;
        this.canContinue = new AtomicBoolean(true);
        this.executors = loadExecutors();
    }

    @SneakyThrows
    private Map<ProducerOperationType, OperationExecutor> loadExecutors() {
        return ImmutableMap.<ProducerOperationType, OperationExecutor>builder()
                .put(ProducerOperationType.CREATE_STREAM_TRANSACTION, new CreateTransactionExecutor())
                .put(ProducerOperationType.ABORT_STREAM_TRANSACTION, new AbortTransactionExecutor())
                .put(ProducerOperationType.MERGE_STREAM_TRANSACTION, new MergeTransactionExecutor())
                .put(ProducerOperationType.STREAM_SEAL, new StreamSealExecutor())
                .put(ProducerOperationType.STREAM_APPEND, new StreamAppendExecutor())
                .put(ProducerOperationType.TABLE_UPDATE, new TableUpdateExecutor())
                .put(ProducerOperationType.TABLE_REMOVE, new TableRemoveExecutor())
                .build();
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
            ProducerOperation<T> op = this.dataSource.nextOperation(this.id);
            if (op == null) {
                // Nothing more to do.
                this.canContinue.set(false);
                break;
            }

            CompletableFuture<Void> result;
            try {
                CompletableFuture<Void> waitOn = op.getWaitOn();
                if (waitOn != null) {
                    result = Futures.exceptionallyExpecting(waitOn, ex -> true, null)
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
        if (ex instanceof ProducerDataSource.UnknownTargetException) {
            // This is OK: some other producer deleted the segment after we requested the operation and until we
            // tried to apply it.
            return true;
        } else if (ex instanceof UnsupportedOperationException) {
            // Operation is not supported. No need to fail the test; just log it.
            TestLogger.log(getLogId(), "Operation '%s' cannot be executed on '%s' because it is not supported.", op.getType(), op.getTarget());
            return true;
        }

        TestLogger.log(getLogId(), "Iteration %s FAILED for Op '%s' with %s.", this.iterationCount, op, ex);
        ex.printStackTrace(System.out);
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
    private CompletableFuture<Void> executeOperation(ProducerOperation<T> operation) {
        OperationExecutor e = this.executors.get(operation.getType());
        Preconditions.checkArgument(e != null, "Unsupported Operation Type: %s.", operation.getType());
        return e.execute(operation);
    }

    //endregion

    //region Operation Executor

    /**
     * Defines an executor for a Producer Operation.
     */
    private abstract class OperationExecutor {
        /**
         * Executes the given {@link ProducerOperation}.
         *
         * @param operation The {@link ProducerOperation} to execute.
         * @return A CompletableFuture that will be completed when the operation execution completes.
         */
        CompletableFuture<Void> execute(ProducerOperation<T> operation) {
            StoreAdapter.Feature requiredFeature = getFeature();
            if (requiredFeature != null) {
                requiredFeature.ensureSupported(Producer.this.store, getOperationName());
            }

            final AtomicLong startTime = new AtomicLong(TIME_PROVIDER.get());
            return executeInternal(operation)
                    .exceptionally(ex -> attemptReconcile(ex, operation))
                    .thenRun(() -> operation.completed((TIME_PROVIDER.get() - startTime.get()) / AbstractTimer.NANOS_TO_MILLIS));
        }

        /**
         * Gets the required {@link StoreAdapter.Feature} that this {@link OperationExecutor} requires in order to execute.
         *
         * @return The result.
         */
        abstract StoreAdapter.Feature getFeature();

        /**
         * Gets a log-friendly name for this {@link OperationExecutor}.
         *
         * @return The result.
         */
        abstract String getOperationName();

        /**
         * Internally executes this operation. All precondition checks have been performed prior to invoking this.
         *
         * @param operation The {@link ProducerOperation} to execute.
         * @return A CompletableFuture that will be completed when the operation execution completes.
         */
        protected abstract CompletableFuture<Void> executeInternal(ProducerOperation<T> operation);

        @SneakyThrows
        private Void attemptReconcile(Throwable ex, ProducerOperation operation) {
            if (Producer.this.dataSource.isClosed(operation.getTarget())) {
                return null;
            } else {
                throw Exceptions.unwrap(ex);
            }
        }
    }

    /**
     * Creates a new Stream Transaction.
     */
    private class CreateTransactionExecutor extends OperationExecutor {
        @Override
        StoreAdapter.Feature getFeature() {
            return StoreAdapter.Feature.Transaction;
        }

        @Override
        String getOperationName() {
            return "create transaction";
        }

        @Override
        protected CompletableFuture<Void> executeInternal(ProducerOperation<T> operation) {
            return Producer.this.store.createTransaction(operation.getTarget(), Producer.this.config.getTimeout())
                    .thenAccept(operation::setResult);
        }
    }

    /**
     * Merges an existing Stream Transaction.
     */
    private class MergeTransactionExecutor extends OperationExecutor {
        @Override
        StoreAdapter.Feature getFeature() {
            return StoreAdapter.Feature.Transaction;
        }

        @Override
        String getOperationName() {
            return "merge transaction";
        }


        @Override
        protected CompletableFuture<Void> executeInternal(ProducerOperation<T> operation) {
            return Producer.this.store.mergeTransaction(operation.getTarget(), Producer.this.config.getTimeout());
        }
    }

    /**
     * Aborts an existing Stream Transaction.
     */
    private class AbortTransactionExecutor extends OperationExecutor {
        @Override
        StoreAdapter.Feature getFeature() {
            return StoreAdapter.Feature.Transaction;
        }

        @Override
        String getOperationName() {
            return "abort transaction";
        }

        @Override
        protected CompletableFuture<Void> executeInternal(ProducerOperation<T> operation) {
            return Producer.this.store.mergeTransaction(operation.getTarget(), Producer.this.config.getTimeout());
        }
    }

    /**
     * Appends a new Event to an existing Stream.
     */
    private class StreamAppendExecutor extends OperationExecutor {
        @Override
        StoreAdapter.Feature getFeature() {
            return StoreAdapter.Feature.Append;
        }

        @Override
        String getOperationName() {
            return "append";
        }

        @Override
        protected CompletableFuture<Void> executeInternal(ProducerOperation<T> operation) {
            Event event = (Event) operation.getUpdate();
            operation.setLength(event.getSerialization().getLength());
            return Producer.this.store.append(operation.getTarget(), event, Producer.this.config.getTimeout());
        }
    }

    /**
     * Seals an existing Stream.
     */
    private class StreamSealExecutor extends OperationExecutor {
        @Override
        StoreAdapter.Feature getFeature() {
            return StoreAdapter.Feature.SealStream;
        }

        @Override
        String getOperationName() {
            return "seal";
        }

        @Override
        protected CompletableFuture<Void> executeInternal(ProducerOperation<T> operation) {
            return Producer.this.store.sealStream(operation.getTarget(), Producer.this.config.getTimeout());
        }
    }

    private class TableUpdateExecutor extends OperationExecutor {
        @Override
        StoreAdapter.Feature getFeature() {
            return StoreAdapter.Feature.Tables;
        }

        @Override
        String getOperationName() {
            return "update";
        }

        @Override
        protected CompletableFuture<Void> executeInternal(ProducerOperation<T> operation) {
            TableUpdate update = (TableUpdate) operation.getUpdate();
            return Producer.this.store.updateTableEntry(operation.getTarget(), update.getKey(), update.getValue(), update.getVersion(), Producer.this.config.getTimeout())
                    .thenAccept(operation::setResult);
        }
    }

    private class TableRemoveExecutor extends OperationExecutor {
        @Override
        StoreAdapter.Feature getFeature() {
            return StoreAdapter.Feature.Tables;
        }

        @Override
        String getOperationName() {
            return "remove";
        }

        @Override
        protected CompletableFuture<Void> executeInternal(ProducerOperation<T> operation) {
            TableUpdate update = (TableUpdate) operation.getUpdate();
            return Producer.this.store.removeTableEntry(operation.getTarget(), update.getKey(), update.getVersion(), Producer.this.config.getTimeout());
        }
    }

    //endregion
}

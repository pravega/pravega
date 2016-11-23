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

import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentMergedException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.server.ExceptionHelpers;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    Producer(String id, TestConfig config, ProducerDataSource dataSource, StoreAdapter store,
             ScheduledExecutorService executor) {
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
        return FutureHelpers.loop(this::canLoop, this::runOneIteration, this.executorService);
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
        return executeOperation(op).exceptionally(ex -> {
            // Log & throw every exception.
            ex = ExceptionHelpers.getRealException(ex);
            TestLogger.log(getLogId(), "Iteration %s FAILED with %s.", this.iterationCount, ex);
            this.canContinue.set(false);
            op.failed(ex);
            throw new CompletionException(ex);
        }).thenRun(op::completed);
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
        //TestLogger.log(getLogId(), "Executing %s.", operation);
        TimeoutTimer timer = new TimeoutTimer(this.config.getTimeout());
        switch (operation.getType()) {
            case CreateTransaction:
                // Create the Transaction, then record it's name in the operation's result.
                return this.store.createTransaction(operation.getTarget(), timer.getRemaining()).thenAccept(
                        operation::setResult);
            case MergeTransaction:
                // Seal & Merge the Transaction.
                return this.store.sealStreamSegment(operation.getTarget(), timer.getRemaining()).thenCompose(
                        v -> this.store.mergeTransaction(operation.getTarget(), timer.getRemaining()));
            case Append:
                // Generate some random data, then append it.
                byte[] appendContent = this.dataSource.generateAppendContent(operation.getTarget());
                AppendContext context = new AppendContext(this.clientId, this.iterationCount.get());
                return this.store.append(operation.getTarget(), appendContent, context,
                        timer.getRemaining()).exceptionally(ex -> attemptReconcile(ex, operation, timer));
            case Seal:
                // Seal the segment.
                return this.store.sealStreamSegment(operation.getTarget(), this.config.getTimeout());
            default:
                throw new IllegalArgumentException("Unsupported Operation Type: " + operation.getType());
        }
    }

    private Void attemptReconcile(Throwable ex, ProducerOperation operation, TimeoutTimer timer) {
        ex = ExceptionHelpers.getRealException(ex);
        boolean reconciled = false;
        if (isPossibleEndOfSegment(ex)) {
            reconciled = this.dataSource.isClosed(operation.getTarget());

            if (!reconciled) {
                // If we get a Sealed/Merged/NotExists exception, verify that the segment really is in that state.
                try {
                    SegmentProperties sp = this.store.getStreamSegmentInfo(operation.getTarget(),
                            timer.getRemaining()).get(timer.getRemaining().toMillis(), TimeUnit.MILLISECONDS);
                    reconciled = sp.isSealed() || sp.isDeleted();
                } catch (Throwable ex2) {
                    ex2 = ExceptionHelpers.getRealException(ex2);
                    reconciled = isPossibleEndOfSegment(ex2);
                }
            }
        }

        if (reconciled) {
            return null;
        } else {
            throw new CompletionException(ex);
        }
    }

    private boolean isPossibleEndOfSegment(Throwable ex) {
        return ex instanceof StreamSegmentSealedException || ex instanceof StreamSegmentNotExistsException || ex
                instanceof StreamSegmentMergedException;
    }

    //endregion
}

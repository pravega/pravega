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
import com.emc.pravega.service.server.ExceptionHelpers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents an Operation Producer for the Self Tester.
 */
public class Producer extends Actor {
    private final String id;
    private final String logId;
    private final AtomicInteger iterationCount;
    private final AtomicBoolean canContinue;

    Producer(String id, TestConfig config, ProducerDataSource dataSource, StoreAdapter store, ScheduledExecutorService executor) {
        super(config, dataSource, store, executor);

        this.id = id;
        this.logId = String.format("Producer[%s]", this.id);
        this.iterationCount = new AtomicInteger();
        this.canContinue = new AtomicBoolean(true);
    }

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

    private CompletableFuture<Void> runOneIteration() {
        int iterationId = iterationCount.incrementAndGet();
        ProducerOperation op = getNextOperation();
        if (op == null) {
            // Nothing more to do.
            this.canContinue.set(false);
            return CompletableFuture.completedFuture(null);
        }

        return executeOperation(op)
                .whenComplete((r, ex) -> {
                    if (ex != null) {
                        ex = ExceptionHelpers.getRealException(ex);
                        TestLogger.log(getLogId(), "Iteration %d FAILED with %s.", iterationId, ex);
                        this.canContinue.set(false);
                        this.dataSource.getState().operationFailed();
                        throw new CompletionException(ex);
                    } else {
                        this.dataSource.getState().operationCompleted();
                        TestLogger.log(getLogId(), "Iteration %d Finished.", iterationId);
                    }
                });
    }

    private boolean canLoop() {
        return isRunning() && this.canContinue.get();
    }

    private ProducerOperation getNextOperation() {
        return this.dataSource.nextOperation();
    }

    private CompletableFuture<Void> executeOperation(ProducerOperation operation) {
        TestLogger.log(getLogId(), "Executing %s.", operation);
        switch (operation.getType()) {
            case CreateTransaction:
                // Create the Transaction, then record it's name in the State object.
                return this.dataSource.createTransaction(operation.getTarget());
            case MergeTransaction:
                // Merge the Transaction, then record it as deleted in the State object.
                return this.store
                        .mergeTransaction(operation.getTarget(), this.config.getTimeout())
                        .thenRun(() -> this.dataSource.transactionMerged(operation.getTarget()));
            case Append:
                // Generate some random data, then append it.
                byte[] appendContent = this.dataSource.generateAppendContent(operation.getTarget());
                return this.store.append(operation.getTarget(), appendContent, this.config.getTimeout());
            default:
                throw new IllegalArgumentException("Unsupported Operation Type: " + operation.getType());
        }
    }

    //endregion
}

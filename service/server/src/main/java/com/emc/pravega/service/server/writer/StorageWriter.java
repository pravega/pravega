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

package com.emc.pravega.service.server.writer;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.contracts.RuntimeStreamingException;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.logs.OperationLog;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Storage Writer. Applies operations from Operation Log to Storage.
 */
@Slf4j
class StorageWriter extends AbstractService implements Writer {
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private final String traceObjectId;
    private final WriterConfig config;
    private final UpdateableContainerMetadata containerMetadata;
    private final OperationLog operationLog;
    private final Storage storage;
    private final Cache cache;
    private final Executor executor;
    private final HashMap<Long, SegmentAggregator> aggregators;
    private final AtomicReference<Throwable> stopException = new AtomicReference<>();
    private final State state;
    private CompletableFuture<Void> currentIteration;
    private boolean closed;

    StorageWriter(WriterConfig config, UpdateableContainerMetadata containerMetadata, OperationLog operationLog, Storage storage, Cache cache, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(containerMetadata, "containerMetadata");
        Preconditions.checkNotNull(operationLog, "operationLog");
        Preconditions.checkNotNull(storage, "storage");
        Preconditions.checkNotNull(cache, "cache");
        Preconditions.checkNotNull(executor, "executor");

        this.traceObjectId = String.format("StorageWriter[%d]", containerMetadata.getContainerId());
        this.config = config;
        this.containerMetadata = containerMetadata;
        this.operationLog = operationLog;
        this.storage = storage;
        this.cache = cache;
        this.executor = executor;
        this.aggregators = new HashMap<>();
        this.state = new State();
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            stopAsync();
            ServiceShutdownListener.awaitShutdown(this, false);

            log.info("{} Closed.", this.traceObjectId);
            this.closed = true;
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        Exceptions.checkNotClosed(this.closed, this);
        this.executor.execute(() -> {
            runOneIteration();
            notifyStarted();
            log.info("{} Started.", this.traceObjectId);
            System.out.println(this.traceObjectId + " Started");
        });
    }

    @Override
    protected void doStop() {
        Exceptions.checkNotClosed(this.closed, this);
        this.executor.execute(() -> {
            Throwable cause = this.stopException.get();
            CompletableFuture<Void> lastIteration = this.currentIteration;
            if (lastIteration != null) {
                try {
                    lastIteration.get(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    if (cause != null) {
                        cause = ex;
                    }
                }
            }

            if (cause == null) {
                // Normal shutdown.
                notifyStopped();
            } else {
                // Shutdown caused by some failure.
                notifyFailed(cause);
            }

            log.info("{} Stopped.", this.traceObjectId);
            System.out.println(this.traceObjectId + " Stopped.");
        });
    }

    //endregion
    private void runOneIteration() {
        assert this.currentIteration == null : "Another iteration is in progress";
        this.currentIteration = readData()
                .thenAcceptAsync(this::aggregate, this.executor)
                .thenApplyAsync(v -> this.flush(), this.executor)
                .thenAcceptAsync(this::acknowledge, this.executor)
                .whenCompleteAsync(this::endOfIteration, this.executor);
    }

    private void endOfIteration(Void result, Throwable ex) {
        this.currentIteration = null;
        if (ex != null) {
            System.err.println("Error: " + ex);
            log.error("{} Error. {}", this.traceObjectId, ex);
            if (isCriticalError(ex)) {
                this.stopException.set(ex);
                stopAsync();
                return;
            }
        }

        if (isRunning()) {
            runOneIteration();
        }
    }

    private CompletableFuture<OperationReadResult> readData() {
        // Calculate the timeout for the operation.
        Duration readTimeout = getReadTimeout();

        // Initiate a Read from the OperationLog, then load it up into a concrete object, and return that.
        return this.operationLog
                .read(this.state.getLastReadSequenceNumber(), this.config.getMaxItemsToReadAtOnce(), readTimeout)
                .thenApplyAsync(this::processReadResult, this.executor);
    }

    private OperationReadResult processReadResult(Iterator<Operation> readResult) {
        OperationReadResult result = new OperationReadResult();
        while (readResult.hasNext()) {
            try {
                result.include(readResult.next());
            } catch (DataCorruptionException ex) {
                throw new RuntimeStreamingException(ex);
            }
        }

        return result;
    }

    private void aggregate(OperationReadResult readResult) {
        if (readResult.getItems().size() > 0) {
            System.out.println(String.format(
                    "ReadResult: Count %d, FirstSeqNo = %d, LastSeqNo = %d",
                    readResult.getItems().size(),
                    readResult.getFirstSequenceNumber(),
                    readResult.getLastSequenceNumber()));

            for (Operation op : readResult.getItems()) {
                System.out.println("\t" + op);
            }

            // We have internalized all operations from this batch; update the internal state.
            this.state.setLastReadSequenceNumber(readResult.getLastSequenceNumber());
        }
    }

    private FlushResult flush() {
        return null;
    }

    private void acknowledge(FlushResult flushResult) {

    }

    private boolean isCriticalError(Throwable ex) {
        return ExceptionHelpers.mustRethrow(ex)
                || ExceptionHelpers.getRealException(ex) instanceof DataCorruptionException;
    }

    private Duration getReadTimeout() {
        //TODO: calculate the time until the first Aggregated Buffer expires (needs to flush), or, if no such thing, the default value (30 mins).
        return this.config.getFlushThresholdTime();
    }

    //region OperationReadResult

    private static class OperationReadResult {
        private final ArrayList<Operation> operations;
        private long firstSequenceNumber;
        private long lastSequenceNumber;

        public OperationReadResult() {
            this.operations = new ArrayList<>();
            this.firstSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
            this.lastSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
        }

        public void include(Operation operation) throws DataCorruptionException {
            if (operation.getSequenceNumber() <= this.lastSequenceNumber) {
                throw new DataCorruptionException(String.format("Operation '%s' has a sequence number that is lower than the previous one (%d).", operation, this.lastSequenceNumber));
            }

            if (this.operations.size() == 0) {
                this.firstSequenceNumber = operation.getSequenceNumber();
            }

            this.operations.add(operation);
            this.lastSequenceNumber = operation.getSequenceNumber();
        }

        public long getFirstSequenceNumber() {
            return this.firstSequenceNumber;
        }

        public long getLastSequenceNumber() {
            return this.lastSequenceNumber;
        }

        public Collection<Operation> getItems() {
            return this.operations;
        }
    }

    //endregion

    //region State

    private static class State {
        private long lastReadSequenceNumber;

        State() {
            this.lastReadSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
        }

        long getLastReadSequenceNumber() {
            return this.lastReadSequenceNumber;
        }

        void setLastReadSequenceNumber(long value) {
            Preconditions.checkArgument(value >= this.lastReadSequenceNumber, "New LastReadSequenceNumber cannot be smaller than the previous one.");
            this.lastReadSequenceNumber = value;
        }

        @Override
        public String toString() {
            return String.format("LastReadSeqNo = %d", this.lastReadSequenceNumber);
        }
    }

    //endregion
}

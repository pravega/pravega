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
import com.emc.pravega.common.function.CallbackHelpers;
import com.emc.pravega.service.contracts.RuntimeStreamingException;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.logs.MemoryOperationLog;
import com.emc.pravega.service.server.logs.OperationLog;
import com.emc.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import lombok.val;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Light-weight version of the DurableLog that only stores operations in memory and does not depend on any other component,
 * unlike the real DurableLog that requires a lot more components to process.
 * <p>
 * Note that even though it uses an UpdateableContainerMetadata, no changes to this metadata are performed (except recording truncation markers & Sequence Numbers).
 * All other changes (Segment-based) must be done externally.
 */
class LightWeightDurableLog extends AbstractIdleService implements OperationLog {
    //region Members

    private final UpdateableContainerMetadata metadata;
    private final MemoryOperationLog log;
    private Consumer<TruncationArgs> truncationCallback;
    private final Executor executor;
    private final boolean autoAssignSequenceNumbers;
    private CompletableFuture<Void> addProcessed;
    private boolean closed;

    //endregion

    //region Constructor

    LightWeightDurableLog(UpdateableContainerMetadata metadata, Executor executor) {
        this(metadata, executor, true);
    }

    LightWeightDurableLog(UpdateableContainerMetadata metadata, Executor executor, boolean autoAssignSequenceNumbers) {
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(executor, "executor");
        this.metadata = metadata;
        this.executor = executor;
        this.autoAssignSequenceNumbers = autoAssignSequenceNumbers;
        this.log = new MemoryOperationLog();
    }

    //endregion

    //region AbstractIdleService Implementation

    @Override
    protected void startUp() throws Exception {
        // This method intentionally left blank.
    }

    @Override
    protected void shutDown() throws Exception {
        // This method intentionally left blank.
    }

    //endregion

    //region OperationLog Implementation

    @Override
    public int getId() {
        return this.metadata.getContainerId();
    }

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;

            // Cancel any pending adds.
            CompletableFuture<Void> addProcessed;
            synchronized (this.log) {
                addProcessed = this.addProcessed;
                this.addProcessed = null;
            }

            if (addProcessed != null) {
                addProcessed.cancel(true);
            }
        }
    }

    @Override
    public CompletableFuture<Long> add(Operation operation, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        return CompletableFuture.supplyAsync(() -> {
            AtomicLong seqNo = new AtomicLong(operation.getSequenceNumber());
            if (this.autoAssignSequenceNumbers) {
                Preconditions.checkArgument(seqNo.get() < 0, "Cannot auto-assign sequence numbers if the operation already has one.");
                seqNo.set(this.metadata.nextOperationSequenceNumber());
                operation.setSequenceNumber(seqNo.get());
            } else {
                Preconditions.checkArgument(seqNo.get() >= 0, "Given operation has no sequence number and auto-assign is not enabled.");
            }

            if (!this.log.addIf(operation, previous -> previous.getSequenceNumber() < seqNo.get())) {
                throw new RuntimeStreamingException(new DataCorruptionException("Sequence numbers out of order."));
            }

            if (operation instanceof MetadataCheckpointOperation) {
                this.metadata.recordTruncationMarker(seqNo.get(), seqNo.get());
            }

            notifyAddProcessed();
            return seqNo.get();
        }, this.executor);
    }

    @Override
    public CompletableFuture<Void> truncate(long upToSequenceNumber, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkArgument(this.metadata.isValidTruncationPoint(upToSequenceNumber), "Invalid Truncation Point. Must refer to a MetadataCheckpointOperation.");

        return CompletableFuture.runAsync(() -> {
            this.log.truncate(o -> o.getSequenceNumber() <= upToSequenceNumber);
            this.metadata.removeTruncationMarkers(upToSequenceNumber);

            // Invoke the truncation callback.
            Consumer<TruncationArgs> callback = this.truncationCallback;
            if (callback != null) {
                Operation lastOperation = this.log.getLast();
                long highestSeqNo = lastOperation == null ? upToSequenceNumber : lastOperation.getSequenceNumber();
                CallbackHelpers.invokeSafely(callback, new TruncationArgs(upToSequenceNumber, highestSeqNo), null);
            }
        }, this.executor);
    }

    @Override
    public CompletableFuture<Iterator<Operation>> read(long afterSequenceNumber, int maxCount, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        val logReadResult = this.log.read(e -> e.getSequenceNumber() > afterSequenceNumber, maxCount);
        if (logReadResult.hasNext()) {
            // Result is readily available; return it.
            return CompletableFuture.completedFuture(logReadResult);
        } else {
            // Result is not yet available; wait for an add and then retry the read.
            return waitForAdd(afterSequenceNumber)
                    .thenComposeAsync(v -> this.read(afterSequenceNumber, maxCount, timeout));
        }
    }

    //endregion

    //region Other Properties

    /**
     * Sets a callback that will invoked on every call to truncate.
     *
     * @param truncationCallback The callback to set.
     */
    public void setTruncationCallback(Consumer<TruncationArgs> truncationCallback) {
        this.truncationCallback = truncationCallback;
    }

    //endregion

    //region Helpers

    private CompletableFuture<Void> waitForAdd(long currentSeqNo) {
        CompletableFuture<Void> result;
        synchronized (this.log) {
            if (this.log.size() > 0 && this.log.getLast().getSequenceNumber() > currentSeqNo) {
                // An add has already been processed that meets or exceeds the given sequence number.
                result = CompletableFuture.completedFuture(null);
            } else {
                if (this.addProcessed == null) {
                    // We need to wait for an add, and nobody else is waiting for it too.
                    this.addProcessed = new CompletableFuture<>();
                }

                result = this.addProcessed;
            }
        }

        return result;
    }

    private void notifyAddProcessed() {
        if (this.addProcessed != null) {
            CompletableFuture<Void> f;
            synchronized (this.log) {
                f = this.addProcessed;
                this.addProcessed = null;
            }

            if (f != null) {
                f.complete(null);
            }
        }
    }

    //endregion

    public static class TruncationArgs {
        private final long highestSequenceNumber;
        private final long truncationSequenceNumber;

        TruncationArgs(long truncationSequenceNumber, long highestSequenceNumber) {
            this.truncationSequenceNumber = truncationSequenceNumber;
            this.highestSequenceNumber = highestSequenceNumber;
        }

        public long getHighestSequenceNumber() {
            return this.highestSequenceNumber;
        }

        public long getTruncationSequenceNumber() {
            return this.truncationSequenceNumber;
        }

        @Override
        public String toString() {
            return String.format("T.SeqNo = %d, H.SeqNo = %d", this.truncationSequenceNumber, this.highestSequenceNumber);
        }
    }
}

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
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.logs.MemoryOperationLog;
import com.emc.pravega.service.server.logs.OperationLog;
import com.emc.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writer-specific mock of OperationLog (without the heavyweight-ness of the actual DurableLog.
 */
class TestOperationLog extends AbstractIdleService implements OperationLog {
    private final UpdateableContainerMetadata metadata;
    private final MemoryOperationLog log;
    private final AtomicLong sequenceNumber;
    private final Executor executor;
    private CompletableFuture<Void> notEmpty;
    private boolean closed;

    TestOperationLog(UpdateableContainerMetadata metadata, Executor executor) {
        this.metadata = metadata;
        this.executor = executor;
        this.log = new MemoryOperationLog();
        this.sequenceNumber = new AtomicLong();
    }

    //region OperationLog Implementation

    @Override
    public int getId() {
        return this.metadata.getContainerId();
    }

    @Override
    public void close() {
        this.closed = true;
    }

    @Override
    public CompletableFuture<Long> add(Operation operation, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        return CompletableFuture.supplyAsync(() -> {
            long seqNo = sequenceNumber.incrementAndGet();
            operation.setSequenceNumber(seqNo);
            this.log.add(operation);
            if (operation instanceof MetadataCheckpointOperation) {
                this.metadata.recordTruncationMarker(seqNo, seqNo);
            }

            notifyNotEmpty();
            return seqNo;
        }, this.executor);
    }

    @Override
    public CompletableFuture<Void> truncate(long upToSequenceNumber, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkArgument(this.metadata.isValidTruncationPoint(upToSequenceNumber), "Invalid Truncation Point. Must refer to a MetadataCheckpointOperation.");

        return CompletableFuture.runAsync(() -> {
            this.log.truncate(o -> o.getSequenceNumber() <= upToSequenceNumber);
            this.metadata.removeTruncationMarkers(upToSequenceNumber);
        }, this.executor);
    }

    @Override
    public CompletableFuture<Iterator<Operation>> read(long afterSequenceNumber, int maxCount, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);

        return waitForNotEmpty()
                .thenApplyAsync(v -> this.log.read(e -> e.getSequenceNumber() > afterSequenceNumber, maxCount), this.executor);
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

    //region Helpers


    private CompletableFuture<Void> waitForNotEmpty() {
        CompletableFuture<Void> result;
        synchronized (this.log) {
            if (this.log.size() == 0) {
                // The log is empty.
                if (this.notEmpty != null) {
                    // We are the first ones to request this, so create the notEmpty future.
                    this.notEmpty = new CompletableFuture<>();
                }

                // Return the notEmpty future.
                result = this.notEmpty;
            } else {
                result = CompletableFuture.completedFuture(null);
            }
        }

        return result;
    }

    private void notifyNotEmpty() {
        if (this.notEmpty != null) {
            CompletableFuture<Void> f;
            synchronized (this.log) {
                f = this.notEmpty;
                this.notEmpty = null;
            }

            if (f != null) {
                f.complete(null);
            }
        }
    }

    //endregion
}

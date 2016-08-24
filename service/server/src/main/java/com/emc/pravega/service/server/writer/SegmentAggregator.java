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
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Aggregates contents for a specific StreamSegment.
 */
@Slf4j
class SegmentAggregator implements AutoCloseable {
    //region Members

    private final UpdateableSegmentMetadata metadata;
    private final String traceObjectId;
    private boolean initialized;
    private boolean closed;

    //endregion

    //region Constructor

    SegmentAggregator(UpdateableSegmentMetadata metadata) {
        Preconditions.checkNotNull(metadata, "metadata");

        this.metadata = metadata;
        this.traceObjectId = String.format("StorageWriter[%d-%d]", metadata.getContainerId(), metadata.getId());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            log.info("{}: Closed.");
            this.closed = true;
        }
    }

    //endregion

    //region Operations

    /**
     * Initializes the SegmentAggregator by pulling information from the given Storage.
     *
     * @param storage The storage to initialize from.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation finished successfully. If any
     * errors occurred during the operation, the Future will be completed with the appropriate exception.
     */
    CompletableFuture<Void> initialize(Storage storage, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!this.initialized, "SegmentAggregator has already been initialized.");

        return storage
                .getStreamSegmentInfo(this.metadata.getName(), timeout)
                .thenAccept(segmentInfo -> {
                    // Check & Update StorageLength in metadata.
                    if (this.metadata.getStorageLength() != segmentInfo.getLength()) {
                        if (this.metadata.getStorageLength() >= 0) {
                            // Only log warning if the StorageLength has actually been initialized, but is different.
                            log.warn("{}: SegmentMetadata has a StorageLength ({}) that is different than the actual one ({}) - updating metadata.", this.traceObjectId, this.metadata.getStorageLength(), segmentInfo.getLength());
                        }

                        // It is very important to keep this value up-to-date and correct.
                        this.metadata.setStorageLength(segmentInfo.getLength());
                    }

                    // Check if the Storage segment is sealed, but it's not in metadata (this is 100% indicative of some data corruption happening).
                    if (!this.metadata.isSealed() && segmentInfo.isSealed()) {
                        throw new RuntimeStreamingException(new DataCorruptionException(String.format("Segment '%s' is sealed in Storage but not in the metadata.", this.metadata.getName())));
                    }

                    this.initialized = true;
                    log.info("{}: Initialized. StorageLength = {}, Sealed = {}.", this.traceObjectId, segmentInfo.getLength(), segmentInfo.isSealed());
                });
    }

    /**
     * Adds the given StorageOperation to the Aggregator.
     *
     * @param operation the Operation to add.
     */
    void add(StorageOperation operation) {
        Exceptions.checkNotClosed(this.closed, this);
        checkInitialized();
    }

    /**
     * Flushes the contents of the Aggregator to the given Storage.
     *
     * @param storage The Storage to flush the contents to.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a summary of the flush operation. If any errors
     * occurred during the flush, the Future will be completed with the appropriate exception.
     */
    CompletableFuture<FlushResult> flush(Storage storage, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        checkInitialized();

        return null;
    }

    //endregion

    //region Helpers

    private void checkInitialized() {
        Preconditions.checkState(this.initialized, "SegmentAggregator is not initialized. Cannot execute this operation.");
    }

    //endregion
}

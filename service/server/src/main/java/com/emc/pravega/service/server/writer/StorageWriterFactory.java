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

import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.OperationLog;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.Writer;
import com.emc.pravega.service.server.WriterFactory;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageFactory;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for StorageWriters.
 */
public class StorageWriterFactory implements WriterFactory {
    private final WriterConfig config;
    private final StorageFactory storageFactory;
    private final ScheduledExecutorService executor;

    /**
     * Creates a new instance of the StorageWriterFactory class.
     *
     * @param config         The Configuration to use for every Writer that is created.
     * @param storageFactory The StorageFactory to use for every Writer creation.
     * @param executor       The Executor to use.
     */
    public StorageWriterFactory(WriterConfig config, StorageFactory storageFactory, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(storageFactory, "storageFactory");
        Preconditions.checkNotNull(executor, "executor");

        this.config = config;
        this.storageFactory = storageFactory;
        this.executor = executor;
    }

    @Override
    public Writer createWriter(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, ReadIndex readIndex, Cache cache) {
        Preconditions.checkArgument(containerMetadata.getContainerId() == operationLog.getId(), "Given containerMetadata and operationLog have different Container Ids.");
        Storage storage = this.storageFactory.getStorageAdapter();
        WriterDataSource dataSource = new StorageWriterDataSource(containerMetadata, operationLog, readIndex, cache);
        return new StorageWriter(this.config, dataSource, storage, this.executor);
    }

    //region StorageWriterDataSource

    @Slf4j
    private static class StorageWriterDataSource implements WriterDataSource {
        private final UpdateableContainerMetadata containerMetadata;
        private final OperationLog operationLog;
        private final Cache cache;
        private final ReadIndex readIndex;
        private final String traceObjectId;

        StorageWriterDataSource(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, ReadIndex readIndex, Cache cache) {
            Preconditions.checkNotNull(containerMetadata, "containerMetadata");
            Preconditions.checkNotNull(operationLog, "operationLog");
            Preconditions.checkNotNull(readIndex, "readIndex");
            Preconditions.checkNotNull(cache, "cache");

            this.containerMetadata = containerMetadata;
            this.operationLog = operationLog;
            this.readIndex = readIndex;
            this.cache = cache;
            this.traceObjectId = String.format("WriterDataSource[%d]", containerMetadata.getContainerId());
        }

        //region WriterDataSource Implementation

        @Override
        public int getId() {
            return this.containerMetadata.getContainerId();
        }

        @Override
        public CompletableFuture<Void> acknowledge(long upToSequence, Duration timeout) {
            log.debug("{}: Acknowledge (UpToSeqNo={}).", this.traceObjectId, upToSequence);
            return this.operationLog.truncate(upToSequence, timeout);
        }

        @Override
        public CompletableFuture<Iterator<Operation>> read(long afterSequence, int maxCount, Duration timeout) {
            log.debug("{}: Read (AfterSeqNo={}, MaxCount={}).", this.traceObjectId, afterSequence, maxCount);
            return this.operationLog.read(afterSequence, maxCount, timeout);
        }

        @Override
        public void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId) {
            log.debug("{}: CompleteMerge (TargetSegmentId={}, SourceSegmentId={}).", this.traceObjectId, targetStreamSegmentId, sourceStreamSegmentId);
            this.readIndex.completeMerge(targetStreamSegmentId, sourceStreamSegmentId);
        }

        @Override
        public boolean isValidTruncationPoint(long operationSequenceNumber) {
            return this.containerMetadata.isValidTruncationPoint(operationSequenceNumber);
        }

        @Override
        public long getClosestValidTruncationPoint(long operationSequenceNumber) {
            return this.containerMetadata.getClosestValidTruncationPoint(operationSequenceNumber);
        }

        @Override
        public void deleteStreamSegment(String streamSegmentName) {
            log.info("{}: DeleteSegment (SegmentName={}).", this.traceObjectId, streamSegmentName);
            this.containerMetadata.deleteStreamSegment(streamSegmentName);
        }

        @Override
        public UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) {
            return this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
        }

        @Override
        public byte[] getAppendData(CacheKey key) {
            return this.cache.get(key);
        }

        //endregion
    }

    //endregion
}

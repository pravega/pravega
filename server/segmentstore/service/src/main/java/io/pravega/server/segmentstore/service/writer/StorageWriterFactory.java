/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.service.writer;

import io.pravega.server.segmentstore.service.Writer;
import io.pravega.server.segmentstore.service.OperationLog;
import io.pravega.server.segmentstore.service.ReadIndex;
import io.pravega.server.segmentstore.service.UpdateableContainerMetadata;
import io.pravega.server.segmentstore.service.UpdateableSegmentMetadata;
import io.pravega.server.segmentstore.service.WriterFactory;
import io.pravega.server.segmentstore.service.logs.operations.Operation;
import io.pravega.server.segmentstore.storage.Storage;
import com.google.common.base.Preconditions;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import lombok.extern.slf4j.Slf4j;

/**
 * Factory for StorageWriters.
 */
public class StorageWriterFactory implements WriterFactory {
    private final WriterConfig config;
    private final ScheduledExecutorService executor;

    /**
     * Creates a new instance of the StorageWriterFactory class.
     *
     * @param config         The Configuration to use for every Writer that is created.
     * @param executor       The Executor to use.
     */
    public StorageWriterFactory(WriterConfig config, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
    }

    @Override
    public Writer createWriter(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, ReadIndex readIndex, Storage storage) {
        Preconditions.checkArgument(containerMetadata.getContainerId() == operationLog.getId(), "Given containerMetadata and operationLog have different Container Ids.");
        WriterDataSource dataSource = new StorageWriterDataSource(containerMetadata, operationLog, readIndex);
        return new StorageWriter(this.config, dataSource, storage, this.executor);
    }

    //region StorageWriterDataSource

    @Slf4j
    private static class StorageWriterDataSource implements WriterDataSource {
        private final UpdateableContainerMetadata containerMetadata;
        private final OperationLog operationLog;
        private final ReadIndex readIndex;
        private final String traceObjectId;

        StorageWriterDataSource(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, ReadIndex readIndex) {
            Preconditions.checkNotNull(containerMetadata, "containerMetadata");
            Preconditions.checkNotNull(operationLog, "operationLog");
            Preconditions.checkNotNull(readIndex, "readIndex");

            this.containerMetadata = containerMetadata;
            this.operationLog = operationLog;
            this.readIndex = readIndex;
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
        public void notifyStorageLengthUpdated(long streamSegmentId) {
            log.debug("{}: notifyStorageLengthUpdated (SegmentId={}).", this.traceObjectId, streamSegmentId);
            this.readIndex.triggerFutureReads(Collections.singleton(streamSegmentId));
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
        public InputStream getAppendData(long streamSegmentId, long startOffset, int length) {
            return this.readIndex.readDirect(streamSegmentId, startOffset, length);
        }

        //endregion
    }

    //endregion
}

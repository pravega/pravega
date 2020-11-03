/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.writer;

import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.Writer;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndex;
import io.pravega.segmentstore.server.logs.PriorityCalculator;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.storage.Storage;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    public Writer createWriter(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, ReadIndex readIndex,
                               ContainerAttributeIndex attributeIndex, Storage storage, CreateProcessors createProcessors) {
        Preconditions.checkArgument(containerMetadata.getContainerId() == operationLog.getId(),
                "Given containerMetadata and operationLog have different Container Ids.");
        WriterDataSource dataSource = new StorageWriterDataSource(containerMetadata, operationLog, readIndex, attributeIndex);
        return new StorageWriter(this.config, dataSource, storage, createProcessors, this.executor);
    }

    //region StorageWriterDataSource

    @Slf4j
    private static class StorageWriterDataSource implements WriterDataSource {
        private final UpdateableContainerMetadata containerMetadata;
        private final OperationLog operationLog;
        private final ReadIndex readIndex;
        private final ContainerAttributeIndex attributeIndex;
        private final String traceObjectId;

        StorageWriterDataSource(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, ReadIndex readIndex,
                                ContainerAttributeIndex attributeIndex) {
            this.containerMetadata = Preconditions.checkNotNull(containerMetadata, "containerMetadata");
            this.operationLog = Preconditions.checkNotNull(operationLog, "operationLog");
            this.readIndex = Preconditions.checkNotNull(readIndex, "readIndex");
            this.attributeIndex = Preconditions.checkNotNull(attributeIndex, "attributeIndex");
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
        public CompletableFuture<Long> persistAttributes(long streamSegmentId, Map<UUID, Long> attributes, Duration timeout) {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.attributeIndex
                    .forSegment(streamSegmentId, timer.getRemaining())
                    .thenCompose(ai -> ai.update(attributes, timer.getRemaining()));
        }

        @Override
        public CompletableFuture<Void> notifyAttributesPersisted(long segmentId, SegmentType segmentType, long rootPointer,
                                                                 long lastSequenceNumber, Duration timeout) {
            List<AttributeUpdate> updates = Arrays.asList(
                    new AttributeUpdate(Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER, AttributeUpdateType.ReplaceIfGreater, rootPointer),
                    new AttributeUpdate(Attributes.ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO, AttributeUpdateType.Replace, lastSequenceNumber));
            UpdateAttributesOperation op = new UpdateAttributesOperation(segmentId, updates);
            op.setInternal(true); // This is internally generated, so we want to ensure it's accepted even on a sealed segment.
            OperationPriority priority = PriorityCalculator.getPriority(segmentType, op.getType());
            return this.operationLog.add(op, priority, timeout);
        }

        @Override
        public CompletableFuture<Void> sealAttributes(long streamSegmentId, Duration timeout) {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.attributeIndex
                    .forSegment(streamSegmentId, timer.getRemaining())
                    .thenCompose(ai -> ai.seal(timer.getRemaining()));
        }

        @Override
        public CompletableFuture<Void> deleteAllAttributes(SegmentMetadata segmentMetadata, Duration timeout) {
            return this.attributeIndex.delete(segmentMetadata.getName(), timeout);
        }

        @Override
        public CompletableFuture<Iterator<Operation>> read(long afterSequence, int maxCount, Duration timeout) {
            log.debug("{}: Read (AfterSeqNo={}, MaxCount={}).", this.traceObjectId, afterSequence, maxCount);
            return this.operationLog.read(afterSequence, maxCount, timeout);
        }

        @Override
        public void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId) throws StreamSegmentNotExistsException {
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
        public UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) {
            return this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
        }

        @Override
        public BufferView getAppendData(long streamSegmentId, long startOffset, int length) {
            try {
                return this.readIndex.readDirect(streamSegmentId, startOffset, length);
            } catch (StreamSegmentNotExistsException ex) {
                // Null is interpreted as "Segment not exists" by the SegmentAggregator.
                return null;
            }
        }

        //endregion
    }

    //endregion
}

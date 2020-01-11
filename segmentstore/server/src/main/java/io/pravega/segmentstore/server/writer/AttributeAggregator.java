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
import io.pravega.common.AbstractTimer;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.logs.operations.AttributeUpdaterOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Aggregates Attribute Updates for a specific Segment.
 */
@Slf4j
class AttributeAggregator implements WriterSegmentProcessor, AutoCloseable {
    //region Members

    private final UpdateableSegmentMetadata metadata;
    private final WriterConfig config;
    private final AbstractTimer timer;
    private final Executor executor;
    private final String traceObjectId;
    private final WriterDataSource dataSource;
    private final AtomicReference<Duration> lastFlush;
    private final AggregatedAttributes aggregatedAttributes;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link AttributeAggregator} class.
     *
     * @param segmentMetadata The Metadata for the Segment to construct this Aggregator for.
     * @param dataSource      The {@link WriterDataSource} to use.
     * @param config          The {@link WriterConfig} to use.
     * @param timer           An {@link AbstractTimer} to use to determine elapsed time.
     * @param executor        An Executor to use for async operations.
     */
    AttributeAggregator(@NonNull UpdateableSegmentMetadata segmentMetadata, @NonNull WriterDataSource dataSource,
                        @NonNull WriterConfig config, @NonNull AbstractTimer timer, @NonNull Executor executor) {
        this.metadata = segmentMetadata;
        this.config = config;
        this.dataSource = dataSource;
        this.timer = timer;
        this.executor = executor;
        this.lastFlush = new AtomicReference<>(timer.getElapsed());

        Preconditions.checkArgument(this.metadata.getContainerId() == dataSource.getId(), "SegmentMetadata.ContainerId is different from WriterDataSource.Id");
        this.traceObjectId = String.format("AttributeAggregator[%d-%d]", this.metadata.getContainerId(), this.metadata.getId());
        this.aggregatedAttributes = new AggregatedAttributes(segmentMetadata.getAttributes().getOrDefault(Attributes.ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO, Operation.NO_SEQUENCE_NUMBER));
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.closed.set(true);
    }

    //endregion

    //region WriterSegmentProcessor Implementation

    @Override
    public long getLowestUncommittedSequenceNumber() {
        return this.aggregatedAttributes.getFirstSequenceNumber();
    }

    @Override
    public boolean isClosed() {
        return this.closed.get();
    }

    /**
     * Gets a value indicating whether a call to {@link #flush} is required given the current state of this aggregator.
     */
    @Override
    public boolean mustFlush() {
        if (isSegmentDeleted()) {
            // There isn't more that we can do.
            return false;
        }

        return this.aggregatedAttributes.hasSeal()
                || this.aggregatedAttributes.size() >= config.getFlushAttributesThreshold()
                || (this.aggregatedAttributes.size() > 0 && getElapsedSinceLastFlush().compareTo(this.config.getFlushThresholdTime()) >= 0);
    }

    @Override
    public String toString() {
        return String.format("[%d: %s] Count = %d, LUSN = %d, LastSeqNo = %d, LastFlush = %ds", this.metadata.getId(), this.metadata.getName(),
                this.aggregatedAttributes.size(), getLowestUncommittedSequenceNumber(), this.aggregatedAttributes.getLastSequenceNumber(), getElapsedSinceLastFlush().toMillis() / 1000);
    }

    /**
     * Adds the given SegmentOperation to the Aggregator.
     *
     * @param operation the Operation to add.
     * @throws DataCorruptionException  If the validation of the given Operation indicates a possible data corruption in
     *                                  the code (offset gaps, out-of-order operations, etc.)
     * @throws IllegalArgumentException If the validation of the given Operation indicates a possible non-corrupting bug
     *                                  in the code.
     */
    @Override
    public void add(SegmentOperation operation) throws DataCorruptionException {
        Exceptions.checkNotClosed(isClosed(), this);
        Preconditions.checkArgument(
                operation.getStreamSegmentId() == this.metadata.getId(),
                "Operation '%s' refers to a different Segment than this one (%s).", operation, this.metadata.getId());
        if (isSegmentDeleted()) {
            return;
        }

        boolean processed = false;
        if (operation instanceof StreamSegmentSealOperation) {
            this.aggregatedAttributes.seal();
            processed = true;
        } else if (operation instanceof AttributeUpdaterOperation) {
            if (this.aggregatedAttributes.hasSeal()) {
                throw new DataCorruptionException(String.format("Illegal operation for a sealed Segment; received '%s'.", operation));
            }

            processed = this.aggregatedAttributes.include((AttributeUpdaterOperation) operation);
        }

        if (processed) {
            log.debug("{}: Add {}; OpCount={}.", this.traceObjectId, operation, this.aggregatedAttributes.size());
        }
    }

    /**
     * Flushes the contents of the Aggregator to the Storage.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a summary of the flush operation. If any errors
     * occurred during the flush, the Future will be completed with the appropriate exception.
     */
    @Override
    public CompletableFuture<WriterFlushResult> flush(Duration timeout) {
        Exceptions.checkNotClosed(isClosed(), this);
        if (!mustFlush()) {
            return CompletableFuture.completedFuture(new WriterFlushResult());
        }

        TimeoutTimer timer = new TimeoutTimer(timeout);

        CompletableFuture<Void> result = handleAttributeException(persistPendingAttributes(
                this.aggregatedAttributes.getAttributes(), this.aggregatedAttributes.getLastSequenceNumber(), timer));
        if (this.aggregatedAttributes.hasSeal()) {
            result = result.thenComposeAsync(v -> handleAttributeException(sealAttributes(timer)));
        }

        return result.thenApply(v -> {
            WriterFlushResult r = new WriterFlushResult();
            r.withFlushedAttributes(this.aggregatedAttributes.size());
            this.aggregatedAttributes.acceptChanges();
            return r;
        });
    }

    //endregion

    //region Helpers

    private CompletableFuture<Void> persistPendingAttributes(Map<UUID, Long> attributes, long lastSeqNo, TimeoutTimer timer) {
        if (attributes.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return this.dataSource
                .persistAttributes(this.metadata.getId(), attributes, timer.getRemaining())
                .thenComposeAsync(
                        rootPointer -> this.dataSource.notifyAttributesPersisted(this.metadata.getId(), rootPointer, lastSeqNo, timer.getRemaining()),
                        this.executor);
    }

    private CompletableFuture<Void> sealAttributes(TimeoutTimer timer) {
        return this.dataSource.sealAttributes(this.metadata.getId(), timer.getRemaining());
    }

    /**
     * Handles expected Attribute-related exceptions. Since the attribute index is a separate segment from the main one,
     * it is highly likely that it may get temporarily out of sync with the main one, thus causing spurious StreamSegmentSealedExceptions
     * or StreamSegmentNotExistsExceptions. If we get either of those, and they are consistent with our current state, the
     * we can safely ignore them; otherwise we should be rethrowing them.
     */
    private <T> CompletableFuture<T> handleAttributeException(CompletableFuture<T> future) {
        return Futures.exceptionallyExpecting(
                future,
                ex -> (ex instanceof StreamSegmentSealedException && this.metadata.isSealed())
                        || ((ex instanceof StreamSegmentNotExistsException || ex instanceof StreamSegmentMergedException)
                        && (this.metadata.isMerged() || this.metadata.isDeleted())),
                null);
    }

    private boolean isSegmentDeleted() {
        return this.metadata.isDeleted() || this.metadata.isMerged();
    }

    private Duration getElapsedSinceLastFlush() {
        return this.timer.getElapsed().minus(this.lastFlush.get());
    }

    //endregion

    //region AggregatedAttributes

    /**
     * Aggregates pending Attribute Updates.
     */
    private static class AggregatedAttributes {
        private final HashMap<UUID, Long> attributes;
        private final AtomicLong lastPersistedSequenceNumber;
        private final AtomicLong firstSequenceNumber;
        private final AtomicLong lastSequenceNumber;
        private final AtomicBoolean sealed;

        AggregatedAttributes(long lastPersistedSequenceNumber) {
            this.attributes = new HashMap<>();
            this.firstSequenceNumber = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
            this.lastSequenceNumber = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
            this.lastPersistedSequenceNumber = new AtomicLong(lastPersistedSequenceNumber);
            this.sealed = new AtomicBoolean(false);
        }

        void seal() {
            this.sealed.set(true);
        }

        boolean hasSeal() {
            return this.sealed.get();
        }

        boolean include(AttributeUpdaterOperation operation) {
            Preconditions.checkState(!this.sealed.get(), "Cannot accept more operations after sealing.");
            if (operation.getSequenceNumber() <= this.lastPersistedSequenceNumber.get()) {
                return false;
            }

            if (this.attributes.isEmpty()) {
                this.firstSequenceNumber.set(operation.getSequenceNumber());
            }

            if (operation.getAttributeUpdates() != null) {
                operation.getAttributeUpdates().stream()
                         .filter(au -> !Attributes.isCoreAttribute(au.getAttributeId()))
                         .forEach(au -> this.attributes.put(au.getAttributeId(), au.getValue()));
            }

            this.lastSequenceNumber.set(operation.getSequenceNumber());
            return true;
        }

        long getFirstSequenceNumber() {
            return this.firstSequenceNumber.get();
        }

        long getLastSequenceNumber() {
            return this.lastSequenceNumber.get();
        }

        int size() {
            return this.attributes.size();
        }

        Map<UUID, Long> getAttributes() {
            return this.attributes;
        }

        /**
         * Notifies that the pending updates have been persisted. Resets the contents of this instance.
         */
        void acceptChanges() {
            this.attributes.clear();
            this.firstSequenceNumber.set(Operation.NO_SEQUENCE_NUMBER);
            this.lastSequenceNumber.set(Operation.NO_SEQUENCE_NUMBER);
            this.lastPersistedSequenceNumber.set(this.lastSequenceNumber.get());
        }
    }

    //endregion
}

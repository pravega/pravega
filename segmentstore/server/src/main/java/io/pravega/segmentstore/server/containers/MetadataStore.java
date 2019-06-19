/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.SerializationException;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.TooManyActiveSegmentsException;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.SegmentMetadata;
import java.io.EOFException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Stores Segment Metadata information and assigns unique Ids to the same.
 */
@Slf4j
@ThreadSafe
public abstract class MetadataStore implements AutoCloseable {
    //region Members

    protected final String traceObjectId;
    protected final Executor executor;
    private final Connector connector;
    @GuardedBy("pendingRequests")
    private final HashMap<String, PendingRequest> pendingRequests;

    //endregion

    //region Constructor and Initialization

    /**
     * Creates a new instance of the MetadataStore class.
     *
     * @param connector A {@link Connector} object that can be used to communicate between the {@link MetadataStore}
     *                  and upstream callers.
     * @param executor  The executor to use for async operations.
     */
    MetadataStore(@NonNull Connector connector, @NonNull Executor executor) {
        this.traceObjectId = String.format("MetadataStore[%d]", connector.containerMetadata.getContainerId());
        this.connector = connector;
        this.executor = executor;
        this.pendingRequests = new HashMap<>();
    }

    @Override
    public void close() {
        // Even though all pending requests should be cancelled from any components that they may be waiting upon, cancel
        // them anyway to ensure the caller will get notified.
        ArrayList<PendingRequest> toCancel;
        synchronized (this.pendingRequests) {
            toCancel = new ArrayList<>(this.pendingRequests.values());
            this.pendingRequests.clear();
        }

        val ex = new ObjectClosedException(this);
        toCancel.forEach(r -> r.completeExceptionally(ex));
    }

    /**
     * Initializes the MetadataStore, if necessary.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the initialization is done.
     */
    abstract CompletableFuture<Void> initialize(Duration timeout);

    //endregion

    //region Create Segments

    /**
     * Creates a new Segment with given name.
     *
     * @param segmentName The case-sensitive Segment Name.
     * @param attributes  The initial attributes for the StreamSegment, if any.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the Segment has been created.
     * If the operation failed, this will contain the exception that caused the failure. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentExistsException} If the Segment already exists.
     * </ul>
     */
    CompletableFuture<Void> createSegment(String segmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, traceObjectId, "createSegment", segmentName);
        long segmentId = this.connector.containerMetadata.getStreamSegmentId(segmentName, true);
        if (isValidSegmentId(segmentId)) {
            // Quick fail: see if this is an active Segment, and if so, don't bother with anything else.
            return Futures.failedFuture(new StreamSegmentExistsException(segmentName));
        }

        ArrayView segmentInfo = SegmentInfo.serialize(SegmentInfo.newSegment(segmentName, attributes));
        CompletableFuture<Void> result = createSegment(segmentName, segmentInfo, new TimeoutTimer(timeout));
        if (log.isTraceEnabled()) {
            result.thenAccept(v -> LoggerHelpers.traceLeave(log, traceObjectId, "createSegment", traceId, segmentName));
        }

        return result;
    }

    /**
     * Attempts to create the given Segment in the Metadata Store.
     *
     * @param segmentName The name of the Segment to create.
     * @param segmentInfo An {@link ArrayView} containing the serialized Segment Info to store for this segment.
     * @param timer       A TimeoutTimer for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the Segment has been successfully created.
     */
    protected abstract CompletableFuture<Void> createSegment(String segmentName, ArrayView segmentInfo, TimeoutTimer timer);

    //endregion

    //region Delete Segments

    /**
     * Deletes a Segment and any associated information from the Metadata Store.
     * Notes:
     * - This method removes both the Segment and its Metadata Store entries.
     * - {@link #clearSegmentInfo} only removes Metadata Store entries.
     *
     * This operation is made of multiple steps and is restart-able. If it was only able to execute partially before being
     * interrupted (by an unexpected exception or system crash), a reinvocation should be able to pick up from where it
     * left off previously. A partial invocation may leave the Segment in an undefined state, so it is highly recommended
     * that such an interrupted call be reinvoked until successful.
     *
     * @param segmentName The case-sensitive Segment Name.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain a Boolean indicating whether  the Segment
     * has been deleted (true means there was a Segment to delete, false means there was no segment to delete). If the
     * operation failed, this will contain the exception that caused the failure.
     */
    CompletableFuture<Boolean> deleteSegment(String segmentName, Duration timeout) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, traceObjectId, "deleteSegment", segmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Find the Segment's Id.
        long segmentId = this.connector.containerMetadata.getStreamSegmentId(segmentName, true);
        CompletableFuture<Void> deleteSegment;
        if (isValidSegmentId(segmentId)) {
            // This segment is currently mapped in the ContainerMetadata.
            if (this.connector.containerMetadata.getStreamSegmentMetadata(segmentId).isDeleted()) {
                // ... but it is marked as Deleted, so nothing more we can do here.
                deleteSegment = CompletableFuture.completedFuture(null);
            } else {
                // Queue it up for deletion. This ensures that any component that is actively using it will be notified.
                deleteSegment = this.connector.getLazyDeleteSegment().apply(segmentId, timer.getRemaining());
            }
        } else {
            // This segment is not currently mapped in the ContainerMetadata. As such, it is safe to delete it directly.
            deleteSegment = this.connector.getDirectDeleteSegment().apply(segmentName, timer.getRemaining());
        }

        // It is OK if the previous action indicated the Segment was deleted. We still need to make sure that any traces
        // of this Segment are cleared from the Metadata Store as this invocation may be a retry of a previous partially
        // executed operation (where we only managed to delete the Segment, but not clear the Metadata).
        val result = Futures
                .exceptionallyExpecting(deleteSegment, ex -> ex instanceof StreamSegmentNotExistsException, null)
                .thenComposeAsync(ignored -> clearSegmentInfo(segmentName, timer.getRemaining()), this.executor);
        if (log.isTraceEnabled()) {
            deleteSegment.thenAccept(v -> LoggerHelpers.traceLeave(log, traceObjectId, "deleteSegment", traceId, segmentName));
        }

        return result;
    }

    /**
     * Removes any information associated with the given Segment from the Metadata Store. This method should be used to
     * clear up any info about Segments that have been merged into others; do not use this to delete Segments.
     *
     * Notes:
     * - {@link #deleteSegment} removes both the Segment and its Metadata Store entries.
     * - This method only removes Metadata Store entries.
     *
     * @param segmentName The Segment Name.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain a Boolean indicating whether all information
     * about this Segment has been removed from the Metadata Store (true indicates removed, false means there was nothing there).
     */
    public abstract CompletableFuture<Boolean> clearSegmentInfo(String segmentName, Duration timeout);

    //endregion

    //region Segment Info

    /**
     * Gets information about a Segment. If the Segment is active, it returns this information directly from the
     * in-memory Metadata. If the Segment is not active, it fetches the information from Storage and returns it, without
     * activating the segment in the Metadata or otherwise touching the DurableLog.
     *
     * @param segmentName The Segment Name.
     * @param timeout     Timeout for the Operation.
     * @return A CompletableFuture that, when complete, will contain a {@link SegmentProperties} object with the desired
     * information. If failed, it will contain the exception that caused the failure.
     */
    CompletableFuture<SegmentProperties> getSegmentInfo(String segmentName, Duration timeout) {
        long streamSegmentId = this.connector.containerMetadata.getStreamSegmentId(segmentName, true);
        CompletableFuture<SegmentProperties> result;
        if (isValidSegmentId(streamSegmentId)) {
            // Looks like the Segment is active and we have it in our Metadata. Return the result from there.
            SegmentMetadata sm = this.connector.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
            if (sm.isDeleted() || sm.isMerged()) {
                result = Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
            } else {
                result = CompletableFuture.completedFuture(sm.getSnapshot());
            }
        } else {
            // The Segment is not yet active.
            // First, check to see if we have a pending assignment. If so, piggyback on that.
            QueuedCallback<SegmentProperties> queuedCallback = checkConcurrentAssignment(segmentName,
                    id -> CompletableFuture.completedFuture(this.connector.containerMetadata.getStreamSegmentMetadata(id).getSnapshot()));

            if (queuedCallback != null) {
                result = queuedCallback.result;
            } else {
                // Not in metadata and no concurrent assignments. Go to Storage and get what's needed.
                result = getSegmentInfoInternal(segmentName, timeout)
                        .thenApply(rawData -> SegmentInfo.deserialize(rawData).getProperties());
            }
        }

        return result;
    }

    /**
     * Gets raw information about a Segment, as it exists in the Metadata Store. This is a sequence of bytes that can
     * be deserialized into a {@link SegmentInfo}.
     *
     * @param segmentName The case-sensitive Segment Name.
     * @param timeout     Timeout for the Operation.
     * @return A CompletableFuture that, when completed, will contain an {@link ArrayView} representing the serialized form
     * of a {@link SegmentInfo} object. If failed, it will contain the exception that caused the failure. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Segment already exists.
     * </ul>
     */
    protected abstract CompletableFuture<ArrayView> getSegmentInfoInternal(String segmentName, Duration timeout);

    /**
     * Updates information about a Segment.
     *
     * @param segmentMetadata A {@link SegmentMetadata} that will be saved in the Metadata Store.
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded.
     */
    CompletableFuture<Void> updateSegmentInfo(SegmentMetadata segmentMetadata, Duration timeout) {
        if (segmentMetadata.isMerged()) {
            return Futures.failedFuture(new StreamSegmentMergedException(segmentMetadata.getName()));
        } else if (segmentMetadata.isDeleted()) {
            return Futures.failedFuture(new StreamSegmentNotExistsException(segmentMetadata.getName()));
        }

        ArrayView toWrite = SegmentInfo.serialize(new SegmentInfo(segmentMetadata.getId(), segmentMetadata.getSnapshot()));
        return updateSegmentInfo(segmentMetadata.getName(), toWrite, timeout);
    }

    /**
     * Updates information about a Segment.
     *
     * @param segmentName The Segment name.
     * @param segmentInfo An {@link ArrayView} representing the serialized form of a {@link SegmentInfo} that will be
     *                    written to the Metadata Store.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded.
     */
    protected abstract CompletableFuture<Void> updateSegmentInfo(String segmentName, ArrayView segmentInfo, Duration timeout);

    //endregion

    //region Segment Id Assignment

    /**
     * Attempts to get an existing SegmentId for the given Segment Name, and then invokes the given Function with the Id.
     * * If the Segment is already mapped in the Metadata, the existing Id is used.
     * * Otherwise if the Segment had previously been assigned an id (and saved in the Metadata Store), that Id will be
     * reused.
     * * Otherwise, it atomically assigns a new Id and stores it in the Metadata and DurableLog.
     *
     * If multiple requests for assignment arrive for the same Segment in parallel (or while an assignment is in progress),
     * they will be queued up in the order received and will be invoked in the same order after assignment.
     *
     * @param segmentName The Segment Name.
     * @param timeout     The timeout for the operation.
     * @param thenCompose A Function that consumes a SegmentId and returns a CompletableFuture that will indicate
     *                    when the consumption of that SegmentId is complete. This Function will be invoked
     *                    synchronously if the SegmentId is already mapped, or async, otherwise, after assignment.
     * @param <T>         Type of the return value.
     * @return A CompletableFuture that, when completed normally, will contain the result of the given Function (thenCompose)
     * applied to the assigned/retrieved SegmentId. If failed, this will contain the exception that caused the failure.
     */
    <T> CompletableFuture<T> getOrAssignSegmentId(String segmentName, Duration timeout, @NonNull Function<Long, CompletableFuture<T>> thenCompose) {
        // Check to see if the metadata already knows about this Segment.
        long segmentId = this.connector.containerMetadata.getStreamSegmentId(segmentName, true);
        if (isValidSegmentId(segmentId)) {
            // We already have a value, just return it (but make sure the Segment has not been deleted).
            if (this.connector.containerMetadata.getStreamSegmentMetadata(segmentId).isDeleted()) {
                return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
            } else {
                // Even though we have the value in the metadata, we need to be very careful not to invoke this callback
                // before any other existing callbacks are invoked. As such, verify if we have an existing PendingRequest
                // for this segment - if so, tag onto it so we invoke these callbacks in the correct order.
                QueuedCallback<T> queuedCallback = checkConcurrentAssignment(segmentName, thenCompose);
                return queuedCallback == null ? thenCompose.apply(segmentId) : queuedCallback.result;
            }
        }

        // See if anyone else is currently waiting to get this StreamSegment's id.
        QueuedCallback<T> queuedCallback;
        boolean needsAssignment = false;
        synchronized (this.pendingRequests) {
            PendingRequest pendingRequest = this.pendingRequests.getOrDefault(segmentName, null);
            if (pendingRequest == null) {
                needsAssignment = true;
                pendingRequest = new PendingRequest();
                this.pendingRequests.put(segmentName, pendingRequest);
            }

            queuedCallback = new QueuedCallback<>(thenCompose);
            pendingRequest.callbacks.add(queuedCallback);
        }

        // We are the first/only ones requesting this id; go ahead and assign an id.
        if (needsAssignment) {
            this.executor.execute(() -> assignSegmentId(segmentName, timeout));
        }

        return queuedCallback.result;
    }

    /**
     * Same as {@link #getOrAssignSegmentId(String, Duration, Function)) except that this simply returns a CompletableFuture
     * with the SegmentId.
     *
     * @param streamSegmentName The case-sensitive StreamSegment Name.
     * @param timeout           The timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain SegmentId. If failed, this will contain the
     * exception that caused the failure.
     */
    @VisibleForTesting
    CompletableFuture<Long> getOrAssignSegmentId(String streamSegmentName, Duration timeout) {
        return getOrAssignSegmentId(streamSegmentName, timeout, CompletableFuture::completedFuture);
    }

    /**
     * Attempts to map a Segment to an Id, by first trying to retrieve an existing id, and, should that not exist,
     * assign a new one. If the operation failed, either synchronously, or asynchronously, the segment assignment will be
     * failed with the causing exception.
     *
     * @param segmentName The name of the Segment to assign id for.
     * @param timeout     Timeout for the operation.
     */
    private void assignSegmentId(String segmentName, Duration timeout) {
        try {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            Futures.exceptionListener(
                    getSegmentInfoInternal(segmentName, timer.getRemaining())
                            .thenComposeAsync(si -> submitAssignmentWithRetry(SegmentInfo.deserialize(si), timer.getRemaining()), this.executor),
                    ex -> failAssignment(segmentName, ex));
        } catch (Throwable ex) {
            log.warn("{}: Unable to assign Id for segment '{}'.", this.traceObjectId, segmentName, ex);
            failAssignment(segmentName, ex);
        }
    }

    /**
     * Same as submitAssignment, but retries exactly once in case TooManyActiveSegmentsException was encountered, in
     * which case it forces a metadata cleanup before retrying. If the second attempt also fails, there will be no more retry
     * and the Exception from the second failure will be the one that this call fails with too.
     *
     * @param segmentInfo The SegmentInfo for the Segment to generate and persist.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the internal SegmentId that was assigned (or the
     * one supplied via SegmentInfo, if any). If the operation failed, then this Future will complete with that exception.
     */
    private CompletableFuture<Long> submitAssignmentWithRetry(SegmentInfo segmentInfo, Duration timeout) {
        return retryWithCleanup(() -> submitAssignment(segmentInfo, false, timeout));
    }

    /**
     * Invokes the {@link Connector#getMapSegmentId()} callback in order to assign an Id to a Segment. Upon completion,
     * this operation will have mapped the given Segment to a new internal Segment Id if none was provided in the given
     * SegmentInfo. If the given SegmentInfo already has a SegmentId set, then all efforts will be made to map that Segment
     * with the requested Segment Id.
     *
     * @param segmentInfo The SegmentInfo for the StreamSegment to generate and persist.
     * @param pin         If true, this Segment's metadata will be pinned to memory.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the internal SegmentId that was assigned (or the
     * one supplied via SegmentInfo, if any). If the operation failed, then this Future will complete with that exception.
     */
    protected CompletableFuture<Long> submitAssignment(SegmentInfo segmentInfo, boolean pin, Duration timeout) {
        SegmentProperties properties = segmentInfo.getProperties();
        if (properties.isDeleted()) {
            // Stream does not exist. Fail the request with the appropriate exception.
            failAssignment(properties.getName(), new StreamSegmentNotExistsException("StreamSegment does not exist."));
            return Futures.failedFuture(new StreamSegmentNotExistsException(properties.getName()));
        }

        long existingSegmentId = this.connector.containerMetadata.getStreamSegmentId(properties.getName(), true);
        if (isValidSegmentId(existingSegmentId)) {
            // Looks like someone else beat us to it.
            completeAssignment(properties.getName(), existingSegmentId);
            return CompletableFuture.completedFuture(existingSegmentId);
        } else {
            return this.connector.getMapSegmentId()
                                 .apply(segmentInfo.getSegmentId(), segmentInfo.getProperties(), pin, timeout)
                                 .thenApply(id -> completeAssignment(properties.getName(), id));
        }
    }

    /**
     * Completes the assignment for the given StreamSegmentName by completing the waiting CompletableFuture.
     */
    private long completeAssignment(String streamSegmentName, long streamSegmentId) {
        assert streamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID : "no valid streamSegmentId given";
        finishPendingRequests(streamSegmentName, PendingRequest::complete, streamSegmentId);
        return streamSegmentId;
    }

    /**
     * Fails the assignment for the given StreamSegment Id with the given reason.
     */
    private void failAssignment(String streamSegmentName, Throwable reason) {
        finishPendingRequests(streamSegmentName, PendingRequest::completeExceptionally, reason);
    }

    private <T> void finishPendingRequests(String streamSegmentName, BiConsumer<PendingRequest, T> completionMethod, T completionArgument) {
        assert streamSegmentName != null : "no streamSegmentName given";
        // Get any pending requests and complete all of them, in order. We are running this in a loop (and replacing
        // the existing PendingRequest with an empty one) because more requests may come in while we are executing the
        // callbacks. In such cases, we collect the new requests in the new object and check it again, after we are done
        // with the current executions.
        while (true) {
            PendingRequest pendingRequest;
            synchronized (this.pendingRequests) {
                pendingRequest = this.pendingRequests.remove(streamSegmentName);
                if (pendingRequest == null || pendingRequest.callbacks.size() == 0) {
                    // No more requests. Safe to exit.
                    break;
                } else {
                    this.pendingRequests.put(streamSegmentName, new PendingRequest());
                }
            }

            completionMethod.accept(pendingRequest, completionArgument);
        }
    }

    /**
     * Attempts to piggyback a task on any existing concurrent assignment, if any such assignment exists.
     *
     * @param segmentName The Name of the StreamSegment to attempt to piggyback on.
     * @param thenCompose A Function that consumes a StreamSegmentId and returns a CompletableFuture that will indicate
     *                    when the consumption of that StreamSegmentId is complete. This Function will be invoked
     *                    synchronously if the StreamSegmentId is already mapped, or async, otherwise, after assignment.
     * @param <T>         Type of the return value.
     * @return A QueuedCallback representing the callback object for this task, if it was piggybacked on any existing
     * assignment. If no assignment was found, returns null.
     */
    private <T> QueuedCallback<T> checkConcurrentAssignment(String segmentName, Function<Long, CompletableFuture<T>> thenCompose) {
        QueuedCallback<T> queuedCallback = null;
        synchronized (this.pendingRequests) {
            PendingRequest pendingRequest = this.pendingRequests.getOrDefault(segmentName, null);
            if (pendingRequest != null) {
                queuedCallback = new QueuedCallback<>(thenCompose);
                pendingRequest.callbacks.add(queuedCallback);
            }
        }
        return queuedCallback;
    }

    private boolean isValidSegmentId(long id) {
        return id != ContainerMetadata.NO_STREAM_SEGMENT_ID;
    }

    /**
     * Retries Future from the given supplier exactly once if encountering TooManyActiveSegmentsException.
     *
     * @param toTry A Supplier that returns a Future to execute. This will be invoked either once or twice.
     * @param <T>   Return type of Future.
     * @return A CompletableFuture with the result, or failure cause.
     */
    private <T> CompletableFuture<T> retryWithCleanup(Supplier<CompletableFuture<T>> toTry) {
        CompletableFuture<T> result = new CompletableFuture<>();
        toTry.get()
             .thenAccept(result::complete)
             .exceptionally(ex -> {
                 // Check if the exception indicates the Metadata has reached capacity. In that case, force a cleanup
                 // and try again, exactly once.
                 try {
                     if (Exceptions.unwrap(ex) instanceof TooManyActiveSegmentsException) {
                         log.debug("{}: Forcing metadata cleanup due to capacity exceeded ({}).", this.traceObjectId,
                                 Exceptions.unwrap(ex).getMessage());

                         CompletableFuture<T> f = this.connector.getMetadataCleanup().get().thenComposeAsync(v -> toTry.get(), this.executor);
                         f.thenAccept(result::complete);
                         Futures.exceptionListener(f, result::completeExceptionally);
                     } else {
                         result.completeExceptionally(ex);
                     }
                 } catch (Throwable t) {
                     result.completeExceptionally(t);
                     throw t;
                 }

                 return null;
             });

        return result;
    }

    //endregion

    //region Helper Classes

    /**
     * Mediates communication between a MetadataStore instance and an external caller.
     */
    @RequiredArgsConstructor
    @Getter
    static class Connector {
        /**
         * The {@link ContainerMetadata} to bind to. All assignments are vetted from here, but the Metadata is not touched
         * directly from this component.
         */
        @NonNull
        private final ContainerMetadata containerMetadata;

        /**
         * A Function that, when invoked, updates the metadata with a new Segment Id assignment.
         */
        @NonNull
        private final MapSegmentId mapSegmentId;

        /**
         * A Function that, when invoked, immediately deletes a Segment. This is invoked when it is certain that the Segment
         * is not used at all, either because it is not loaded in the Metadata or because its Metadata Entry is corrupt.
         */
        @NonNull
        private final DirectDeleteSegment directDeleteSegment;

        /**
         * A Function that, when invoked, submits a Segment for eventual deletion. This will be invoked when it is possible
         * that the Segment is still actively being used, and we need a graceful deletion.
         */
        @NonNull
        private final LazyDeleteSegment lazyDeleteSegment;

        /**
         * A Supplier that, when invoked, executes the Segment Container's Metadata Cleanup task which is responsible with
         * evicting inactive Segments. This is invoked when a Segment Id assignment is in progress which cannot complete
         * due to {@link TooManyActiveSegmentsException} being thrown.
         */
        @NonNull
        private Supplier<CompletableFuture<Void>> metadataCleanup;

        @FunctionalInterface
        public interface MapSegmentId {
            CompletableFuture<Long> apply(long segmentId, SegmentProperties segmentProperties, boolean pin, Duration timeout);
        }

        @FunctionalInterface
        public interface DirectDeleteSegment {
            CompletableFuture<Void> apply(String segmentName, Duration timeout);
        }

        @FunctionalInterface
        public interface LazyDeleteSegment {
            CompletableFuture<Void> apply(long segmentId, Duration timeout);
        }
    }

    /**
     * A pending request for a Segment Assignment, which keeps track of all queued callbacks.
     * Note that this class in itself is not thread safe, so the caller should take precautions to ensure thread safety.
     */
    @NotThreadSafe
    private static class PendingRequest {
        private final ArrayList<QueuedCallback<?>> callbacks = new ArrayList<>();

        /**
         * Invokes all queued callbacks, in order, with the given SegmentId as a parameter.
         */
        void complete(long segmentId) {
            for (QueuedCallback<?> callback : this.callbacks) {
                try {
                    callback.complete(segmentId);
                } catch (Throwable ex) {
                    callback.completeExceptionally(ex);
                }
            }
        }

        /**
         * Invokes all queued callbacks, in order, with the given Throwable as a failure cause.
         */
        void completeExceptionally(Throwable ex) {
            for (QueuedCallback<?> callback : this.callbacks) {
                callback.completeExceptionally(ex);
            }
        }
    }

    /**
     * A single callback that is queued up for a Pending Request. The 'result' is what is returned to the caller, which
     * is completed indirectly with the result of the invocation to 'callback'.
     */
    @RequiredArgsConstructor
    private static class QueuedCallback<T> {
        final CompletableFuture<T> result = new CompletableFuture<>();
        final Function<Long, CompletableFuture<T>> callback;

        void complete(long segmentId) {
            Futures.completeAfter(() -> this.callback.apply(segmentId), this.result);
        }

        void completeExceptionally(Throwable ex) {
            this.result.completeExceptionally(ex);
        }
    }

    @Data
    @Builder
    protected static class SegmentInfo {
        private static final SegmentInfoSerializer SERIALIZER = new SegmentInfoSerializer();
        private final long segmentId;
        private final SegmentProperties properties;

        static SegmentInfo newSegment(String name, Collection<AttributeUpdate> attributeUpdates) {
            val infoBuilder = StreamSegmentInformation
                    .builder()
                    .name(name);
            if (attributeUpdates != null) {
                infoBuilder.attributes(attributeUpdates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue)));
            }

            return builder()
                    .segmentId(ContainerMetadata.NO_STREAM_SEGMENT_ID)
                    .properties(infoBuilder.build())
                    .build();
        }

        @SneakyThrows(IOException.class)
        static ArrayView serialize(SegmentInfo state) {
            return SERIALIZER.serialize(state);
        }

        @SneakyThrows(IOException.class)
        static SegmentInfo deserialize(ArrayView contents) {
            try {
                return SERIALIZER.deserialize(contents);
            } catch (EOFException | SerializationException ex) {
                throw new CompletionException(new DataCorruptionException("Unable to deserialize Segment Info.", ex));
            }
        }

        static class SegmentInfoBuilder implements ObjectBuilder<SegmentInfo> {
        }

        private static class SegmentInfoSerializer extends VersionedSerializer.WithBuilder<SegmentInfo, SegmentInfo.SegmentInfoBuilder> {
            @Override
            protected SegmentInfo.SegmentInfoBuilder newBuilder() {
                return SegmentInfo.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(SegmentInfo s, RevisionDataOutput output) throws IOException {
                output.writeLong(s.getSegmentId());
                SegmentProperties sp = s.getProperties();
                output.writeUTF(sp.getName());
                output.writeLong(sp.getLength());
                output.writeLong(sp.getStartOffset());
                output.writeBoolean(sp.isSealed());

                // We only serialize Core Attributes. Extended Attributes can be retrieved from the AttributeIndex.
                output.writeMap(Attributes.getCoreNonNullAttributes(sp.getAttributes()), RevisionDataOutput::writeUUID, RevisionDataOutput::writeLong);
            }

            private void read00(RevisionDataInput input, SegmentInfo.SegmentInfoBuilder builder) throws IOException {
                builder.segmentId(input.readLong());
                val infoBuilder = StreamSegmentInformation
                        .builder()
                        .name(input.readUTF())
                        .length(input.readLong())
                        .startOffset(input.readLong())
                        .sealed(input.readBoolean());
                infoBuilder.attributes(input.readMap(RevisionDataInput::readUUID, RevisionDataInput::readLong));
                builder.properties(infoBuilder.build());
            }
        }
    }

    //endregion
}

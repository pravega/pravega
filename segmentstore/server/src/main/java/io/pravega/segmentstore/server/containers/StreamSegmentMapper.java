/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncMap;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.TooManyActiveSegmentsException;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
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
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Helps assign unique Ids to StreamSegments and persists them in Metadata.
 */
@Slf4j
@ThreadSafe
public class StreamSegmentMapper extends SegmentStateMapper {
    //region Members

    private final String traceObjectId;
    private final ContainerMetadata containerMetadata;
    private final OperationLog durableLog;
    private final Supplier<CompletableFuture<Void>> metadataCleanup;
    private final Executor executor;
    @GuardedBy("assignmentLock")
    private final HashMap<String, PendingRequest> pendingRequests;
    private final Object assignmentLock = new Object();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentMapper class.
     *
     * @param containerMetadata The StreamSegmentContainerMetadata to bind to. All assignments are vetted from here,
     *                          but the Metadata is not touched directly from this component.
     * @param durableLog        The Durable Log to bind to. All assignments are durably stored here.
     * @param stateStore        A AsyncMap that can be used to store Segment State.
     * @param metadataCleanup   A callback returning a CompletableFuture that will be invoked when a forced metadata cleanup
     *                          is requested.
     * @param storage           The Storage to use for all external operations (create segment, get info, etc.)
     * @param executor          The executor to use for async operations.
     */
    public StreamSegmentMapper(ContainerMetadata containerMetadata, OperationLog durableLog, AsyncMap<String, SegmentState> stateStore,
                               Supplier<CompletableFuture<Void>> metadataCleanup, Storage storage, Executor executor) {
        super(stateStore, storage);
        Preconditions.checkNotNull(containerMetadata, "containerMetadata");
        Preconditions.checkNotNull(durableLog, "durableLog");
        Preconditions.checkNotNull(metadataCleanup, "metadataCleanup");
        Preconditions.checkNotNull(executor, "executor");

        this.traceObjectId = String.format("StreamSegmentMapper[%d]", containerMetadata.getContainerId());
        this.containerMetadata = containerMetadata;
        this.durableLog = durableLog;
        this.metadataCleanup = metadataCleanup;
        this.executor = executor;
        this.pendingRequests = new HashMap<>();
    }

    //endregion

    //region Create Segments

    /**
     * Creates a new StreamSegment with given name (in Storage) and persists the given attributes (in Storage).
     *
     * @param streamSegmentName The case-sensitive StreamSegment Name.
     * @param attributes        The initial attributes for the StreamSegment, if any.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed normally.
     * If the operation failed, this will contain the exception that caused the failure.
     */
    public CompletableFuture<Void> createNewStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, traceObjectId, "createNewStreamSegment", streamSegmentName);
        long segmentId = this.containerMetadata.getStreamSegmentId(streamSegmentName, true);
        if (isValidStreamSegmentId(segmentId)) {
            // Quick fail: see if this is an active Segment, and if so, don't bother with anything else.
            return Futures.failedFuture(new StreamSegmentExistsException(streamSegmentName));
        }

        CompletableFuture<Void> result = createSegmentInStorageWithRecovery(streamSegmentName, attributes, new TimeoutTimer(timeout));
        if (log.isTraceEnabled()) {
            result.thenAccept(v -> LoggerHelpers.traceLeave(log, traceObjectId, "createNewStreamSegment", traceId, streamSegmentName));
        }

        return result;
    }

    /**
     * Attempts to create the given Segment in Storage, with possible recovery from a previous incomplete
     * attempt. When this method completes successfully, the Storage will have the Segment created, as well as a State File
     * with the appropriate contents.
     * <p>
     * The recovery part handles these three major cases:
     * <ul>
     * <li>Segment exists and has a valid State File: the operation will fail with StreamSegmentExistsException.
     * <li>Segment exists, has a length of zero, and a missing or invalid State File: the state file will be recreated using
     * the given attributes and the operation will complete successfully (pending a successful State File creation).
     * <li>Segment exists, has a non-zero length, and either a valid or invalid/missing State File: the state file will be
     * recreated (if needed) using the given attributes and the operation will fail with StreamSegmentExistsException.
     * </ul>
     *
     * @param segmentName The name of the Segment to create.
     * @param attributes  The initial Attributes for the Segment, if any.
     * @param timer       A TimeoutTimer to determine how much time is left to complete the operation.
     * @return A CompletableFuture that, when completed, will indicate that the Segment has been successfully created.
     */
    private CompletableFuture<Void> createSegmentInStorageWithRecovery(String segmentName, Collection<AttributeUpdate> attributes, TimeoutTimer timer) {
        SegmentRollingPolicy rollingPolicy = getRollingPolicy(attributes);

        return Futures
                .exceptionallyCompose(
                        this.storage.create(segmentName, rollingPolicy, timer.getRemaining()),
                        ex -> handleStorageCreateException(segmentName, Exceptions.unwrap(ex), timer))
                .thenComposeAsync(segmentProps -> saveState(segmentProps, attributes, timer.getRemaining())
                                .thenRunAsync(() -> {
                                    // Need to create the state file before we throw any further exceptions in order to recover from
                                    // previous partial executions (where we created a segment but no or empty state file).
                                    if (segmentProps.getLength() > 0) {
                                        throw new CompletionException(new StreamSegmentExistsException(segmentName));
                                    }
                                }, this.executor),
                        this.executor);
    }

    /**
     * Exception handler in the case when a Segment fails to be created in Storage. This only handles
     * StreamSegmentExistsException; all other exceptions are "bubbled" up automatically.
     * <p>
     * This method simply checks the integrity of the State File; if it exists and is valid, then the Segment is considered
     * fully created and the original exception is bubbled up. If the State File does not exist or it is not valid, then
     * the most up-to-date information about the segment is returned (as it exists in Storage).
     *
     * @param segmentName       The name of the Segment involved.
     * @param originalException The exception that triggered this.
     * @param timer             A TimeoutTimer to determine how much time is left to complete the operation.
     * @return A CompletableFuture that, when completed normally, will contain information about the Segment. If the Segment
     * already exists or another error happened, this will be completed with the appropriate exception.
     */
    private CompletableFuture<SegmentProperties> handleStorageCreateException(String segmentName, Throwable originalException, TimeoutTimer timer) {
        if (!(originalException instanceof StreamSegmentExistsException)) {
            // Some other kind of exception that we can't handle here.
            return Futures.failedFuture(originalException);
        }

        return getState(segmentName, timer.getRemaining())
                .exceptionally(ex -> {
                    ex = Exceptions.unwrap(ex);
                    if (ex instanceof StreamSegmentNotExistsException || ex instanceof DataCorruptionException) {
                        // Segment exists, but the State File is missing or corrupt. We have the data needed to rebuild it,
                        // so ignore any exceptions coming this way.
                        log.warn("{}: Missing or corrupt State File for existing Segment '{}'; recreating.", this.traceObjectId, segmentName, ex);
                        return null;
                    }

                    // All other exceptions need to be bubbled up.
                    throw new CompletionException(ex);
                })
                .thenComposeAsync(s -> {
                    if (s == null) {
                        // Segment exists, but no (or corrupted) State File - move on.
                        return this.storage.getStreamSegmentInfo(segmentName, timer.getRemaining());
                    } else {
                        // Both Segment and State File exist; nothing to rebuild, so re-throw original exception.
                        return Futures.failedFuture(originalException);
                    }
                }, this.executor);
    }

    //endregion

    //region GetSegmentInfo

    /**
     * Gets information about a StreamSegment. If the Segment is active, it returns this information directly from the
     * in-memory Metadata. If the Segment is not active, it fetches the information from Storage and returns it, without
     * activating the segment in the Metadata or otherwise touching the DurableLog.
     *
     * @param streamSegmentName The case-sensitive StreamSegment Name.
     * @param timeout           Timeout for the Operation.
     * @return A CompletableFuture that, when complete, will contain a SegmentProperties object with the desired
     * information. If failed, it will contain the exception that caused the failure.
     */
    CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        long streamSegmentId = this.containerMetadata.getStreamSegmentId(streamSegmentName, true);
        CompletableFuture<SegmentProperties> result;
        if (isValidStreamSegmentId(streamSegmentId)) {
            // Looks like the Segment is active and we have it in our Metadata. Return the result from there.
            SegmentMetadata sm = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
            if (sm.isDeleted() || sm.isMerged()) {
                result = Futures.failedFuture(new StreamSegmentNotExistsException(streamSegmentName));
            } else {
                result = CompletableFuture.completedFuture(sm.getSnapshot());
            }
        } else {
            // The Segment is not yet active.
            // First, check to see if we have a pending assignment. If so, piggyback on that.
            QueuedCallback<SegmentProperties> queuedCallback = checkConcurrentAssignment(streamSegmentName,
                    id -> CompletableFuture.completedFuture(this.containerMetadata.getStreamSegmentMetadata(id).getSnapshot()));

            if (queuedCallback != null) {
                result = queuedCallback.result;
            } else {
                // Not in metadata and no concurrent assignments. Go to Storage and get what's needed.
                result = getSegmentInfoFromStorage(streamSegmentName, timeout);
            }
        }

        return result;
    }

    //endregion

    //region Segment Id Assignment

    /**
     * Attempts to get an existing StreamSegmentId for the given case-sensitive StreamSegment Name, and then invokes the
     * given Function with the Id.
     * * If the Segment is already mapped in the Metadata, the existing Id is used.
     * * Otherwise if the Segment had previously been assigned an id (and saved in the State Store), that Id will be
     * reused.
     * * Otherwise, it atomically assigns a new Id and stores it in the Metadata and DurableLog.
     *
     * If multiple requests for assignment arrive for the same StreamSegment in parallel (or while an assignment is in progress),
     * they will be queued up in the order received and will be invoked in the same order after assignment
     *
     * @param streamSegmentName The case-sensitive StreamSegment Name.
     * @param timeout           The timeout for the operation.
     * @param thenCompose       A Function that consumes a StreamSegmentId and returns a CompletableFuture that will indicate
     *                          when the consumption of that StreamSegmentId is complete. This Function will be invoked
     *                          synchronously if the StreamSegmentId is already mapped, or async, otherwise, after assignment.
     * @param <T>               Type of the return value.
     * @return A CompletableFuture that, when completed normally, will contain the result of the given Function (thenCompose)
     * applied to the assigned/retrieved StreamSegmentId. If failed, this will contain the exception that caused the failure.
     */
    <T> CompletableFuture<T> getOrAssignStreamSegmentId(String streamSegmentName, Duration timeout, Function<Long, CompletableFuture<T>> thenCompose) {
        // Check to see if the metadata already knows about this Segment.
        Preconditions.checkNotNull(thenCompose, "thenCompose");
        long streamSegmentId = this.containerMetadata.getStreamSegmentId(streamSegmentName, true);
        if (isValidStreamSegmentId(streamSegmentId)) {
            // We already have a value, just return it (but make sure the Segment has not been deleted).
            if (this.containerMetadata.getStreamSegmentMetadata(streamSegmentId).isDeleted()) {
                return Futures.failedFuture(new StreamSegmentNotExistsException(streamSegmentName));
            } else {
                // Even though we have the value in the metadata, we need to be very careful not to invoke this callback
                // before any other existing callbacks are invoked. As such, verify if we have an existing PendingRequest
                // for this segment - if so, tag onto it so we invoke these callbacks in the correct order.
                QueuedCallback<T> queuedCallback = checkConcurrentAssignment(streamSegmentName, thenCompose);
                return queuedCallback == null ? thenCompose.apply(streamSegmentId) : queuedCallback.result;
            }
        }

        // See if anyone else is currently waiting to get this StreamSegment's id.
        QueuedCallback<T> queuedCallback;
        boolean needsAssignment = false;
        synchronized (this.assignmentLock) {
            PendingRequest pendingRequest = this.pendingRequests.getOrDefault(streamSegmentName, null);
            if (pendingRequest == null) {
                needsAssignment = true;
                pendingRequest = new PendingRequest();
                this.pendingRequests.put(streamSegmentName, pendingRequest);
            }

            queuedCallback = new QueuedCallback<>(thenCompose);
            pendingRequest.callbacks.add(queuedCallback);
        }

        // We are the first/only ones requesting this id; go ahead and assign an id.
        if (needsAssignment) {
            this.executor.execute(() -> assignStreamSegmentId(streamSegmentName, timeout));
        }

        return queuedCallback.result;
    }

    /**
     * Same as getOrAssignStreamSegmentId(String, Duration, Function) except that this simply returns a CompletableFuture
     * with the SegmentId.
     *
     * @param streamSegmentName The case-sensitive StreamSegment Name.
     * @param timeout           The timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the result of the given Function (thenCompose)
     * applied to the assigned/retrieved StreamSegmentId. If failed, this will contain the exception that caused the failure.
     */
    @VisibleForTesting
    public CompletableFuture<Long> getOrAssignStreamSegmentId(String streamSegmentName, Duration timeout) {
        return getOrAssignStreamSegmentId(streamSegmentName, timeout, CompletableFuture::completedFuture);
    }

    /**
     * Attempts to map a StreamSegment to an Id, by first trying to retrieve an existing id, and, should that not exist,
     * assign a new one.
     *
     * @param streamSegmentName The name of the StreamSegment to map.
     * @param timeout           Timeout for the operation.
     */
    private void assignStreamSegmentId(String streamSegmentName, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        Futures.exceptionListener(
                this.storage.getStreamSegmentInfo(streamSegmentName, timer.getRemaining())
                            .thenComposeAsync(si -> attachState(si, timer.getRemaining()), this.executor)
                            .thenComposeAsync(si -> submitToOperationLogWithRetry(si, timer.getRemaining()), this.executor),
                ex -> failAssignment(streamSegmentName, ex));
    }

    /**
     * Same as submitToOperationLog, but retries exactly once in case TooManyActiveSegmentsException was encountered, in
     * which case it forces a metadata cleanup before retrying. If the second attempt also fails, there will be no more retry
     * and the Exception from the second failure will be the one that this call fails with too.
     *
     * @param segmentInfo           The SegmentInfo for the StreamSegment to generate and persist.
     * @param timeout               Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the internal SegmentId that was assigned (or the
     * one supplied via SegmentInfo, if any). If the operation failed, then this Future will complete with that exception.
     */
    private CompletableFuture<Long> submitToOperationLogWithRetry(SegmentInfo segmentInfo, Duration timeout) {
        return retryWithCleanup(() -> submitToOperationLog(segmentInfo, timeout));
    }

    /**
     * Submits a StreamSegmentMapOperation to the OperationLog. Upon completion, this operation
     * will have mapped the given Segment to a new internal Segment Id if none was provided in the given SegmentInfo.
     * If the given SegmentInfo already has a SegmentId set, then all efforts will be made to map that Segment with the
     * requested Segment Id.
     *
     * @param segmentInfo           The SegmentInfo for the StreamSegment to generate and persist.
     * @param timeout               Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the internal SegmentId that was assigned (or the
     * one supplied via SegmentInfo, if any). If the operation failed, then this Future will complete with that exception.
     */
    private CompletableFuture<Long> submitToOperationLog(SegmentInfo segmentInfo, Duration timeout) {
        SegmentProperties properties = segmentInfo.getProperties();
        if (properties.isDeleted()) {
            // Stream does not exist. Fail the request with the appropriate exception.
            failAssignment(properties.getName(), new StreamSegmentNotExistsException("StreamSegment does not exist."));
            return Futures.failedFuture(new StreamSegmentNotExistsException(properties.getName()));
        }

        long existingSegmentId = this.containerMetadata.getStreamSegmentId(properties.getName(), true);
        if (isValidStreamSegmentId(existingSegmentId)) {
            // Looks like someone else beat us to it.
            completeAssignment(properties.getName(), existingSegmentId);
            return CompletableFuture.completedFuture(existingSegmentId);
        } else {
            StreamSegmentMapOperation op = new StreamSegmentMapOperation(properties);
            if (segmentInfo.getSegmentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                op.setStreamSegmentId(segmentInfo.getSegmentId());
            }

            return this.durableLog.add(op, timeout)
                    .thenApply(seqNo -> completeAssignment(properties.getName(), op.getStreamSegmentId()));
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
            synchronized (this.assignmentLock) {
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
     * @param streamSegmentName The Name of the StreamSegment to attempt to piggyback on.
     * @param thenCompose       A Function that consumes a StreamSegmentId and returns a CompletableFuture that will indicate
     *                          when the consumption of that StreamSegmentId is complete. This Function will be invoked
     *                          synchronously if the StreamSegmentId is already mapped, or async, otherwise, after assignment.
     * @param <T>               Type of the return value.
     * @return A QueuedCallback representing the callback object for this task, if it was piggybacked on any existing
     * assignment. If no assignment was found, returns null.
     */
    private <T> QueuedCallback<T> checkConcurrentAssignment(String streamSegmentName, Function<Long, CompletableFuture<T>> thenCompose) {
        QueuedCallback<T> queuedCallback = null;
        synchronized (this.assignmentLock) {
            PendingRequest pendingRequest = this.pendingRequests.getOrDefault(streamSegmentName, null);
            if (pendingRequest != null) {
                queuedCallback = new QueuedCallback<>(thenCompose);
                pendingRequest.callbacks.add(queuedCallback);
            }
        }
        return queuedCallback;
    }

    private boolean isValidStreamSegmentId(long id) {
        return id != ContainerMetadata.NO_STREAM_SEGMENT_ID;
    }

    /**
     * Extracts the SegmentRollingPolicy from the given AttributeUpdate Collection. If the list is empty or does not have
     * an Attributes.ROLLOVER_SIZE attribute, then a NO_ROLLING policy is returned.
     */
    private SegmentRollingPolicy getRollingPolicy(Collection<AttributeUpdate> attributes) {
        SegmentRollingPolicy rollingPolicy = SegmentRollingPolicy.NO_ROLLING;
        if (attributes != null) {
            AttributeUpdate a = attributes.stream().filter(au -> au.getAttributeId() == Attributes.ROLLOVER_SIZE).findFirst().orElse(null);
            if (a != null) {
                rollingPolicy = new SegmentRollingPolicy(a.getValue());
            }
        }

        return rollingPolicy;
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
                         CompletableFuture<T> f = this.metadataCleanup.get().thenComposeAsync(v -> toTry.get(), this.executor);
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

    //endregion
}

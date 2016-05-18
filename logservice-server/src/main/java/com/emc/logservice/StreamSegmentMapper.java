package com.emc.logservice;

import com.emc.logservice.core.TimeoutTimer;
import com.emc.logservice.logs.SequentialLog;
import com.emc.logservice.logs.operations.Operation;
import com.emc.logservice.logs.operations.StreamSegmentMapOperation;

import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Helps assign unique Ids to StreamSegments and persists them in Metadata.
 */
public class StreamSegmentMapper {
    //region Members

    private final StreamSegmentContainerMetadata containerMetadata;
    private final SequentialLog<Operation, Long> durableLog;
    private final HashMap<String, CompletableFuture<Long>> pendingRequests;
    private final HashSet<Long> pendingIdAssignments;
    private final Object SyncRoot = new Object();
    private final Object MetadataLock = new Object();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentMapper class.
     *
     * @param containerMetadata The StreamSegmentContainerMetadata to bind to. All assignments are vetted and stored here,
     *                          but the Metadata is not persisted with every assignment.
     * @param durableLog        The Durable Log to bind to. All assignments are durably stored here (the metadata is not persisted
     *                          with every stream map)
     * @throws NullPointerException If any of the arguments are null.
     */
    public StreamSegmentMapper(StreamSegmentContainerMetadata containerMetadata, SequentialLog<Operation, Long> durableLog) {
        if (containerMetadata == null) {
            throw new NullPointerException("containerMetadata");
        }

        if (durableLog == null) {
            throw new NullPointerException("durableLog");
        }

        this.containerMetadata = containerMetadata;
        this.durableLog = durableLog;
        this.pendingRequests = new HashMap<>();
        this.pendingIdAssignments = new HashSet<>();
    }

    //endregion

    //region Operations

    /**
     * Attempts to get an existing StreamSegmentId for the given case-sensitive StreamSegment Name.
     * If no such mapping exists, atomically assigns a new one and stores it in the Metadata and DurableLog.
     * <p>
     * If multiple requests for assignment arrive for the same StreamSegment in parallel, the subsequent ones (after the
     * first one) will wait for the first one to complete and return the same result (this will not result in double-assignment).
     *
     * @param streamSegmentName The case-sensitive StreamSegment Name.
     * @param timeout           The timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the StreamSegment Id requested. If the operation
     * failed, this will contain the exception that caused the failure.
     */
    public CompletableFuture<Long> getOrAssignStreamSegmentId(String streamSegmentName, Duration timeout) {
        // Check to see if the metadata already knows about this stream.
        long streamId = this.containerMetadata.getStreamSegmentId(streamSegmentName);
        if (streamId >= 0) {
            // We already have a value, just return it.
            return CompletableFuture.completedFuture(streamId);
        }

        // See if anyone else is currently waiting to get this stream's id.
        CompletableFuture<Long> result;
        boolean needsAssignment = false;
        synchronized (SyncRoot) {
            result = this.pendingRequests.getOrDefault(streamSegmentName, null);
            if (result == null) {
                needsAssignment = true;
                result = new CompletableFuture<>();
                this.pendingRequests.put(streamSegmentName, result);
            }
        }

        // We are the first/only ones requesting this id; go ahead and assign an id.
        if (needsAssignment) {
            //TODO: use a better thread pool.
            CompletableFuture.runAsync(() -> assignStreamId(streamSegmentName, timeout));
        }

        return result;
    }

    /**
     * Assigns a new Id to the given StreamSegmentName.
     *
     * @param streamSegmentName
     * @param timeout
     */
    private void assignStreamId(String streamSegmentName, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        CompletableFuture<Void> result = getStorageStreamInfo(streamSegmentName, timer.getRemaining())
                .thenCompose(streamInfo -> persistInDurableLog(streamInfo, timer.getRemaining()));

        result.exceptionally(ex ->
        {
            failAssignment(streamSegmentName, StreamSegmentContainerMetadata.NoStreamSegmentId, ex);
            throw new CompletionException(ex);
        });
    }

    /**
     * Stores the Mapping in DurableLog.
     *
     * @param streamSegmentInfo
     * @param timeout
     * @return
     */
    private CompletableFuture<Void> persistInDurableLog(StreamSegmentInformation streamSegmentInfo, Duration timeout) {
        if (streamSegmentInfo.isDeleted()) {
            // Stream does not exist. Fail the request with the appropriate exception.
            failAssignment(streamSegmentInfo.getStreamSegmentName(), StreamSegmentContainerMetadata.NoStreamSegmentId, new FileNotFoundException("Stream does not exist."));
            return CompletableFuture.completedFuture(null);
        }

        long streamId = this.containerMetadata.getStreamSegmentId(streamSegmentInfo.getStreamSegmentName());
        if (streamId >= 0) {
            // Looks like someone else beat us to it.
            completeAssignment(streamSegmentInfo.getStreamSegmentName(), streamId);
            return CompletableFuture.completedFuture(null);
        }
        else {
            final long newStreamId = generateUniqueStreamId(streamSegmentInfo.getStreamSegmentName());
            CompletableFuture<Long> logAddResult = this.durableLog.add(new StreamSegmentMapOperation(newStreamId, streamSegmentInfo), timeout);
            return logAddResult.thenAccept(seqNo ->
            {
                updateMetadata(streamSegmentInfo.getStreamSegmentName(), newStreamId, streamSegmentInfo);
                completeAssignment(streamSegmentInfo.getStreamSegmentName(), newStreamId);
            });
        }
    }

    /**
     * Retrieves information about the StreamSegment from Storage.
     *
     * @param streamSegmentName
     * @param timeout
     * @return
     */
    private CompletableFuture<StreamSegmentInformation> getStorageStreamInfo(String streamSegmentName, Duration timeout) {
        // TODO: go to storage to figure out details (stream length, sealed, exists?)
        return CompletableFuture.completedFuture(new StreamSegmentInformation(streamSegmentName, 0, false, false, new Date()));
    }

    /**
     * Updates metadata with the new mapping.
     *
     * @param streamSegmentName
     * @param streamSegmentId
     * @param streamInfo
     */
    private void updateMetadata(String streamSegmentName, long streamSegmentId, StreamSegmentInformation streamInfo) {
        synchronized (MetadataLock) {
            // Map it to the stream name and update the Stream Metadata.
            this.containerMetadata.mapStreamSegmentId(streamSegmentName, streamSegmentId);
            StreamSegmentMetadata sm = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
            sm.setStorageLength(streamInfo.getLength());
            sm.setDurableLogLength(streamInfo.getLength()); // TODO: this will need to be set/reset in recovery. This is the default (failback) value.

            if (streamInfo.isSealed()) {
                sm.markSealed();
            }

            // No need to 'markDeleted()' because that would have triggered an exception upstream and we
            // wouldn't have gotten here in the first place.
        }
    }

    /**
     * Generates a unique StreamSegment Id that does not currently exist in the Metadata or in the pending Id assingments.
     *
     * @param streamSegmentName
     * @return
     */
    private long generateUniqueStreamId(String streamSegmentName) {
        // Get the last 32 bits of the current time (in millis), and move those to the upper portion of our ID.
        long streamId = System.currentTimeMillis() << 32;
        streamId |= streamSegmentName.hashCode() & 0xffffffffL;
        synchronized (SyncRoot) {
            while (this.containerMetadata.getStreamSegmentMetadata(streamId) != null || this.pendingIdAssignments.contains(streamId)) {
                streamId++;
            }

            this.pendingIdAssignments.add(streamId);
        }

        return streamId;
    }

    /**
     * Completes the assignment for the given StreamSegmentName by completing the waiting CompletableFuture.
     *
     * @param streamSegmentName
     * @param streamSegmentId
     */
    private void completeAssignment(String streamSegmentName, long streamSegmentId) {
        // Get the pending request and complete it.
        CompletableFuture<Long> pendingRequest;
        synchronized (SyncRoot) {
            pendingRequest = this.pendingRequests.getOrDefault(streamSegmentName, null);
            this.pendingRequests.remove(streamSegmentName);
            this.pendingIdAssignments.remove(streamSegmentId);
        }

        if (pendingRequest != null) {
            pendingRequest.complete(streamSegmentId);
        }
    }

    /**
     * Fails the assignment for the given StreamSegment Id with the given reason.
     *
     * @param streamSegmentName Required.
     * @param streamSegmentId   Optional
     * @param reason
     */
    private void failAssignment(String streamSegmentName, long streamSegmentId, Throwable reason) {
        // Get the pending request and complete it.
        CompletableFuture<Long> pendingRequest;
        synchronized (SyncRoot) {
            pendingRequest = this.pendingRequests.getOrDefault(streamSegmentName, null);
            this.pendingRequests.remove(streamSegmentName);
            this.pendingIdAssignments.remove(streamSegmentId);
        }

        if (pendingRequest != null) {
            pendingRequest.completeExceptionally(reason);
        }
    }

    //endregion
}

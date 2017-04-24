/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.server.writer;

import io.pravega.service.server.UpdateableSegmentMetadata;
import io.pravega.service.server.logs.operations.Operation;
import java.io.InputStream;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Data Source for a StorageWriter
 */
interface WriterDataSource {
    /**
     * Gets a value indicating the Id of the StreamSegmentContainer this WriterDataSource is for.
     */
    int getId();

    /**
     * Acknowledges that all Operations with Sequence Numbers up to and including the given one have been successfully committed.
     *
     * @param upToSequence The Sequence up to where to acknowledge.
     * @param timeout      The timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the acknowledgment completed. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Void> acknowledge(long upToSequence, Duration timeout);

    /**
     * Reads a number of entries from the Data Source.
     *
     * @param afterSequence The Sequence of the last entry before the first one to read.
     * @param maxCount      The maximum number of entries to read.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain an Iterator with the result. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Iterator<Operation>> read(long afterSequence, int maxCount, Duration timeout);

    /**
     * Indicates that the given sourceStreamSegmentId is merged into the given targetStreamSegmentId.
     *
     * @param targetStreamSegmentId The Id of the StreamSegment to merge into.
     * @param sourceStreamSegmentId The Id of the StreamSegment to merge.
     */
    void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId);

    /**
     * Indicates that the metadata for the given StreamSegmentId has been updated to reflect the most accurate length
     * of the Segment in Storage.
     * @param streamSegmentId The Id of the StreamSegment to notify on.
     */
    void notifyStorageLengthUpdated(long streamSegmentId);

    /**
     * Gets an InputStream representing uncommitted data in a Segment.
     *
     * @param streamSegmentId The Id of the StreamSegment to fetch data for.
     * @param startOffset     The offset where to begin fetching data from.
     * @param length          The number of bytes to fetch.
     * @return An InputStream with the requested data, of the requested length, or null if not available.
     */
    InputStream getAppendData(long streamSegmentId, long startOffset, int length);

    /**
     * Gets a value indicating whether the given Operation Sequence Number is a valid Truncation Point, as set by
     * setValidTruncationPoint().
     *
     * @param operationSequenceNumber The Sequence number to query.
     */
    boolean isValidTruncationPoint(long operationSequenceNumber);

    /**
     * Gets a value representing the highest Truncation Point that is smaller than or equal to the given Sequence Number.
     *
     * @param operationSequenceNumber The Sequence number to query.
     */
    long getClosestValidTruncationPoint(long operationSequenceNumber);

    /**
     * Marks the StreamSegment as deleted in the Container Metadata.
     *
     * @param streamSegmentName The name of the StreamSegment to delete.
     */
    void deleteStreamSegment(String streamSegmentName);

    /**
     * Gets the StreamSegmentMetadata mapped to the given StreamSegment Id.
     *
     * @param streamSegmentId The Id of the StreamSegment to query for.
     * @return The mapped StreamSegmentMetadata, or null if none is.
     */
    UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId);
}

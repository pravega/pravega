/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.writer;

import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.Operation;
import java.time.Duration;
import java.util.Map;
import java.util.Queue;
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
     * Instructs the Data Source to durably persist the given Attributes, which have been collected for recently flushed
     * appends.
     *
     * @param streamSegmentId The Id of the StreamSegment to persist for.
     * @param attributes      The Attributes to persist (Key=AttributeId, Value=Attribute Value).
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed and contain a value
     * that would need to be passed to {@link #notifyAttributesPersisted}. If the operation failed, this Future
     * will complete with the appropriate exception.
     */
    CompletableFuture<Long> persistAttributes(long streamSegmentId, Map<AttributeId, Long> attributes, Duration timeout);

    /**
     * Indicates that a batch of Attributes for a Segment have been durably persisted in Storage (after an invocation of
     * {@link #persistAttributes}) and updates the required Segment's Core Attributes to keep track of the state.
     * of the current
     *
     * @param segmentId          The Id of the Segment to persist for.
     * @param segmentType        The {@link SegmentType} for the Segment.
     * @param rootPointer        The Root Pointer to set as {@link Attributes#ATTRIBUTE_SEGMENT_ROOT_POINTER} for the segment.
     * @param lastSequenceNumber The Sequence number of the last Operation that updated attributes. This will be set as
     *                           {@link Attributes#ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO} for the segment.
     * @param timeout            Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed. If the operation
     * failed, this Future will complete with the appropriate exception. Notable exceptions:
     * - {@link BadAttributeUpdateException}: If the rootPointer is less than the current value for {@link Attributes#ATTRIBUTE_SEGMENT_ROOT_POINTER}.
     */
    CompletableFuture<Void> notifyAttributesPersisted(long segmentId, SegmentType segmentType, long rootPointer,
                                                      long lastSequenceNumber, Duration timeout);

    /**
     * Instructs the DataSource to seal and compact the Attribute Index for the given Segment.
     *
     * @param streamSegmentId The Id of the StreamSegment to seal Attributes for.
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed. If the operation
     * failed, the Future will complete with the appropriate exception.
     */
    CompletableFuture<Void> sealAttributes(long streamSegmentId, Duration timeout);

    /**
     * Instructs the DataSource to delete the Attribute Index for the given Segment.
     *
     * @param segmentMetadata The metadata for the Segment to delete attributes for.
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed. If the operation
     * failed, the Future will complete with the appropriate exception.
     */
    CompletableFuture<Void> deleteAllAttributes(SegmentMetadata segmentMetadata, Duration timeout);

    /**
     * Reads a number of entries from the Data Source.
     *
     * @param maxCount The maximum number of entries to read.
     * @param timeout  Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain an Iterator with the result. If the operation
     * failed, this Future will complete with the appropriate exception.
     */
    CompletableFuture<Queue<Operation>> read(int maxCount, Duration timeout);

    /**
     * Indicates that the given sourceStreamSegmentId is merged into the given targetStreamSegmentId.
     *
     * @param targetStreamSegmentId The Id of the StreamSegment to merge into.
     * @param sourceStreamSegmentId The Id of the StreamSegment to merge.
     * @throws StreamSegmentNotExistsException If targetStreamSegmentId is mapped to a Segment that is marked as Deleted.
     */
    void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId) throws StreamSegmentNotExistsException;

    /**
     * Gets an InputStream representing uncommitted data in a Segment.
     *
     * @param streamSegmentId The Id of the StreamSegment to fetch data for.
     * @param startOffset     The offset where to begin fetching data from.
     * @param length          The number of bytes to fetch.
     * @return An {@link BufferView} with the requested data, of the requested length, or null if not available.
     */
    BufferView getAppendData(long streamSegmentId, long startOffset, int length);

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
     * Gets the StreamSegmentMetadata mapped to the given StreamSegment Id.
     *
     * @param streamSegmentId The Id of the StreamSegment to query for.
     * @return The mapped StreamSegmentMetadata, or null if none is.
     */
    UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId);
}

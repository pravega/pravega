/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.contracts.ReadResult;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;

/**
 * Defines a ReadIndex for StreamSegments, that allows adding data only at the end.
 */
public interface ReadIndex extends AutoCloseable {
    /**
     * Appends a range of bytes at the end of the Read Index for the given StreamSegmentId.
     *
     * @param streamSegmentId The Id of the StreamSegment to append to.
     * @param offset          The offset in the StreamSegment where to write this append. The offset must be at the end
     *                        of the StreamSegment as it exists in the ReadIndex.
     * @param data            The data to append.
     * @throws IllegalArgumentException If the offset does not match the expected value (end of StreamSegment in ReadIndex).
     * @throws IllegalArgumentException If the offset + data.length exceeds the metadata DurableLogLength of the StreamSegment.
     */
    void append(long streamSegmentId, long offset, byte[] data);

    /**
     * Executes Step 1 of the 2-Step Merge Process.
     * <ol>
     * <li>Step 1: The StreamSegments are merged (Source->Target@Offset) in Metadata and a ReadIndex Redirection is put in place.
     * At this stage, the Source still exists as a physical object in Storage, and we need to keep its ReadIndex around, pointing
     * to the old object.
     * <li>Step 2: The StreamSegments are physically merged in the Storage. The Source StreamSegment does not exist anymore.
     * The ReadIndex entries of the two Streams are actually joined together.
     * </ol>
     *
     * @param targetStreamSegmentId The Id of the StreamSegment to merge into.
     * @param offset                The offset in the Target StreamSegment where to merge the Source StreamSegment.
     *                              The offset must be at the end of the StreamSegment as it exists in the ReadIndex.
     * @param sourceStreamSegmentId The Id of the StreamSegment to merge.
     * @throws IllegalArgumentException If the offset does not match the expected value (end of StreamSegment in ReadIndex).
     * @throws IllegalArgumentException If the offset + SourceStreamSegment.length exceeds the metadata DurableLogLength
     *                                  of the target StreamSegment.
     */
    void beginMerge(long targetStreamSegmentId, long offset, long sourceStreamSegmentId);

    /**
     * Executes Step 2 of the 2-Step Merge Process. See 'beginMerge' for the description of the Merge Process.
     *
     * @param targetStreamSegmentId The Id of the StreamSegment to merge into.
     * @param sourceStreamSegmentId The Id of the StreamSegment to merge.
     * @throws IllegalArgumentException If the 'beginMerge' method was not called for the pair before.
     */
    void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId);

    /**
     * Reads a contiguous sequence of bytes of the given length starting at the given offset from the given Segment.
     * Every byte in the range must meet the following conditions:
     * <ul>
     * <li> It must exist in this segment. This excludes bytes from merged transactions and future reads.
     * <li> It must be part of data that is not yet committed to Storage (tail part) - as such, it must be fully in the cache.
     * </ul>
     * Notes:
     * <ul>
     * <li> This method allows reading from partially merged transactions (on which beginMerge was called but not completeMerge).
     * This is acceptable because this method is only meant to be used by internal clients (not by an outside request)
     * and it prebuilds the result into the returned InputStream.
     * <li> This method will not cause cache statistics to be updated. As such, Cache entry generations will not be
     * updated for those entries that are touched.
     * </ul>
     *
     * @param streamSegmentId The Id of the StreamSegment to read from.
     * @param startOffset     The offset in the StreamSegment where to start reading.
     * @param length          The number of bytes to read.
     * @return An InputStream containing the requested data, or null if all of the conditions of this read cannot be met.
     * @throws IllegalStateException    If the read index is in recovery mode.
     * @throws IllegalArgumentException If the parameters are invalid (offset, length or offset+length are not in the Segment's range).
     */
    InputStream readDirect(long streamSegmentId, long startOffset, int length);

    /**
     * Reads a number of bytes from the StreamSegment ReadIndex.
     *
     * @param streamSegmentId The Id of the StreamSegment to read from.
     * @param offset          The offset in the StreamSegment where to start reading from.
     * @param maxLength       The maximum number of bytes to read.
     * @param timeout         Timeout for the operation.
     * @return A ReadResult containing the data to be read.
     */
    ReadResult read(long streamSegmentId, long offset, int maxLength, Duration timeout);

    /**
     * Triggers all eligible pending Future Reads for the given StreamSegmentIds.
     *
     * @param streamSegmentIds The Ids of the StreamSegments to trigger reads for.
     */
    void triggerFutureReads(Collection<Long> streamSegmentIds);

    /**
     * Clears the entire contents of the ReadIndex.
     *
     * @throws IllegalStateException If the operation cannot be performed due to the current state of the system (for example,
     *                               if the system is in Recovery Mode).
     */
    void clear();

    /**
     * Removes all internal indices that point to the given StreamSegments, but only if they are marked as Deleted in
     * the metadata or missing metadata altogether (i.e., they have been recycled).
     *
     * @param segmentIds An Iterator of SegmentIds for the Segments to clean up. If this is null, then all the Segment Ids
     *                   registered in this ReadIndex are eligible for removal.
     */
    void cleanup(Iterator<Long> segmentIds);

    /**
     * Puts the ReadIndex in Recovery Mode. Some operations may not be available in Recovery Mode.
     *
     * @param recoveryMetadataSource The Metadata Source to use. This Metadata must be in sync with the ReadIndex base
     *                               ContainerMetadata. If, upon exiting recovery mode, they disagree, it could lead to
     *                               serious errors, and will be reported as DataCorruptionExceptions (see exitRecoveryMode).
     * @throws IllegalStateException If the ReadIndex is already in recovery mode.
     * @throws NullPointerException  If the parameter is null.
     */
    void enterRecoveryMode(ContainerMetadata recoveryMetadataSource);

    /**
     * Takes the ReadIndex out of Recovery Mode, enabling all operations.
     *
     * @param successfulRecovery Indicates whether recovery was successful. If not, the ReadIndex may be cleared out to
     *                           avoid further issues.
     * @throws IllegalStateException   If the ReadIndex is already in recovery mode.
     * @throws NullPointerException    If the parameter is null.
     * @throws DataCorruptionException If the new Metadata Store does not contain information about a StreamSegment in
     *                                 the Read Index or it has conflicting information about it.
     */
    void exitRecoveryMode(boolean successfulRecovery) throws DataCorruptionException;

    @Override
    void close();
}

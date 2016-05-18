package com.emc.logservice;

import java.time.Duration;
import java.util.Collection;

/**
 * Defines a cache for StreamSegments, that allows adding data only at the end.
 */
public interface StreamSegmentCache extends AutoCloseable {
    /**
     * Appends a range of bytes at the end of the Read Index for the given StreamSegmentId.
     *
     * @param streamSegmentId The Id of the StreamSegment to add to.
     * @param offset          The offset in the StreamSegment where to write this add. The offset must be at the end
     *                        of the StreamSegment as it exists in the cache.
     * @param data            The data to add.
     * @throws IllegalArgumentException If the offset does not match the expected value (end of stream in Cache).
     * @throws IllegalArgumentException If the offset + data.length exceeds the metadata DurableLogLength of the StreamSegment.
     */
    void append(long streamSegmentId, long offset, byte[] data);

    /**
     * Executes Step 1 of the 2-Step Merge Process.
     * <ol>
     * <li>Step 1: The StreamSegments are merged (Source->Target@Offset) in Metadata and a Cache Redirection is put in place.
     * At this stage, the Source still exists as a physical object in Storage, and we need to keep its Cache around, pointing
     * to the old object.
     * <li>Step 2: The StreamSegments are physically merged in the Storage. The Source StreamSegment does not exist anymore.
     * The Cache entries of the two Streams are actually joined together.
     * </ol>
     *
     * @param targetStreamSegmentId     The Id of the StreamSegment to merge into.
     * @param offset                    The offset in the Target StreamSegment where to merge the Source StreamSegment.
     *                                  The offset must be at the end of the StreamSegment as it exists in the cache.
     * @param sourceStreamSegmentId     The Id of the StreamSegment to merge.
     * @param sourceStreamSegmentLength The length of the Source StreamSegment. This number is only used for verification
     *                                  against the actual length of the StreamSegment in the Cache.
     * @throws IllegalArgumentException If the offset does not match the expected value (end of StreamSegment in Cache).
     * @throws IllegalArgumentException If the offset + SourceStreamSegment.length exceeds the metadata DurableLogLength
     *                                  of the target StreamSegment.
     */
    void beginMerge(long targetStreamSegmentId, long offset, long sourceStreamSegmentId, long sourceStreamSegmentLength);

    /**
     * Executes Step 2 of the 2-Step Merge Process. See 'beginMerge' for the description of the Merge Process.
     *
     * @param targetStreamSegmentId The Id of the StreamSegment to merge into.
     * @param sourceStreamSegmentId The Id of the StreamSegment to merge.
     * @throws IllegalArgumentException If the 'beginMerge' method was not called for the pair before.
     */
    void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId);

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
     * Clears the entire contents of the Cache.
     *
     * @throws IllegalStateException If the operation cannot be performed due to the current state of the system.
     */
    void clear();

    /**
     * Puts the cache in Recovery Mode. Some operations may not be available in Recovery Mode.
     *
     * @param recoveryMetadataSource The Metadata Source to use.
     * @throws IllegalStateException If the Cache is already in recovery mode.
     * @throws NullPointerException  If the parameter is null.
     */
    void enterRecoveryMode(StreamSegmentMetadataSource recoveryMetadataSource);

    /**
     * Puts the Caceh out of Recovery Mode, enabling all operations.
     *
     * @param finalMetadataSource The Metadata Source to use.
     * @param success             Indicates whether recovery was successful. If not, the cache may be cleared out to
     *                            avoid further issues.
     * @throws IllegalStateException    If the Cache is already in recovery mode.
     * @throws NullPointerException     If the parameter is null.
     * @throws IllegalArgumentException If the new Metadata Store does not contain information about a StreamSegment in
     *                                  the Read Index or it has conflicting information about it.
     *                                  TODO: should this be different, like DataCorruptionException.
     */
    void exitRecoveryMode(StreamSegmentMetadataSource finalMetadataSource, boolean success);
}

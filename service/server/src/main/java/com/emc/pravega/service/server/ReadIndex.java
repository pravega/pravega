/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server;

import com.emc.pravega.service.contracts.ReadResult;

import java.time.Duration;
import java.util.Collection;

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
     * @throws IllegalArgumentException If the offset does not match the expected value (end of StreamSegment in
     *                                  ReadIndex).
     * @throws IllegalArgumentException If the offset + data.length exceeds the metadata DurableLogLength of the
     *                                  StreamSegment.
     */
    void append(long streamSegmentId, long offset, byte[] data);

    /**
     * Executes Step 1 of the 2-Step Merge Process.
     * <ol>
     * <li>Step 1: The StreamSegments are merged (Source->Target@Offset) in Metadata and a ReadIndex Redirection is
     * put in place.
     * At this stage, the Source still exists as a physical object in Storage, and we need to keep its ReadIndex
     * around, pointing
     * to the old object.
     * <li>Step 2: The StreamSegments are physically merged in the Storage. The Source StreamSegment does not exist
     * anymore.
     * The ReadIndex entries of the two Streams are actually joined together.
     * </ol>
     *
     * @param targetStreamSegmentId The Id of the StreamSegment to merge into.
     * @param offset                The offset in the Target StreamSegment where to merge the Source StreamSegment. The
     *                              offset must be at the end of the StreamSegment as it exists in the ReadIndex.
     * @param sourceStreamSegmentId The Id of the StreamSegment to merge.
     * @throws IllegalArgumentException If the offset does not match the expected value (end of StreamSegment in
     *                                  ReadIndex).
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
     * @throws IllegalStateException If the operation cannot be performed due to the current state of the system (for
     *                               example, if the system is in Recovery Mode).
     */
    void clear();

    /**
     * Removes all internal indices that point to StreamSegments that do not exist anymore (i.e., have been deleted).
     */
    void performGarbageCollection();

    /**
     * Puts the ReadIndex in Recovery Mode. Some operations may not be available in Recovery Mode.
     *
     * @param recoveryMetadataSource The Metadata Source to use. This Metadata must be in sync with the ReadIndex base
     *                               ContainerMetadata. If, upon exiting recovery mode, they disagree, it could lead to
     *                               serious errors, and will be reported as DataCorruptionExceptions (see
     *                               exitRecoveryMode).
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

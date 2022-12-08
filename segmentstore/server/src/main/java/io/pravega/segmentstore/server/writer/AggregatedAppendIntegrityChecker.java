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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/**
 * Helper class to check internal data integrity of AggregatedAppends at the StorageWriter level before flushing to LTS.
 * The main purpose of this class is to compute and keep track of hashes of Append contents upon ingestion and then
 * verify that the integrity of such Appends at the end of the ingestion pipeline (StorageWriter level).
 */
@Slf4j
class AggregatedAppendIntegrityChecker implements AutoCloseable {
    //region Members

    @GuardedBy("this")
    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private final Queue<AggregatedAppendIntegrityInfo> appendIntegrityInfo;
    @GuardedBy("this")
    private BufferView previousPartialAppendData;
    private final String traceObjectId;
    private final long segmentId;

    //endregion

    //region Constructor

    AggregatedAppendIntegrityChecker(long containerId, long segmentId) {
        this.traceObjectId = String.format("AggregatedAppendIntegrityChecker[%d-%d]", containerId, segmentId);
        this.segmentId = segmentId;
        this.appendIntegrityInfo = new ArrayDeque<>();
        this.previousPartialAppendData = null;
    }

    //endregion

    //region Autocloseable

    @Override
    public synchronized void close() {
        this.appendIntegrityInfo.clear();
        this.previousPartialAppendData = null;
    }

    //endregion

    //region AppendIntegrityChecker Implementation

    /**
     * Adds the original information about the Append, including the computed hash of its contents, to the
     * appendIntegrityInfo queue.
     *
     * @param segmentId Segment related to the Append.
     * @param offset Append offset.
     * @param length Append length.
     * @param hash Hash computed from the original Append contents.
     */
    public synchronized void addAppendIntegrityInfo(long segmentId, long offset, long length, long hash) {
        if (hash == StreamSegmentAppendOperation.NO_HASH) {
            // No data integrity checks enabled, do nothing.
            return;
        }
        Preconditions.checkArgument(this.segmentId == segmentId, "Appending integrity information for a wrong segment: " + segmentId);
        this.appendIntegrityInfo.add(new AggregatedAppendIntegrityInfo(offset, length, hash));
    }

    /**
     * Validates that the {@link BufferView} contents associated to multiple appends to be flushed to LTS match with the
     * individual Append hashes that were originally ingested (or recovered). This method also deals with cases in which
     * an Append may be split across two consecutive flushes.
     *
     * @param segmentId Segment related to the Append.
     * @param offset Initial offset where aggregatedAppends start.
     * @param aggregatedAppends Data contents of multiple Appends to eb flushed to LTS.
     * @throws DataCorruptionException If there is a mismatch between the original Append hashes and the contents of the
     * aggregatedAppends at the same offset.
     */
    synchronized void checkAppendIntegrity(long segmentId, long offset, BufferView aggregatedAppends) throws DataCorruptionException {
        Preconditions.checkArgument(this.segmentId == segmentId, "Appending integrity information for a wrong segment: " + segmentId);
        if (aggregatedAppends == null || this.appendIntegrityInfo.isEmpty()) {
            // No data to check, do nothing.
            return;
        }

        // Get the appends for queued for this specific Segment.
        Iterator<AggregatedAppendIntegrityInfo> iterator = this.appendIntegrityInfo.iterator();
        int accumulatedLength = 0;
        int checkedBytes = 0;
        AggregatedAppendIntegrityInfo integrityInfo;

        // If we had a partial append in the previous block, prepend it to the current aggregated appends to check integrity.
        if (this.previousPartialAppendData != null) {
            aggregatedAppends = BufferView.builder(2).add(this.previousPartialAppendData).add(aggregatedAppends).build();
            offset -= this.previousPartialAppendData.getLength();
            this.previousPartialAppendData = null;
        }

        while (iterator.hasNext() && accumulatedLength < aggregatedAppends.getLength()) {
            integrityInfo = iterator.next();
            if (integrityInfo.getContentHash() == StreamSegmentAppendOperation.NO_HASH || integrityInfo.getOffset() < offset) {
                // Old or not hashed operation, just remove.
                iterator.remove();
                continue;
            }

            // The available append integrity information belongs to a greater offset (e.g., data lost after a recovery).
            // We cannot check the data up to that offset, so skipping it.
            if (accumulatedLength == 0 && integrityInfo.getOffset() > offset) {
                long toSkip = integrityInfo.getOffset() - offset;
                accumulatedLength += toSkip;
                log.info("{}: Setting offset to check in aggregated append to {} (skipping {} bytes).", this.traceObjectId,
                        accumulatedLength, toSkip);
            }

            // Do the integrity check if the input data contains the whole contents of the original Append.
            // Otherwise, it means that the input data ends with a partial Append, and we need to handle it.
            if (aggregatedAppends.getLength() >= accumulatedLength + integrityInfo.getLength()) {
                long hash = aggregatedAppends.slice(accumulatedLength, (int) integrityInfo.getLength()).hash();
                if (hash != integrityInfo.getContentHash()) {
                    log.error("Append integrity check failed. SegmentId = {}, Offset = {}, Length = {}, Original Hash = {}, " +
                                    "Current Hash = {}.", segmentId, integrityInfo.getOffset() + accumulatedLength,
                               integrityInfo.getLength(), integrityInfo.getContentHash(), hash);
                    throw new DataCorruptionException(String.format("Data read from cache for Segment %s (hash = %s) " +
                                    "differs from original data appended (hash = %s).", this.segmentId, hash,
                            integrityInfo.getContentHash()));
                }
                checkedBytes = accumulatedLength;
            } else {
                // An Append has been split across 2 blocks of data to be flushed to LTS. We keep the first part of
                // the Append at the end of the current block, so it can be used in the next execution of this method
                // to check the hash of the Append using the second part of it at the beginning of the next block.
                previousPartialAppendData = aggregatedAppends.slice(accumulatedLength, aggregatedAppends.getLength() - accumulatedLength);
            }
            accumulatedLength += integrityInfo.getLength();
        }

        log.debug("{}: Checked integrity of {} bytes to be flushed to LTS ({} trailing bytes will be checked in next flush).",
                this.traceObjectId, checkedBytes, aggregatedAppends.getLength() - checkedBytes);
    }

    //endregion

    //region Helper Classes

    @Data
    private static class AggregatedAppendIntegrityInfo {
        private final long offset;
        private final long length;
        private final long contentHash;
    }

    //endregion
}

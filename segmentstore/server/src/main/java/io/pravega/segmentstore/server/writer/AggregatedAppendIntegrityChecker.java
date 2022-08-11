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

import com.google.common.base.Preconditions;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.server.DataCorruptionException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Helper class to check internal data integrity of AggregatedAppends at the StorageWriter level before flushing to LTS.
 * The main purpose of this class is to compute and keep track of hashes of Append contents upon ingestion and then
 * verify that the integrity of such Appends at the end of the ingestion pipeline (StorageWriter level).
 */
@Slf4j
public class AggregatedAppendIntegrityChecker implements AutoCloseable {
    //region Members

    // Represents that no hash has been computed for a given Append.
    public static final long NO_HASH = Long.MIN_VALUE;

    @GuardedBy("this")
    private final Queue<AggregatedAppendIntegrityInfo> appendIntegrityInfo;
    private final AtomicReference<BufferView> previousPartialAppendData;
    private final String traceObjectId;
    private final long segmentId;

    //endregion

    //region Constructor

    public AggregatedAppendIntegrityChecker(long containerId, long segmentId) {
        this.traceObjectId = String.format("AggregatedAppendIntegrityChecker[%d-%d]", containerId, segmentId);
        this.segmentId = segmentId;
        this.appendIntegrityInfo = new LinkedBlockingQueue<>();
        this.previousPartialAppendData = new AtomicReference<>();
    }

    //endregion

    //region Autocloseable

    @Override
    public synchronized void close() {
        this.appendIntegrityInfo.clear();
        this.previousPartialAppendData.set(null);
    }

    //endregion

    //region AppendIntegrityChecker Implementation

    public synchronized void addAppendIntegrityInfo(long segmentId, long offset, long length, long hash) {
        if (hash == NO_HASH) {
            // No data integrity checks enabled, do nothing.
            return;
        }
        Preconditions.checkArgument(this.segmentId == segmentId, "Appending integrity information for a wrong segment: " + segmentId);
        this.appendIntegrityInfo.add(new AggregatedAppendIntegrityInfo(segmentId, offset, length, hash));
    }

    public synchronized void checkAppendIntegrity(long segmentId, long offset, BufferView aggregatedAppends) throws DataCorruptionException {
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
        while (iterator.hasNext() && accumulatedLength < aggregatedAppends.getLength()) {
            integrityInfo = iterator.next();
            if (integrityInfo.getContentHash() == NO_HASH || integrityInfo.getOffset() < offset) {
                // Old or not hashed operation, just remove.
                iterator.remove();
            } else {
                // If we had a partial append in the previous block, prepend it to the current aggregated appends to check integrity.
                if (this.previousPartialAppendData.get() != null) {
                    aggregatedAppends = BufferView.builder(2).add(this.previousPartialAppendData.get()).add(aggregatedAppends).build();
                    offset -= this.previousPartialAppendData.get().getLength();
                    this.previousPartialAppendData.set(null);
                }

                // If the last AggregatedAppend from the previous flush had a partial Append, but we do not have the partial data,
                // we need to skip the second part of that Append to continue checking integrity from the right offset.
                if (accumulatedLength == 0 && integrityInfo.getOffset() != offset) {
                    accumulatedLength += integrityInfo.getOffset() - offset;
                }

                // Do the integrity check if the input data contains the whole contents of the original Append.
                // Otherwise, it means that the input data ends with a partial Append, and we need to handle it.
                if (aggregatedAppends.getLength() >= accumulatedLength + integrityInfo.getLength()) {
                    long hash = aggregatedAppends.slice(accumulatedLength, (int) integrityInfo.getLength()).hash();
                    if (hash != integrityInfo.getContentHash()) {
                        log.error("Append integrity check failed. SegmentId = {}, Offset = {}, Length = {}, Original Hash = {}, Current Hash = {}.",
                                segmentId, integrityInfo.getOffset() + accumulatedLength, integrityInfo.getLength(), integrityInfo.getContentHash(), hash);
                        throw new DataCorruptionException(String.format("Data read from cache for Segment %s (hash = %s) differs from original data appended (hash = %s).",
                                integrityInfo.getSegmentId(), hash, integrityInfo.getContentHash()));
                    }
                    checkedBytes = accumulatedLength;
                } else {
                    // An append has been split across 2 blocks of data to be flushed to LTS. We keep the first part of
                    // the append at the end of the current block, so it can be used in the next execution of this method
                    // to check the hash of the append using the second part of it at the beginning of the next block.
                    previousPartialAppendData.set(aggregatedAppends.slice(accumulatedLength, aggregatedAppends.getLength() - accumulatedLength));
                }
                accumulatedLength += integrityInfo.getLength();
            }
        }
        log.info("{}: Checked integrity of {} bytes to be flushed to LTS ({} trailing bytes will be checked in next flush).",
                this.traceObjectId, checkedBytes, aggregatedAppends.getLength() - checkedBytes);
    }

    //endregion

    //region Helper Classes

    @Data
    private static class AggregatedAppendIntegrityInfo {
        private final long segmentId;
        private final long offset;
        private final long length;
        private final long contentHash;
    }

    //endregion
}

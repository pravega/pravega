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
import io.pravega.segmentstore.server.DataCorruptionException;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Helper class to check internal data integrity of Appends within Pravega ingestion pipeline. The main purpose of this
 * class is to compute and keep track of hashes of Append contents upon ingestion and then verify that the integrity of
 * such Appends at the end of the ingestion pipeline (when data is moved to LTS).
 */
@Slf4j
public class AppendIntegrityChecker implements AutoCloseable {
    //region Members

    // Represents that no hash has been computed for a given Append.
    public static final long NO_HASH = Long.MIN_VALUE;

    @GuardedBy("this")
    private final Queue<AggregatedAppendIntegrityInfo> appendIntegrityInfo;

    //endregion

    //region Constructor

    public AppendIntegrityChecker() {
        this.appendIntegrityInfo = new LinkedBlockingQueue<>();
    }

    //endregion

    //region Autocloseable

    @Override
    public synchronized void close() {
        this.appendIntegrityInfo.clear();
    }

    //endregion

    //region AppendIntegrityChecker Implementation

    public static long computeDataHash(@NonNull BufferView data) {
        // FIXME: Before using BufferView.hashCode(), we need to have a common implementation of that method to all subclasses
        return Arrays.hashCode(data.getCopy());
    }

    public synchronized void addAppendIntegrityInfo(long segmentId, long offset, long length, long hash) {
        if (hash == NO_HASH) {
            // No data integrity checks enabled, do nothing.
            return;
        }
        this.appendIntegrityInfo.add(new AggregatedAppendIntegrityInfo(segmentId, offset, length, hash));
    }

    public synchronized void checkAppendIntegrity(long segmentId, long offset, BufferView aggregatedAppends) throws DataCorruptionException {
        if (aggregatedAppends == null || this.appendIntegrityInfo.isEmpty()) {
            // No data to check, do nothing.
            return;
        }

        // Get the appends for queued for this specific Segment.
        Iterator<AggregatedAppendIntegrityInfo> iterator = this.appendIntegrityInfo.iterator();
        int accumulatedLength = 0;
        AggregatedAppendIntegrityInfo integrityInfo;
        while (iterator.hasNext() && accumulatedLength < aggregatedAppends.getLength()) {
            integrityInfo = iterator.next();
            if (integrityInfo.getContentHash() == NO_HASH || integrityInfo.getOffset() < offset) {
                // Old or not hashed operation, just remove.
                iterator.remove();
            } else {
                // If the last AggregatedAppend from the previous flush had a partial Append, we need to skip the second part of that Append.
                if (accumulatedLength == 0 && integrityInfo.getOffset() != offset) {
                    accumulatedLength += integrityInfo.getOffset() - offset;
                }

                // Do the integrity check if the input data contains the whole contents of the original Append.
                // Otherwise, it means that the input data ends with a partial Append, for which we cannot determine the hash.
                if (aggregatedAppends.getLength() >= accumulatedLength + integrityInfo.getLength()) {
                    long hash = computeDataHash(aggregatedAppends.slice(accumulatedLength, (int) integrityInfo.getLength()));
                    if (hash != integrityInfo.getContentHash()) {
                        log.error("Append integrity check failed. SegmentId = {}, Offset = {}, Length = {}, Original Hash = {}, Current Hash = {}.",
                                segmentId, integrityInfo.getOffset() + accumulatedLength, integrityInfo.getLength(), integrityInfo.getContentHash(), hash);
                        throw new DataCorruptionException(String.format("Data read from cache for Segment %s (hash = %s) differs from original data appended (hash = %s).",
                                integrityInfo.getSegmentId(), hash, integrityInfo.getContentHash()));
                    }
                }
                accumulatedLength += integrityInfo.getLength();
            }
        }
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

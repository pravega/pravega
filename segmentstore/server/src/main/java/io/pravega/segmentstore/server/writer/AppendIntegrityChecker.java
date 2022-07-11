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

    //region AppendIntegrityChecker Implementation

    public static long computeDataHash(BufferView data) {
        return computeDataHash(data.getCopy());
    }

    public static long computeDataHash(byte[] data) {
        return Arrays.hashCode(data);
    }

    @Override
    public synchronized void close() {
        this.appendIntegrityInfo.clear();
    }

    public synchronized void addAppendIntegrityInfo(long segmentId, long offset, long length, long hash) {
        if (hash == NO_HASH) {
            // No data integrity checks enabled, do nothing.
            return;
        }
        this.appendIntegrityInfo.add(new AggregatedAppendIntegrityInfo(segmentId, offset, length, hash));
    }

    public synchronized void checkAppendIntegrity(long segmentId, long offset, BufferView data) throws DataCorruptionException {
        if (data == null || this.appendIntegrityInfo.isEmpty()) {
            // No data to check, do nothing.
            return;
        }

        // Get the appends for queued for this specific Segment.
        Iterator<AggregatedAppendIntegrityInfo> iterator = this.appendIntegrityInfo.iterator();
        int accumulatedLength = 0;
        byte[] dataContents = data.getCopy();
        while (iterator.hasNext() && accumulatedLength <= data.getLength()) {
            AggregatedAppendIntegrityInfo integrityInfo = iterator.next();
            if (integrityInfo.getContentHash() == NO_HASH || integrityInfo.getOffset() < offset) {
                // Old or not hashed operation, just remove.
                iterator.remove();
            } else {
                // Check integrity for this append if we have some hash to compare to.
                if (dataContents.length >= accumulatedLength + integrityInfo.getLength()) {
                    long hash = Arrays.hashCode(Arrays.copyOfRange(dataContents, accumulatedLength, (int) (accumulatedLength + integrityInfo.getLength())));
                    if (hash != integrityInfo.getContentHash())  {
                        log.error("Append integrity check failed. SegmentId = {}, Offset = {}, Length = {}, Original Hash = {}, Current Hash = {}.",
                                segmentId, integrityInfo.getOffset() + accumulatedLength, integrityInfo.getLength(), integrityInfo.getContentHash(), hash);
                        throw new DataCorruptionException("Data read from cache differs from original data appended.");
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

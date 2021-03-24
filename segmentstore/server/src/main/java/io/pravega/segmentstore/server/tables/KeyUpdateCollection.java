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
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.tables.TableKey;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Collection of Keys to their associated {@link BucketUpdate.KeyUpdate}s.
 */
@ThreadSafe
@RequiredArgsConstructor
class KeyUpdateCollection {
    private final Map<BufferView, BucketUpdate.KeyUpdate> updates = Collections.synchronizedMap(new HashMap<>());
    private final AtomicInteger totalUpdateCount = new AtomicInteger(0);
    private final AtomicLong lastIndexedOffset = new AtomicLong(-1L);
    private final AtomicLong highestCopiedOffset = new AtomicLong(TableKey.NO_VERSION);
    @Getter
    private final int length;

    /**
     * Gets a value representing the total number of updates processed, including duplicated keys.
     * @return The total update count.
     */
    int getTotalUpdateCount() {
        return this.totalUpdateCount.get();
    }

    /**
     * Get a value representing the Segment offset before which every single byte has been indexed (i.e., the last offset
     * of the last update).
     * @return The last indexed offset.
     */
    long getLastIndexedOffset() {
        return this.lastIndexedOffset.get();
    }

    /**
     * Gets a value representing the highest offset encountered that was copied over during a compaction.
     * If no copied entry was encountered, then this will be set to {@link TableKey#NO_VERSION}.
     * @return The highest copied offset.
     */
    long getHighestCopiedOffset() {
        return this.highestCopiedOffset.get();
    }

    /**
     * Includes the given {@link BucketUpdate.KeyUpdate} into this collection.
     *
     * If we get multiple updates for the same key, only the one with highest version will be kept. Due to compaction,
     * it is possible that a lower version of a Key will end up after a higher version of the same Key, in which case
     * the higher version should take precedence.
     *
     * @param update         The {@link BucketUpdate.KeyUpdate} to include.
     * @param entryLength    The total length of the given update, as serialized in the Segment.
     * @param originalOffset If this update was a result of a compaction copy, then this should be the original offset
     *                       where it was copied from (as serialized in the Segment). If no such information exists, then
     *                       {@link TableKey#NO_VERSION} should be used.
     */
    void add(BucketUpdate.KeyUpdate update, int entryLength, long originalOffset) {
        val existing = this.updates.get(update.getKey());
        if (existing == null || update.supersedes(existing)) {
            this.updates.put(update.getKey(), update);
        }

        // Update remaining counters, regardless of whether we considered this update or not.
        this.totalUpdateCount.incrementAndGet();
        long lastOffset = update.getOffset() + entryLength;
        this.lastIndexedOffset.updateAndGet(e -> Math.max(lastOffset, e));
        if (originalOffset >= 0) {
            this.highestCopiedOffset.updateAndGet(e -> Math.max(e, originalOffset + entryLength));
        }
    }

    /**
     * Gets a collection of {@link BucketUpdate.KeyUpdate} instances with unique keys that are ready for indexing.
     *
     * @return The result.
     */
    Collection<BucketUpdate.KeyUpdate> getUpdates() {
        return this.updates.values();
    }
}
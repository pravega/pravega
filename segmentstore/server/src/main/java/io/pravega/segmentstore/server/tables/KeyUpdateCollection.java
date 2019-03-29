/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.tables.TableKey;
import java.util.Collection;
import java.util.HashMap;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.val;

/**
 * Collection of Keys to their associated {@link BucketUpdate.KeyUpdate}s.
 */
@NotThreadSafe
class KeyUpdateCollection {
    private final HashMap<HashedArray, BucketUpdate.KeyUpdate> updates = new HashMap<>();
    /**
     * The total number of updates processed, including duplicated keys.
     */
    @Getter
    private int totalUpdateCount;

    /**
     * The Segment offset before which every single byte has been indexed (i.e., the last offset of the last update).
     */
    @Getter
    private long lastIndexedOffset = -1L;

    /**
     * The highest offset encountered that was copied over during a compaction. If no copied entry was encountered,
     * then this will be set to {@link TableKey#NO_VERSION}.
     */
    @Getter
    private long highestCopiedOffset = TableKey.NO_VERSION;

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
        this.highestCopiedOffset = Math.max(this.highestCopiedOffset, originalOffset + entryLength);
        this.totalUpdateCount++;
        long lastOffset = update.getOffset() + entryLength;
        if (lastOffset > this.lastIndexedOffset) {
            this.lastIndexedOffset = lastOffset;
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
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Represents an update to a Table Bucket.
 */
@RequiredArgsConstructor
class BucketUpdate {
    //region Members

    /**
     * The updated bucket, as fetched by the {@link IndexWriter}.
     */
    @Getter
    private final TableBucket bucket;
    private final Map<HashedArray, KeyInfo> existingKeys = new HashMap<>();
    private final Map<HashedArray, KeyUpdate> updatedKeys = new HashMap<>();

    //endregion

    //region Operations

    /**
     * Records an existing Key that is relevant to this Bucket Update.
     *
     * @param keyInfo A {@link KeyInfo} to record. Any existing recordings for this {@link KeyInfo#getKey()} will be
     *                overwritten with this value.
     */
    void withExistingKey(KeyInfo keyInfo) {
        this.existingKeys.put(keyInfo.getKey(), keyInfo);
    }

    /**
     * Records a key update that is relevant to this Bucket Update.
     *
     * @param update A {@link KeyUpdate} to record. Any existing (update) recordings for this {@link KeyUpdate#getKey()}
     *               will be overwritten with this value.
     */
    void withKeyUpdate(KeyUpdate update) {
        this.updatedKeys.put(update.getKey(), update);
    }

    /**
     * Gets a collection of {@link KeyInfo} instances recorded in this Bucket Update.
     */
    Collection<KeyInfo> getExistingKeys() {
        return this.existingKeys.values();
    }

    /**
     * Gets a collection of {@link KeyUpdate} instances recorded in this Bucket Update.
     */
    Collection<KeyUpdate> getKeyUpdates() {
        return this.updatedKeys.values();
    }

    /**
     * Gets a value indicating whether the Key represented by the given {@link HashedArray} is recorded as being updated.
     *
     * @param key The Key to check.
     * @return True if updated, false otherwise.
     */
    boolean isKeyUpdated(HashedArray key) {
        return this.updatedKeys.containsKey(key);
    }

    /**
     * Gets a value indicating whether any Key updates are recorded in this Bucket Update.
     */
    boolean hasUpdates() {
        return !this.updatedKeys.isEmpty();
    }

    /**
     * Gets a value representing the last offset of any update - which will now become the Bucket offset.
     *
     * @return The bucket offset, or -1 if no such offset (i.e., if everything in this bucket was deleted).
     */
    long getBucketOffset() {
        return this.updatedKeys.values().stream()
                               .filter(u -> !u.isDeleted())
                               .mapToLong(KeyUpdate::getOffset).max().orElse(-1);
    }

    //endregion
}
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

import com.google.common.base.Preconditions;
import io.pravega.common.util.HashedArray;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Represents an update to a Table Bucket.
 */
@RequiredArgsConstructor
@NotThreadSafe
class BucketUpdate {
    //region Members

    /**
     * The updated bucket, as fetched by the {@link IndexWriter}.
     */
    @Getter
    private final TableBucket bucket;
    private final Map<HashedArray, KeyInfo> existingKeys = new HashMap<>();
    private final Map<HashedArray, KeyUpdate> updatedKeys = new HashMap<>();
    private long maxUpdateOffset = -1;

    //endregion

    //region Operations

    /**
     * Records an existing Key that is relevant to this Bucket Update.
     *
     * @param keyInfo A {@link KeyInfo} to record. Any existing recordings for this {@link KeyInfo#getKey()} will be
     *                overwritten with this value.
     */
    void withExistingKey(KeyInfo keyInfo) {
        Preconditions.checkArgument(keyInfo.getOffset() >= 0, "KeyInfo.getOffset() must be a non-negative number.");
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
        if (!update.isDeleted()) {
            this.maxUpdateOffset = Math.max(update.getOffset(), this.maxUpdateOffset);
        }
    }

    /**
     * Gets a collection of {@link KeyInfo} instances recorded in this Bucket Update.
     */
    Collection<KeyInfo> getExistingKeys() {
        return Collections.unmodifiableCollection(this.existingKeys.values());
    }

    /**
     * Gets a collection of {@link KeyUpdate} instances recorded in this Bucket Update.
     */
    Collection<KeyUpdate> getKeyUpdates() {
        return Collections.unmodifiableCollection(this.updatedKeys.values());
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
        if (this.maxUpdateOffset >= 0) {
            // We have non-deletion updates; return the pre-computed value.
            return this.maxUpdateOffset;
        } else {
            // No updates (or all updates are deletions). Get the offset from the remaining existing keys (if any left).
            return this.existingKeys.values().stream()
                                    .filter(ek -> !this.updatedKeys.containsKey(ek.getKey()))
                                    .mapToLong(KeyInfo::getOffset).max().orElse(-1);
        }
    }

    //endregion

    //region KeyInfo and KeyUpdate

    /**
     * General Key Information.
     */
    @RequiredArgsConstructor
    @Getter
    static class KeyInfo {
        /**
         * The Key.
         */
        @NonNull
        private final HashedArray key;

        /**
         * The offset at which the key exists in the Segment.
         */
        private final long offset;

        @Override
        public String toString() {
            return String.format("Offset=%s, Key={%s}", this.offset, key);
        }
    }

    /**
     * An update to a particular Key.
     */
    @Getter
    static class KeyUpdate extends KeyInfo {
        /**
         * If true, indicates the Key has been deleted (as opposed from being updated).
         */
        private final boolean deleted;

        /**
         * Creates a new instance of the KeyUpdate class.
         *
         * @param key     A {@link HashedArray} representing the Key that is updated.
         * @param offset  The offset in the Segment where the update is serialized.
         * @param deleted True if the Key has been deleted via this update, false otherwise.
         */
        KeyUpdate(HashedArray key, long offset, boolean deleted) {
            super(key, offset);
            this.deleted = deleted;
        }

        @Override
        public String toString() {
            return (this.deleted ? "[DELETED] " : "") + super.toString();
        }
    }

    //endregion
}
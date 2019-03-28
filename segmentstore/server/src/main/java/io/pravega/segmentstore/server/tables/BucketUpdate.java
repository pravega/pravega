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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
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

    /**
     * Gets a collection of {@link KeyInfo} instances recorded in this Bucket Update.
     */
    @Getter
    private final Collection<KeyInfo> existingKeys;
    private final Map<HashedArray, KeyUpdate> updatedKeys;

    /**
     * The bucket offset, or -1 if no such offset (i.e., if everything in this bucket was deleted).
     */
    @Getter
    private final long bucketOffset;

    //endregion

    //region Operations

    /**
     * Creates a new Builder for a {@link BucketUpdate} for the given Table Bucket.
     * @param bucket The {@link TableBucket} to create a {@link BucketUpdate.Builder} for.
     * @return A new builder instance.
     */
    static BucketUpdate.Builder forBucket(TableBucket bucket) {
        return new BucketUpdate.Builder(bucket);
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

    //endregion

    //region Builder

    @RequiredArgsConstructor
    static class Builder {
        @NonNull
        @Getter
        private final TableBucket bucket;
        private final Map<HashedArray, KeyInfo> existingKeys = new HashMap<>();
        private final Map<HashedArray, KeyUpdate> updatedKeys = new HashMap<>();

        /**
         * Records an existing Key that is relevant to this Bucket Update.
         *
         * @param keyInfo A {@link KeyInfo} to record. Any existing recordings for this {@link KeyInfo#getKey()} will be
         *                overwritten with this value.
         */
        Builder withExistingKey(KeyInfo keyInfo) {
            Preconditions.checkArgument(keyInfo.getOffset() >= 0, "KeyInfo.getOffset() must be a non-negative number.");
            this.existingKeys.put(keyInfo.getKey(), keyInfo);
            return this;
        }

        /**
         * Records a key update that is relevant to this Bucket Update.
         *
         * @param update A {@link KeyUpdate} to record. Any existing (update) recordings for this {@link KeyUpdate#getKey()}
         *               will be overwritten with this value.
         */
        Builder withKeyUpdate(KeyUpdate update) {
            this.updatedKeys.put(update.getKey(), update);
            return this;
        }

        /**
         * Creates a new {@link BucketUpdate} using the information contained in this builder.
         *
         * @return A new {@link BucketUpdate} instance.
         */
        BucketUpdate build() {
            // Exclude updated keys that have smaller versions than existing keys.
            ArrayList<HashedArray> toRemove = new ArrayList<>();
            long bucketOffset = -1;
            for (KeyUpdate u : this.updatedKeys.values()) {
                KeyInfo existingKey = this.existingKeys.get(u.getKey());
                if (!u.isDeleted() && existingKey != null && existingKey.supersedes(u)) {
                    toRemove.add(u.getKey());
                } else if (!u.isDeleted()) {
                    bucketOffset = Math.max(bucketOffset, u.getOffset());
                }
            }

            toRemove.forEach(this.updatedKeys::remove);

            if (bucketOffset < 0) {
                // No updates (or all updates are deletions). Get the offset from the remaining existing keys (if any left).
                bucketOffset = this.existingKeys.values().stream()
                        .filter(ek -> !this.updatedKeys.containsKey(ek.getKey()))
                        .mapToLong(KeyInfo::getOffset)
                        .max().orElse(-1);
            }

            return new BucketUpdate(this.bucket, Collections.unmodifiableCollection(this.existingKeys.values()),
                    Collections.unmodifiableMap(this.updatedKeys), bucketOffset);
        }
    }

    //endregion

    //region KeyInfo and KeyUpdate

    /**
     * General Key Information.
     */
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

        /**
         * Version of the key.
         */
        private final long version;

        KeyInfo(@NonNull HashedArray key, long offset, long version) {
            Preconditions.checkArgument(version <= offset, "version (%s) cannot be lower than offset (%s).", version, offset);
            this.key = key;
            this.offset = offset;
            this.version = version;
        }

        /**
         * Determines whether this {@link KeyInfo} instance supersedes the given {@link KeyInfo} instance. A supersedes B
         * if one of the following is true:
         * - A has a higher version than B (irrespective of Segment Offsets)
         * - A has the same version as B, but A has a higher Segment Offset than B.
         *
         * @param other The {@link KeyInfo} instance to compare to.
         * @return True if this instance supersedes the other instance.
         */
        boolean supersedes(KeyInfo other) {
            return this.version > other.version
                    || (this.version == other.version && this.offset > other.offset);
        }

        @Override
        public String toString() {
            return String.format("Offset=%s, Version=%s, Key={%s}", this.offset, this.version, this.key);
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
        KeyUpdate(HashedArray key, long offset, long version, boolean deleted) {
            super(key, offset, version);
            this.deleted = deleted;
        }

        @Override
        public String toString() {
            return (this.deleted ? "[DELETED] " : "") + super.toString();
        }
    }

    //endregion
}
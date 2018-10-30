/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.hash.HashHelper;
import io.pravega.common.util.BitConverter;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.storage.Cache;
import lombok.Getter;

/**
 * Cache Key for the Segment Attribute Index.
 */
class CacheKey extends Cache.Key {
    //region Members

    private static final HashHelper HASH = HashHelper.seededWith(CacheKey.class.getName());
    private static final int SERIALIZATION_LENGTH = Long.BYTES + Long.BYTES;
    @Getter
    private final long segmentId;
    @Getter
    private final int entryId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the CacheKey class.
     *
     * @param segmentId The Segment Id that the key refers to.
     * @param entryId   The Cache Entry Id that the key refers to.
     */
    CacheKey(long segmentId, int entryId) {
        Preconditions.checkArgument(segmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "segmentId");
        this.segmentId = segmentId;
        this.entryId = entryId;
    }

    @VisibleForTesting
    CacheKey(byte[] serialization) {
        Preconditions.checkNotNull(serialization, "serialization");
        Preconditions.checkArgument(serialization.length == SERIALIZATION_LENGTH, "Invalid serialization length.");

        this.segmentId = BitConverter.readLong(serialization, 0);
        this.entryId = BitConverter.readInt(serialization, Long.BYTES);
    }

    //endregion

    //region Overrides

    @Override
    public byte[] serialize() {
        byte[] result = new byte[SERIALIZATION_LENGTH];
        BitConverter.writeLong(result, 0, this.segmentId);
        BitConverter.writeInt(result, Long.BYTES, this.entryId);
        return result;
    }

    @Override
    public int hashCode() {
        return HASH.hash(this.entryId);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CacheKey)) {
            return false;
        }

        CacheKey other = (CacheKey) obj;
        return this.segmentId == other.segmentId
                && this.entryId == other.entryId;
    }

    @Override
    public String toString() {
        return String.format("SegmentId = %d, EntryId = %d", this.segmentId, this.entryId);
    }

    //endregion
}

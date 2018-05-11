/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.hash.HashHelper;
import io.pravega.common.util.BitConverter;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.storage.Cache;
import com.google.common.base.Preconditions;

/**
 * ReadIndex-specific implementation of Cache.Key.
 */
class CacheKey extends Cache.Key {
    //region Members

    private static final HashHelper HASH = HashHelper.seededWith(CacheKey.class.getName());
    private static final int SERIALIZATION_LENGTH = Long.BYTES + Long.BYTES;
    private final long streamSegmentId;
    private final long offset;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the CacheKey class.
     *
     * @param streamSegmentId The StreamSegmentId that the key refers to.
     * @param offset          The Offset within the StreamSegment that the key refers to.
     */
    CacheKey(long streamSegmentId, long offset) {
        Preconditions.checkArgument(streamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "streamSegmentId");
        Preconditions.checkArgument(offset >= 0, "offset");

        this.streamSegmentId = streamSegmentId;
        this.offset = offset;
    }

    /**
     * Creates a new instance of the CacheKey class from the given serialization.
     *
     * @param serialization The serialization of the key.
     */
    @VisibleForTesting
    CacheKey(byte[] serialization) {
        Preconditions.checkNotNull(serialization, "serialization");
        Preconditions.checkArgument(serialization.length == SERIALIZATION_LENGTH, "Invalid serialization length.");

        this.streamSegmentId = BitConverter.readLong(serialization, 0);
        this.offset = BitConverter.readLong(serialization, Long.BYTES);
    }

    //endregion

    //region Cache.Key Implementation

    @Override
    public byte[] serialize() {
        byte[] result = new byte[SERIALIZATION_LENGTH];
        BitConverter.writeLong(result, 0, this.streamSegmentId);
        BitConverter.writeLong(result, Long.BYTES, this.offset);
        return result;
    }

    @Override
    public int hashCode() {
        return HASH.hash(this.offset);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CacheKey)) {
            return false;
        }

        CacheKey other = (CacheKey) obj;
        return this.streamSegmentId == other.streamSegmentId
                && this.offset == other.offset;
    }

    //endregion

    //region Properties

    /**
     * Gets a value representing the Id of the StreamSegment for this Cache Key.
     */
    public long getStreamSegmentId() {
        return this.streamSegmentId;
    }

    /**
     * Gets a value representing the Offset within the StreamSegment for this Cache Key.
     */
    public long getOffset() {
        return this.offset;
    }

    @Override
    public String toString() {
        return String.format("SegmentId = %d, Offset = %d", this.streamSegmentId, this.offset);
    }

    //endregion
}

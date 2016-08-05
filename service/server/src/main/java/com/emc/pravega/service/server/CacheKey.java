package com.emc.pravega.service.server;

import com.emc.pravega.common.util.BitConverter;
import com.emc.pravega.service.storage.Cache;
import com.google.common.base.Preconditions;

/**
 * ReadIndex-specific implementation of Cache.Key.
 */
public class CacheKey extends Cache.Key {
    private static final int SERIALIZATION_LENGTH = Long.BYTES + Long.BYTES;
    private final long streamSegmentId;
    private final long offset;
    private final int hash;

    public CacheKey(long streamSegmentId, long offset) {
        Preconditions.checkArgument(streamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "streamSegmentId");
        Preconditions.checkArgument(offset >= 0, "offset");

        this.streamSegmentId = streamSegmentId;
        this.offset = offset;
        this.hash = calculateHash();
    }

    CacheKey(byte[] serialization) {
        Preconditions.checkNotNull(serialization, "serialization");
        Preconditions.checkArgument(serialization.length == SERIALIZATION_LENGTH, "Invalid serialization length.");

        this.streamSegmentId = BitConverter.readLong(serialization, 0);
        this.offset = BitConverter.readLong(serialization, Long.BYTES);
        this.hash = calculateHash();
    }

    private int calculateHash() {
        return Long.hashCode(this.streamSegmentId) * 31 + Long.hashCode(this.offset);
    }

    //region Cache.Key Implementation

    @Override
    public byte[] getSerialization() {
        byte[] result = new byte[SERIALIZATION_LENGTH];
        BitConverter.writeLong(result, 0, this.streamSegmentId);
        BitConverter.writeLong(result, Long.BYTES, this.offset);
        return result;
    }

    @Override
    public int hashCode() {
        return this.hash;
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
     *
     * @return
     */
    public long getStreamSegmentId() {
        return this.streamSegmentId;
    }

    /**
     * Gets a value representing the Offset within the StreamSegment for this Cache Key.
     *
     * @return
     */
    public long getOffset() {
        return this.offset;
    }

    @Override
    public String toString() {
        return String.format("SegmentId = %d, Offset = %d, InCache = %s", this.streamSegmentId, this.offset, isInCache());
    }

    //endregion
}

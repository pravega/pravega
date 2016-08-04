package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.util.BitConverter;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.storage.Cache;
import com.google.common.base.Preconditions;

/**
 * ReadIndex-specific implementation of Cache.Key.
 */
public class CacheKey implements Cache.Key {
    private static final int SERIALIZATION_LENGTH = Integer.BYTES + Long.BYTES + Long.BYTES;
    private final int containerId;
    private final long segmentId;
    private final long offset;
    private final int hash;

    //region Constructor

    /**
     * Creates a new instance of the CacheKey class.
     *
     * @param containerId The Id of the StreamSegmentContainer for the ReadIndex that accesses the Cache.
     * @param segmentId   The Id of the StreamSegment for this CacheKey.
     * @param offset      The Offset in the StreamSegment for this CacheKey.
     */
    public CacheKey(int containerId, long segmentId, long offset) {
        Preconditions.checkArgument(containerId >= 0, "containerId must be a non-negative integer");
        Preconditions.checkArgument(segmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "segmentId");
        Preconditions.checkArgument(offset >= 0, "offset must be a non-negative number");

        this.containerId = containerId;
        this.segmentId = segmentId;
        this.offset = offset;
        this.hash = calculateHash();
    }

    /**
     * Creates a new instance of the CacheKey class.
     *
     * @param serialization The serialization to build the CacheKey from.
     */
    CacheKey(byte[] serialization) {
        Preconditions.checkNotNull(serialization, "serialization");
        Preconditions.checkArgument(serialization.length == SERIALIZATION_LENGTH, "Invalid serialization length.");
        this.containerId = BitConverter.readInt(serialization, 0);
        this.segmentId = BitConverter.readLong(serialization, Integer.BYTES);
        this.offset = BitConverter.readLong(serialization, Integer.BYTES + Long.BYTES);
        this.hash = calculateHash();
    }

    private int calculateHash() {
        return (Integer.hashCode(this.containerId) * 31 + Long.hashCode(this.segmentId)) * 31 + Long.hashCode(this.offset);
    }

    //endregion

    //region Cache.Key implementation

    @Override
    public byte[] getSerialization() {
        byte[] result = new byte[SERIALIZATION_LENGTH];
        BitConverter.writeInt(result, 0, this.containerId);
        BitConverter.writeLong(result, Integer.BYTES, this.segmentId);
        BitConverter.writeLong(result, Integer.BYTES + Long.BYTES, this.offset);
        return result;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the Offset within the StreamSegment this Cache Key refers to.
     * @return
     */
    public long getOffset(){
        return this.offset;
    }

    @Override
    public String toString() {
        return String.format("ContainerId = %d, SegmentId = %d, Offset = %d", this.containerId, this.segmentId, this.offset);
    }

    //endregion

    //region hashCode & equals - necessary for InMemoryCache

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CacheKey)) {
            return false;
        }

        CacheKey other = (CacheKey) obj;
        return this.containerId == other.containerId
                && this.segmentId == other.segmentId
                && this.offset == other.offset;
    }

    //endregion
}

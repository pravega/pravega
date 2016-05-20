package com.emc.logservice.containers;

import com.emc.logservice.SegmentMetadataCollection;
import com.emc.logservice.UpdateableSegmentMetadata;

import java.util.Date;

/**
 * Metadata for a particular Stream Segment.
 */
public class StreamSegmentMetadata implements UpdateableSegmentMetadata {
    //region Members

    private final String name;
    private final long streamSegmentId;
    private final long parentStreamSegmentId;
    private long storageLength;
    private long durableLogLength;
    private boolean sealed;
    private boolean deleted;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentMetadata class for a stand-alone StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param streamSegmentId   The Id of the StreamSegment.
     * @throws IllegalArgumentException If either of the arguments are invalid.
     */
    public StreamSegmentMetadata(String streamSegmentName, long streamSegmentId) {
        this(streamSegmentName, streamSegmentId, SegmentMetadataCollection.NoStreamSegmentId);
    }

    /**
     * Creates a new instance of the StreamSegmentMetadata class for a child (batch) StreamSegment.
     *
     * @param streamSegmentName     The name of the StreamSegment.
     * @param streamSegmentId       The Id of the StreamSegment.
     * @param parentStreamSegmentId The Id of the Parent StreamSegment.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    public StreamSegmentMetadata(String streamSegmentName, long streamSegmentId, long parentStreamSegmentId) {
        if (streamSegmentName == null) {
            throw new IllegalArgumentException("streamSegmentName");
        }

        if (streamSegmentId == SegmentMetadataCollection.NoStreamSegmentId) {
            throw new IllegalArgumentException("streamSegmentId");
        }

        this.name = streamSegmentName;
        this.streamSegmentId = streamSegmentId;
        this.parentStreamSegmentId = parentStreamSegmentId;
        this.sealed = false;
        this.deleted = false;
        this.storageLength = this.durableLogLength = -1;
    }

    //endregion

    //region SegmentProperties Implementation

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean isSealed() {
        return this.sealed;
    }

    @Override
    public boolean isDeleted() {
        return this.deleted;
    }

    @Override
    public long getLength() {
        return this.durableLogLength; // ReadableLength is essentially DurableLogLength.
    }

    @Override
    public Date getLastModified() {
        return new Date(); // TODO: implement properly; maybe change everytime durableLogLength changes...
    }

    //endregion

    //region SegmentMetadata Implementation

    @Override
    public long getId() {
        return this.streamSegmentId;
    }

    @Override
    public long getParentId() {
        return this.parentStreamSegmentId;
    }

    @Override
    public long getStorageLength() {
        return this.storageLength;
    }

    @Override
    public long getDurableLogLength() {
        return this.durableLogLength;
    }

    @Override
    public String toString() {
        return String.format("Id = %d, StorageLength = %d, DLOffset = %d, Sealed = %s, Deleted = %s, Name = %s", getId(), getStorageLength(), getDurableLogLength(), isSealed(), isDeleted(), getName());
    }

    //endregion

    //region UpdateableSegmentMetadata Implementation

    @Override
    public void setStorageLength(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Storage Length must be a non-negative number.");
        }

        if (value < this.storageLength) {
            throw new IllegalArgumentException("New Storage Length cannot be smaller than the previous one.");
        }

        this.storageLength = value;
    }

    @Override
    public void setDurableLogLength(long value) {
        if (value < 0) {
            throw new IllegalArgumentException(String.format("Durable Log Length must be a non-negative number. Given: %d.", value));
        }

        if (value < this.durableLogLength) {
            throw new IllegalArgumentException(String.format("New Durable Log Length cannot be smaller than the previous one. Previous: %d, Given: %d.", this.durableLogLength, value));
        }

        this.durableLogLength = value;
    }

    @Override
    public void markSealed() {
        this.sealed = true;
    }

    @Override
    public void markDeleted() {
        this.deleted = true;
    }

    //endregion
}

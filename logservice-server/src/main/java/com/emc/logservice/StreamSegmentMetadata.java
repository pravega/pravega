package com.emc.logservice;

/**
 * Metadata for a particular Stream Segment.
 */
public class StreamSegmentMetadata implements ReadOnlyStreamSegmentMetadata {
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
        this(streamSegmentName, streamSegmentId, StreamSegmentContainerMetadata.NoStreamSegmentId);
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

        if (streamSegmentId == StreamSegmentContainerMetadata.NoStreamSegmentId) {
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

    //region ReadOnlyStreamSegmentMetadata Implementation

    @Override
    public String getName() {
        return this.name;
    }

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

    /**
     * Sets the current StorageLength for this StreamSegment.
     *
     * @param value The StorageLength to set.
     * @throws IllegalArgumentException If the value is invalid.
     */
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
    public long getDurableLogLength() {
        return this.durableLogLength;
    }

    /**
     * Sets the current DurableLog Length for this StreamSegment.
     *
     * @param value
     * @throws IllegalArgumentException If the value is invalid.
     */
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
    public boolean isSealed() {
        return this.sealed;
    }

    /**
     * Marks this StreamSegment as sealed for modifications.
     */
    public void markSealed() {
        this.sealed = true;
    }

    @Override
    public boolean isDeleted() {
        return this.deleted;
    }

    /**
     * Marks this StreamSegment as deleted.
     */
    public void markDeleted() {
        this.deleted = true;
    }

    @Override
    public String toString() {
        return String.format("Id = %d, StorageLength = %d, DLOffset = %d, Sealed = %s, Deleted = %s, Name = %s", getId(), getStorageLength(), getDurableLogLength(), isSealed(), isDeleted(), getName());
    }

    //endregion
}

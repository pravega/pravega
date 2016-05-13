package com.emc.logservice;

/**
 * Defines an immutable StreamSegment Metadata.
 */
public interface ReadOnlyStreamSegmentMetadata {
    /**
     * Gets a value indicating the name of this StreamSegment.
     *
     * @return
     */
    String getName();

    /**
     * Gets a value indicating the id of this StreamSegment.
     *
     * @return
     */
    long getId();

    /**
     * Gets a value indicating the id of this StreamSegment's parent.
     *
     * @return
     */
    long getParentId();

    /**
     * Gets a value indicating the length of this StreamSegment for the part that exists in Storage Only.
     *
     * @return
     */
    long getStorageLength();

    /**
     * Gets a value indicating the length of this entire StreamSegment (the part in Storage + the part in DurableLog).
     *
     * @return
     */
    long getDurableLogLength();

    /**
     * Gets a value indicating whether this StreamSegment is sealed for modifications.
     *
     * @return
     */
    boolean isSealed();

    /**
     * Gets a value indicating whether this StreamSegment is deleted (does not exist).
     *
     * @return
     */
    boolean isDeleted();
}

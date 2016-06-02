package com.emc.logservice.server;

import com.emc.logservice.contracts.AppendContext;

/**
 * Defines an updateable StreamSegment Metadata.
 */
public interface UpdateableSegmentMetadata extends SegmentMetadata {
    /**
     * Sets the current StorageLength for this StreamSegment.
     *
     * @param value The StorageLength to set.
     * @throws IllegalArgumentException If the value is invalid.
     */
    void setStorageLength(long value);

    /**
     * Sets the current DurableLog Length for this StreamSegment.
     *
     * @param value
     * @throws IllegalArgumentException If the value is invalid.
     */
    void setDurableLogLength(long value);

    /**
     * Marks this StreamSegment as sealed for modifications.
     */
    void markSealed();

    /**
     * Marks this StreamSegment as deleted.
     */
    void markDeleted();

    /**
     * Records the given Append Context and marks it as the one for the last committed Append Context.
     * @param appendContext The AppendContext to record.
     */
    void recordAppendContext(AppendContext appendContext);
}
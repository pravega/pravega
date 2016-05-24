package com.emc.logservice;

/**
 * Defines an immutable Stream Segment Container Metadata.
 */
public interface ContainerMetadata extends SegmentMetadataCollection {
    /**
     * Gets a value indicating the Id of the StreamSegmentContainer this Metadata refers to.
     *
     * @return
     */
    String getContainerId();

    /**
     * Gets a value indicating whether we are currently in Recovery Mode.
     *
     * @return
     */
    boolean isRecoveryMode();

    /**
     * Gets the next available Operation Sequence Number. Atomically increments the value by 1 with every call.
     *
     * @return The next available Operation Sequence Number.
     * @throws IllegalStateException If the Metadata is in Recovery Mode.
     */
    long getNewOperationSequenceNumber();
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server;

/**
 * Defines the Operations on Metadata that can pertain to Recovery Mode, including taking it in/out of that mode.
 */
public interface RecoverableMetadata {

    /**
     * Puts the Metadata into Recovery Mode. Recovery Mode indicates that the Metadata is about to be
     * regenerated from various sources, and is not yet ready for normal operation.
     * <p>
     * If the Metadata is in Recovery Mode, some operations may not be executed, while others are allowed to. Inspect
     * the documentation for each method to find the behavior of each.
     *
     * @throws IllegalStateException If the Metadata is already in Recovery Mode.
     */
    void enterRecoveryMode();

    /**
     * Takes the Metadata out of Recovery Mode.
     *
     * @throws IllegalStateException If the Metadata is not in Recovery Mode.
     */
    void exitRecoveryMode();

    /**
     * Resets the Metadata to its original state.
     *
     * @throws IllegalStateException If the Metadata is not in Recovery Mode.
     */
    void reset();

    /**
     * Sets the current Operation Sequence Number.
     *
     * @param value The new Operation Sequence Number.
     * @throws IllegalStateException    If the Metadata is not in Recovery Mode.
     * @throws IllegalArgumentException If the new Sequence Number is not greater than the previous one.
     */
    void setOperationSequenceNumber(long value);
}

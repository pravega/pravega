package com.emc.logservice;

/**
 * Defines a Metadata that can enter/exit Recovery Mode.
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
}

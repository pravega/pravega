/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.storage;

/**
 * Represents the base for an addressing scheme inside a DurableDataLog. This can be used for accurately locating where
 * DataFrames are stored inside a DurableDataLog.
 * <p/>
 * Subclasses would be specific to DurableDataLog implementations.
 */
public abstract class LogAddress {
    private final long sequence;

    /**
     * Creates a new instance of the LogAddress class.
     * @param sequence The sequence of the address (location).
     */
    public LogAddress(long sequence) {
        this.sequence = sequence;
    }

    /**
     * Gets a value indicating the Sequence of the address (location).
     */
    public long getSequence() {
        return this.sequence;
    }

    @Override
    public String toString() {
        return String.format("Sequence = %d", this.sequence);
    }
}

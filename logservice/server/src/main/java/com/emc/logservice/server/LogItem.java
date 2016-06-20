package com.emc.logservice.server;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Defines the minimum structure of an Item that is appended to a Sequential Log.
 */
public interface LogItem {
    /**
     * Gets a value indicating the Sequence Number for this item.
     * The Sequence Number is a unique, strictly monotonically increasing number that assigns order to items.
     *
     * @return The Sequence Number.
     */
    long getSequenceNumber();

    /**
     * Serializes this item to the given OutputStream.
     *
     * @param output The OutputStream to serialize to.
     * @throws IOException           If the given OutputStream threw one.
     * @throws IllegalStateException If the serialization conditions are not met.
     */
    void serialize(OutputStream output) throws IOException;
}

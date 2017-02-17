/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

import com.emc.pravega.service.server.logs.SerializationException;

import java.io.InputStream;

/**
 * Factory for LogItems.
 */
public interface LogItemFactory<T extends LogItem> {
    /**
     * Deserializes a LogItem from the given InputStream.
     *
     * @param input The InputStream to deserialize from.
     * @return The deserialized LogItem.
     * @throws SerializationException If the LogItem could not be deserialized.
     */
    T deserialize(InputStream input) throws SerializationException;
}

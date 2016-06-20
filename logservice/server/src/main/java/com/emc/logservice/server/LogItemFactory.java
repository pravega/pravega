package com.emc.logservice.server;

import com.emc.logservice.server.logs.SerializationException;

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

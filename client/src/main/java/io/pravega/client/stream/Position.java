/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import io.pravega.client.stream.impl.PositionInternal;
import java.nio.ByteBuffer;

/**
 * A position in a stream. Used to indicate where a reader died. See {@link ReaderGroup#readerOffline(String, Position)}
 * Note that this is serializable so that it can be written to an external datastore.
 *
 */
public interface Position {
    
    /**
     * Used internally. Do not call.
     *
     * @return Implementation of position object interface
     */
    PositionInternal asImpl();
    
    /**
     * Serializes the position to a compact byte array.
     *
     * @return compact byte array
     */
    ByteBuffer toBytes();
    
    /**
     * Deserializes the position from its serialized from obtained from calling {@link #toBytes()}.
     * 
     * @param serializedPosition A serialized position.
     * @return The position object.
     */
    static Position fromBytes(ByteBuffer serializedPosition) {
        return PositionInternal.fromBytes(serializedPosition);
    }
}

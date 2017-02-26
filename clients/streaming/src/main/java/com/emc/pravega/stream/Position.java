/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

import com.emc.pravega.stream.impl.PositionInternal;

import java.io.Serializable;

/**
 * A position in a stream. Used to indicate where a reader died. See {@link ReaderGroup#readerOffline(String, Position)}
 * Note that this is serializable so that it can be written to an external datastore.
 *
 */
public interface Position extends Serializable {
    
    /**
     * Used internally. Do not call.
     *
     * @return Implementation of position object interface
     */
    PositionInternal asImpl();
}

/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.logs.operations;

import io.pravega.service.server.logs.SerializationException;

import java.io.DataInputStream;

/**
 * Log Operation that deals with Metadata Operations. This is generally an internal-only operation and is not necessarily
 * the direct outcome of an external call.
 */
public abstract class MetadataOperation extends Operation {
    //region Constructor

    /**
     * Creates a new instance of the MetadataOperation class.
     */
    MetadataOperation() {
        super();
    }

    MetadataOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion
}

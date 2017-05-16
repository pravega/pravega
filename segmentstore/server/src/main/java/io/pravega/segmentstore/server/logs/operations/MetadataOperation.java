/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs.operations;

import io.pravega.segmentstore.server.logs.SerializationException;

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

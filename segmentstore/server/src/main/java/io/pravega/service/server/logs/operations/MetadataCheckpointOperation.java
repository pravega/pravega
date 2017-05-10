/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server.logs.operations;

import io.pravega.service.server.logs.SerializationException;
import java.io.DataInputStream;

/**
 * Log Operation that contains a checkpoint of the Metadata at a particular point in time.
 */
public class MetadataCheckpointOperation extends CheckpointOperationBase {
    //region Constructor

    /**
     * Creates a new instance of the MetadataCheckpointOperation class.
     */
    public MetadataCheckpointOperation() {
        super();
    }

    protected MetadataCheckpointOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region Operation Implementation

    @Override
    protected OperationType getOperationType() {
        return OperationType.MetadataCheckpoint;
    }

    //endregion
}

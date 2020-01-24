/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs.operations;

/**
 * Log Operation that contains a checkpoint of the Metadata at a particular point in time that contains only information
 * about the Storage state of Segments (essentially those items that are not updated using regular Log Operations).
 */
public class StorageMetadataCheckpointOperation extends CheckpointOperationBase {
    static class Serializer extends SerializerBase<StorageMetadataCheckpointOperation> {
        @Override
        protected OperationBuilder<StorageMetadataCheckpointOperation> newBuilder() {
            return new OperationBuilder<>(new StorageMetadataCheckpointOperation());
        }
    }

}

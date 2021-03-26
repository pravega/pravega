/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

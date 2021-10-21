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
package io.pravega.segmentstore.storage.metadata;

import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.mocks.MockStorageMetadata;
import lombok.Data;

import java.io.Serializable;

/**
 * Storage Metadata.
 * All storage related metadata is stored in {@link ChunkMetadataStore} using set of key-value pairs.
 * The String value returned by {@link StorageMetadata#getKey()} is used as key.
 * Notable derived classes are {@link SegmentMetadata} and {@link ChunkMetadata} which form the core of metadata related
 * to {@link ChunkedSegmentStorage} functionality.
 */
@Data
public abstract class StorageMetadata implements Serializable {

    /**
     * Retrieves the key associated with the metadata.
     *
     * @return key.
     */
    public abstract String getKey();

    /**
     * Creates a deep copy of this instance.
     *
     * @return A deep copy of this instance.
     */
    public abstract StorageMetadata deepCopy();

    /**
     * Serializer that implements {@link VersionedSerializer}.
     */
    public static class StorageMetadataSerializer extends VersionedSerializer.MultiType<StorageMetadata> {
        /**
         * Declare all supported serializers of subtypes.
         *
         * @param builder A MultiType.Builder that can be used to declare serializers.
         */
        @Override
        protected void declareSerializers(Builder builder) {
            // Unused values (Do not repurpose!):
            // - 0: Unsupported Serializer.
            builder.serializer(MockStorageMetadata.class, 1, new MockStorageMetadata.Serializer())
                    .serializer(ChunkMetadata.class, 2, new ChunkMetadata.Serializer())
                    .serializer(SegmentMetadata.class, 3, new SegmentMetadata.Serializer())
                    .serializer(ReadIndexBlockMetadata.class, 4, new ReadIndexBlockMetadata.Serializer());
        }
    }
}

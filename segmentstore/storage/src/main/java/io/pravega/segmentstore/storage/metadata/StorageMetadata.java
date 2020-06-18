/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.segmentstore.storage.mocks.MockStorageMetadata;
import lombok.Data;

import java.io.Serializable;

/**
 * Storage Metadata.
 * All storage related metadata is stored in {@link ChunkMetadataStore} using set of key-value pairs.
 * The String value returned by {@link StorageMetadata#getKey()} is used as key.
 * Notable derived classes are {@link SegmentMetadata} and {@link ChunkMetadata} which form the core of metadata related
 * to {@link io.pravega.segmentstore.storage.chunklayer.ChunkStorageManager} functionality.
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
     * @return
     */
    public abstract StorageMetadata deepCopy();

    /**
     * Helper method that converts empty string to null value.
     *
     * @param toConvert String to convert.
     * @return If toConvert is null then it returns empty string. Otherwise returns original string.
     */
    public static String toNullableString(String toConvert) {
        if (toConvert.length() == 0) {
            return null;
        }
        return toConvert;
    }

    /**
     * Helper method that converts null value to empty string.
     *
     * @param toConvert String to convert.
     * @return If toConvert is null then it returns empty string. Otherwise returns original string.
     */
    public static String fromNullableString(String toConvert) {
        if (null == toConvert) {
            return "";
        }
        return toConvert;
    }

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
                    .serializer(SegmentMetadata.class, 3, new SegmentMetadata.Serializer());
        }
    }
}
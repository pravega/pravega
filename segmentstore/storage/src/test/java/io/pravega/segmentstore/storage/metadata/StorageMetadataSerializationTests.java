/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import io.pravega.segmentstore.storage.mocks.MockStorageMetadata;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for serialization of {@link StorageMetadata}.
 */
public class StorageMetadataSerializationTests {

    @Test
    public void testMockMetadataSerialization() throws Exception {
        testStorageMetadataSerialization(new MockStorageMetadata("Foo", "Bar"));
    }

    @Test
    public void testSegmentMetadataSerialization() throws Exception {
        testStorageMetadataSerialization(SegmentMetadata.builder()
                .name("name")
                .length(1)
                .chunkCount(2)
                .startOffset(3)
                .status(5)
                .maxRollinglength(6)
                .firstChunk("firstChunk")
                .lastChunk("lastChunk")
                .lastModified(7)
                .firstChunkStartOffset(8)
                .lastChunkStartOffset(9)
                .ownerEpoch(10)
                .build());

        // With nullable values
        testStorageMetadataSerialization(SegmentMetadata.builder()
                .name("name")
                .length(1)
                .chunkCount(2)
                .startOffset(3)
                .status(5)
                .maxRollinglength(6)
                .firstChunk(null)
                .lastChunk(null)
                .lastModified(7)
                .firstChunkStartOffset(8)
                .lastChunkStartOffset(9)
                .ownerEpoch(10)
                .build());
    }

    @Test
    public void testStorageMetadataSerialization() throws Exception {
        testStorageMetadataSerialization(ChunkMetadata.builder()
                .name("name")
                .nextChunk("nextChunk")
                .length(1)
                .build());
        // With nullable values
        testStorageMetadataSerialization(ChunkMetadata.builder()
                .name("name")
                .length(1)
                .build());
    }

    private void testStorageMetadataSerialization(StorageMetadata original) throws Exception {
        val serializer = new StorageMetadata.StorageMetadataSerializer();
        val bytes = serializer.serialize(original);
        val obj = serializer.deserialize(bytes);
        Assert.assertEquals(original, obj);
    }

    @Test
    public void testTransactionDataSerialization() throws Exception {
        testTransactionDataSerialization(
                BaseMetadataStore.TransactionData.builder()
                        .key("key")
                        .persisted(false)
                        .pinned(false)
                        .version(111)
                        .value(SegmentMetadata.builder()
                                .name("name")
                                .length(1)
                                .chunkCount(2)
                                .startOffset(3)
                                .status(5)
                                .maxRollinglength(6)
                                .firstChunk("firstChunk")
                                .lastChunk("lastChunk")
                                .lastModified(7)
                                .firstChunkStartOffset(8)
                                .lastChunkStartOffset(9)
                                .ownerEpoch(10)
                                .build())
                        .build());
        // With nullable values
        testTransactionDataSerialization(
                BaseMetadataStore.TransactionData.builder()
                        .key("key")
                        .persisted(false)
                        .pinned(false)
                        .version(111)
                        .value(null)
                        .build());
    }

    private void testTransactionDataSerialization(BaseMetadataStore.TransactionData original) throws Exception {
        val serializer = new BaseMetadataStore.TransactionData.TransactionDataSerializer();
        val bytes = serializer.serialize(original);
        val obj = serializer.deserialize(bytes);
        Assert.assertEquals(original, obj);
    }
}

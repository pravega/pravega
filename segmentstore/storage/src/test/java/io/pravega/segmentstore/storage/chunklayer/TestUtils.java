/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadata;
import lombok.val;
import org.junit.Assert;

/**
 * Test utility.
 */
public class TestUtils {
    public static void checkSegmentLayout(ChunkMetadataStore metadataStore, String targetSegmentName, long lengthOfChunk, int numberOfchunks, long expectedStartOffset, long expectedLength) throws Exception {
        val segmentMetadata = getSegmentMetadata(metadataStore, targetSegmentName);
        Assert.assertNotNull(segmentMetadata);
        Assert.assertEquals(expectedLength, segmentMetadata.getLength());
        Assert.assertEquals(expectedStartOffset, segmentMetadata.getStartOffset());
        // Assert
        Assert.assertNotNull(segmentMetadata.getFirstChunk());
        Assert.assertNotNull(segmentMetadata.getLastChunk());

        int i = 0;
        String current = segmentMetadata.getFirstChunk();
        while (null != current) {
            val chunk = getChunkMetadata(metadataStore, current);
            current = chunk.getNextChunk();
            Assert.assertEquals(lengthOfChunk, chunk.getLength());
            i++;
        }
        Assert.assertEquals(numberOfchunks, i);
    }

    public static void checkSegmentLayout(ChunkMetadataStore metadataStore, String targetSegmentName, long[] expectedLengths) throws Exception {
        checkSegmentLayout(metadataStore, targetSegmentName, expectedLengths, expectedLengths[expectedLengths.length - 1]);
    }

    public static void checkSegmentLayout(ChunkMetadataStore metadataStore, String targetSegmentName, long[] expectedLengths, long lastChunkLengthInStorage) throws Exception {
        val segmentMetadata = getSegmentMetadata(metadataStore, targetSegmentName);
        Assert.assertNotNull(segmentMetadata);

        // Assert
        Assert.assertNotNull(segmentMetadata.getFirstChunk());
        Assert.assertNotNull(segmentMetadata.getLastChunk());
        int expectedLength = 0;
        int i = 0;
        String current = segmentMetadata.getFirstChunk();
        while (null != current) {
            val chunk = getChunkMetadata(metadataStore, current);
            current = chunk.getNextChunk();

            Assert.assertEquals("Chunk " + Integer.toString(i) + " has unexpected length",
                    i == expectedLengths.length-1 ? lastChunkLengthInStorage : expectedLengths[i],
                    chunk.getLength());
            expectedLength += chunk.getLength();
            i++;
        }
        Assert.assertEquals(expectedLengths.length, i);
        Assert.assertEquals(expectedLength, segmentMetadata.getLength());
    }

    public static StorageMetadata get(ChunkMetadataStore metadataStore, String key) throws Exception {
        try (val txn = metadataStore.beginTransaction()) {
            return metadataStore.get(txn, key);
        }
    }

    public static SegmentMetadata getSegmentMetadata(ChunkMetadataStore metadataStore, String key) throws Exception {
        try (val txn = metadataStore.beginTransaction()) {
            return (SegmentMetadata) metadataStore.get(txn, key);
        }
    }

    public static ChunkMetadata getChunkMetadata(ChunkMetadataStore metadataStore, String key) throws Exception {
        try (val txn = metadataStore.beginTransaction()) {
            return (ChunkMetadata) metadataStore.get(txn, key);
        }
    }
}

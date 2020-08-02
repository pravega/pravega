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

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Test utility.
 */
public class TestUtils {
    /**
     * Checks the bounds for the given segment.
     *
     * @param metadataStore       Metadata store to query.
     * @param segmentName         Name of the segment.
     * @param expectedStartOffset Expected start offset.
     * @param expectedLength      Expected length.
     * @throws Exception Exceptions are thrown in case of any errors.
     */
    public static void checkSegmentBounds(ChunkMetadataStore metadataStore, String segmentName, long expectedStartOffset, long expectedLength) throws Exception {
        val segmentMetadata = getSegmentMetadata(metadataStore, segmentName);
        Assert.assertNotNull(segmentMetadata);
        Assert.assertEquals(expectedLength, segmentMetadata.getLength());
        Assert.assertEquals(expectedStartOffset, segmentMetadata.getStartOffset());
    }

    /**
     * Checks the layout of the chunks for given segment.
     *
     * @param metadataStore  Metadata store to query.
     * @param segmentName    Name of the segment.
     * @param lengthOfChunk  Length of each chunk.
     * @param numberOfchunks Number of chunks.
     * @throws Exception Exceptions are thrown in case of any errors.
     */
    public static void checkSegmentLayout(ChunkMetadataStore metadataStore, String segmentName, long lengthOfChunk, int numberOfchunks) throws Exception {
        val segmentMetadata = getSegmentMetadata(metadataStore, segmentName);
        Assert.assertNotNull(segmentMetadata);
        // Assert
        Assert.assertNotNull(segmentMetadata.getFirstChunk());
        Assert.assertNotNull(segmentMetadata.getLastChunk());

        int i = 0;
        val chunks = getChunkList(metadataStore, segmentName);
        for (val chunk : chunks) {
            Assert.assertEquals(lengthOfChunk, chunk.getLength());
            i++;
        }
        Assert.assertEquals(numberOfchunks, chunks.size());
    }

    /**
     * Checks the layout of the chunks for given segment.
     *
     * @param metadataStore   Metadata store to query.
     * @param segmentName     Name of the segment.
     * @param expectedLengths Array of expected lengths.
     * @throws Exception Exceptions are thrown in case of any errors.
     */
    public static void checkSegmentLayout(ChunkMetadataStore metadataStore, String segmentName, long[] expectedLengths) throws Exception {
        checkSegmentLayout(metadataStore, segmentName, expectedLengths, expectedLengths[expectedLengths.length - 1]);
    }

    /**
     * Checks the layout of the chunks for given segment.
     *
     * @param metadataStore            Metadata store to query.
     * @param segmentName              Name of the segment.
     * @param expectedLengths          Array of expected lengths.
     * @param lastChunkLengthInStorage Length of the last chunk in storage.
     * @throws Exception Exceptions are thrown in case of any errors.
     */
    public static void checkSegmentLayout(ChunkMetadataStore metadataStore, String segmentName, long[] expectedLengths, long lastChunkLengthInStorage) throws Exception {
        val segmentMetadata = getSegmentMetadata(metadataStore, segmentName);
        Assert.assertNotNull(segmentMetadata);

        // Assert
        Assert.assertNotNull(segmentMetadata.getFirstChunk());
        Assert.assertNotNull(segmentMetadata.getLastChunk());
        int expectedLength = 0;
        int i = 0;
        val chunks = getChunkList(metadataStore, segmentName);
        for (val chunk : chunks) {
            Assert.assertEquals("Chunk " + Integer.toString(i) + " has unexpected length",
                    i == expectedLengths.length - 1 ? lastChunkLengthInStorage : expectedLengths[i],
                    chunk.getLength());
            expectedLength += chunk.getLength();
            i++;
        }
        Assert.assertEquals(expectedLengths.length, chunks.size());

        Assert.assertEquals(expectedLengths.length, i);
        Assert.assertEquals(expectedLength, segmentMetadata.getLength());
        Assert.assertEquals(expectedLengths.length, segmentMetadata.getChunkCount());
    }

    /**
     * Retrieves the {@link StorageMetadata} with given key from given {@link ChunkMetadataStore}.
     *
     * @param metadataStore Metadata store to query.
     * @param key           Key.
     * @return {@link StorageMetadata} if found, null otherwise.
     * @throws Exception Exceptions are thrown in case of any errors.
     */
    public static StorageMetadata get(ChunkMetadataStore metadataStore, String key) throws Exception {
        try (val txn = metadataStore.beginTransaction()) {
            return txn.get(key);
        }
    }

    /**
     * Retrieves the {@link SegmentMetadata} with given key from given {@link ChunkMetadataStore}.
     *
     * @param metadataStore Metadata store to query.
     * @param key           Key.
     * @return {@link SegmentMetadata} if found, null otherwise.
     * @throws Exception Exceptions are thrown in case of any errors.
     */
    public static SegmentMetadata getSegmentMetadata(ChunkMetadataStore metadataStore, String key) throws Exception {
        try (val txn = metadataStore.beginTransaction()) {
            return (SegmentMetadata) txn.get(key);
        }
    }

    /**
     * Retrieves the {@link ChunkMetadata} with given key from given {@link ChunkMetadataStore}.
     *
     * @param metadataStore Metadata store to query.
     * @param key           Key.
     * @return {@link ChunkMetadata} if found, null otherwise.
     * @throws Exception Exceptions are thrown in case of any errors.
     */
    public static ChunkMetadata getChunkMetadata(ChunkMetadataStore metadataStore, String key) throws Exception {
        try (val txn = metadataStore.beginTransaction()) {
            return (ChunkMetadata) txn.get(key);
        }
    }

    /**
     * Gets the list of chunks for the given segment.
     *
     * @param metadataStore Metadata store to query.
     * @param key           Key.
     * @return List of {@link ChunkMetadata} for the segment.
     * @throws Exception Exceptions are thrown in case of any errors.
     */
    public static ArrayList<ChunkMetadata> getChunkList(ChunkMetadataStore metadataStore, String key) throws Exception {
        try (val txn = metadataStore.beginTransaction()) {
            val segmentMetadata = getSegmentMetadata(metadataStore, key);
            Assert.assertNotNull(segmentMetadata);
            ArrayList<ChunkMetadata> chunkList = new ArrayList<ChunkMetadata>();
            String current = segmentMetadata.getFirstChunk();
            while (null != current) {
                val chunk = (ChunkMetadata) txn.get(current);
                chunkList.add(chunk);
                current = chunk.getNextChunk();
            }
            return chunkList;
        }
    }

    /**
     * Checks if all chunks actually exist in storage for given segment.
     *
     * @param storageProvider {@link ChunkStorage} instance to check.
     * @param metadataStore   {@link ChunkMetadataStore} instance to check.
     * @param segmentName     Segment name to check.
     * @throws Exception Exceptions are thrown in case of any errors.
     */
    public static void checkChunksExistInStorage(ChunkStorage storageProvider, ChunkMetadataStore metadataStore, String segmentName) throws Exception {
        int chunkCount = 0;
        long dataSize = 0;
        val segmentMetadata = getSegmentMetadata(metadataStore, segmentName);
        HashSet<String> visited = new HashSet<>();
        val chunkList = getChunkList(metadataStore, segmentName);
        for (ChunkMetadata chunkMetadata : chunkList) {
            Assert.assertTrue(storageProvider.exists(chunkMetadata.getName()));
            val info = storageProvider.getInfo(chunkMetadata.getName());
            Assert.assertTrue(String.format("Actual %s, Expected %d", chunkMetadata, info.getLength()),
                    chunkMetadata.getLength() <= info.getLength());
            chunkCount++;
            Assert.assertTrue("Chunk length should be non negative", info.getLength() >= 0);
            Assert.assertTrue(info.getLength() <= segmentMetadata.getMaxRollinglength());
            Assert.assertTrue(info.getLength() >= chunkMetadata.getLength());
            Assert.assertFalse("All chunks should be unique", visited.contains(info.getName()));
            visited.add(info.getName());
            dataSize += chunkMetadata.getLength();
        }
        Assert.assertEquals(chunkCount, segmentMetadata.getChunkCount());
        Assert.assertEquals(dataSize, segmentMetadata.getLength() - segmentMetadata.getFirstChunkStartOffset());
    }

    /**
     * Asserts that SegmentMetadata and its associated list of ChunkMetadata matches expected values.
     *
     * @param expectedSegmentMetadata   Expected {@link SegmentMetadata}.
     * @param expectedChunkMetadataList Expected list of {@link ChunkMetadata}.
     * @param actualSegmentMetadata     Actual {@link SegmentMetadata}.
     * @param actualChunkMetadataList   Actual list of {@link ChunkMetadata}.
     * @throws Exception {@link AssertionError} is thrown in case of mismatch.
     */
    public static void assertEquals(SegmentMetadata expectedSegmentMetadata, ArrayList<ChunkMetadata> expectedChunkMetadataList, SegmentMetadata actualSegmentMetadata, ArrayList<ChunkMetadata> actualChunkMetadataList) throws Exception {
        Assert.assertEquals(expectedSegmentMetadata, actualSegmentMetadata);
        Assert.assertEquals(expectedChunkMetadataList.size(), actualChunkMetadataList.size());

        for (int i = 0; i < expectedChunkMetadataList.size(); i++) {
            val expectedChunkMetadata = expectedChunkMetadataList.get(i);
            val actualChunkMetadata = actualChunkMetadataList.get(i);
            Assert.assertEquals(expectedChunkMetadata, actualChunkMetadata);
        }
    }
}

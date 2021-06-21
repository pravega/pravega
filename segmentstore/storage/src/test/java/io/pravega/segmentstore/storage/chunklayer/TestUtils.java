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
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.ReadIndexBlockMetadata;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StorageMetadata;
import io.pravega.shared.NameUtils;
import lombok.val;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;

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
        long expectedLength = 0;
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
     * Checks the existence of read index block metadata records for given segment.
     * @param chunkedSegmentStorage  ChunkedSegmentStorage.
     * @param metadataStore  Metadata store to query.
     * @param segmentName    Name of the segment.
     * @param startOffset    Start offset of the segment.
     * @param endOffset      End offset of the segment.
     * @param checkReadIndex True if readIndex entries should be checked.
     * @throws Exception Exceptions are thrown in case of any errors.
     */
    public static void checkReadIndexEntries(ChunkedSegmentStorage chunkedSegmentStorage, ChunkMetadataStore metadataStore, String segmentName, long startOffset, long endOffset, boolean checkReadIndex) throws Exception {
        val blockSize = chunkedSegmentStorage.getConfig().getIndexBlockSize();
        val segmentReadIndex = chunkedSegmentStorage.getReadIndexCache().getSegmentsReadIndexCache().getIfPresent(segmentName);
        try (val txn = metadataStore.beginTransaction(true, new String[] {segmentName})) {
            val segmentMetadata = (SegmentMetadata) txn.get(segmentName).get();
            Assert.assertNotNull(segmentMetadata);
            TreeMap<Long, String> index = new TreeMap<>();
            String current = segmentMetadata.getFirstChunk();
            long offset = segmentMetadata.getFirstChunkStartOffset();
            while (null != current) {
                val chunk = (ChunkMetadata) txn.get(current).get();
                Assert.assertNotNull(chunk);
                if (checkReadIndex && startOffset <= offset) {
                    Assert.assertNotNull("Offset=" + offset, segmentReadIndex.getOffsetToChunkNameIndex().get(offset));
                    Assert.assertEquals("Offset=" + offset, chunk.getName(), segmentReadIndex.getOffsetToChunkNameIndex().get(offset).getChunkName());
                }
                index.put(offset, chunk.getName());
                offset += chunk.getLength();
                current = chunk.getNextChunk();
            }
            if (checkReadIndex) {
                for (val entry : segmentReadIndex.getOffsetToChunkNameIndex().entrySet()) {
                    Assert.assertNotNull("Offset=" + entry.getKey(), index.get(entry.getKey()));
                    Assert.assertEquals("Offset=" + entry.getKey(), entry.getValue().getChunkName(), index.get(entry.getKey()));
                }
            }

            long blockStartOffset;
            for (blockStartOffset = 0; blockStartOffset < segmentMetadata.getLength(); blockStartOffset +=  blockSize) {
                // For all offsets below start offset, there should not be any index entries.
                if (segmentMetadata.getStartOffset() > blockStartOffset) {
                    Assert.assertNull("for offset:" + blockStartOffset, txn.get(NameUtils.getSegmentReadIndexBlockName(segmentName, blockStartOffset)).get());
                }

                // For all valid offsets, there should be index entries.
                if (segmentMetadata.getStartOffset() <= blockStartOffset) {
                    val blockIndexEntry = (ReadIndexBlockMetadata) txn.get(NameUtils.getSegmentReadIndexBlockName(segmentName, blockStartOffset)).get();
                    Assert.assertNotNull("for offset:" + blockStartOffset, blockIndexEntry);
                    Assert.assertNotNull("for offset:" + blockStartOffset, txn.get(blockIndexEntry.getChunkName()));
                    val mappedChunk = index.floorEntry(blockStartOffset);
                    Assert.assertNotNull(mappedChunk);
                    Assert.assertEquals("for offset:" + blockStartOffset, mappedChunk.getValue(), blockIndexEntry.getChunkName());
                }
            }
            // For all offsets after end of the segment, there should not be any index entries
            Assert.assertNull("for offset:" + segmentMetadata.getLength(),
                    txn.get(NameUtils.getSegmentReadIndexBlockName(segmentName, segmentMetadata.getLength())).get());
            Assert.assertNull("for offset:" + segmentMetadata.getLength() + blockSize,
                    txn.get(NameUtils.getSegmentReadIndexBlockName(segmentName, segmentMetadata.getLength() + blockSize)).get());
        }
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
        try (val txn = metadataStore.beginTransaction(true, new String[] {key})) {
            return txn.get(key).get();
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
        try (val txn = metadataStore.beginTransaction(true, new String[] {key})) {
            return (SegmentMetadata) txn.get(key).get();
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
        try (val txn = metadataStore.beginTransaction(true, new String[] {key})) {
            return (ChunkMetadata) txn.get(key).get();
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
        try (val txn = metadataStore.beginTransaction(true, new String[] {key})) {
            val segmentMetadata = getSegmentMetadata(metadataStore, key);
            Assert.assertNotNull(segmentMetadata);
            ArrayList<ChunkMetadata> chunkList = new ArrayList<ChunkMetadata>();
            String current = segmentMetadata.getFirstChunk();
            while (null != current) {
                val chunk = (ChunkMetadata) txn.get(current).get();
                Assert.assertNotNull(chunk);
                chunkList.add(chunk);
                current = chunk.getNextChunk();
            }
            return chunkList;
        }
    }

    /**
     * Checks if all chunks actually exist in storage for given segment.
     *
     * @param chunkStorage {@link ChunkStorage} instance to check.
     * @param metadataStore   {@link ChunkMetadataStore} instance to check.
     * @param segmentName     Segment name to check.
     * @throws Exception Exceptions are thrown in case of any errors.
     */
    public static void checkChunksExistInStorage(ChunkStorage chunkStorage, ChunkMetadataStore metadataStore, String segmentName) throws Exception {
        int chunkCount = 0;
        long dataSize = 0;
        val segmentMetadata = getSegmentMetadata(metadataStore, segmentName);
        HashSet<String> visited = new HashSet<>();
        val chunkList = getChunkList(metadataStore, segmentName);
        for (ChunkMetadata chunkMetadata : chunkList) {
            Assert.assertTrue(chunkStorage.exists(chunkMetadata.getName()).get());
            val info = chunkStorage.getInfo(chunkMetadata.getName()).get();
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

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

import io.pravega.segmentstore.storage.metadata.BaseMetadataStore;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * Tests for {@link UtilsWrapper}.
 */
public class UtilsWrapperTests extends ThreadPooledTestSuite {

    Random random = new Random();

    @Test
    public void testGetExtendedChunkInfoListNoStorageCheck() throws Exception {
        testGetExtendedChunkInfoList(false);
    }

    @Test
    public void testGetExtendedChunkInfoList() throws Exception {
        testGetExtendedChunkInfoList(true);
    }

    private void testGetExtendedChunkInfoList(boolean checkStorage) throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        val segmentName = "test";
        // Different number of chunks
        testGetExtendedChunkInfoList(config, segmentName, checkStorage, new long[] {}, new long[] {},  new int[] {});
        testGetExtendedChunkInfoList(config, segmentName, checkStorage, new long[] {1}, new long[] {1},  new int[] {});
        testGetExtendedChunkInfoList(config, segmentName, checkStorage, new long[] {1, 2, 3}, new long[] {1, 2, 3},  new int[] {});

        // Missing chunks
        testGetExtendedChunkInfoList(config, segmentName, checkStorage, new long[] {1, 2, 3}, new long[] {1, 2, 3},  new int[] {1});
        testGetExtendedChunkInfoList(config, segmentName, checkStorage, new long[] {1, 2, 3}, new long[] {1, 2, 3},  new int[] {1, 3});

        // Different length in storage and metadata
        testGetExtendedChunkInfoList(config, segmentName, checkStorage, new long[] {1, 2, 3}, new long[] {4, 5, 6},  new int[] {1});
        testGetExtendedChunkInfoList(config, segmentName, checkStorage, new long[] {1, 2, 3}, new long[] {4, 5, 6},  new int[] {1, 3});
    }

    private void testGetExtendedChunkInfoList(ChunkedSegmentStorageConfig config, String segmentName, boolean shouldCheckStorage,
                                              long[] lengthsInStorage, long[] lengthsInMetadata, int[] chunksToDelete) throws Exception {
        // Set up
        @Cleanup
        ChunkStorage chunkStorage = new InMemoryChunkStorage(executorService());
        @Cleanup
        BaseMetadataStore metadataStore = new InMemoryMetadataStore(config, executorService());
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(42, chunkStorage, metadataStore, executorService(), config);
        chunkedSegmentStorage.initialize(123);

        // Insert metadata
        TestUtils.insertMetadata(segmentName, 25, 123, lengthsInMetadata, lengthsInStorage, false, false, metadataStore, chunkedSegmentStorage);
        val chunkMetadataList = TestUtils.getChunkList(metadataStore, segmentName);

        // Delete few chunks
        HashSet<String> deletedChunkNames = new HashSet<>();
        for (int i = 0; i < chunksToDelete.length; i++) {
            val name = chunkMetadataList.get(i).getName();
            deletedChunkNames.add(name);
            chunkStorage.delete(ChunkHandle.writeHandle(name)).join();
        }

        // Test
        UtilsWrapper wrapper = new UtilsWrapper(chunkedSegmentStorage, 128, Duration.ZERO);

        // check the output
        checkExtendedChunkInfoList(wrapper, segmentName, shouldCheckStorage, lengthsInStorage, deletedChunkNames, chunkMetadataList);
    }

    private void checkExtendedChunkInfoList(UtilsWrapper wrapper, String segmentName, boolean shouldCheckStorage, long[] lengthsInStorage, Set<String> deletedChunkNames, List<ChunkMetadata> metadataList) throws Exception {
        val infoList = wrapper.getExtendedChunkInfoList(segmentName, shouldCheckStorage).join();
        Assert.assertEquals(metadataList.size(), infoList.size());

        // Check
        long startOffset = 0;
        for (int i = 0; i < infoList.size(); i++) {
            val info = infoList.get(i);
            val metadata = metadataList.get(i);
            Assert.assertEquals(info.getChunkName(), metadata.getName());
            Assert.assertEquals(info.getLengthInMetadata(), metadata.getLength());
            if (shouldCheckStorage && !deletedChunkNames.contains(info.getChunkName())) {
                Assert.assertEquals(lengthsInStorage[i], info.getLengthInStorage());
                Assert.assertTrue(info.isExistsInStorage());
            } else {
                Assert.assertEquals(0, info.getLengthInStorage());
                Assert.assertFalse(info.isExistsInStorage());
            }
            Assert.assertEquals(startOffset, info.getStartOffset());
            startOffset += info.getLengthInMetadata();
        }
    }

    @Test
    public void testCopy() throws Exception {
        testCopy(new long[] {1});
        testCopy(new long[] {1, 2});
        testCopy(new long[] {2, 2, 2, 2});
        testCopy(new long[] {1, 2, 3, 4});
    }

    private void testCopy(long[] chunkLengths) throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        val segmentName = "test";
        // Set up
        @Cleanup
        ChunkStorage chunkStorage = new InMemoryChunkStorage(executorService());
        @Cleanup
        BaseMetadataStore metadataStore = new InMemoryMetadataStore(config, executorService());
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(42, chunkStorage, metadataStore, executorService(), config);
        chunkedSegmentStorage.initialize(123);

        TestUtils.insertMetadata(segmentName, 25, 123, chunkLengths, chunkLengths, false, false, chunkedSegmentStorage.getMetadataStore(), chunkedSegmentStorage);
        val chunkMetadataList = TestUtils.getChunkList(chunkedSegmentStorage.getMetadataStore(), segmentName);

        // Test
        UtilsWrapper wrapper = new UtilsWrapper(chunkedSegmentStorage, 128, Duration.ZERO);

        byte[][] expected = new byte[chunkMetadataList.size()][];
        int sum = 0;
        for (int i = 0; i < chunkMetadataList.size(); i++) {
            val info = chunkMetadataList.get(i);
            val length = Math.toIntExact(info.getLength());

            // create random data
            expected[i] = new byte[length];
            random.nextBytes(expected[i]);

            // Overwrite chunk
            wrapper.overwriteChunk(info.getName(), new ByteArrayInputStream(expected[i]), length).join();
            val actual = new ByteArrayOutputStream(length);

            // read the same data back
            wrapper.copyFromChunk(info.getName(), actual).join();
            Assert.assertArrayEquals(expected[i], actual.toByteArray());
            sum += length;
        }

        // Read back and validate segment
        val segmentContents = new ByteArrayOutputStream(sum);
        wrapper.copyFromSegment(segmentName, segmentContents).join();
        long startOffset = 0;
        for (int i = 0; i < expected.length; i++) {
            assertContentEquals(segmentContents.toByteArray(), startOffset, expected[i]);
            startOffset += expected[i].length;
        }
    }

    private void assertContentEquals(byte[] data, long startOffset, byte[] expected) {
        for (int j = 0; j < expected.length; j++) {
            Assert.assertEquals(expected[j], data[Math.toIntExact(startOffset) + j]);
        }
    }

    @Test
    public void testEvictReadIndexCache() throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        // Set up
        @Cleanup
        ChunkStorage chunkStorage = new InMemoryChunkStorage(executorService());
        @Cleanup
        val metadataStore = new InMemoryMetadataStore(config, executorService());
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(42, chunkStorage, metadataStore, executorService(), config);
        chunkedSegmentStorage.initialize(123);
        val chunkSizes = new long[] {1};

        // Insert metadata
        TestUtils.insertMetadata("a", 42, 10, chunkSizes, chunkSizes, true, true, chunkedSegmentStorage.getMetadataStore(), chunkedSegmentStorage);
        TestUtils.insertMetadata("b", 42, 10, chunkSizes, chunkSizes, true, true, chunkedSegmentStorage.getMetadataStore(), chunkedSegmentStorage);
        TestUtils.insertMetadata("c", 42, 10, chunkSizes, chunkSizes, true, true, chunkedSegmentStorage.getMetadataStore(), chunkedSegmentStorage);

        // Test
        UtilsWrapper wrapper = new UtilsWrapper(chunkedSegmentStorage, 128, Duration.ZERO);

        wrapper.evictMetadataCache().join();

        Assert.assertNotNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("a", 0));
        Assert.assertNotNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("b", 0));
        Assert.assertNotNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("c", 0));

        wrapper.evictReadIndexCacheForSegment("a").join();
        Assert.assertNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("a", 0));
        Assert.assertNotNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("b", 0));
        Assert.assertNotNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("c", 0));

        wrapper.evictReadIndexCacheForSegment("b").join();
        Assert.assertNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("a", 0));
        Assert.assertNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("b", 0));
        Assert.assertNotNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("c", 0));

        wrapper.evictReadIndexCache().join();
        Assert.assertNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("a", 0));
        Assert.assertNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("b", 0));
        Assert.assertNull(wrapper.getChunkedSegmentStorage().getReadIndexCache().findFloor("c", 0));
    }

    @Test
    public void testInvalidParameters() throws Exception {
        // Set up
        @Cleanup
        ChunkStorage chunkStorage = new InMemoryChunkStorage(executorService());
        @Cleanup
        val metadataStore = new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(42, chunkStorage, metadataStore, executorService(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        chunkedSegmentStorage.initialize(123);

        AssertExtensions.assertThrows("Should not allow null chunkStorage",
                () -> new UtilsWrapper(null, 10, Duration.ZERO),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows("Should not allow null duration",
                () -> new UtilsWrapper(chunkedSegmentStorage, 10, null),
                ex -> ex instanceof NullPointerException);

        val wrapper = new UtilsWrapper(chunkedSegmentStorage, 10, Duration.ZERO);

        AssertExtensions.assertThrows("Should not allow null segmentName",
                () -> wrapper.evictReadIndexCacheForSegment(null),
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null chunkName",
                () -> wrapper.copyFromChunk(null, new ByteArrayOutputStream(1)),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows("Should not allow null outputStream",
                () -> wrapper.copyFromChunk("test", null),
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null chunkName",
                () -> wrapper.overwriteChunk(null, new ByteArrayInputStream(new byte[] {}), 1),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows("Should not allow null inputStream",
                () -> wrapper.overwriteChunk("test", null, 1),
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null segmentName",
                () -> wrapper.copyFromSegment(null, new ByteArrayOutputStream(1)),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows("Should not allow null outputStream",
                () -> wrapper.copyFromSegment("test", null),
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null outputStream",
                () -> wrapper.getExtendedChunkInfoList(null, false),
                ex -> ex instanceof NullPointerException);
    }

    @Test
    public void testCheckChunkSegmentStorageSanity() throws Exception {
        val chunkName = "TestChunk";
        @Cleanup
        BaseChunkStorage spyChunkStorage = spy(new InMemoryChunkStorage(executorService()));
        Exception exceptionToThrow = new ChunkStorageException("test", "Test Exception", new IOException("Test Exception"));
        val clazz = ChunkStorageException.class;
        doThrow(exceptionToThrow).when(spyChunkStorage).doCreateWithContent(any(), anyInt(), any());

        testSanity(spyChunkStorage, "");
    }

    private void testSanity(BaseChunkStorage spyChunkStorage, String message) {
        @Cleanup
        val metadataStore = new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());
        @Cleanup
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(42, spyChunkStorage, metadataStore, executorService(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        chunkedSegmentStorage.initialize(123);

        UtilsWrapper wrapper = new UtilsWrapper(chunkedSegmentStorage, 128, Duration.ZERO);

        AssertExtensions.assertThrows(message,
                () -> wrapper.checkChunkSegmentStorageSanity("TestChunk", 100),
                ex -> ex instanceof ChunkStorageException);
    }

    @Test
    public void testCheckChunkSegmentStorageSanity1 () throws Exception {
        val chunkName = "TestChunk";
        @Cleanup
        BaseChunkStorage spyChunkStorage = spy(new InMemoryChunkStorage(executorService()));
        Exception exceptionToThrow = new ChunkStorageException("test", "Test Exception", new IOException("Test Exception"));
        val clazz = ChunkStorageException.class;
        doThrow(exceptionToThrow).when(spyChunkStorage).doGetInfo(any());

        testSanity(spyChunkStorage, "");
    }

    @Test
    public void testCheckChunkSegmentStorageSanity2 () throws Exception {
        val chunkName = "TestChunk";
        @Cleanup
        BaseChunkStorage spyChunkStorage = spy(new InMemoryChunkStorage(executorService()));
        Exception exceptionToThrow = new ChunkStorageException("test", "Test Exception", new IOException("Test Exception"));
        val clazz = ChunkStorageException.class;
        doThrow(exceptionToThrow).when(spyChunkStorage).doOpenWrite(any());

        testSanity(spyChunkStorage, "");
    }

    @Test
    public void testCheckChunkSegmentStorageSanity3 () throws Exception {
        val chunkName = "TestChunk";
        @Cleanup
        BaseChunkStorage spyChunkStorage = spy(new InMemoryChunkStorage(executorService()));
        Exception exceptionToThrow = new ChunkStorageException("test", "Test Exception", new IOException("Test Exception"));
        val clazz = ChunkStorageException.class;
        doThrow(exceptionToThrow).when(spyChunkStorage).doRead(any(), anyLong(), anyInt(), any(), anyInt());

        testSanity(spyChunkStorage, "");
    }

    @Test
    public void testCheckChunkSegmentStorageSanity4 () throws Exception {
        val chunkName = "TestChunk";
        @Cleanup
        BaseChunkStorage spyChunkStorage = spy(new InMemoryChunkStorage(executorService()));
        Exception exceptionToThrow = new ChunkStorageException("test", "Test Exception", new IOException("Test Exception"));
        val clazz = ChunkStorageException.class;
        doThrow(exceptionToThrow).when(spyChunkStorage).doDelete(any());

        testSanity(spyChunkStorage, "");
    }
}

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

import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemoryTaskQueueManager;
import io.pravega.test.common.AssertExtensions;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.test.common.AssertExtensions.assertMayThrow;

/**
 * Scenario tests using ChunkedSegmentStorage, ChunkStorage and ChunkMetadataStore together.
 *
 * The derived classes are expected to override getChunkStorage to return instance of specific type derived from ChunkStorage.
 * In addition the method populate can be overriden to handle implementation specific logic. (Eg. NoOpChunkStorage)
 */
@Slf4j
public abstract class SimpleStorageTests extends StorageTestBase {
    private static final int CONTAINER_ID = 42;
    private static final int WRITE_COUNT = 5;
    private static final int THREAD_POOL_SIZE = 3;

    ChunkStorage chunkStorage;
    ChunkMetadataStore chunkMetadataStore;

    @Override
    protected int getThreadPoolSize() {
        return THREAD_POOL_SIZE;
    }

    /**
     * Creates a new instance of the Storage implementation to be tested. This will be cleaned up (via close()) upon
     * test termination.
     */
    @Override
    protected Storage createStorage() throws Exception {
        ScheduledExecutorService executor = executorService();
        synchronized (SimpleStorageTests.class) {
            if (null == chunkStorage) {
                chunkMetadataStore = getMetadataStore();
                chunkStorage = getChunkStorage();
            }
        }
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID,
                chunkStorage, chunkMetadataStore, executor, getDefaultConfig());
        chunkedSegmentStorage.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();
        return chunkedSegmentStorage;
    }

    protected ChunkedSegmentStorageConfig getDefaultConfig() {
        return ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
    }

    abstract protected ChunkStorage getChunkStorage() throws Exception;

    /**
     * Creates a new instance of Storage with forked metadata state.
     *
     * Any changes made inside forked instance are not visible in other instances. Useful for testing zombie scenarios.
     *
     * @param storage Storage to fork.
     * @return New forked storage.
     * @throws Exception Exceptions in case of any errors.
     */
    protected Storage forkStorage(ChunkedSegmentStorage storage) throws Exception {
        ScheduledExecutorService executor = executorService();
        ChunkedSegmentStorage forkedChunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID, storage.getChunkStorage(),
                getCloneMetadataStore(storage.getMetadataStore()),
                executor,
                storage.getConfig());
        forkedChunkedSegmentStorage.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();
        return forkedChunkedSegmentStorage;
    }

    /**
     * Creates a new instance of {@link ChunkMetadataStore} to be used during the test.
     *
     * @return A new instance of {@link InMemoryMetadataStore}.
     * @throws Exception Exceptions in case of any errors.
     */
    protected ChunkMetadataStore getMetadataStore() throws Exception {
        return new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());
    }

    /**
     * Creates a cloned metadata store. All state is copied to a new instance.
     *
     * @param metadataStore ChunkMetadataStore store to cloned.
     * @return Cloned instance of ChunkMetadataStore.
     * @throws Exception Exceptions in case of any errors.
     */
    protected ChunkMetadataStore getCloneMetadataStore(ChunkMetadataStore metadataStore) throws Exception {
        return InMemoryMetadataStore.clone((InMemoryMetadataStore) metadataStore);
    }

    /**
     * Tests fencing abilities. We create two different Storage objects with different owner ids.
     * Part 1: Creation:
     * * We create the Segment on Storage1:
     * ** We verify that Storage1 can execute all operations.
     * ** We verify that Storage2 can execute only read-only operations.
     * * We open the Segment on Storage2:
     * ** We verify that Storage1 can execute only read-only operations.
     * ** We verify that Storage2 can execute all operations.
     */
    @Test
    @Override
    public void testFencing() throws Exception {
        final long epoch1 = 1;
        final long epoch2 = 2;
        final String segmentName = "segment";
        try (val storage1 = createStorage();
             val storage2 = createStorage()) {
            storage1.initialize(epoch1);
            storage2.initialize(epoch2);

            // Create segment in Storage1 (thus Storage1 owns it for now).
            storage1.create(segmentName, TIMEOUT).join();

            // Storage1 should be able to execute all operations.
            SegmentHandle handle1 = storage1.openWrite(segmentName).join();
            verifyWriteOperationsSucceed(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Open the segment in Storage2 (thus Storage2 owns it for now).
            SegmentHandle handle2 = storage2.openWrite(segmentName).join();

            // Storage1 should be able to execute only read-only operations.
            verifyWriteOperationsFail(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            // Storage2 should be able to execute all operations.
            verifyReadOnlyOperationsSucceed(handle2, storage2);
            verifyWriteOperationsSucceed(handle2, storage2);

            // Seal and Delete (these should be run last, otherwise we can't run our test).
            verifyFinalWriteOperationsFail(handle1, storage1);
            verifyFinalWriteOperationsSucceed(handle2, storage2);
        }
    }

    @Test
    public void testZombieFencing() throws Exception {
        final long epoch1 = 1;
        final long epoch2 = 2;
        final String segmentName = "segment";
        try (val storage1 = createStorage()) {
            storage1.initialize(epoch1);

            // Create segment in Storage1 (thus Storage1 owns it for now).
            storage1.create(segmentName, TIMEOUT).join();

            // Storage1 should be able to execute all operations.
            SegmentHandle handle1 = storage1.openWrite(segmentName).join();
            verifyWriteOperationsSucceed(handle1, storage1);
            verifyReadOnlyOperationsSucceed(handle1, storage1);

            try (val storage2 = forkStorage((ChunkedSegmentStorage) storage1)) {
                storage2.initialize(epoch2);
                // Open the segment in Storage2 (thus Storage2 owns it for now).
                SegmentHandle handle2 = storage2.openWrite(segmentName).join();

                ((ChunkedSegmentStorage) storage1).getMetadataStore().markFenced();

                // Storage1 should be able to execute only read-only operations.
                verifyWriteOperationsFail(handle1, storage1);
                verifyReadOnlyOperationsSucceed(handle1, storage1);

                // Storage2 should be able to execute all operations.
                verifyReadOnlyOperationsSucceed(handle2, storage2);
                verifyWriteOperationsSucceed(handle2, storage2);

                // Seal and Delete (these should be run last, otherwise we can't run our test).
                verifyFinalWriteOperationsFail(handle1, storage1);
                verifyFinalWriteOperationsSucceed(handle2, storage2);
            }
        }
    }

    /**
     * Tests a read scenario with no issues or failures.
     */
    @Test
    public void testNormalRead() throws Exception {
        // Write data.
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);
            SegmentHandle handle = s.openWrite(segmentName).join();

            long expectedLength = 0;

            ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
            for (int i = 0; i < WRITE_COUNT; i++) {
                byte[] data = new byte[i + 1];
                populate(data);
                s.write(handle, expectedLength, new ByteArrayInputStream(data), data.length, null).join();
                writtenData.write(data);
                expectedLength += data.length;
            }

            // Check written data via a Read Operation, from every offset from 0 to length/2
            byte[] expectedData = writtenData.toByteArray();
            val readHandle = s.openRead(segmentName).join();
            for (int startOffset = 0; startOffset < expectedLength / 2; startOffset++) {
                int readLength = (int) (expectedLength - 2 * startOffset);
                byte[] actualData = new byte[readLength];
                int readBytes = s.read(readHandle, startOffset, actualData, 0, actualData.length, null).join();

                Assert.assertEquals("Unexpected number of bytes read with start offset " + startOffset, actualData.length, readBytes);
                AssertExtensions.assertArrayEquals("Unexpected data read back with start offset " + startOffset,
                        expectedData, startOffset, actualData, 0, readLength);
            }
        }
    }

    /**
     * Tests general GetInfoOperation behavior.
     */
    @Test
    public void testGetInfo() throws Exception {
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);
            SegmentHandle handle = s.openWrite(segmentName).join();

            long expectedLength = 0;

            for (int i = 0; i < WRITE_COUNT; i++) {
                byte[] data = new byte[i + 1];
                s.write(handle, expectedLength, new ByteArrayInputStream(data), data.length, null).join();
                expectedLength += data.length;
            }

            SegmentProperties result = s.getStreamSegmentInfo(segmentName, null).join();

            validateProperties("pre-seal", segmentName, result, expectedLength, false);

            // Seal.
            s.seal(handle, null).join();
            result = s.getStreamSegmentInfo(segmentName, null).join();
            validateProperties("post-seal", segmentName, result, expectedLength, true);

            // Inexistent segment.
            AssertExtensions.assertFutureThrows(
                    "GetInfo succeeded on missing segment.",
                    s.getStreamSegmentInfo("non-existent", null),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    private void validateProperties(String stage, String segmentName, SegmentProperties sp, long expectedLength, boolean expectedSealed) {
        Assert.assertNotNull("No result from GetInfoOperation (" + stage + ").", sp);
        Assert.assertEquals("Unexpected name (" + stage + ").", segmentName, sp.getName());
        Assert.assertEquals("Unexpected length (" + stage + ").", expectedLength, sp.getLength());
        Assert.assertEquals("Unexpected sealed status (" + stage + ").", expectedSealed, sp.isSealed());
    }

    /**
     * Tests the exists API.
     */
    @Test
    public void testExists() throws Exception {
        String segmentName = "foo_open";
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);

            // Not exists.
            Assert.assertFalse("Unexpected result for missing segment (no files).", s.exists("nonexistent", null).join());
        }
    }

    @Test
    public void testSimpleReadWrite() throws Exception {
        String segmentName = createSegmentName("testSimpleReadWrite");
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);
            Assert.assertTrue("Expected the segment to exist.", s.exists(segmentName, null).join());

            SegmentHandle writeHandle = s.openWrite(segmentName).join();
            Assert.assertEquals(segmentName, writeHandle.getSegmentName());
            Assert.assertEquals(false, writeHandle.isReadOnly());

            byte[] writeBuffer = new byte[10];
            populate(writeBuffer);

            s.write(writeHandle, 0, new ByteArrayInputStream(writeBuffer), writeBuffer.length, null).join();

            SegmentHandle readHandle = s.openRead(segmentName).join();
            Assert.assertEquals(segmentName, readHandle.getSegmentName());
            Assert.assertEquals(true, readHandle.isReadOnly());
            byte[] readBuffer = new byte[writeBuffer.length];
            int bytesRead = s.read(readHandle, 0, readBuffer, 0, writeBuffer.length, null).get();
            Assert.assertEquals(writeBuffer.length, bytesRead);
            Assert.assertArrayEquals(writeBuffer, readBuffer);
        }
    }

    @Test
    public void testConsecutiveReads() throws Exception {
        String segmentName = createSegmentName("testConsecutiveReads");
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);
            Assert.assertTrue("Expected the segment to exist.", s.exists(segmentName, null).join());

            SegmentHandle writeHandle = s.openWrite(segmentName).join();
            Assert.assertEquals(segmentName, writeHandle.getSegmentName());
            Assert.assertEquals(false, writeHandle.isReadOnly());

            byte[] writeBuffer = new byte[15];
            populate(writeBuffer);

            s.write(writeHandle, 0, new ByteArrayInputStream(writeBuffer), writeBuffer.length, null).join();

            SegmentHandle readHandle = s.openRead(segmentName).join();
            Assert.assertEquals(segmentName, readHandle.getSegmentName());
            Assert.assertEquals(true, readHandle.isReadOnly());

            byte[] readBuffer = new byte[writeBuffer.length];

            int totalBytesRead = 0;
            for (int i = 1; i <= 5; i++) {
                int remaining = i;
                while (remaining > 0) {
                    int bytesRead = s.read(readHandle, totalBytesRead, readBuffer, totalBytesRead, remaining, null).get();
                    remaining -= bytesRead;
                    totalBytesRead += bytesRead;
                }
            }
            Assert.assertEquals(writeBuffer.length, totalBytesRead);
            Assert.assertArrayEquals(writeBuffer, readBuffer);
        }
    }

    @Test
    public void testConsecutiveWrites() throws Exception {
        String segmentName = createSegmentName("testConsecutiveWrites");
        try (Storage s = createStorage()) {
            s.initialize(DEFAULT_EPOCH);
            createSegment(segmentName, s);
            Assert.assertTrue("Expected the segment to exist.", s.exists(segmentName, null).join());

            SegmentHandle writeHandle = s.openWrite(segmentName).join();
            Assert.assertEquals(segmentName, writeHandle.getSegmentName());
            Assert.assertFalse(writeHandle.isReadOnly());

            byte[] writeBuffer = new byte[15];
            populate(writeBuffer);

            int totalBytesWritten = 0;
            for (int i = 1; i <= 5; i++) {
                s.write(writeHandle, totalBytesWritten, new ByteArrayInputStream(writeBuffer, totalBytesWritten, i), i, null).join();
                totalBytesWritten += i;
            }

            SegmentHandle readHandle = s.openRead(segmentName).join();
            Assert.assertEquals(segmentName, readHandle.getSegmentName());
            Assert.assertTrue(readHandle.isReadOnly());
            byte[] readBuffer = new byte[writeBuffer.length];
            int bytesRead = s.read(readHandle, 0, readBuffer, 0, writeBuffer.length, null).get();
            Assert.assertEquals(writeBuffer.length, bytesRead);
            Assert.assertArrayEquals(writeBuffer, readBuffer);
        }
    }

    //region synchronization unit tests

    /**
     * This test case simulates two hosts writing at the same offset at the same time.
     *
     * @throws Exception if an unexpected error occurred.
     */
    @Test(timeout = 30000)
    public void testParallelWriteTwoHosts() throws Exception {
        String segmentName = "foo_write";
        int appendCount = 1;

        try (Storage s1 = createStorage();
             Storage s2 = createStorage()) {
            s1.initialize(DEFAULT_EPOCH);
            s2.initialize(DEFAULT_EPOCH);
            s1.create(segmentName, TIMEOUT).join();
            SegmentHandle writeHandle1 = s1.openWrite(segmentName).join();
            SegmentHandle writeHandle2 = s2.openWrite(segmentName).join();
            long offset = 0;
            byte[] writeData = populate("Segment_%s_Append".length());
            for (int j = 0; j < appendCount; j++) {
                ByteArrayInputStream dataStream1 = new ByteArrayInputStream(writeData);
                ByteArrayInputStream dataStream2 = new ByteArrayInputStream(writeData);
                CompletableFuture<Void> f1 = s1.write(writeHandle1, offset, dataStream1, writeData.length, TIMEOUT);
                // TODO FIX THIS.
                f1.join();
                CompletableFuture<Void> f2 = s2.write(writeHandle2, offset, dataStream2, writeData.length, TIMEOUT);
                //f2.join();
                assertMayThrow("Write expected to complete OR throw BadOffsetException." +
                                "threw an unexpected exception.",
                        () -> CompletableFuture.allOf(f1, f2),
                        ex -> ex instanceof BadOffsetException);

                // Make sure at least one operation is success.
                Assert.assertTrue("At least one of the two parallel writes should succeed.",
                        !f1.isCompletedExceptionally() || !f2.isCompletedExceptionally());
                offset += writeData.length;
            }
            Assert.assertTrue("Writes at the same offset are expected to be idempotent.",
                    s1.getStreamSegmentInfo(segmentName, TIMEOUT).join().getLength() == offset);

            offset = 0;
            byte[] readBuffer = new byte[writeData.length];
            for (int j = 0; j < appendCount; j++) {
                int bytesRead = s1.read(writeHandle1, j * readBuffer.length, readBuffer,
                        0, readBuffer.length, TIMEOUT).join();
                Assert.assertEquals(String.format("Unexpected number of bytes read from offset %d.", offset),
                        readBuffer.length, bytesRead);
                AssertExtensions.assertArrayEquals(String.format("Unexpected read result from offset %d.", offset),
                        readBuffer, 0, readBuffer, 0, bytesRead);
            }

            s1.delete(writeHandle1, TIMEOUT).join();
        }
    }

    @Override
    public void testListSegmentsWithOneSegment() {
    }

    @Override
    public void testListSegmentsNextNoSuchElementException() {
    }
    //endregion

}

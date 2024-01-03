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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFullException;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.StorageWrapper;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StatusFlags;
import io.pravega.segmentstore.storage.mocks.AbstractInMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemoryTaskQueueManager;
import io.pravega.segmentstore.storage.noop.NoOpChunkStorage;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link ChunkedSegmentStorage}.
 * The focus is on testing the ChunkedSegmentStorage implementation itself very thoroughly.
 * It uses {@link NoOpChunkStorage} as {@link ChunkStorage}.
 */
@Slf4j
public class ChunkedSegmentStorageTests extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofSeconds(3000);
    private static final int CONTAINER_ID = 42;
    private static final int OWNER_EPOCH = 100;
    private static final int THREAD_POOL_SIZE = 3;
    protected final Random rnd = new Random(0);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Override
    @Before
    public void before() throws Exception {
        super.before();
    }

    @Override
    @After
    public void after() throws Exception {
        super.after();
    }

    @Override
    protected int getThreadPoolSize() {
        return THREAD_POOL_SIZE;
    }

    public ChunkStorage createChunkStorage() throws Exception {
        return new NoOpChunkStorage(executorService());
    }

    public ChunkMetadataStore createMetadataStore() throws Exception {
        return new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());
    }

    public TestContext getTestContext() throws Exception {
        return new TestContext(executorService());
    }

    public TestContext getTestContext(ChunkedSegmentStorageConfig config) throws Exception {
        return new TestContext(executorService(), config);
    }

    /**
     * Test {@link ChunkedSegmentStorage#supportsTruncation()}.
     */
    @Test
    public void testSupportsTruncate() throws Exception {
        @Cleanup
        val chunkStorage = createChunkStorage();
        @Cleanup
        val metadataStore = createMetadataStore();
        @Cleanup
        val chunkedSegmentStorage = new ChunkedSegmentStorage(42, chunkStorage, metadataStore, executorService(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        Assert.assertTrue(chunkedSegmentStorage.supportsTruncation());
    }

    /**
     * Tests {@link ChunkedSegmentStorage#supportsAtomicWrites()}
     */
    @Test
    public void testSupportsAtomicWrites() throws Exception {
        @Cleanup
        val chunkStorage = createChunkStorage();
        @Cleanup
        val metadataStore = createMetadataStore();
        @Cleanup
        val chunkedSegmentStorage = new ChunkedSegmentStorage(42, chunkStorage, metadataStore, executorService(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        Assert.assertTrue(chunkedSegmentStorage.supportsAtomicWrites());
    }

    /**
     * Test initialization.
     *
     * @throws Exception
     */
    @Test
    public void testInitialization() throws Exception {
        @Cleanup
        val chunkStorage = createChunkStorage();
        @Cleanup
        val metadataStore = createMetadataStore();
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        @Cleanup
        val chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID, chunkStorage, metadataStore, executorService(), config);

        testUninitialized(chunkedSegmentStorage);

        chunkedSegmentStorage.initialize(1);

        Assert.assertNotNull(chunkedSegmentStorage.getMetadataStore());
        Assert.assertEquals(chunkStorage, chunkedSegmentStorage.getChunkStorage());
        Assert.assertEquals(1, chunkedSegmentStorage.getEpoch());

        Assert.assertEquals(metadataStore, chunkedSegmentStorage.getMetadataStore());
        Assert.assertEquals(chunkStorage, chunkedSegmentStorage.getChunkStorage());
        Assert.assertNotNull(chunkedSegmentStorage.getSystemJournal());
        Assert.assertEquals(chunkedSegmentStorage.getSystemJournal().getConfig().getStorageMetadataRollingPolicy(),
                chunkedSegmentStorage.getConfig().getStorageMetadataRollingPolicy());
        Assert.assertEquals(1, chunkedSegmentStorage.getEpoch());
        Assert.assertEquals(CONTAINER_ID, chunkedSegmentStorage.getContainerId());
        Assert.assertEquals(0, chunkedSegmentStorage.getConfig().getMinSizeLimitForConcat());
        Assert.assertEquals(Long.MAX_VALUE, chunkedSegmentStorage.getConfig().getMaxSizeLimitForConcat());
        chunkedSegmentStorage.close();

        testUninitialized(chunkedSegmentStorage);
    }

    private void testUninitialized(ChunkedSegmentStorage chunkedSegmentStorage) {
        String testSegmentName = "foo";
        AssertExtensions.assertThrows(
                "getStreamSegmentInfo succeeded on uninitialized instance.",
                () -> chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "Seal  succeeded on uninitialized instance.",
                () -> chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "openWrite succeeded on uninitialized instance.",
                () -> chunkedSegmentStorage.openWrite(testSegmentName),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "openRead succeeded on uninitialized instance.",
                () -> chunkedSegmentStorage.openRead(testSegmentName),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "write succeeded on uninitialized instance.",
                () -> chunkedSegmentStorage.write(SegmentStorageHandle.writeHandle(testSegmentName), 0, new ByteArrayInputStream(new byte[1]), 1, null),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "read succeeded on uninitialized instance.",
                () -> chunkedSegmentStorage.read(SegmentStorageHandle.readHandle(testSegmentName), 0, new byte[1], 0, 1, null),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "Concat succeeded on uninitialized instance.",
                () -> chunkedSegmentStorage.concat(SegmentStorageHandle.readHandle(testSegmentName), 0, "inexistent", null),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "Concat succeeded on uninitialized instance.",
                () -> chunkedSegmentStorage.delete(SegmentStorageHandle.readHandle(testSegmentName), null),
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Test exceptions for opertions on non-existent chunk.
     */
    @Test
    public void testSegmentNotExistsExceptionForNonExistent() throws Exception {
        String testSegmentName = "foo";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(1); // Force rollover after each byte.
        @Cleanup
        TestContext testContext = getTestContext();
        Assert.assertFalse(testContext.chunkedSegmentStorage.exists(testSegmentName, null).get());

        AssertExtensions.assertFutureThrows(
                "getStreamSegmentInfo succeeded on missing segment.",
                testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Seal succeeded on missing segment.",
                testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "openWrite succeeded on missing segment.",
                testContext.chunkedSegmentStorage.openWrite(testSegmentName),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "openRead succeeded on missing segment.",
                testContext.chunkedSegmentStorage.openRead(testSegmentName),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "write succeeded on missing segment.",
                testContext.chunkedSegmentStorage.write(SegmentStorageHandle.writeHandle(testSegmentName), 0, new ByteArrayInputStream(new byte[1]), 1, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "read succeeded on missing segment.",
                testContext.chunkedSegmentStorage.read(SegmentStorageHandle.readHandle(testSegmentName), 0, new byte[1], 0, 1, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.chunkedSegmentStorage.concat(SegmentStorageHandle.writeHandle(testSegmentName), 0, "inexistent", null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.chunkedSegmentStorage.delete(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.chunkedSegmentStorage.truncate(SegmentStorageHandle.writeHandle(testSegmentName), 0, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

    }

    @Test
    public void testSegmentNotExistsExceptionForDeletedSegment() throws Exception {
        String testSegmentName = "foo";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(1); // Force rollover after each byte.
        @Cleanup
        TestContext testContext = getTestContext();
        val h = testContext.chunkedSegmentStorage.create(testSegmentName, null).get();
        Assert.assertTrue(testContext.chunkedSegmentStorage.exists(testSegmentName, null).get());

        // Delete
        testContext.chunkedSegmentStorage.delete(h, null).get();
        Assert.assertFalse(testContext.chunkedSegmentStorage.exists(testSegmentName, null).get());

        AssertExtensions.assertFutureThrows(
                "getStreamSegmentInfo succeeded on missing segment.",
                testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Seal succeeded on missing segment.",
                testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "openWrite succeeded on missing segment.",
                testContext.chunkedSegmentStorage.openWrite(testSegmentName),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "openRead succeeded on missing segment.",
                testContext.chunkedSegmentStorage.openRead(testSegmentName),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "write succeeded on missing segment.",
                testContext.chunkedSegmentStorage.write(SegmentStorageHandle.writeHandle(testSegmentName), 0, new ByteArrayInputStream(new byte[1]), 1, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "read succeeded on missing segment.",
                testContext.chunkedSegmentStorage.read(SegmentStorageHandle.readHandle(testSegmentName), 0, new byte[1], 0, 1, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.chunkedSegmentStorage.concat(SegmentStorageHandle.writeHandle(testSegmentName), 0, "inexistent", null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.chunkedSegmentStorage.delete(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.chunkedSegmentStorage.truncate(SegmentStorageHandle.writeHandle(testSegmentName), 0, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

    }

    @Test
    public void testDeleteInvalidParameters() throws Exception {
        String testSegmentName = "foo";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(1); // Force rollover after each byte.
        @Cleanup
        TestContext testContext = getTestContext();

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.chunkedSegmentStorage.delete(null, null),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Test scenarios when storage is no more primary.
     */
    @Test
    public void testStorageNotPrimaryException() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        testContext.chunkedSegmentStorage.initialize(2);
        int maxRollingLength = 1;
        int ownerEpoch = OWNER_EPOCH;
        testContext.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch);

        // These operations should always succeed.
        testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).join();
        val h = testContext.chunkedSegmentStorage.openRead(testSegmentName).join();
        testContext.chunkedSegmentStorage.read(h, 0, new byte[0], 0, 0, null);

        // These operations should never succeed.
        AssertExtensions.assertFutureThrows(
                "Seal succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "openWrite succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.openWrite(testSegmentName),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "delete succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.delete(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "write succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.write(SegmentStorageHandle.writeHandle(testSegmentName),
                        0,
                        new ByteArrayInputStream(new byte[10]),
                        10,
                        null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "truncate succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.truncate(SegmentStorageHandle.writeHandle(testSegmentName),
                        0, null),
                ex -> ex instanceof StorageNotPrimaryException);

        testContext.insertMetadata("source", maxRollingLength, 1);
        testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle("source"), null).join();
        AssertExtensions.assertFutureThrows(
                "concat succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.concat(SegmentStorageHandle.writeHandle(testSegmentName), 0, "source", null),
                ex -> ex instanceof StorageNotPrimaryException);

    }

    /**
     * Test scenarios when storage is no more after fencing.
     */
    @Test
    public void testStorageNotPrimaryExceptionOnFencing() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        testContext.chunkedSegmentStorage.initialize(2);
        int maxRollingLength = 1;
        testContext.insertMetadata(testSegmentName, maxRollingLength, OWNER_EPOCH);
        testContext.insertMetadata("source", maxRollingLength, 1);
        testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle("source"), null).join();

        testContext.metadataStore.markFenced();

        // These operations should always succeed.
        testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).join();
        val h = testContext.chunkedSegmentStorage.openRead(testSegmentName).join();
        testContext.chunkedSegmentStorage.read(h, 0, new byte[0], 0, 0, null);

        // These operations should never succeed.
        AssertExtensions.assertFutureThrows(
                "Seal succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "delete succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.delete(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "concat succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.concat(SegmentStorageHandle.writeHandle(testSegmentName), 0, "source", null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "create succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.create("newSegment", null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "truncate succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.truncate(SegmentStorageHandle.writeHandle(testSegmentName),
                        0, null),
                ex -> ex instanceof StorageNotPrimaryException);

    }

    /**
     * Test scenarios when storage is no more primary with fencing after OpenWrite.
     */
    @Test
    public void testStorageNotPrimaryExceptionOnFencingAfterOpenWrite() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        testContext.chunkedSegmentStorage.initialize(2);
        int maxRollingLength = 1;
        testContext.insertMetadata(testSegmentName, maxRollingLength, OWNER_EPOCH);

        // These operations should always succeed.
        testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).join();

        testContext.metadataStore.markFenced();
        AssertExtensions.assertFutureThrows(
                "write succeeded on segment not owned.",
                testContext.chunkedSegmentStorage.write(SegmentStorageHandle.writeHandle(testSegmentName),
                        0,
                        new ByteArrayInputStream(new byte[10]),
                        10,
                        null),
                ex -> ex instanceof StorageNotPrimaryException);
    }

    /**
     * Test simple scenario for storage that does not support any appends.
     *
     * @throws Exception
     */
    @Test
    public void testSimpleScenarioWithNonAppendProvider() throws Exception {
        String testSegmentName = "foo";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(3).build());

        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setShouldSupportAppend(false);

        // Step 1: Create segment.
        val h = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        Assert.assertEquals(h.getSegmentName(), testSegmentName);
        Assert.assertFalse(h.isReadOnly());
        HashSet<String> chunksBefore = new HashSet<>();
        chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));

        // Check metadata is stored.
        val segmentMetadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertNotNull(segmentMetadata);
        Assert.assertEquals(segmentMetadata.getName(), testSegmentName);
        Assert.assertEquals(segmentMetadata.getKey(), testSegmentName);

        // Check exists
        Assert.assertTrue(testContext.chunkedSegmentStorage.exists(testSegmentName, null).get());

        // Check getStreamSegmentInfo.
        SegmentProperties info = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 0);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Write some data.
        long writeAt = 0;
        for (int i = 1; i < 5; i++) {
            testContext.chunkedSegmentStorage.write(h, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
        }

        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName,
                new long[]{
                        1,      // First write
                        2,      // Second write
                        2, 1,   // Third write
                        2, 2    // Fourth write
                });
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, 0, 10);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, 0, 10, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

        // Check getStreamSegmentInfo.
        info = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 10);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Open write handle.
        val hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        Assert.assertEquals(hWrite.getSegmentName(), testSegmentName);
        Assert.assertFalse(hWrite.isReadOnly());

        testContext.chunkedSegmentStorage.write(hWrite, 10, new ByteArrayInputStream(new byte[4]), 4, null).join();
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName,
                new long[]{
                        1,      // First write
                        2,      // Second write
                        2, 1,   // Third write
                        2, 2,   // Fourth write
                        2, 2    // Recent write
                });
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, 0, 14);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, 0, 14, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
        HashSet<String>  chunksAfter = new HashSet<>();
        chunksAfter.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));
        TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);

        info = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 14);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Make sure calling create again does not succeed
        AssertExtensions.assertFutureThrows(
                "Create succeeded on missing segment.",
                testContext.chunkedSegmentStorage.create(testSegmentName, policy, null),
                ex -> ex instanceof StreamSegmentExistsException);

        testContext.chunkedSegmentStorage.delete(hWrite, null);
    }

    /**
     * Test simple scenario.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testSimpleScenario() throws Exception {
        String testSegmentName = "foo";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        @Cleanup
        TestContext testContext = getTestContext();
        testSimpleScenario(testSegmentName, policy, testContext);
    }

    private void testSimpleScenario(String testSegmentName, SegmentRollingPolicy policy, TestContext testContext) throws Exception {
        // Step 1: Create segment.
        val h = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        Assert.assertEquals(h.getSegmentName(), testSegmentName);
        Assert.assertFalse(h.isReadOnly());
        HashSet<String> chunksBefore = new HashSet<>();
        chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));

        // Check metadata is stored.
        val segmentMetadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertNotNull(segmentMetadata);
        Assert.assertEquals(segmentMetadata.getName(), testSegmentName);
        Assert.assertEquals(segmentMetadata.getKey(), testSegmentName);

        // Check exists
        Assert.assertTrue(testContext.chunkedSegmentStorage.exists(testSegmentName, null).get());

        // Check getStreamSegmentInfo.
        SegmentProperties info = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 0);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Write some data.
        long writeAt = 0;
        for (int i = 1; i < 5; i++) {
            testContext.chunkedSegmentStorage.write(h, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
        }
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, 2, 5);
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, 0, 10);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, 0, 10, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

        // Check getStreamSegmentInfo.
        info = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 10);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Open write handle.
        val hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        Assert.assertEquals(hWrite.getSegmentName(), testSegmentName);
        Assert.assertFalse(hWrite.isReadOnly());

        testContext.chunkedSegmentStorage.write(hWrite, 10, new ByteArrayInputStream(new byte[4]), 4, null).join();
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, 2, 7);
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, 0, 14);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, 0, 14, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
        HashSet<String>  chunksAfter = new HashSet<>();
        chunksAfter.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));
        TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);

        info = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 14);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Make sure calling create again does not succeed
        AssertExtensions.assertFutureThrows(
                "Create succeeded on missing segment.",
                testContext.chunkedSegmentStorage.create(testSegmentName, policy, null),
                ex -> ex instanceof StreamSegmentExistsException);

        checkDataRead(testSegmentName, testContext, 0, 14);

        testContext.chunkedSegmentStorage.delete(hWrite, null);
    }

    private CompletableFuture<Void> testSimpleScenarioAsync(String testSegmentName, SegmentRollingPolicy policy, TestContext testContext, Executor executor) {
        // Step 1: Create segment.
        return testContext.chunkedSegmentStorage.create(testSegmentName, policy, null)
                .thenComposeAsync(h -> {
                    Assert.assertEquals(h.getSegmentName(), testSegmentName);
                    Assert.assertFalse(h.isReadOnly());

                    // Check exists
                    return testContext.chunkedSegmentStorage.exists(testSegmentName, null)
                            .thenApplyAsync(exists -> {
                                Assert.assertTrue(exists);
                                return null;
                            }, executor)
                            .thenComposeAsync(v -> {
                                // Check getStreamSegmentInfo.
                                return testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null)
                                        .thenComposeAsync(info -> {
                                            Assert.assertFalse(info.isDeleted());
                                            Assert.assertEquals(info.getName(), testSegmentName);
                                            Assert.assertEquals(info.getLength(), 0);
                                            Assert.assertEquals(info.getStartOffset(), 0);

                                            return testContext.chunkedSegmentStorage.write(h, 0, new ByteArrayInputStream(new byte[10]), 10, null)
                                                    .thenComposeAsync(x -> checkDataReadAsync(testSegmentName, testContext, 0, 10, executor), executor)
                                                    .thenComposeAsync(x -> testContext.chunkedSegmentStorage.delete(SegmentStorageHandle.writeHandle(testSegmentName), null), executor);
                                        }, executor);
                            }, executor);
                }, executor);
    }

    /**
     * Test Read.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testRead() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        // Setup a segment with 5 chunks with given lengths.
        val segment = testContext.insertMetadata(testSegmentName, 1024, 1,
                new long[]{1, 2, 3, 4, 5});

        int total = 15;

        val h = testContext.chunkedSegmentStorage.openRead(testSegmentName).get();

        // Read all bytes at once.
        byte[] output = new byte[total];
        int bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, 0, total, null).get();
        Assert.assertEquals(total, bytesRead);

        // Read bytes at varying lengths but same starting offset.
        for (int i = 0; i < 15; i++) {
            bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, 0, i, null).get();
            Assert.assertEquals(i, bytesRead);
        }

        // Read bytes at varying lengths and different offsets.
        for (int i = 0; i < 15; i++) {
            bytesRead = testContext.chunkedSegmentStorage.read(h, 15 - i - 1, output, 0, i, null).get();
            Assert.assertEquals(i, bytesRead);
        }

        // Read bytes at varying sizes.
        int totalBytesRead = 0;
        for (int i = 5; i > 0; i--) {
            bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, totalBytesRead, i, null).get();
            totalBytesRead += bytesRead;
            Assert.assertEquals(i, bytesRead);
        }
        Assert.assertEquals(total, totalBytesRead);
    }

    /**
     * Test Cold Read.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testColdRead() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .indexBlockSize(3)
                .maxIndexedSegments(1)
                .maxIndexedChunksPerSegment(1)
                .build());
        // Setup a segment with 5 chunks with given lengths.
        val segment = testContext.insertMetadata(testSegmentName, 1024, 1,
                new long[]{1, 2, 3, 4, 5}, false, true);

        int total = 15;
        val h = testContext.chunkedSegmentStorage.openRead(testSegmentName).get();

        // Read all bytes at once.
        byte[] output = new byte[total];
        int bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, 0, total, null).get();
        Assert.assertEquals(total, bytesRead);

        // Read bytes at varying lengths but same starting offset.
        for (int i = 0; i < 15; i++) {
            testContext.chunkedSegmentStorage.getReadIndexCache().cleanUp();
            bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, 0, i, null).get();
            Assert.assertEquals(i, bytesRead);
        }

        // Read bytes at varying lengths and different offsets.
        for (int i = 0; i < 15; i++) {
            testContext.chunkedSegmentStorage.getReadIndexCache().cleanUp();
            bytesRead = testContext.chunkedSegmentStorage.read(h, 15 - i - 1, output, 0, i, null).get();
            Assert.assertEquals(i, bytesRead);
        }

        // Read bytes at varying sizes.
        int totalBytesRead = 0;
        for (int i = 5; i > 0; i--) {
            testContext.chunkedSegmentStorage.getReadIndexCache().cleanUp();
            bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, totalBytesRead, i, null).get();
            totalBytesRead += bytesRead;
            Assert.assertEquals(i, bytesRead);
        }
        Assert.assertEquals(total, totalBytesRead);
    }

    /**
     * Test Cold Read.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testReadNoIndex() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .maxIndexedSegments(1)
                .maxIndexedChunksPerSegment(1)
                .build());
        // Setup a segment with 5 chunks with given lengths.
        val segment = testContext.insertMetadata(testSegmentName, 1024, 1,
                new long[]{1, 2, 3, 4, 5}, false, false);

        int total = 15;
        val h = testContext.chunkedSegmentStorage.openRead(testSegmentName).get();

        // Read all bytes at once.
        byte[] output = new byte[total];
        int bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, 0, total, null).get();
        Assert.assertEquals(total, bytesRead);

        // Read bytes at varying lengths but same starting offset.
        for (int i = 0; i < 15; i++) {
            testContext.chunkedSegmentStorage.getReadIndexCache().cleanUp();
            bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, 0, i, null).get();
            Assert.assertEquals(i, bytesRead);
        }

        // Read bytes at varying lengths and different offsets.
        for (int i = 0; i < 15; i++) {
            testContext.chunkedSegmentStorage.getReadIndexCache().cleanUp();
            bytesRead = testContext.chunkedSegmentStorage.read(h, 15 - i - 1, output, 0, i, null).get();
            Assert.assertEquals(i, bytesRead);
        }

        // Read bytes at varying sizes.
        int totalBytesRead = 0;
        for (int i = 5; i > 0; i--) {
            testContext.chunkedSegmentStorage.getReadIndexCache().cleanUp();
            bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, totalBytesRead, i, null).get();
            totalBytesRead += bytesRead;
            Assert.assertEquals(i, bytesRead);
        }
        Assert.assertEquals(total, totalBytesRead);
    }

    /**
     * Test Read.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testReadInvalidParameters() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        // Setup a segment.
        val segment = testContext.insertMetadata(testSegmentName, 1024, 1, new long[]{25});

        int validStart = 10;
        int validLength = 15;
        val hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        testContext.chunkedSegmentStorage.truncate(hWrite, validStart, null).get();

        val h = testContext.chunkedSegmentStorage.openRead(testSegmentName).get();
        // Read all bytes at once.
        byte[] output = new byte[validLength];
        byte[] smallerBuffer = new byte[validLength - 1];
        byte[] biggerBuffer = new byte[validLength + 1];

        int bytesRead = testContext.chunkedSegmentStorage.read(h, validStart, output, 0, validLength, null).get();
        Assert.assertEquals(validLength, bytesRead);

        // StreamSegmentTruncatedException
        // Read from the truncated part.
        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.read(h, 0, output, 0, output.length, TIMEOUT),
                ex -> ex instanceof StreamSegmentTruncatedException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.read(h, validStart - 1, output, 0, output.length, TIMEOUT),
                ex -> ex instanceof StreamSegmentTruncatedException);

        // IllegalArgumentException
        // Beyond last valid offset
        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.read(h, validStart + validLength, output, 0, output.length, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // ArrayIndexOutOfBoundsException
        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.read(h, -1, output, 0, validLength, null),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.read(h, validStart, output, -1, validLength, null),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.read(h, validStart, output, 0, -1, null),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.read(h, validStart, output, -1, validLength, null),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.read(h, validStart, output, validLength, validLength, null),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.read(h, 0, smallerBuffer, 0, validLength, null),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);
    }

    /**
     * Test Read failure in case of IO Errors.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testReadIOFailures() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        // Setup a segment with 5 chunks with given lengths.
        val segment = testContext.insertMetadata(testSegmentName, 1024, 1,
                new long[]{1, 2, 3, 4, 5});

        int total = 15;
        // Introduce failure by deleting some chunks.
        val chunks = TestUtils.getChunkList(testContext.metadataStore, testSegmentName);
        testContext.chunkStorage.delete(ChunkHandle.writeHandle(chunks.get(0).getName())).join();
        testContext.chunkStorage.delete(ChunkHandle.writeHandle(chunks.get(2).getName())).join();
        testContext.chunkStorage.delete(ChunkHandle.writeHandle(chunks.get(4).getName())).join();

        val h = testContext.chunkedSegmentStorage.openRead(testSegmentName).get();

        // Read all bytes at once.
        byte[] output = new byte[total];
        AssertExtensions.assertFutureThrows("read() allowed for missing chunks",
                testContext.chunkedSegmentStorage.read(h, 0, output, 0, total, null),
                ex -> ex instanceof ChunkNotFoundException);

        // Read sections that contain missing chunks.
        for (int i = 0; i < 5; i++) {
            AssertExtensions.assertFutureThrows("read() allowed for allowed for missing chunks",
                    testContext.chunkedSegmentStorage.read(h, i, output, i, 5, null),
                    ex -> ex instanceof ChunkNotFoundException);
        }

        // Read should succeed when chunks are not actually missing.
        int bytesRead;
        // 2nd chunk.
        bytesRead = testContext.chunkedSegmentStorage.read(h, 1, output, 1, 2, null).get();
        Assert.assertEquals(2, bytesRead);
        // 4th chunk.
        bytesRead = testContext.chunkedSegmentStorage.read(h, 6, output, 6, 4, null).get();
        Assert.assertEquals(4, bytesRead);

        // Recreate chunks with shorter lengths.
        var chunkHandle = testContext.chunkStorage.create(chunks.get(0).getName()).join();
        testContext.chunkStorage.write(chunkHandle, 0, 1, new ByteArrayInputStream(new byte[1])).join();
        chunkHandle = testContext.chunkStorage.create(chunks.get(2).getName()).join();
        testContext.chunkStorage.write(chunkHandle, 0, 1, new ByteArrayInputStream(new byte[1])).join();
        chunkHandle = testContext.chunkStorage.create(chunks.get(4).getName()).join();
        testContext.chunkStorage.write(chunkHandle, 0, 1, new ByteArrayInputStream(new byte[1])).join();

        // Read all bytes at once.
        AssertExtensions.assertFutureThrows("read() allowed for invalid chunks",
                testContext.chunkedSegmentStorage.read(h, 0, output, 0, total, null),
                ex -> ex instanceof IndexOutOfBoundsException || ex instanceof IllegalArgumentException);

        // Read sections that contain missing chunks.
        for (int i = 0; i < 5; i++) {
            AssertExtensions.assertFutureThrows("read() allowed for allowed for invalid chunks",
                    testContext.chunkedSegmentStorage.read(h, i, output, i, 5, null),
                    ex -> ex instanceof IndexOutOfBoundsException || ex instanceof IllegalArgumentException);
        }
    }

    /**
     * Test Write.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testWrite() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.

        // Create
        val hWrite = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        HashSet<String> chunksBefore = new HashSet<>();
        chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));

        // Write some data.
        long writeAt = 0;
        for (int i = 1; i < 5; i++) {
            testContext.chunkedSegmentStorage.write(hWrite, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
        }
        HashSet<String>  chunksAfter = new HashSet<>();
        chunksAfter.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));
        TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);

        int total = 10;

        checkDataRead(testSegmentName, testContext, 0, total);
    }


    /**
     * Test Write after repeated failure.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testWriteAfterWriteFailure() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        SegmentRollingPolicy policy = new SegmentRollingPolicy(20); // Force rollover after every 20 byte.

        // Create
        val hWrite = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        HashSet<String> chunksBefore = new HashSet<>();
        chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));

        // Write some data.
        long writeAt = 0;
        for (int i = 1; i < 5; i++) {
            testContext.chunkedSegmentStorage.write(hWrite, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            // Append some data to the last chunk to simulate partial write during failure
            val lastChunkMetadata = TestUtils.getChunkMetadata(testContext.metadataStore,
                    TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName).getLastChunk());
            testContext.chunkStorage.write(ChunkHandle.writeHandle(lastChunkMetadata.getName()), lastChunkMetadata.getLength(), 1, new ByteArrayInputStream(new byte[1])).join();
            writeAt += i;
        }

        checkDataRead(testSegmentName, testContext, 0, 10);
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, new long[] {1, 2, 3, 4});
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, 0, 10);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, 0, 10, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
        HashSet<String>  chunksAfter = new HashSet<>();
        chunksAfter.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));
        TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);
    }

    /**
     * Test Write for sequential scheduling.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testWriteSequential() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.

        // Create
        val hWrite = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        HashSet<String> chunksBefore = new HashSet<>();
        chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));

        // Write some data sequentially.
        val bytes = populate(100);

        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < bytes.length; i++) {
            futures.add(testContext.chunkedSegmentStorage.write(hWrite, i, new ByteArrayInputStream(bytes, i, 1), 1, null));
        }
        Futures.allOf(futures).join();
        HashSet<String>  chunksAfter = new HashSet<>();
        chunksAfter.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));
        TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);
        checkDataRead(testSegmentName, testContext, 0, bytes.length, bytes);
    }

    /**
     * Test write with invalid arguments.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testWriteInvalidParameters() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        // Setup a segment.
        val segment = testContext.insertMetadata(testSegmentName, 1024, 1, new long[]{10, 10, 5});

        int validStart = 10;
        int validLength = 15;
        val hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        testContext.chunkedSegmentStorage.truncate(hWrite, validStart, null).get();

        val h = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        // Read all bytes at once.
        byte[] input = new byte[1];
        val inputStream = new ByteArrayInputStream(input);
        // Invalid parameters
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(null, 1, inputStream, validLength, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(h, -1, inputStream, validLength, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(h, 0, inputStream, -1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(h, 0, null, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(null, 0, inputStream, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(SegmentStorageHandle.readHandle(testSegmentName), 0, inputStream, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // Bad offset
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(h, 0, inputStream, 1, TIMEOUT),
                ex -> ex instanceof BadOffsetException);
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(h, validStart, inputStream, 1, TIMEOUT),
                ex -> ex instanceof BadOffsetException);
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(h, validStart + validLength + 1, inputStream, 1, TIMEOUT),
                ex -> ex instanceof BadOffsetException);
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(h, validStart + validLength - 1, inputStream, 1, TIMEOUT),
                ex -> ex instanceof BadOffsetException);
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(h, validStart + 2, inputStream, 1, TIMEOUT),
                ex -> ex instanceof BadOffsetException
                        && ((BadOffsetException) ex).getGivenOffset() == validStart + 2
                        && ((BadOffsetException) ex).getExpectedOffset() == validStart + validLength);
        // Sealed segment
        testContext.chunkedSegmentStorage.seal(h, TIMEOUT).join();
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.write(h, validStart + validLength, inputStream, 1, TIMEOUT),
                ex -> ex instanceof StreamSegmentSealedException);
    }

    /**
     * Test various operations on deleted segment.
     *
     * @throws Exception
     */
    @Test
    public void testSegmentNotExistsExceptionForDeleted() throws Exception {
        String testSegmentName = "foo";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(1); // Force rollover after each byte.
        @Cleanup
        TestContext testContext = getTestContext();
        Assert.assertFalse(testContext.chunkedSegmentStorage.exists(testSegmentName, null).get());

        // Step 1: Create segment.
        val h = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        Assert.assertEquals(h.getSegmentName(), testSegmentName);
        Assert.assertFalse(h.isReadOnly());
        val segmentMetadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertNotNull(segmentMetadata);
        Assert.assertEquals(segmentMetadata.getName(), testSegmentName);
        Assert.assertEquals(segmentMetadata.getKey(), testSegmentName);
        Assert.assertTrue(testContext.chunkedSegmentStorage.exists(testSegmentName, null).get());

        testContext.chunkedSegmentStorage.delete(h, null).join();
        Assert.assertFalse(testContext.chunkedSegmentStorage.exists(testSegmentName, null).get());
        val segmentMetadataAfterDelete = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertFalse(segmentMetadataAfterDelete.isActive());

        AssertExtensions.assertFutureThrows(
                "getStreamSegmentInfo succeeded on missing segment.",
                testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Seal succeeded on missing segment.",
                testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Seal succeeded on missing segment.",
                testContext.chunkedSegmentStorage.openWrite(testSegmentName),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Seal succeeded on missing segment.",
                testContext.chunkedSegmentStorage.openRead(testSegmentName),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.chunkedSegmentStorage.truncate(SegmentStorageHandle.writeHandle(testSegmentName), 0, null),
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    @Test
    public void testOpenWriteAfterFailoverWithNoDataNoAppend() throws Exception {
        testOpenWriteAfterFailoverWithNoData(false);
    }

    @Test
    public void testOpenWriteAfterFailoverWithNoData() throws Exception {
        testOpenWriteAfterFailoverWithNoData(true);
    }

    /**
     * Test failover scenario on empty segment.
     *
     * @throws Exception
     */
    public void testOpenWriteAfterFailoverWithNoData(boolean shouldAppend) throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .appendEnabled(shouldAppend)
                .build());
        testContext.chunkedSegmentStorage.initialize(2);
        int maxRollingLength = 1;
        int ownerEpoch = 1;
        testContext.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch);

        val hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        Assert.assertEquals(hWrite.getSegmentName(), testSegmentName);
        Assert.assertFalse(hWrite.isReadOnly());

        val metadataAfter = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertEquals(2, metadataAfter.getOwnerEpoch());
        Assert.assertEquals(0, metadataAfter.getLength());
    }

    @Test
    public void testOpenReadAfterFailoverWithNoDataNoAppend() throws Exception {
        testOpenReadAfterFailoverWithNoData(false);
    }

    @Test
    public void testOpenReadAfterFailoverWithNoData() throws Exception {
        testOpenReadAfterFailoverWithNoData(true);
    }

    /**
     * Test failover scenario on empty segment.
     *
     * @throws Exception
     */
    public void testOpenReadAfterFailoverWithNoData(boolean shouldAppend) throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .appendEnabled(shouldAppend)
                .build());
        testContext.chunkedSegmentStorage.initialize(2);
        int maxRollingLength = 1;
        int ownerEpoch = 1;
        testContext.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch);

        val hRead = testContext.chunkedSegmentStorage.openRead(testSegmentName).get();
        Assert.assertEquals(hRead.getSegmentName(), testSegmentName);
        Assert.assertTrue(hRead.isReadOnly());

        val metadataAfter = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertEquals(2, metadataAfter.getOwnerEpoch());
        Assert.assertEquals(0, metadataAfter.getLength());
    }

    /**
     * Test failover scenario.
     *
     * @throws Exception
     */
    @Test
    public void testFailoverBehavior() throws Exception {
        String testSegmentName = "foo";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(1000);
        @Cleanup
        TestContext testContext = getTestContext();
        testContext.chunkedSegmentStorage.initialize(1);
        val h = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        int writeAt = 0;
        for (int epoch = 2; epoch < 5; epoch++) {
            testContext.chunkedSegmentStorage.initialize(epoch);

            val hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
            Assert.assertEquals(hWrite.getSegmentName(), testSegmentName);
            Assert.assertFalse(hWrite.isReadOnly());

            testContext.chunkedSegmentStorage.write(h, writeAt, new ByteArrayInputStream(new byte[10]), 10, null).join();

            val metadataAfter = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            Assert.assertEquals(epoch, metadataAfter.getOwnerEpoch());
            writeAt += 10;
        }
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, new long[]{10, 10, 10});
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, 0, 30, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
    }

    /**
     * Test failover scenario for segment with default settings.
     *
     * @throws Exception
     */
    @Test
    public void testOpenWriteAfterFailoverWithAppend() throws Exception {
        String testSegmentName = "foo";
        int ownerEpoch = 2;
        int maxRollingLength = OWNER_EPOCH;

        // Migration from lazy mode before to NO lazy mode after
        testOpenWriteAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{10}, 24, 24, true, true);
        testOpenWriteAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 24, true, true);
        testOpenWriteAfterFailover(NameUtils.getAttributeSegmentName(testSegmentName), ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 24, true, true);

        // Migration from NO lazy mode before to NO lazy mode after
        testOpenWriteAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{10}, 24, 10, true, false);
        testOpenWriteAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 10, true, false);
        testOpenWriteAfterFailover(NameUtils.getAttributeSegmentName(testSegmentName), ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 10, true, false);

    }

    @Test
    public void testOpenWriteAfterFailoverWithNoAppend() throws Exception {
        String testSegmentName = "foo";
        int ownerEpoch = 2;
        int maxRollingLength = OWNER_EPOCH;

        for (boolean useLazyCommitsBefore : new boolean[]{ true, false}) {
            testOpenWriteAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{10}, 24, 10, false, useLazyCommitsBefore);
            testOpenWriteAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 10, false, useLazyCommitsBefore);
            testOpenWriteAfterFailover(NameUtils.getAttributeSegmentName(testSegmentName), ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 10, false, useLazyCommitsBefore);

        }
    }


    /**
     * Test failover scenario for segment with default settings.
     *
     * @throws Exception
     */
    @Test
    public void testOpenReadAfterFailoverWithAppend() throws Exception {
        String testSegmentName = "foo";
        int ownerEpoch = 2;
        int maxRollingLength = OWNER_EPOCH;

        // Migration from lazy mode before to NO lazy mode after
        testOpenReadAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{10}, 24, 24, true, true);
        testOpenReadAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 24, true, true);
        testOpenReadAfterFailover(NameUtils.getAttributeSegmentName(testSegmentName), ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 24, true, true);

        // Migration from NO lazy mode before to NO lazy mode after
        testOpenReadAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{10}, 24, 10, true, false);
        testOpenReadAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 10, true, false);
        testOpenReadAfterFailover(NameUtils.getAttributeSegmentName(testSegmentName), ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 10, true, false);

    }

    @Test
    public void testOpenReadAfterFailoverWithNoAppend() throws Exception {
        String testSegmentName = "foo";
        int ownerEpoch = 2;
        int maxRollingLength = OWNER_EPOCH;

        for (boolean useLazyCommitsBefore : new boolean[]{ true, false}) {
            testOpenReadAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{10}, 24, 10, false, useLazyCommitsBefore);
            testOpenReadAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 10, false, useLazyCommitsBefore);
            testOpenReadAfterFailover(NameUtils.getAttributeSegmentName(testSegmentName), ownerEpoch, maxRollingLength, new long[]{7, 8, 9, 10}, 24, 10, false, useLazyCommitsBefore);
        }
    }

    private void testOpenWriteAfterFailover(String testSegmentName, int ownerEpoch, int maxRollingLength, long[] chunks,
                                            int lastChunkLengthInStorage, int expectedLastChunkLength,
                                            boolean shouldAppend, boolean useLazyCommitBefore) throws Exception {
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .appendEnabled(shouldAppend)
                .build());
        testContext.chunkedSegmentStorage.initialize(ownerEpoch);
        val inserted = TestUtils.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch - 1,
                chunks, chunks,
                true, true,
                testContext.metadataStore, testContext.chunkedSegmentStorage,
                useLazyCommitBefore ? StatusFlags.ACTIVE : StatusFlags.ACTIVE | StatusFlags.ATOMIC_WRITES);
        // Set bigger offset for last chunk
        TestUtils.addChunk(testContext.chunkStorage, inserted.getLastChunk(), lastChunkLengthInStorage);

        // Open with new instance
        val hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        Assert.assertEquals(hWrite.getSegmentName(), testSegmentName);
        Assert.assertFalse(hWrite.isReadOnly());

        // Check metadata
        val metadataAfter = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertEquals(ownerEpoch, metadataAfter.getOwnerEpoch());
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, chunks, expectedLastChunkLength);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, 0, Arrays.stream(chunks).sum(), true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

        Assert.assertEquals(metadataAfter.getLength(), testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get().getLength());
    }

    private void testOpenReadAfterFailover(String testSegmentName, int ownerEpoch, int maxRollingLength, long[] chunks,
                                           int lastChunkLengthInStorage, int expectedLastChunkLength,
                                           boolean shouldAppend, boolean useLazyCommitBefore) throws Exception {
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .appendEnabled(shouldAppend)
                .build());
        testContext.chunkedSegmentStorage.initialize(ownerEpoch);
        val inserted = TestUtils.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch - 1,
                chunks, chunks,
                true, true,
                testContext.metadataStore, testContext.chunkedSegmentStorage,
                useLazyCommitBefore ? StatusFlags.ACTIVE : StatusFlags.ACTIVE | StatusFlags.ATOMIC_WRITES);
        // Set bigger offset for last chunk
        TestUtils.addChunk(testContext.chunkStorage, inserted.getLastChunk(), lastChunkLengthInStorage);

        // Open with new instance
        val hRead = testContext.chunkedSegmentStorage.openRead(testSegmentName).get();
        Assert.assertEquals(hRead.getSegmentName(), testSegmentName);
        Assert.assertTrue(hRead.isReadOnly());

        // Check metadata
        val metadataAfter = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertEquals(ownerEpoch, metadataAfter.getOwnerEpoch());
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, chunks, expectedLastChunkLength);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, 0, Arrays.stream(chunks).sum(), true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

        Assert.assertEquals(metadataAfter.getLength(), testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get().getLength());
    }

    /**
     * Test simple concat.
     *
     * @throws Exception
     */
    @Test
    public void testSimpleConcat() throws Exception {
        @Cleanup
        TestContext testContext = getTestContext();
        for (int maxChunkLength = 1; maxChunkLength <= 3; maxChunkLength++) {
            testSimpleConcat(testContext, maxChunkLength, 1, 1);
            testSimpleConcat(testContext, maxChunkLength, 1, 2);
            testSimpleConcat(testContext, maxChunkLength, 2, 1);
            testSimpleConcat(testContext, maxChunkLength, 2, 2);
            testSimpleConcat(testContext, maxChunkLength, 3, 3);
        }
    }

    private void testSimpleConcat(TestContext testContext, int maxChunkLength, int nChunks1, int nChunks2) throws Exception {
        String targetSegmentName = "target" + UUID.randomUUID().toString();
        String sourceSegmentName = "source" + UUID.randomUUID().toString();

        // Populate segments.
        val h1 = populateSegment(testContext, targetSegmentName, maxChunkLength, nChunks1);
        val h2 = populateSegment(testContext, sourceSegmentName, maxChunkLength, nChunks2);
        HashSet<String> chunksBefore = new HashSet<>();
        chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, targetSegmentName));
        chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, sourceSegmentName));

        // Concat.
        testContext.chunkedSegmentStorage.seal(h2, null).join();
        testContext.chunkedSegmentStorage.concat(h1, (long) nChunks1 * (long) maxChunkLength, sourceSegmentName, null).join();

        HashSet<String>  chunksAfter = new HashSet<String>();
        chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, targetSegmentName));

        // Validate.
        TestUtils.checkSegmentLayout(testContext.metadataStore, targetSegmentName, maxChunkLength, nChunks1 + nChunks2);
        TestUtils.checkSegmentBounds(testContext.metadataStore, targetSegmentName, 0, ((long) nChunks1 + (long) nChunks2) * maxChunkLength);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, targetSegmentName, 0, ((long) nChunks1 + (long) nChunks2) * maxChunkLength, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, targetSegmentName);
        TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);
    }

    @Test
    public void testSimpleConcatWithDefrag() throws Exception {
        @Cleanup
        TestContext testContext = getTestContext();
        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setShouldSupportConcat(true);
        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setShouldSupportAppend(true);

        for (int maxChunkLength = 1; maxChunkLength <= 3; maxChunkLength++) {
            testSimpleConcat(testContext, maxChunkLength, 1, 1);
            testSimpleConcat(testContext, maxChunkLength, 1, 2);
            testSimpleConcat(testContext, maxChunkLength, 2, 1);
            testSimpleConcat(testContext, maxChunkLength, 2, 2);
            testSimpleConcat(testContext, maxChunkLength, 3, 3);
        }
    }

    private void testBaseConcat(TestContext testContext, long maxRollingLength, long[] targetLayout, long[] sourceLayout, long[] resultLayout) throws Exception {
        val source = testContext.insertMetadata("source", maxRollingLength, 1, sourceLayout);
        val target = testContext.insertMetadata("target", maxRollingLength, 1, targetLayout);
        HashSet<String> chunksBefore = new HashSet<>();
        chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, "source"));
        chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, "target"));

        // Concat.
        testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle("source"), null).join();
        val sourceInfo = testContext.chunkedSegmentStorage.getStreamSegmentInfo("target", null).join();
        val targetInfo = testContext.chunkedSegmentStorage.getStreamSegmentInfo("target", null).join();
        testContext.chunkedSegmentStorage.concat(SegmentStorageHandle.writeHandle("target"),
                targetInfo.getLength(),
                "source",
                null).join();

        // Validate.
        TestUtils.checkSegmentLayout(testContext.metadataStore, "target", resultLayout);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, "target");
        TestUtils.checkSegmentBounds(testContext.metadataStore, "target", 0, Arrays.stream(resultLayout).sum());
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, "target", 0, Arrays.stream(resultLayout).sum(), true);
        HashSet<String>  chunksAfter = new HashSet<>();
        chunksAfter.addAll(TestUtils.getChunkNameList(testContext.metadataStore, "target"));
        TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);
        // Cleanup
        testContext.chunkedSegmentStorage.delete(SegmentStorageHandle.writeHandle("target"), null).join();
    }

    /**
     * Test concat with invalid arguments.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testConcatInvalidParameters() throws Exception {
        String targetSegmentName = "target";
        String validSourceSegmentName = "validsource";
        String truncatedSource = "truncatedSource";
        String unsealedSourceSegmentName = "unsealedsource";
        @Cleanup
        TestContext testContext = getTestContext();
        // Setup a segment.
        val targetSegment = testContext.insertMetadata(targetSegmentName, 1024, 1, new long[]{25});

        val validSourceSegment = testContext.insertMetadata(validSourceSegmentName, 1024, 1, new long[]{25});
        testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle(validSourceSegmentName), null).get();

        val invalidSourceSegment = testContext.insertMetadata(truncatedSource, 1024, 1, new long[]{25});
        testContext.chunkedSegmentStorage.truncate(SegmentStorageHandle.writeHandle(truncatedSource), 1, TIMEOUT).get();
        testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle(truncatedSource), null).get();

        val unsealedSourceSegment = testContext.insertMetadata(unsealedSourceSegmentName, 1024, 1, new long[]{25});

        int validStart = 10;
        int validEnd = 25;
        val hWrite = testContext.chunkedSegmentStorage.openWrite(targetSegmentName).get();
        testContext.chunkedSegmentStorage.truncate(hWrite, validStart, TIMEOUT).get();

        val h = testContext.chunkedSegmentStorage.openWrite(targetSegmentName).get();

        // IllegalArgumentException
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(h, -1, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(null, validEnd, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(h, validEnd, null, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // BadOffsetException
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(h, validEnd + 1, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof BadOffsetException);

        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(h, validEnd - 1, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof BadOffsetException);

        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(h, 0, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof BadOffsetException);

        // BadOffsetException
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(h, validEnd + 1, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof BadOffsetException);

        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(h, validEnd - 1, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof BadOffsetException);

        // Not sealed
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(h, validEnd, unsealedSourceSegmentName, TIMEOUT),
                ex -> ex instanceof IllegalStateException);

        // StreamSegmentTruncatedException
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(h, validEnd, truncatedSource, TIMEOUT),
                ex -> ex instanceof StreamSegmentTruncatedException);

        // StreamSegmentTruncatedException
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(h, validEnd, "nonExistent", TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Seal target segment
        testContext.chunkedSegmentStorage.seal(h, TIMEOUT).join();
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.concat(h, 0, truncatedSource, TIMEOUT),
                ex -> ex instanceof StreamSegmentSealedException);

    }

    /**
     * Test write with invalid arguments.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testEmptySegment() throws Exception {
        String testSegmentName = "foo";
        String emptySegmentName = "empty";
        String nonEmptySegmentName = "nonempty";
        @Cleanup
        TestContext testContext = getTestContext();
        long endOffset = 25;
        // Setup a segment.
        testContext.insertMetadata(testSegmentName, 1024, 1, new long[]{endOffset});
        testContext.insertMetadata(nonEmptySegmentName, 1024, 1, new long[]{endOffset});

        val hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        testContext.chunkedSegmentStorage.truncate(hWrite, endOffset, null).get();

        val info = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, TIMEOUT).get();
        Assert.assertEquals(endOffset, info.getStartOffset());
        Assert.assertEquals(info.getLength(), info.getStartOffset());

        val hTarget = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        testContext.chunkedSegmentStorage.truncate(hTarget, endOffset, null).get();
        testContext.chunkedSegmentStorage.truncate(hTarget, endOffset, null).get();

        byte[] bytes = new byte[0];

        // Read should fail
        AssertExtensions.assertFutureThrows(
                "read succeeded on invalid offset.",
                testContext.chunkedSegmentStorage.read(hTarget, endOffset, bytes, 0, 0, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        testContext.chunkedSegmentStorage.write(hTarget, endOffset, new ByteArrayInputStream(bytes), 0, TIMEOUT).join();
        val info2 = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, TIMEOUT).get();
        Assert.assertEquals(endOffset, info2.getStartOffset());
        Assert.assertEquals(info2.getLength(), info2.getStartOffset());

        // Create an empty segment and concat it
        val hSource = testContext.chunkedSegmentStorage.create(emptySegmentName, TIMEOUT).get();
        val infoEmpty = testContext.chunkedSegmentStorage.getStreamSegmentInfo(emptySegmentName, TIMEOUT).get();
        Assert.assertEquals(0, infoEmpty.getStartOffset());
        Assert.assertEquals(infoEmpty.getLength(), infoEmpty.getStartOffset());
        testContext.chunkedSegmentStorage.seal(hSource, TIMEOUT).get();

        testContext.chunkedSegmentStorage.concat(hTarget, endOffset, emptySegmentName, TIMEOUT).get();

        // Now concat non-empty
        testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle(nonEmptySegmentName), TIMEOUT).get();
        testContext.chunkedSegmentStorage.concat(hTarget, endOffset, nonEmptySegmentName, TIMEOUT).get();
        val info4 = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, TIMEOUT).get();
        Assert.assertEquals(endOffset + endOffset, info4.getLength());
        Assert.assertEquals(endOffset, info4.getStartOffset());
    }

    @Test
    public void testBasicConcatWithDefrag() throws Exception {
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(3).build());
        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setShouldSupportAppend(true);
        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setShouldSupportConcat(true);

        // Populate segments
        val sourceLayout = new long[]{1, 2, 3, 4, 5};
        val targetLayout = new long[]{10};
        val resultLayout = new long[]{25};
        int maxRollingLength = 1024;

        testBaseConcat(testContext, maxRollingLength,
                targetLayout, sourceLayout,
                resultLayout);
        return;
    }

    @Test
    public void testBaseConcatWithDefragWithMinMaxLimits() throws Exception {
        // Set limits.
        val maxRollingSize = 30;
        ChunkedSegmentStorageConfig config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .maxSizeLimitForConcat(20)
                .minSizeLimitForConcat(10)
                .build();
        @Cleanup
        TestContext testContext = getTestContext(config);
        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setShouldSupportConcat(true);

        // no-op.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{1},
                new long[]{21, 21, 21},
                new long[]{1, 21, 21, 21});

        // no-op - max rollover size.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{30},
                new long[]{29, 2},
                new long[]{30, 29, 2});
        // no-op - max rollover size.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{30},
                new long[]{1, 2, 3, 4},
                new long[]{30, 10});

        // small chunks followed by normal chunks.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{10},
                new long[]{1, 1, 1, 3, 1, 1, 3, 1, 3},
                new long[]{25});

        // normal chunks followed by small chunks.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{10},
                new long[]{3, 1, 1, 1, 3, 1, 1, 3, 1},
                new long[]{25});

        // consecutive normal.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{10},
                new long[]{1, 3, 3, 3, 1, 2, 2},
                new long[]{25});

        testBaseConcat(testContext, maxRollingSize,
                new long[]{10},
                new long[]{5, 5, 5},
                new long[]{25});

        // all small chunks.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{10},
                new long[]{2, 2, 2, 2, 2, 2, 2, 1},
                new long[]{25});

        testBaseConcat(testContext, maxRollingSize,
                new long[]{10},
                new long[]{12, 3},
                new long[]{25});

        testBaseConcat(testContext, maxRollingSize,
                new long[]{10},
                new long[]{13, 2},
                new long[]{25});

        // First chunk is greater than max concat size
        testBaseConcat(testContext, maxRollingSize,
                new long[]{13},
                new long[]{11, 1},
                new long[]{25});

        // First chunk is greater than max concat size
        testBaseConcat(testContext, maxRollingSize,
                new long[]{13},
                new long[]{10, 2},
                new long[]{25});
    }

    @Test
    public void testBaseConcatWithDefragWithMinMaxLimitsNoAppends() throws Exception {
        // Set limits.
        val maxRollingSize = 30;
        ChunkedSegmentStorageConfig config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .maxSizeLimitForConcat(20)
                .minSizeLimitForConcat(10)
                .appendEnabled(false)
                .build();
        @Cleanup
        TestContext testContext = getTestContext(config);
        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setShouldSupportConcat(true);

        // Normal case.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{11},
                new long[]{12},
                new long[]{23});

        // Bigger than max allowed.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{10},
                new long[]{20},
                new long[]{10, 20});

        // Target is bigger than max allowed after first concat.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{11},
                new long[]{12, 13},
                new long[]{23, 13});

        // One of the chunks in the middle is smaller than min size allowed.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{11},
                new long[]{12, 5, 13},
                new long[]{23, 5, 13});

        // All chunks are smaller, resultant chunk gets bigger than max size allowed.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{11},
                new long[]{2, 2, 2, 2, 2, 2},
                new long[]{21, 2});

        // Chunks are already at max rolling size.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{30},
                new long[]{2, 30, 2, 30, 2, 30},
                new long[]{30, 2, 30, 2, 30, 2, 30});

        // Test max rollover size.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{11},
                new long[]{9, 10},
                new long[]{30});

        // Test max rollover size.
        testBaseConcat(testContext, maxRollingSize,
                new long[]{20},
                new long[]{10, 10},
                new long[]{30, 10});
    }

    /**
     * Test Concat after repeated failure when concat using append mode is on.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testConcatUsingAppendsAfterWriteFailure() throws Exception {
        long maxRollingSize = 20;
        // Last chunk of target in these tests always has garbage at end which can not be overwritten.
        testConcatUsingAppendsAfterWriteFailure(maxRollingSize,
                new long[] {5},
                new long[] {1, 2, 3, 4},
                new int[] {},
                new long[] {5, 10},
                15);

        // First chunk in source has garbage at the end.
        testConcatUsingAppendsAfterWriteFailure(maxRollingSize,
                new long[] {5},
                new long[] {1, 2, 3, 4},
                new int[]  {0}, // add garbage to these chunks
                new long[] {5, 1, 9},
                15);

        // First two chunk in source has garbage at the end.
        testConcatUsingAppendsAfterWriteFailure(maxRollingSize,
                new long[] {5},
                new long[] {1, 2, 3, 4},
                new int[]  {0, 1},  // add garbage to these chunks
                new long[] {5, 1, 2, 7},
                15);

        // First three chunks in source has garbage at the end.
        testConcatUsingAppendsAfterWriteFailure(maxRollingSize,
                new long[] {5},
                new long[] {1, 2, 3, 4},
                new int[]  {0, 1, 2},  // add garbage to these chunks
                new long[] {5, 1, 2, 3, 4},
                15);

        // All chunks in source has garbage at the end.
        testConcatUsingAppendsAfterWriteFailure(maxRollingSize,
                new long[] {5},
                new long[] {1, 2, 3, 4},
                new int[]  {0, 1, 2, 3},  // add garbage to these chunks
                new long[] {5, 1, 2, 3, 4},
                15);
    }

    private void testConcatUsingAppendsAfterWriteFailure(long maxRollingSize,
                                                         long[] targetLayoutBefore,
                                                         long[] sourceLayout,
                                                         int[] chunksWithGarbageIndex,
                                                         long[] targetLayoutAfter,
                                                         long expectedLength) throws Exception {
        String targetSegmentName = "target";
        String sourceSegmentName = "source";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(maxRollingSize); // Force rollover after every 20 byte.
        ChunkedSegmentStorageConfig config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .maxSizeLimitForConcat(100)
                .minSizeLimitForConcat(100)
                .indexBlockSize(3)
                .build();

        @Cleanup
        TestContext testContext = getTestContext(config);
        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setShouldSupportConcat(true);

        // Create target
        testContext.insertMetadata(targetSegmentName, maxRollingSize, 1, targetLayoutBefore);

        // Create source
        testContext.insertMetadata(sourceSegmentName, maxRollingSize, 1, sourceLayout);
        val hSource = testContext.chunkedSegmentStorage.openWrite(sourceSegmentName).get();
        testContext.chunkedSegmentStorage.seal(hSource, null).get();

        // Add some garbage data at the end of last chunk
        val lastChunkMetadata = TestUtils.getChunkMetadata(testContext.metadataStore,
                TestUtils.getSegmentMetadata(testContext.metadataStore, targetSegmentName).getLastChunk());
        testContext.chunkStorage.write(ChunkHandle.writeHandle(lastChunkMetadata.getName()), lastChunkMetadata.getLength(), 1, new ByteArrayInputStream(new byte[1])).join();

        // Write some garbage at the end.
        val sourceList = TestUtils.getChunkList(testContext.metadataStore, sourceSegmentName);
        for (int i : chunksWithGarbageIndex) {
            // Append some data to the last chunk to simulate partial write during failure
            val chunkMetadata = TestUtils.getChunkMetadata(testContext.metadataStore, sourceList.get(i).getName());
            testContext.chunkStorage.write(ChunkHandle.writeHandle(chunkMetadata.getName()), chunkMetadata.getLength(), 1, new ByteArrayInputStream(new byte[1])).join();
        }
        val hTarget = testContext.chunkedSegmentStorage.openWrite(targetSegmentName).get();
        val concatAt = Arrays.stream(targetLayoutBefore).sum();
        testContext.chunkedSegmentStorage.concat(hTarget, concatAt, sourceSegmentName, null).join();
        val list = TestUtils.getChunkList(testContext.metadataStore, targetSegmentName);
        checkDataRead(targetSegmentName, testContext, 0, expectedLength);
        TestUtils.checkSegmentLayout(testContext.metadataStore, targetSegmentName, targetLayoutAfter);
        TestUtils.checkSegmentBounds(testContext.metadataStore, targetSegmentName, 0, expectedLength);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, targetSegmentName, 0, expectedLength, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, targetSegmentName);
    }

    @Test
    public void testBaseConcatWithDefrag() throws Exception {
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(3).build());
        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setShouldSupportConcat(true);

        // Populate segments
        testBaseConcat(testContext, 1024,
                new long[]{10},
                new long[]{1, 2, 3, 4, 5},
                new long[]{25});

        // Populate segments
        testBaseConcat(testContext, 1024,
                new long[]{1, 2, 3, 4, 5},
                new long[]{10},
                new long[]{1, 2, 3, 4, 15});

        // Populate segments
        testBaseConcat(testContext, 1024,
                new long[]{1, 2},
                new long[]{3, 4, 5, 6, 7},
                new long[]{1, 27});

        // Only one object
        testBaseConcat(testContext, 1024,
                new long[]{10},
                new long[]{15},
                new long[]{25});
    }

    @Test
    public void testSimpleTruncate() throws Exception {
        testTruncate(1, 5, 10, 5, 10);
    }

    /**
     * Test truncate with invalid arguments.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testTruncateInvalidParameters() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(3).build());
        // Setup a segment.
        val segment = testContext.insertMetadata(testSegmentName, 1024, 1, new long[]{25});

        int validStart = 10;
        int validEnd = 25;
        val h = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        testContext.chunkedSegmentStorage.truncate(h, validStart, null).get();

        // Invalid parameters
        AssertExtensions.assertFutureThrows("truncate() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.truncate(h, -1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("truncate() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.truncate(null, 11, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("truncate() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.truncate(SegmentStorageHandle.readHandle(testSegmentName), 11, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("truncate() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.truncate(h, validEnd + 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

    }

    @Test
    public void testTruncateNoOpTruncateOffset() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(3).build());
        // Setup a segment.
        val segment = testContext.insertMetadata(testSegmentName, 1024, 1, new long[]{25});

        int validStart = 10;
        int validEnd = 25;
        val h = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        testContext.chunkedSegmentStorage.truncate(h, validStart, null).get();
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, validStart, validEnd, true);

        // Test truncate offset < start offset
        testContext.chunkedSegmentStorage.truncate(h, validStart - 1, TIMEOUT).join();
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, validStart, validEnd);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, validStart, validEnd, true);

        // Test truncate offset == start offset
        testContext.chunkedSegmentStorage.truncate(h, validStart, TIMEOUT).join();
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, validStart, validEnd);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, validStart, validEnd, true);
    }

    @Test
    public void testRepeatedTruncates() throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(4).build();
        testRepeatedTruncates(config, 1, 3);
        testRepeatedTruncates(config, 3, 1);
        testRepeatedTruncates(config, 2, 3);
        testRepeatedTruncates(config, 3, 2);
        testRepeatedTruncates(config, 3, 3);

    }

    private void testRepeatedTruncates(ChunkedSegmentStorageConfig config, long maxChunkLength, int numberOfChunks) throws Exception {
        @Cleanup
        TestContext testContext = getTestContext(config);
        String testSegmentName = "testSegmentName";

        // Populate segment.
        val expectedLength = Math.toIntExact(maxChunkLength * numberOfChunks);
        val h1 = populateSegment(testContext, testSegmentName, maxChunkLength, numberOfChunks);
        byte[] buffer = new byte[expectedLength];

        // Perform series of truncates.
        for (int truncateAt = 0; truncateAt < expectedLength; truncateAt++) {
            HashSet<String> chunksBefore = new HashSet<>();
            chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));

            testContext.chunkedSegmentStorage.truncate(h1, truncateAt, null).join();
            TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, truncateAt, expectedLength);
            val expectedChunkLengths = calculateExpectedChunkLengths(config, maxChunkLength, truncateAt, expectedLength);
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, expectedChunkLengths);
            TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, truncateAt, expectedLength, true);
            TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
            HashSet<String>  chunksAfter = new HashSet<>();
            chunksAfter.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));
            TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);

            val metadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            // length doesn't change.
            Assert.assertEquals(expectedLength, metadata.getLength());
            // start offset should match i.
            Assert.assertEquals(truncateAt, metadata.getStartOffset());
            // Check first chunk start offset.
            if (metadata.getFirstChunkStartOffset() % maxChunkLength == 0) {
                Assert.assertEquals(maxChunkLength * (numberOfChunks - expectedChunkLengths.length), metadata.getFirstChunkStartOffset());
            } else {
                val threshold = config.getMinPercentForTruncateRelocation() * maxChunkLength / 100;
                Assert.assertEquals(maxChunkLength * (numberOfChunks - expectedChunkLengths.length) + threshold, metadata.getFirstChunkStartOffset());
            }

            // try to read some bytes.
            val bytesRead = testContext.chunkedSegmentStorage.read(h1, truncateAt, buffer, 0, buffer.length - truncateAt, null).get().intValue();
            Assert.assertEquals(buffer.length - truncateAt, bytesRead);
            if (truncateAt > 0) {
                AssertExtensions.assertFutureThrows(
                        "read succeeded on missing segment.",
                        testContext.chunkedSegmentStorage.read(h1, truncateAt - 1, buffer, 0, buffer.length - truncateAt, null),
                        ex -> ex instanceof StreamSegmentTruncatedException);
            }
        }
    }

    @Test
    public void testRepeatedTruncatesOnLargeChunkVaryingSizes() throws Exception {
        @Cleanup
        TestContext testContext = getTestContext();
        String testSegmentName = "testSegmentName";
        // Set up.
        val h1 = populateSegment(testContext, testSegmentName, 10, 1);
        byte[] buffer = new byte[10];

        int truncateAt = 0;
        for (int i = 0; i < 4; i++) {
            HashSet<String> chunksBefore = new HashSet<>();
            chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));

            testContext.chunkedSegmentStorage.truncate(h1, truncateAt, null).join();

            // Check layout.
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, new long[]{10});
            TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
            HashSet<String>  chunksAfter = new HashSet<>();
            chunksAfter.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));
            TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);

            // Validate.
            val metadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            Assert.assertEquals(10, metadata.getLength());
            Assert.assertEquals(truncateAt, metadata.getStartOffset());
            Assert.assertEquals(0, metadata.getFirstChunkStartOffset());

            // validate read.
            val bytesRead = testContext.chunkedSegmentStorage.read(h1, truncateAt, buffer, 0, 10 - truncateAt, null).get().intValue();
            Assert.assertEquals(10 - truncateAt, bytesRead);
            truncateAt += i;
        }
    }

    @Test
    public void testRepeatedTruncatesOnLargeChunk() throws Exception {
        @Cleanup
        TestContext testContext = getTestContext();
        String testSegmentName = "testSegmentName";
        // Populate.
        val h1 = populateSegment(testContext, testSegmentName, 10, 1);
        byte[] buffer = new byte[10];
        for (int truncateAt = 0; truncateAt < 9; truncateAt++) {
            HashSet<String> chunksBefore = new HashSet<>();
            chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));
            testContext.chunkedSegmentStorage.truncate(h1, truncateAt, null).join();

            // Check layout.
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, new long[]{10});
            TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

            // Validate info
            val metadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            Assert.assertEquals(10, metadata.getLength());
            Assert.assertEquals(truncateAt, metadata.getStartOffset());
            Assert.assertEquals(0, metadata.getFirstChunkStartOffset());
            TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
            HashSet<String>  chunksAfter = new HashSet<>();
            chunksAfter.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));
            TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);

            // Validate read.
            val bytesRead = testContext.chunkedSegmentStorage.read(h1, truncateAt, buffer, 0, 10 - truncateAt, null).get().intValue();
            Assert.assertEquals(10 - truncateAt, bytesRead);
        }
    }

    @Test
    public void testRepeatedTruncatesAtFullLength() throws Exception {
        @Cleanup
        TestContext testContext = getTestContext();
        String testSegmentName = "testSegmentName";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2);
        // create.
        val h1 = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        long expectedLength = 0;
        for (int i = 1; i < 5; i++) {
            val info1 = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get();
            Assert.assertEquals(expectedLength, info1.getLength());
            Assert.assertEquals(info1.getLength(), info1.getStartOffset());

            // Write some data.
            byte[] buffer = new byte[i];
            testContext.chunkedSegmentStorage.write(h1, info1.getStartOffset(), new ByteArrayInputStream(buffer), buffer.length, null).join();
            val info2 = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get();
            expectedLength += i;
            Assert.assertEquals(expectedLength, info2.getLength());
            HashSet<String> chunksBefore = new HashSet<>();
            chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));

            // Now truncate
            testContext.chunkedSegmentStorage.truncate(h1, info2.getLength(), null).join();

            // Validate info
            val metadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            Assert.assertEquals(expectedLength, metadata.getLength());
            Assert.assertEquals(expectedLength, metadata.getStartOffset());
            Assert.assertEquals(expectedLength, metadata.getFirstChunkStartOffset());
            Assert.assertEquals(expectedLength, metadata.getLastChunkStartOffset());
            Assert.assertEquals(null, metadata.getLastChunk());
            Assert.assertEquals(null, metadata.getFirstChunk());
            TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
            HashSet<String>  chunksAfter = new HashSet<>();
            chunksAfter.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));
            TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);

            // Validate Exceptions.
            val expectedLength2 = expectedLength;
            val h = testContext.chunkedSegmentStorage.openRead(testSegmentName).get();
            AssertExtensions.assertFutureThrows(
                    "read succeeded on invalid offset.",
                    testContext.chunkedSegmentStorage.read(h, expectedLength - 1, new byte[1], 0, 1, null),
                    ex -> ex instanceof StreamSegmentTruncatedException && ((StreamSegmentTruncatedException) ex).getStartOffset() == expectedLength2);
            AssertExtensions.assertFutureThrows(
                    "read succeeded on invalid offset.",
                    testContext.chunkedSegmentStorage.read(h, expectedLength, new byte[1], 0, 1, null),
                    ex -> ex instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testTruncateVariousOffsets() throws Exception {
        long maxChunkSize = 3;
        int numberOfChunks = 4;
        for (int i = 0; i < numberOfChunks; i++) {
            for (int j = 0; j < maxChunkSize; j++) {
                val truncateAt = i * maxChunkSize + j;
                testTruncate(maxChunkSize, truncateAt, numberOfChunks, numberOfChunks - i, maxChunkSize * numberOfChunks);
            }
        }
    }

    @Test
    public void testBaseTruncate() throws Exception {
        testTruncate(1, 1, 2, 1, 2);
        testTruncate(1, 2, 4, 2, 4);

        testTruncate(3, 1, 2, 2, 6);
        testTruncate(3, 3, 4, 3, 12);
    }

    @Test
    public void testBaseRelocatingTruncate() throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .indexBlockSize(3)
                .relocateOnTruncateEnabled(true)
                .minSizeForTruncateRelocationInbytes(10)
                .minPercentForTruncateRelocation(80)
                .build();
        val numberOfChunks = 3;
        val maxChunkSize = 20;
        val threshold = 16;
        testRelocatingTruncate(config, numberOfChunks, maxChunkSize, threshold);
    }

    @Test
    public void testRelocatingTruncateHalfEmpty() throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .indexBlockSize(3)
                .relocateOnTruncateEnabled(true)
                .minSizeForTruncateRelocationInbytes(1)
                .minPercentForTruncateRelocation(50)
                .build();
        for (int threshold = 16; threshold > 2; threshold /= 2) {
            testRelocatingTruncate(config, 1, threshold, threshold / 2);
        }
    }

    @Test
    public void testMultipleRelocatingTruncate() throws Exception {
        // Force multiple relocation every time relocating half chunk
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .indexBlockSize(3)
                .relocateOnTruncateEnabled(true)
                .minSizeForTruncateRelocationInbytes(1)
                .minPercentForTruncateRelocation(50)
                .build();

        @Cleanup
        TestContext testContext = getTestContext(config);
        String testSegmentName = "testSegmentName";
        int maxChunkLength = 32;
        int truncateAt = 0;
        // Populate
        val h1 = populateSegment(testContext, testSegmentName, maxChunkLength, 1);

        for (int threshold = maxChunkLength; threshold > 2; threshold /= 2) {
            truncateAt += threshold / 2;
            testTruncate(testContext, testSegmentName, maxChunkLength, truncateAt, 1, maxChunkLength, threshold / 2);
        }
    }

    @Test
    public void testRelocatingTruncateWithMaxSize() throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .indexBlockSize(3)
                .relocateOnTruncateEnabled(true)
                .minSizeForTruncateRelocationInbytes(1)
                .maxSizeForTruncateRelocationInbytes(10)
                .minPercentForTruncateRelocation(1)
                .build();

        @Cleanup
        TestContext testContext = getTestContext(config);
        val h1 = populateSegment(testContext, "test1", 11, 1);
        val h2 = populateSegment(testContext, "test2", 10, 1);
        testTruncate(testContext, "test1", 11, 10, 1, 11, 11);
        testTruncate(testContext, "test2", 10, 9, 1, 10, 1);
    }

    private void testRelocatingTruncate(ChunkedSegmentStorageConfig config, int numberOfChunks, long maxChunkSize, int threshold) throws Exception {
        for (int i = 0; i < numberOfChunks; i++) {
            testTruncate(config, maxChunkSize, i * maxChunkSize, numberOfChunks, numberOfChunks - i, maxChunkSize * numberOfChunks, maxChunkSize);
            testTruncate(config, maxChunkSize, i * maxChunkSize + threshold - 1, numberOfChunks, numberOfChunks - i, maxChunkSize * numberOfChunks, maxChunkSize);
            for (int j = threshold; j < maxChunkSize; j++) {
                testTruncate(config, maxChunkSize, i * maxChunkSize + j, numberOfChunks, numberOfChunks - i, maxChunkSize * numberOfChunks, maxChunkSize - j);
            }
        }
    }

    @Test
    public void testRepeatedTruncateWithRelocation() throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .indexBlockSize(3)
                .relocateOnTruncateEnabled(true)
                .minSizeForTruncateRelocationInbytes(5)
                .minPercentForTruncateRelocation(80)
                .build();
        testRepeatedTruncates(config, 10, 1);
        testRepeatedTruncates(config, 10, 2);
        testRepeatedTruncates(config, 10, 3);
    }

    private void testTruncate(long maxChunkLength, long truncateAt, int chunksCountBefore, int chunksCountAfter, long expectedLength) throws Exception {
        testTruncate(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(3).build(), maxChunkLength, truncateAt, chunksCountBefore, chunksCountAfter, expectedLength, maxChunkLength);
    }

    private void testTruncate(ChunkedSegmentStorageConfig config, long maxChunkLength, long truncateAt,
                              int chunksCountBefore, int chunksCountAfter, long expectedLength,
                              long expectedFirstChunkLength) throws Exception {
        @Cleanup
        TestContext testContext = getTestContext(config);
        String testSegmentName = "testSegmentName";
        // Populate
        val h1 = populateSegment(testContext, testSegmentName, maxChunkLength, chunksCountBefore);
        testTruncate(testContext, testSegmentName, maxChunkLength, truncateAt, chunksCountAfter, expectedLength, expectedFirstChunkLength);
    }

    private void testTruncate(TestContext testContext, String testSegmentName, long maxChunkLength, long truncateAt,
                              int chunksCountAfter, long expectedLength,
                              long expectedFirstChunkLength) throws Exception {
        HashSet<String> chunksBefore = new HashSet<>();
        chunksBefore.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));

        // Perform truncate.
        testContext.chunkedSegmentStorage.truncate(SegmentStorageHandle.writeHandle(testSegmentName), truncateAt, null).join();

        // Check layout.
        long[] expectedLengths = calculateExpectedChunkLengths(maxChunkLength, chunksCountAfter, expectedFirstChunkLength);
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, expectedLengths);
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, truncateAt, expectedLength);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, truncateAt, expectedLength, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
        HashSet<String>  chunksAfter = new HashSet<>();
        chunksAfter.addAll(TestUtils.getChunkNameList(testContext.metadataStore, testSegmentName));
        TestUtils.checkGarbageCollectionQueue(testContext.chunkedSegmentStorage, chunksBefore, chunksAfter);
    }

    private long[] calculateExpectedChunkLengths(long maxChunkLength, int chunksCountAfter, long expectedFirstChunkLength) {
        long[] expectedLengths = new long[chunksCountAfter];
        Arrays.fill(expectedLengths, maxChunkLength);
        expectedLengths[0] = expectedFirstChunkLength;
        return expectedLengths;
    }

    private long[] calculateExpectedChunkLengths(ChunkedSegmentStorageConfig config, long maxChunkLength, long startOffset, long length ) {
        val chunkCount = Math.toIntExact(length / maxChunkLength - startOffset / maxChunkLength); // Note two independent int divisions.
        long[] expectedLengths = new long[chunkCount];
        Arrays.fill(expectedLengths, maxChunkLength);
        if (config.isRelocateOnTruncateEnabled()
                && maxChunkLength > config.getMinSizeForTruncateRelocationInbytes()
                && maxChunkLength <= config.getMaxSizeForTruncateRelocationInbytes()) {
            val threshold = config.getMinPercentForTruncateRelocation() * maxChunkLength / 100;
            val offset = startOffset % maxChunkLength;
            expectedLengths[0] = offset >= threshold ? maxChunkLength - threshold : maxChunkLength;
        }
        return expectedLengths;
    }

    /**
     * Test read and write with multiple failovers.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testReadWriteWithMultipleFailovers() throws Exception {
        @Cleanup
        CleanupHelper cleanupHelper = new CleanupHelper();
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();
        cleanupHelper.add(testContext);
        // Create
        testContext.chunkedSegmentStorage.create(testSegmentName, null).get();

        // Write some data.
        long writeAt = 0;
        long epoch = CONTAINER_ID;
        ArrayList<Long> lengths = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            // Create a new test context and initialize with new epoch.
            val hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
            testContext.chunkedSegmentStorage.write(hWrite, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
            lengths.add((long) i);

            // Read in same epoch.
            checkDataRead(testSegmentName, testContext, 0, writeAt);

            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, Longs.toArray(lengths));

            // Fork the context.
            val oldTestCotext = testContext;
            testContext = oldTestCotext.fork(epoch++);
            cleanupHelper.add(testContext);

            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, Longs.toArray(lengths));

            // Fence out old store.
            oldTestCotext.metadataStore.markFenced();

            // Read in new epoch.
            checkDataRead(testSegmentName, testContext, 0, writeAt);
        }

        int total = 10;

        // Create a new test context and initialize with new epoch.
        testContext = testContext.fork(epoch++);
        cleanupHelper.add(testContext);

        checkDataRead(testSegmentName, testContext, 0, total);
    }

    /**
     * Test read and write with multiple failovers.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testReadWriteWithMultipleFailoversWithGarbage() throws Exception {
        @Cleanup
        CleanupHelper cleanupHelper = new CleanupHelper();
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        cleanupHelper.add(testContext);

        // Create
        testContext.chunkedSegmentStorage.create(testSegmentName, null).get();

        // Write some data.
        long writeAt = 0;
        long epoch = CONTAINER_ID;
        SegmentHandle hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        ArrayList<Long> lengths = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            // Create a new test context and initialize with new epoch.
            testContext.chunkedSegmentStorage.write(hWrite, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
            lengths.add((long) i);

            // Read in same epoch.
            checkDataRead(testSegmentName, testContext, 0, writeAt);
            val lengthsArray = Longs.toArray(lengths);
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, lengthsArray);
            TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, 0, Arrays.stream(lengthsArray).sum(), false);

            // Fork the context.
            val oldTestCotext = testContext;
            testContext = oldTestCotext.fork(epoch++);
            cleanupHelper.add(testContext);

            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, Longs.toArray(lengths));

            // Make sure to open segment with new instance before writing garbage to old instance.
            hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();

            // Write some garbage
            oldTestCotext.chunkedSegmentStorage.write(hWrite, writeAt, new ByteArrayInputStream(new byte[10]), 10, null).join();

            // Fence out old store.
            oldTestCotext.metadataStore.markFenced();

            AssertExtensions.assertFutureThrows("write() allowed after fencing",
                    oldTestCotext.chunkedSegmentStorage.write(hWrite, writeAt + 10, new ByteArrayInputStream(new byte[10]), 10, null),
                    ex -> ex instanceof StorageNotPrimaryException);
            // Read in new epoch.
            checkDataRead(testSegmentName, testContext, 0, writeAt);
        }

        int total = 10;

        // Create a new test context and initialize with new epoch.
        testContext = testContext.fork(epoch++);
        cleanupHelper.add(testContext);

        checkDataRead(testSegmentName, testContext, 0, total);
    }

    /**
     * Test truncate, read and write with multiple failovers.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testTruncateWithMultipleFailoversWithGarbage() throws Exception {
        @Cleanup
        CleanupHelper cleanupHelper = new CleanupHelper();
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(3).build());
        cleanupHelper.add(testContext);

        // Create
        testContext.chunkedSegmentStorage.create(testSegmentName, null).get();

        // Write some data.
        long writeAt = 0;
        long truncateAt = 0;
        long epoch = CONTAINER_ID;
        SegmentHandle hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        ArrayList<Long> lengths = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            // Truncate and Read in same epoch.
            testContext.chunkedSegmentStorage.truncate(hWrite, truncateAt, null).get();
            checkDataRead(testSegmentName, testContext, truncateAt, writeAt);
            TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, truncateAt, writeAt);
            TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, truncateAt, writeAt, false);

            // Create a new test context and initialize with new epoch.
            testContext.chunkedSegmentStorage.write(hWrite, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
            lengths.add((long) i);
            TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, truncateAt, writeAt);
            TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, truncateAt, writeAt, false);
            checkDataRead(testSegmentName, testContext, truncateAt, writeAt);

            truncateAt++;

            // Fork the context.
            val oldTestCotext = testContext;
            testContext = oldTestCotext.fork(epoch++);
            cleanupHelper.add(testContext);

            // Make sure to open segment with new instance before writing garbage to old instance.
            hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();

            // Write some garbage
            oldTestCotext.chunkedSegmentStorage.write(hWrite, writeAt, new ByteArrayInputStream(new byte[10]), 10, null).join();

            // Fence out old store.
            boolean exceptionThrown = false;
            oldTestCotext.metadataStore.markFenced();

            AssertExtensions.assertFutureThrows("write() allowed after fencing",
                    oldTestCotext.chunkedSegmentStorage.write(hWrite, writeAt + 10, new ByteArrayInputStream(new byte[10]), 10, null),
                    ex -> ex instanceof StorageNotPrimaryException);
            // Read in new epoch.
            checkDataRead(testSegmentName, testContext, truncateAt, writeAt);
        }

        int total = 10;

        // Create a new test context and initialize with new epoch.
        testContext = testContext.fork(epoch++);
        cleanupHelper.add(testContext);

        checkDataRead(testSegmentName, testContext, truncateAt, total);
    }

    @Test
    public void testTruncateWithFailover() throws Exception {
        @Cleanup
        CleanupHelper cleanupHelper = new CleanupHelper();
        String testSegmentName = "foo";
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .garbageCollectionDelay(Duration.ZERO)
                .indexBlockSize(3)
                .build();
        TestContext testContext = getTestContext(config);
        cleanupHelper.add(testContext);
        // Create
        testContext.chunkedSegmentStorage.create(testSegmentName, null).get();

        // Write some data.
        long offset = 0;
        int i = 2;
        long epoch = testContext.chunkedSegmentStorage.getEpoch();
        SegmentHandle hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();

        // Create a new test context and initialize with new epoch.
        testContext.chunkedSegmentStorage.write(hWrite, offset, new ByteArrayInputStream(new byte[i]), i, null).join();
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, offset, offset + i);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, offset, offset + i, true);
        checkDataRead(testSegmentName, testContext, offset, i);
        offset += i;
        // Fork the context.
        testContext = testContext.fork(++epoch);
        cleanupHelper.add(testContext);
        val oldTestCotext = testContext;
        val newTestContext = oldTestCotext.fork(++epoch);
        cleanupHelper.add(newTestContext);
        // Fence out old store.
        oldTestCotext.metadataStore.markFenced();

        // Truncate and Read in new epoch.
        // Make sure to open segment with new instance before writing garbage to old instance.
        hWrite = newTestContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        newTestContext.chunkedSegmentStorage.truncate(hWrite, offset, null).get();

        TestUtils.checkSegmentBounds(newTestContext.metadataStore, testSegmentName, offset, offset);
        TestUtils.checkReadIndexEntries(newTestContext.chunkedSegmentStorage, newTestContext.metadataStore, testSegmentName, offset, offset, false);

        AssertExtensions.assertFutureThrows("openWrite() allowed after fencing",
                oldTestCotext.chunkedSegmentStorage.openWrite(testSegmentName),
                ex -> ex instanceof StorageNotPrimaryException);
        AssertExtensions.assertFutureThrows("openRead() allowed after fencing",
                oldTestCotext.chunkedSegmentStorage.openRead(testSegmentName),
                ex -> ex instanceof StorageNotPrimaryException);

    }

    // Very useful test, but takes couple seconds.
    //@Test(timeout = 180000)
    public void testParallelSegmentOperationsLargeLoad() throws Exception {
        testParallelSegmentOperations(1000, 10);
    }

    // Very useful test, but takes couple seconds.
    //@Test(timeout = 1800000)
    public void testParallelSegmentOperationsExtraLargeLoad() throws Exception {
        testParallelSegmentOperations(10000, 10);
    }

    @Test
    public void testParallelSegmentOperations() throws Exception {
        testParallelSegmentOperations(100, 10);
    }

    public void testParallelSegmentOperations(int numberOfRequests, int threadPoolSize) throws Exception {
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        @Cleanup
        TestContext testContext = getTestContext();
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        CompletableFuture[] futures = new CompletableFuture[numberOfRequests];
        for (int i = 0; i < numberOfRequests; i++) {
            String testSegmentName = "test" + i;
            val f = testSimpleScenarioAsync(testSegmentName, policy, testContext, executor);
            futures[i] = f;
        }
        CompletableFuture.allOf(futures).join();
    }

    // Very useful test, but takes couple seconds.
    //@Test
    public void testParallelSegmentOperationsWithReentryLargeLoad() throws Exception {
        int numberOfRequests = 1000;
        testParallelSegmentOperationsWithReentry(numberOfRequests, 10, true);
    }

    // Very useful test, but takes couple seconds.
    //@Test(timeout = 1800000)
    public void testParallelSegmentOperationsWithReentryExtraLargeLoad() throws Exception {
        int numberOfRequests = 10000;
        testParallelSegmentOperationsWithReentry(numberOfRequests, 1000, false);
    }

    @Test
    public void testParallelSegmentOperationsWithReentry() throws Exception {
        int numberOfRequests = 10;
        testParallelSegmentOperationsWithReentry(numberOfRequests, 10, true);
    }

    private void testParallelSegmentOperationsWithReentry(int numberOfRequests, int threadPoolSize, boolean shouldBlock) throws Exception {
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        @Cleanup
        TestContext testContext = getTestContext();

        if (!(testContext.metadataStore instanceof InMemoryMetadataStore)) {
            return;
        }
        CompletableFuture<Void> futureToWaitOn = shouldBlock ? new CompletableFuture<Void>() : CompletableFuture.completedFuture(null);
        // Step 1: Populate system segment.
        // Write some data to system segment so that we can read it back in call back.
        val systemSegment = "SystemSegment";
        val h = testContext.chunkedSegmentStorage.create(systemSegment, policy, null).get();
        testContext.chunkedSegmentStorage.write(h, 0, new ByteArrayInputStream(new byte[1]), 1, null).join();

        // Step 2: Setup call backs.
        // Set up a call back which will be invoked during get call.
        ((InMemoryMetadataStore) testContext.metadataStore).setReadCallback(transactionData -> {
            // Make sure we don't invoke read for system segment itself.
            if (!transactionData.getKey().equals(systemSegment)) {
                return futureToWaitOn.thenComposeAsync(v -> checkDataReadAsync(systemSegment, testContext, 0, 1, executorService()), executorService())
                        .thenApplyAsync(v -> null, executorService());
            }
            return CompletableFuture.completedFuture(null);
        });
        // Set up a call back which will be invoked during writeAll call.
        ((InMemoryMetadataStore) testContext.metadataStore).setWriteCallback(transactionDataList -> {
            // Make sure we don't invoke read for system segment itself.
            if (transactionDataList.stream().filter(t -> !t.getKey().equals(systemSegment)).findAny().isPresent()) {
                return futureToWaitOn.thenComposeAsync(v -> checkDataReadAsync(systemSegment, testContext, 0, 1, executorService()), executorService())
                        .thenApplyAsync(v -> null, executorService());
            }
            return CompletableFuture.completedFuture(null);
        });

        // Step 3: Perform operations on multiple segments.
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        CompletableFuture[] futures = new CompletableFuture[numberOfRequests];
        for (int i = 0; i < numberOfRequests; i++) {
            String testSegmentName = "test" + i;
            val f = testSimpleScenarioAsync(testSegmentName, policy, testContext, executor);
            futures[i] = f;
        }
        if (shouldBlock) {
            futureToWaitOn.complete(null);
        }
        CompletableFuture.allOf(futures).join();
    }

    // Very useful test, but takes couple seconds.
    //@Test(timeout = 180000)
    public void testParallelReadRequestsOnSingleSegmentLargeLoad() throws Exception {
        int numberOfRequests = 1000;
        testParallelReadRequestsOnSingleSegment(numberOfRequests, 10);
    }

    // Very useful test, but takes couple seconds.
    //@Test(timeout = 1800000)
    public void testParallelReadRequestsOnSingleSegmentExtraLargeLoad() throws Exception {
        int numberOfRequests = 10000;
        testParallelReadRequestsOnSingleSegment(numberOfRequests, 10);
    }

    @Test
    public void testParallelReadRequestsOnSingleSegment() throws Exception {
        int numberOfRequests = 100;
        testParallelReadRequestsOnSingleSegment(numberOfRequests, 10);
    }

    private void testParallelReadRequestsOnSingleSegment(int numberOfRequests, int threadPoolSize) throws Exception {
        String testSegmentName = "testSegment";

        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        @Cleanup
        TestContext testContext = getTestContext();

        // Step 1: Create system segment.
        val h = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        Assert.assertEquals(h.getSegmentName(), testSegmentName);
        Assert.assertFalse(h.isReadOnly());

        // Step 2: Write some data.
        long writeAt = 0;
        for (int i = 1; i < 5; i++) {
            testContext.chunkedSegmentStorage.write(h, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
        }
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, 2, 5);
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, 0, 10);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, 0, 10, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

        // Step 3: Read data back.
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        CompletableFuture[] futures = new CompletableFuture[numberOfRequests];
        for (int i = 0; i < numberOfRequests; i++) {
            CompletableFuture<Void> f = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null)
                    .thenComposeAsync(info -> {
                        Assert.assertFalse(info.isSealed());
                        Assert.assertFalse(info.isDeleted());
                        Assert.assertEquals(info.getName(), testSegmentName);
                        Assert.assertEquals(info.getLength(), 10);
                        Assert.assertEquals(info.getStartOffset(), 0);
                        return checkDataReadAsync(testSegmentName, testContext, 0, 10, executor);
                    }, executor);
            futures[i] = f;
        }
        CompletableFuture.allOf(futures).join();
    }

    // Very useful test, but takes couple seconds.
    //@Test(timeout = 180000)
    public void testParallelReadRequestsOnSingleSegmentWithReentryLargeLoad() throws Exception {
        int numberOfRequests = 1000;
        testParallelReadRequestsOnSingleSegmentWithReentry(numberOfRequests, 10, true);
    }

    // Very useful test, but takes couple seconds.
    //@Test(timeout = 1800000)
    public void testParallelReadRequestsOnSingleSegmentWithReentryExtraLargeLoad() throws Exception {
        int numberOfRequests = 10000;
        testParallelReadRequestsOnSingleSegmentWithReentry(numberOfRequests, 10, true);
    }

    @Test
    public void testParallelReadRequestsOnSingleSegmentWithReentry() throws Exception {
        int numberOfRequests = 10;
        testParallelReadRequestsOnSingleSegmentWithReentry(numberOfRequests, 10, true);
    }

    private void testParallelReadRequestsOnSingleSegmentWithReentry(int numberOfRequests, int threadPoolSize, boolean shouldBlock) throws Exception {
        String testSegmentName = "testSegment";

        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(3).build());
        if (!(testContext.metadataStore instanceof InMemoryMetadataStore)) {
            return;
        }

        CompletableFuture<Void> futureToWaitOn = shouldBlock ? new CompletableFuture<Void>() : CompletableFuture.completedFuture(null);

        // Step 1: Populate dummy system segment segment.
        // Write some data to system segment so that we can read it back in call back.
        val systemSegment = "SystemSegment";
        val hSystem = testContext.chunkedSegmentStorage.create(systemSegment, policy, null).get();
        testContext.chunkedSegmentStorage.write(hSystem, 0, new ByteArrayInputStream(new byte[1]), 1, null).join();

        // Step 2: Create test segment.
        val h = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        Assert.assertEquals(h.getSegmentName(), testSegmentName);
        Assert.assertFalse(h.isReadOnly());

        // Step 3: Write some data to test segment.
        long writeAt = 0;
        for (int i = 1; i < 5; i++) {
            testContext.chunkedSegmentStorage.write(h, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
        }
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, 2, 5);
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, 0, 10);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName, 0, 10, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

        // Step 4: Setup call backs that read system segment on each read.
        // Set up a call back which will be invoked during get call.

        ((InMemoryMetadataStore) testContext.metadataStore).setReadCallback(transactionData -> {
            // Make sure we don't invoke read for system segment itself.
            if (!transactionData.getKey().equals(systemSegment)) {
                return futureToWaitOn.thenComposeAsync(v -> checkDataReadAsync(systemSegment, testContext, 0, 1, executorService()), executorService())
                        .thenApplyAsync(v -> null, executorService());
            }
            return CompletableFuture.completedFuture(null);
        });

        // Step 5: Read back data concurrently.
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        CompletableFuture[] futures = new CompletableFuture[numberOfRequests];
        for (int i = 0; i < numberOfRequests; i++) {
            CompletableFuture<Void> f = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null)
                    .thenComposeAsync(info -> {
                        Assert.assertFalse(info.isSealed());
                        Assert.assertFalse(info.isDeleted());
                        Assert.assertEquals(info.getName(), testSegmentName);
                        Assert.assertEquals(info.getLength(), 10);
                        Assert.assertEquals(info.getStartOffset(), 0);
                        return checkDataReadAsync(testSegmentName, testContext, 0, 10, executor);
                    }, executor);
            futures[i] = f;
        }
        if (shouldBlock) {
            futureToWaitOn.complete(null);
        }
        CompletableFuture.allOf(futures).join();
    }

    /**
     * Test concurrent writes to storage system segments by simulating concurrent writes.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSystemSegmentConcurrency() throws Exception {

        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(3).build());
        // Force parallel writes irrespective of thread pool size for tests themselves.
        val writeSize = 10;
        val numWrites = 10;
        val numOfStorageSystemSegments = SystemJournal.getChunkStorageSystemSegments(CONTAINER_ID).length;
        val data = new byte[numOfStorageSystemSegments][writeSize * numWrites];

        var futures = new ArrayList<CompletableFuture<Void>>();
        // To make sure all write operations are concurrent.
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newFixedThreadPool(numOfStorageSystemSegments);

        for (int i = 0; i < numOfStorageSystemSegments; i++) {
            final int k = i;
            futures.add(CompletableFuture.runAsync(() -> {

                populate(data[k]);
                String systemSegmentName = SystemJournal.getChunkStorageSystemSegments(CONTAINER_ID)[k];
                val h = testContext.chunkedSegmentStorage.create(systemSegmentName, null).join();
                // Init
                long offset = 0;
                for (int j = 0; j < numWrites; j++) {
                    testContext.chunkedSegmentStorage.write(h, offset, new ByteArrayInputStream(data[k], writeSize * j, writeSize), writeSize, null).join();
                    offset += writeSize;
                }
                val info = testContext.chunkedSegmentStorage.getStreamSegmentInfo(systemSegmentName, null).join();
                Assert.assertEquals(writeSize * numWrites, info.getLength());
                byte[] out = new byte[writeSize * numWrites];
                val hr = testContext.chunkedSegmentStorage.openRead(systemSegmentName).join();
                testContext.chunkedSegmentStorage.read(hr, 0, out, 0, writeSize * numWrites, null).join();
                Assert.assertArrayEquals(data[k], out);
            }, executor));
        }

        Futures.allOf(futures).join();
    }

    @Test
    public void testSimpleScenarioWithBlockIndexEntries() throws Exception {
        String testSegmentName = "foo";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(3).build());
        testSimpleScenario(testSegmentName, policy, testContext);
    }

    @Test
    public void testReadWriteWithBlockIndexEntries() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder().indexBlockSize(3).build());
        testReadWriteWithFixedSize(testSegmentName, testContext);
    }

    private void testReadWriteWithFixedSize(String testSegmentName, TestContext testContext) throws Exception {
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.
        val total = 28;
        val numberOfWrites = 7;
        val bytesToWrite = populate(total);
        // Write some data.
        int writeAt = 0;
        val h = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        Assert.assertEquals(h.getSegmentName(), testSegmentName);
        Assert.assertFalse(h.isReadOnly());
        for (int i = 1; i <= numberOfWrites; i++) {
            testContext.chunkedSegmentStorage.write(h, writeAt, new ByteArrayInputStream(bytesToWrite, writeAt, i), i, null).join();
            writeAt += i;
        }
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, 2, 14);
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, 0, 28);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, testSegmentName,
                0, 28, true);
        checkDataRead(testSegmentName, testContext, 0, 28, bytesToWrite);
        checkDataReadPermutations(testContext, testSegmentName, total, numberOfWrites, bytesToWrite);
    }

    private void checkDataReadPermutations(TestContext testContext, String segmentName, int total, int numberOfWrites,
                                           byte[] expected) throws InterruptedException, ExecutionException {
        val h = testContext.chunkedSegmentStorage.openRead(segmentName).join();
        // Read all bytes at once.
        byte[] output = new byte[total];
        int bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, 0, total, null).get();
        Assert.assertEquals(total, bytesRead);
        checkData(expected, output, 0, 0, total);

        // Read bytes at varying lengths but same starting offset.
        for (int i = 0; i < total; i++) {
            output = new byte[total];
            bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, 0, i, null).get();
            Assert.assertEquals(i, bytesRead);
            checkData(expected, output, 0, 0, i);
        }

        // Read bytes at varying lengths and different offsets.
        for (int i = 0; i < total; i++) {
            output = new byte[total];
            bytesRead = testContext.chunkedSegmentStorage.read(h, total - i - 1, output, 0, i, null).get();
            Assert.assertEquals(i, bytesRead);
            checkData(expected, output, total - i - 1, 0, i);
        }

        // Read bytes at varying sizes.
        int totalBytesRead = 0;
        for (int i = numberOfWrites; i > 0; i--) {
            output = new byte[total];
            val bufferOffset = totalBytesRead;
            bytesRead = testContext.chunkedSegmentStorage.read(h, 0, output, bufferOffset, i, null).get();
            totalBytesRead += bytesRead;
            Assert.assertEquals(i, bytesRead);
            checkData(expected, output, 0, bufferOffset, i);
        }
        Assert.assertEquals(total, totalBytesRead);
    }

    @Test
    public void testReadHugeChunks() throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext();
        // Setup a segment with 5 chunks with given lengths.
        val segment = testContext.insertMetadata(testSegmentName, 10L * Integer.MAX_VALUE, 1,
                new long[]{
                        Integer.MAX_VALUE + 1L,
                        Integer.MAX_VALUE + 2L,
                        Integer.MAX_VALUE + 3L,
                        Integer.MAX_VALUE + 4L,
                        Integer.MAX_VALUE + 5L});

        val h = testContext.chunkedSegmentStorage.openRead(testSegmentName).get();

        byte[] output = new byte[10];
        // Read bytes
        for (long i = 0; i < 5; i++) {
            val bytesRead = testContext.chunkedSegmentStorage.read(h, i * Integer.MAX_VALUE, output, 0, 10, null).get();
            Assert.assertEquals(10, bytesRead.intValue());
        }
    }

    @Test
    public void testConcatHugeChunks() throws Exception {
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .minSizeLimitForConcat(Integer.MAX_VALUE)
                .maxSizeLimitForConcat(100L * Integer.MAX_VALUE)
                .indexBlockSize(Integer.MAX_VALUE / 2)
                .build());
        testBaseConcat(testContext, 10L * Integer.MAX_VALUE,
                new long[]{Integer.MAX_VALUE + 1L},
                new long[]{Integer.MAX_VALUE + 1L, Integer.MAX_VALUE + 1L},
                new long[]{3L * Integer.MAX_VALUE + 3L});
    }

    // This is a time-consuming test and should be eventually made optional
    @Test
    public void testRelocateHugeChunks() throws Exception {
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .relocateOnTruncateEnabled(true)
                .maxBufferSizeForChunkDataTransfer(128 * 1024 * 128)
                .maxSizeForTruncateRelocationInbytes(10L * Integer.MAX_VALUE)
                .indexBlockSize(Integer.MAX_VALUE / 2)
                .build();
        @Cleanup
        TestContext testContext = getTestContext(config);
        String testSegmentName = "testSegmentName";

        testContext.insertMetadata(testSegmentName, 10L * Integer.MAX_VALUE, 1, new long[]{
                10L * Integer.MAX_VALUE,
                10L * Integer.MAX_VALUE,
                10L * Integer.MAX_VALUE
        });

        // Truncate inside the 1st chunk
        testTruncate(testContext,
                testSegmentName,
                10L * Integer.MAX_VALUE,
                9L * Integer.MAX_VALUE - 1,
                3,
                30L * Integer.MAX_VALUE,
                1L * Integer.MAX_VALUE + 1);

        // Truncate inside the 3rd chunk
        testTruncate(testContext,
                testSegmentName,
                10L * Integer.MAX_VALUE,
                29L * Integer.MAX_VALUE - 1,
                1,
                30L * Integer.MAX_VALUE,
                1L * Integer.MAX_VALUE + 1);
    }

    @Test
    public void testWritesWithFlakyMetadataStore() throws Exception {
        val primes = new int[] { 2, 3, 5, 7, 11};
        for (int i = 0; i < primes.length; i++) {
            for (int j = 0; j < primes.length; j++) {
                testWritesWithFlakyMetadataStore(primes[i], primes[j]);
            }
        }
    }

    public void testWritesWithFlakyMetadataStore(int failFrequency, int length) throws Exception {
        String testSegmentName = "foo";
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .build());

        val invocationCount = new AtomicInteger(0);
        val testMetadataStore = (InMemoryMetadataStore) testContext.metadataStore;
        testMetadataStore.setMaxEntriesInTxnBuffer(1);
        testMetadataStore.setWriteCallback(dummy -> {
            if (invocationCount.incrementAndGet() % failFrequency == 0) {
                return CompletableFuture.failedFuture(new IntentionalException("Intentional"));
            }
            return CompletableFuture.completedFuture(null);
        });

        val h = testContext.chunkedSegmentStorage.create(testSegmentName, null).get();

        byte[] data = populate(100);

        int currentOffset = 0;

        SegmentMetadata expectedSegmentMetadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        ChunkMetadata expectedChunkMetadata = TestUtils.getChunkMetadata(testContext.metadataStore, expectedSegmentMetadata.getLastChunk());

        testMetadataStore.evictAllEligibleEntriesFromBuffer();
        testMetadataStore.evictFromCache();

        while (currentOffset < data.length) {
            try {
                int toWrite = Math.min(length, data.length - currentOffset);
                expectedSegmentMetadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
                expectedChunkMetadata = TestUtils.getChunkMetadata(testContext.metadataStore, expectedSegmentMetadata.getLastChunk());
                testContext.chunkedSegmentStorage.write(h, currentOffset, new ByteArrayInputStream(data, currentOffset, toWrite), toWrite, null).get();
                currentOffset += toWrite;
            } catch (Exception e) {
                if (!(Exceptions.unwrap(e) instanceof IntentionalException)) {
                    throw e;
                }
                val actual = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
                val actualChunkMetadata = TestUtils.getChunkMetadata(testContext.metadataStore, expectedSegmentMetadata.getLastChunk());
                Assert.assertEquals(expectedSegmentMetadata, actual);
                Assert.assertEquals(expectedChunkMetadata, actualChunkMetadata);
            } finally {
                val info = testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).get();
                Assert.assertEquals(info.getLength(), currentOffset);
            }
        }

        testMetadataStore.setWriteCallback(null);

        checkDataRead(testSegmentName, testContext, 0, data.length, data);
    }

    @Test
    public void testFullStorage() throws Exception {
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .maxSafeStorageSize(1000)
                .build());

        Assert.assertFalse(testContext.chunkedSegmentStorage.isSafeMode());
        val h = testContext.chunkedSegmentStorage.create("test", TIMEOUT).get();
        testContext.chunkedSegmentStorage.write(h, 0, new ByteArrayInputStream(new byte[10]), 10, TIMEOUT).get();

        testContext.chunkedSegmentStorage.create("segment", TIMEOUT).get();
        testContext.chunkedSegmentStorage.create("_system/something", TIMEOUT).get();

        // Simulate storage full.
        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setUsedSizeToReturn(1000);
        testContext.chunkedSegmentStorage.updateStorageStats().join();

        Assert.assertTrue(testContext.chunkedSegmentStorage.isSafeMode());

        // These operations should pass
        Assert.assertEquals(10, testContext.chunkedSegmentStorage.getStreamSegmentInfo("test", TIMEOUT).get().getLength());
        checkDataRead("test", testContext, 0, 10);

        val h3 = testContext.chunkedSegmentStorage.create("A", TIMEOUT).get();
        testContext.chunkedSegmentStorage.seal(h3, TIMEOUT).get();
        testContext.chunkedSegmentStorage.delete(h3, TIMEOUT).get();
        testContext.chunkedSegmentStorage.write(SegmentStorageHandle.writeHandle("_system/something"),
                0, new ByteArrayInputStream(new byte[10]), 10, TIMEOUT).get();

        // These operations should fail
        AssertExtensions.assertFutureThrows("write() should throw an exception",
                testContext.chunkedSegmentStorage.write(h, 10, new ByteArrayInputStream(new byte[10]), 10, TIMEOUT),
                ex -> ex instanceof StorageFullException);

        AssertExtensions.assertFutureThrows("conact() should throw an exception",
                testContext.chunkedSegmentStorage.concat(h, 10, "A", TIMEOUT),
                ex -> ex instanceof StorageFullException);

        // Remove storage full
        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setUsedSizeToReturn(50);
        testContext.chunkedSegmentStorage.updateStorageStats().join();
        Assert.assertFalse(testContext.chunkedSegmentStorage.isSafeMode());

        testContext.chunkedSegmentStorage.write(SegmentStorageHandle.writeHandle("test"), 10,
                new ByteArrayInputStream(new byte[10]), 10, TIMEOUT).get();
        testContext.chunkedSegmentStorage.write(SegmentStorageHandle.writeHandle("segment"), 0,
                new ByteArrayInputStream(new byte[10]), 10, TIMEOUT).get();
        testContext.chunkedSegmentStorage.write(SegmentStorageHandle.writeHandle("_system/something"),
                10, new ByteArrayInputStream(new byte[10]), 10, TIMEOUT).get();
        Assert.assertEquals(20, testContext.chunkedSegmentStorage.getStreamSegmentInfo("test", TIMEOUT).get().getLength());
        Assert.assertEquals(10, testContext.chunkedSegmentStorage.getStreamSegmentInfo("segment", TIMEOUT).get().getLength());
        Assert.assertEquals(20, testContext.chunkedSegmentStorage.getStreamSegmentInfo("_system/something", TIMEOUT).get().getLength());

        checkDataRead("test", testContext, 0, 20);
        checkDataRead("segment", testContext, 0, 10);

        val h4 = testContext.chunkedSegmentStorage.create("B", TIMEOUT).get();
        testContext.chunkedSegmentStorage.delete(h4, TIMEOUT).get();

        testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle("segment"), TIMEOUT).get();
        testContext.chunkedSegmentStorage.concat(SegmentStorageHandle.writeHandle("test"), 20, "segment", TIMEOUT).get();
    }

    @Test
    public void testGetReference() throws Exception {
        @Cleanup
        TestContext testContext = getTestContext();
        // Null when not ChunkedSegmentStorage
        Assert.assertNull(ChunkedSegmentStorage.getReference(mock(Storage.class)));

        // Null when inner is not ChunkedSegmentStorage
        val wrapper1 = mock(StorageWrapper.class);
        doReturn(mock(Storage.class)).when(wrapper1).getInner();
        Assert.assertNull(ChunkedSegmentStorage.getReference(wrapper1));

        // Matches when ChunkedSegmentStorage
        Assert.assertEquals(testContext.chunkedSegmentStorage, ChunkedSegmentStorage.getReference(testContext.chunkedSegmentStorage));

        // Matches when inner is ChunkedSegmentStorage
        val wrapper2 = mock(StorageWrapper.class);
        doReturn(testContext.chunkedSegmentStorage).when(wrapper2).getInner();
        Assert.assertEquals(testContext.chunkedSegmentStorage, ChunkedSegmentStorage.getReference(wrapper2));
    }


    /**
     * Test simple scenario.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testSimpleScenarioWithSelfCheck() throws Exception {
        String testSegmentName = "foo";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 bytes.
        @Cleanup
        TestContext testContext = getTestContext(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .selfCheckForMetadataEnabled(true)
                .selfCheckForDataEnabled(true)
                .build());
        testSimpleScenario(testSegmentName, policy, testContext);
    }

    private void checkDataRead(String testSegmentName, TestContext testContext, long offset, long length) throws InterruptedException, java.util.concurrent.ExecutionException {
        checkDataRead(testSegmentName, testContext, offset, length, null);
    }

    private void checkDataRead(String testSegmentName, TestContext testContext,
                               long offset,
                               long length,
                               byte[] expected) throws InterruptedException, java.util.concurrent.ExecutionException {
        val hRead = testContext.chunkedSegmentStorage.openRead(testSegmentName).get();

        // Read all bytes at once.
        long size = Math.toIntExact(length - offset);
        byte[] output = new byte[Math.toIntExact(length - offset)];
        int bufferOffset = 0;
        int bytesRead = 0;
        while (bytesRead < size) {
            bytesRead += testContext.chunkedSegmentStorage.read(hRead, offset, output, bufferOffset, Math.toIntExact(size), null).get();
        }
        Assert.assertEquals(size, bytesRead);
        if (null != expected) {
            checkData(expected, output);
        }
    }

    private CompletableFuture<Void> checkDataReadAsync(String testSegmentName, TestContext testContext, long offset, long length, Executor executor) {
        return checkDataReadAsync(testSegmentName, testContext, offset, length, null, executor);
    }

    private CompletableFuture<Void> checkDataReadAsync(String testSegmentName,
                                       TestContext testContext,
                                       long offset,
                                       long length,
                                       byte[] expected,
                                       Executor executor) {
        return testContext.chunkedSegmentStorage.openRead(testSegmentName)
                .thenComposeAsync(hRead -> {
                    // Read all bytes at once.
                    long size = Math.toIntExact(length - offset);
                    byte[] output = new byte[Math.toIntExact(length - offset)];
                    int bufferOffset = 0;
                    AtomicInteger bytesReadRef = new AtomicInteger();
                    return Futures.loop(
                            () -> bytesReadRef.get() < size,
                            () -> {
                                return testContext.chunkedSegmentStorage.read(hRead, offset, output, bufferOffset, Math.toIntExact(size), null)
                                        .thenApplyAsync(bytesRead -> {
                                            bytesReadRef.addAndGet(bytesRead);
                                            return null;
                                        }, executor);
                            }, executor)
                            .thenRunAsync(() -> {
                                Assert.assertEquals(size, bytesReadRef.get());
                                if (null != expected) {
                                    checkData(expected, output);
                                }
                            }, executor);
                }, executor);
    }

    protected void populate(byte[] data) {
        // Do nothing. The NoOpChunkStorage used in this test will ignore data written/read.
    }

    protected byte[] populate(int size) {
        byte[] bytes = new byte[size];
        populate(bytes);
        return bytes;
    }

    protected void checkData(byte[] expected, byte[] output) {
        // Do nothing. The NoOpChunkStorage used in this test will ignore data written/read.
    }

    protected void checkData(byte[] expected, byte[] output, int expectedStartIndex, int outputStartIndex, int length) {
        // Do nothing. The NoOpChunkStorage used in this test will ignore data written/read.
    }

    private SegmentHandle populateSegment(TestContext testContext, String targetSegmentName, long maxChunkLength, int numberOfchunks) throws Exception {
        SegmentRollingPolicy policy = new SegmentRollingPolicy(maxChunkLength); // Force rollover after each byte.
        // Create segment
        val h = testContext.chunkedSegmentStorage.create(targetSegmentName, policy, null).get();

        // Write some data.
        long dataSize = numberOfchunks * maxChunkLength;
        testContext.chunkedSegmentStorage.write(h, 0, new ByteArrayInputStream(new byte[Math.toIntExact(dataSize)]), Math.toIntExact(dataSize), null).join();

        TestUtils.checkSegmentLayout(testContext.metadataStore, targetSegmentName, maxChunkLength, numberOfchunks);
        TestUtils.checkSegmentBounds(testContext.metadataStore, targetSegmentName, 0, dataSize);
        TestUtils.checkReadIndexEntries(testContext.chunkedSegmentStorage, testContext.metadataStore, targetSegmentName, 0, dataSize, true);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, targetSegmentName);
        return h;
    }

    /**
     * Test context.
     */
    public static class TestContext implements AutoCloseable {
        @Getter
        protected ChunkedSegmentStorageConfig config;

        @Getter
        protected ChunkStorage chunkStorage;

        @Getter
        protected ChunkMetadataStore metadataStore;

        @Getter
        protected ChunkedSegmentStorage chunkedSegmentStorage;

        @Getter
        protected ScheduledExecutorService executor;

        @Getter
        protected AbstractTaskQueueManager<GarbageCollector.TaskInfo> taskQueue;

        protected TestContext() {
        }

        public TestContext(ScheduledExecutorService executor) throws Exception {
            this(executor, ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        }

        public TestContext(ScheduledExecutorService executor, ChunkedSegmentStorageConfig config) throws Exception {
            this.executor = executor;
            this.config = config;
            chunkStorage = createChunkStorage();
            metadataStore = createMetadataStore();
            taskQueue = createTaskQueue();
            chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID, chunkStorage, metadataStore, this.executor, config);
            chunkedSegmentStorage.initialize(1);
            chunkedSegmentStorage.getGarbageCollector().initialize(taskQueue).join();
        }

        /**
         * Creates a fork of current context with same chunk storage but forked metadata store.
         * This simulates multiple segment store instances writing to same storage but different states. (Eg After failover)
         */
        public TestContext fork(long epoch) throws Exception {
            val forkedContext = createNewInstance();
            forkedContext.executor = Preconditions.checkNotNull(this.executor);
            forkedContext.chunkStorage = Preconditions.checkNotNull(this.chunkStorage);
            forkedContext.config = Preconditions.checkNotNull(this.config);
            // This will create a copy of metadata store
            forkedContext.metadataStore = getForkedMetadataStore();

            // Use the same same chunk storage, but different metadata store to simulate multiple zombie instances
            // writing to the same underlying storage.
            forkedContext.chunkedSegmentStorage = new ChunkedSegmentStorage(CONTAINER_ID,
                    this.chunkStorage,
                    forkedContext.metadataStore,
                    this.executor,
                    this.config);
            forkedContext.chunkedSegmentStorage.initialize(epoch);
            forkedContext.taskQueue = createTaskQueue();
            forkedContext.chunkedSegmentStorage.getGarbageCollector().initialize(taskQueue).join();
            return forkedContext;
        }

        /**
         * Expected to be overrriden by derived classes.
         */
        protected TestContext createNewInstance() {
            return new TestContext();
        }

        /**
         * Creates a clone of metadata store.
         * @return
         */
        public ChunkMetadataStore getForkedMetadataStore() {
            return InMemoryMetadataStore.clone((InMemoryMetadataStore) this.metadataStore);
        }

        /**
         * Gets {@link ChunkMetadataStore} to use for the tests.
         */
        public ChunkMetadataStore createMetadataStore() throws Exception {
            return new InMemoryMetadataStore(config, executor);
        }

        /**
         * Gets {@link ChunkStorage} to use for the tests.
         */
        public ChunkStorage createChunkStorage() throws Exception {
            return new NoOpChunkStorage(executor);
        }

        public AbstractTaskQueueManager<GarbageCollector.TaskInfo> createTaskQueue() throws Exception {
            return new InMemoryTaskQueueManager();
        }

        /**
         * Creates and inserts metadata for a test segment.
         */
        public SegmentMetadata insertMetadata(String testSegmentName, int maxRollingLength, int ownerEpoch) throws Exception {
            return TestUtils.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch, metadataStore);
        }

        public SegmentMetadata insertMetadata(String testSegmentName, long maxRollingLength, int ownerEpoch, long[] chunkLengths) throws Exception {
            return insertMetadata(testSegmentName, maxRollingLength, ownerEpoch, chunkLengths, true, true);
        }

        /**
         * Creates and inserts metadata for a test segment.
         */
        public SegmentMetadata insertMetadata(String testSegmentName, long maxRollingLength, int ownerEpoch, long[] chunkLengths,
                                              boolean addIndex, boolean addIndexMetadata) throws Exception {
            return TestUtils.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch,
                    chunkLengths, chunkLengths,
                    addIndex, addIndexMetadata,
                    metadataStore,
                    chunkedSegmentStorage,
                    StatusFlags.ACTIVE | StatusFlags.ATOMIC_WRITES );
        }

        /*
        // Useful methods - unused. Commented to avoid chekstyle violation.
        private void insertMetadata(StorageMetadata storageMetadata) throws Exception {
            try (val txn = metadataStore.beginTransaction()) {
                metadataStore.create(txn, storageMetadata);
                txn.commit();
            }
        }

        private void updateMetadata(StorageMetadata storageMetadata) throws Exception {
            try (val txn = metadataStore.beginTransaction()) {
                metadataStore.create(txn, storageMetadata);
                txn.commit();
            }
        }
        */

        @Override
        public void close() throws Exception {
            CleanupHelper.close("chunkedSegmentStorage", chunkedSegmentStorage);
            CleanupHelper.close("chunkStorage", chunkStorage);
            CleanupHelper.close("metadataStore", metadataStore);

            this.config = null;
            this.chunkedSegmentStorage = null;
            this.chunkStorage = null;
            this.metadataStore = null;
        }
    }


    /**
     * Runs all {@link ChunkedSegmentStorageTests} with all self checks on. It uses {@InMemoryChunkStorage}.
     */
    public static class ChunkedSegmentStorageTestsWithSelfCheck extends ChunkedSegmentStorageTests {

        @Override
        public ChunkStorage createChunkStorage() {
            return new InMemoryChunkStorage(executorService());
        }

        @Override
        public TestContext getTestContext() throws Exception {
            return new SelfCheckTestContext(executorService());
        }

        @Override
        public TestContext getTestContext(ChunkedSegmentStorageConfig config) throws Exception {
            return new SelfCheckTestContext(executorService(), config);
        }

        @Override
        protected void populate(byte[] data) {
            rnd.nextBytes(data);
        }

        @Override
        protected void checkData(byte[] expected, byte[] output) {
            Assert.assertArrayEquals(expected, output);
        }

        @Override
        protected void checkData(byte[] expected, byte[] output, int expectedStartIndex, int outputStartIndex, int length) {
            AssertExtensions.assertArrayEquals("Data check failed", expected, expectedStartIndex, output, outputStartIndex, length);
        }

        @Override
        public void testReadHugeChunks() {
            // Do not execute this test because it creates very large chunks (few multiples of Integer.MAX_VALUE).
            // Allocating such huge byte arrays is not desirable with InMemoryChunkStorage.
        }

        @Override
        public void testConcatHugeChunks(){
            // Do not execute this test because it creates very large chunks (few multiples of Integer.MAX_VALUE).
            // Allocating such huge byte arrays is not desirable with InMemoryChunkStorage.
        }

        @Override
        public void testRelocateHugeChunks(){
            // Do not execute this test because it creates very large chunks (few multiples of Integer.MAX_VALUE).
            // Allocating such huge byte arrays is not desirable with InMemoryChunkStorage.
        }

        public class SelfCheckTestContext extends ChunkedSegmentStorageTests.TestContext {
            SelfCheckTestContext(ScheduledExecutorService executorService) throws Exception {
                super(executorService, ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .selfCheckEnabled(true)
                        .selfCheckForMetadataEnabled(true)
                        .selfCheckForDataEnabled(true)
                        .build());
            }

            SelfCheckTestContext(ScheduledExecutorService executorService, ChunkedSegmentStorageConfig config) throws Exception {
                super(executorService, config.toBuilder()
                        .selfCheckEnabled(true)
                        .selfCheckForMetadataEnabled(true)
                        .selfCheckForDataEnabled(true)
                        .build());
            }

            @Override
            public ChunkStorage createChunkStorage() {
                return new InMemoryChunkStorage(executorService());
            }
        }
    }
}

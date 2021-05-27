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
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.ReadIndexBlockMetadata;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.metadata.StatusFlags;
import io.pravega.segmentstore.storage.metadata.StorageMetadataException;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemoryTaskQueue;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link GarbageCollector}.
 */
@Slf4j
public class GarbageCollectorTests extends ThreadPooledTestSuite {
    public static final int CONTAINER_ID = 42;
    public static final long TXN_ID = 123;
    protected static final Duration TIMEOUT = Duration.ofSeconds(3000);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Before
    public void before() throws Exception {
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    protected int getThreadPoolSize() {
        return 5;
    }

    protected ChunkMetadataStore getMetadataStore() throws Exception {
        return new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService());
    }

    protected ChunkStorage getChunkStorage() throws Exception {
        return new InMemoryChunkStorage(executorService());
    }

    /**
     * Test Initialization
     */
    @Test
    public void testInitializationInvalidArgs() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService());
        garbageCollector.initialize(new InMemoryTaskQueue());

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
    }

    /**
     * Test Initialization
     */
    @Test
    public void testInitialization() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService(),
                System::currentTimeMillis,
                d -> CompletableFuture.completedFuture(null));
        garbageCollector.initialize(new InMemoryTaskQueue());

        AssertExtensions.assertThrows("Should not allow null chunkStorage",
                () -> {
                        @Cleanup val x = new GarbageCollector(containerId,
                        null,
                        metadataStore,
                        ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                        executorService(),
                        System::currentTimeMillis,
                        d -> CompletableFuture.completedFuture(null));
                },
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null metadataStore",
                () -> {
                    @Cleanup val x = new GarbageCollector(containerId,
                        chunkStorage,
                        null,
                        ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                        executorService(),
                        System::currentTimeMillis,
                        d -> CompletableFuture.completedFuture(null));
                },
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null config",
                () -> {
                    @Cleanup val x = new GarbageCollector(containerId,
                            chunkStorage,
                            metadataStore,
                            null,
                            executorService(),
                            System::currentTimeMillis,
                            d -> CompletableFuture.completedFuture(null));
                },
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null executorService",
                () -> {
                    @Cleanup val x = new GarbageCollector(containerId,
                            chunkStorage,
                            metadataStore,
                            ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                            null,
                            System::currentTimeMillis,
                            d -> CompletableFuture.completedFuture(null));
                },
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows("Should not allow null currentTimeSupplier",
                () -> {
                    @Cleanup val x = new GarbageCollector(containerId,
                            chunkStorage,
                            metadataStore,
                            ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                            executorService(),
                            null,
                            d -> CompletableFuture.completedFuture(null));
                },
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows("Should not allow null delaySupplier",
                () -> {
                    @Cleanup val x = new GarbageCollector(containerId,
                        chunkStorage,
                        metadataStore,
                        ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                        executorService(),
                        System::currentTimeMillis,
                        null);
                },
                ex -> ex instanceof NullPointerException);
    }

    @Test
    public void testInitializationWithNullQueue() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executorService(),
                System::currentTimeMillis,
                d -> CompletableFuture.completedFuture(null));

        Assert.assertNull(garbageCollector.getTaskQueue());

        // Should not throw an exception.
        garbageCollector.addChunkToGarbage(1, "chunk3", 10, 0);
        garbageCollector.addChunksToGarbage(1, Collections.singleton("chunk1"));
        garbageCollector.trackNewChunk(1, "chunk2");
        garbageCollector.addSegmentToGarbage(1, "segment");
        garbageCollector.addSegmentToGarbage(GarbageCollector.TaskInfo.builder()
                .scheduledDeleteTime(11)
                .name("segment2")
                .attempts(0).build());
    }

    /**
     * Test for chunk that is marked active but added as garbage.
     */
    @Test
    public void testActiveChunk() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        insertChunk(chunkStorage, "activeChunk", dataSize);
        insertChunkMetadata(metadataStore, "activeChunk", dataSize, 1);

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .build(),
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addChunksToGarbage(TXN_ID, Collections.singleton("activeChunk")).join();

        // Validate state before
        Assert.assertEquals(1, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("activeChunk", testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).peek().getName());

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
        Assert.assertTrue(chunkStorage.exists("activeChunk").get());
        Assert.assertNotNull(getChunkMetadata(metadataStore, "activeChunk"));
    }

    /**
     * Test for chunk that is marked inactive and added as garbage.
     */
    @Test
    public void testDeletedChunk() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        insertChunk(chunkStorage, "deletedChunk", dataSize);
        insertChunkMetadata(metadataStore, "deletedChunk", dataSize, 0);

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .build(),
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addChunksToGarbage(TXN_ID, Collections.singleton("deletedChunk")).join();

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("deletedChunk", testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).peek().getName());

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        Assert.assertFalse(chunkStorage.exists("deletedChunk").get());
        Assert.assertNull(getChunkMetadata(metadataStore, "deletedChunk"));
    }

    /**
     * Test for chunk that is marked inactive, added as garbage but missing from storage.
     */
    @Test
    public void testDeletedChunkMissingFromStorage() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        insertChunkMetadata(metadataStore, "deletedChunk", dataSize, 0);

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();
        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .build(),
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addChunksToGarbage(TXN_ID, Collections.singleton("deletedChunk")).join();

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("deletedChunk", testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).peek().getName());

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        Assert.assertFalse(chunkStorage.exists("deletedChunk").get());
        Assert.assertNull(getChunkMetadata(metadataStore, "deletedChunk"));
    }

    /**
     * Test for chunk that does not exist in metadata but added as garbage.
     */
    @Test
    public void testNonExistentChunk() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .build(),
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addChunksToGarbage(TXN_ID, Collections.singleton("nonExistingChunk")).join();

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("nonExistingChunk", testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).peek().getName());

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
    }

    /**
     * Test for chunk that is marked active but added as garbage.
     */
    @Test
    public void testNewChunkOnSuccessful() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .build(),
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        insertChunk(chunkStorage, "newChunk", dataSize);
        garbageCollector.trackNewChunk(TXN_ID, "newChunk").join();
        insertChunkMetadata(metadataStore, "newChunk", dataSize, 1);

        // Validate state before
        Assert.assertEquals(1, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("newChunk", testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).peek().getName());

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
        Assert.assertTrue(chunkStorage.exists("newChunk").get());
        Assert.assertNotNull(getChunkMetadata(metadataStore, "newChunk"));
    }

    /**
     * Test for chunk that does not exist in metadata but added as garbage.
     */
    @Test
    public void testNewChunkOnFailure() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();
        int dataSize = 1;

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .build(),
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        insertChunk(chunkStorage, "newChunk", dataSize);
        garbageCollector.trackNewChunk(TXN_ID, "newChunk").join();

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("newChunk", testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).peek().getName());
        Assert.assertNull(getChunkMetadata(metadataStore, "newChunk"));
        Assert.assertTrue(chunkStorage.exists("newChunk").get());

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
        Assert.assertFalse(chunkStorage.exists("newChunk").get());
    }

    /**
     * Test for a mix bag of chunks.
     */
    @Test
    public void testMixedChunk() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        insertChunk(chunkStorage, "deletedChunk", dataSize);
        insertChunkMetadata(metadataStore, "deletedChunk", dataSize, 0);

        insertChunk(chunkStorage, "activeChunk", dataSize);
        insertChunkMetadata(metadataStore, "activeChunk", dataSize, 1);

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .build(),
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addChunksToGarbage(TXN_ID, Arrays.asList("activeChunk", "nonExistingChunk", "deletedChunk")).join();

        // Validate state before
        assertQueueEquals(garbageCollector.getTaskQueueName(), testTaskQueue, new String[]{"activeChunk", "nonExistingChunk", "deletedChunk"});

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 3);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(3, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
        Assert.assertFalse(chunkStorage.exists("deletedChunk").get());
        Assert.assertTrue(chunkStorage.exists("activeChunk").get());
    }

    /**
     * Test for IO exception.
     */
    @Test
    public void testIOException() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        insertChunk(chunkStorage, "deletedChunk", dataSize);
        insertChunkMetadata(metadataStore, "deletedChunk", dataSize, 0);

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        chunkStorage.setReadOnly(chunkStorage.openWrite("deletedChunk").get(), true);

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .build(),
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addChunksToGarbage(TXN_ID, Arrays.asList("deletedChunk")).join();

        // Validate state before
        assertQueueEquals(garbageCollector.getTaskQueueName(), testTaskQueue, new String[]{"deletedChunk"});

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        assertQueueEquals(garbageCollector.getTaskQueueName(), testTaskQueue, new String[]{"deletedChunk"});
    }

    /**
     * Test for ChunkNotFound exception.
     */
    @Test
    public void testChunkNotFound() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        insertChunkMetadata(metadataStore, "missingChunk", dataSize, 0);

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .build(),
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addChunksToGarbage(TXN_ID, Arrays.asList("missingChunk")).join();

        // Validate state before
        assertQueueEquals(garbageCollector.getTaskQueueName(), testTaskQueue, new String[]{"missingChunk"});

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

    }

    /**
     * Test for metadata exception.
     */
    @Test
    public void testMetadataException() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        insertChunk(chunkStorage, "deletedChunk", dataSize);
        insertChunkMetadata(metadataStore, "deletedChunk", dataSize, 0);

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        metadataStore.markFenced();

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .build(),
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addChunksToGarbage(TXN_ID, Arrays.asList("deletedChunk")).join();

        // Validate state before
        assertQueueEquals(garbageCollector.getTaskQueueName(), testTaskQueue, new String[]{"deletedChunk"});

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        assertQueueEquals(garbageCollector.getTaskQueueName(), testTaskQueue, new String[]{"deletedChunk"});
    }

    /**
     * Test for segment that is marked inactive and added as garbage.
     */
    @Test
    public void testDeletedSegment() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .garbageCollectionDelay(Duration.ofMillis(1))
                .garbageCollectionSleep(Duration.ofMillis(1))
                .build();
        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                config,
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        insertSegment(metadataStore, chunkStorage, config, "testSegment", 10, 1,
                new long[] {1, 2, 3, 4},  false, 0);

        // Add some garbage
        garbageCollector.addSegmentToGarbage(TXN_ID, "testSegment").join();

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals(1, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals("testSegment", testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).peek().getName());

        garbageCollector.processBatch(testTaskQueue.drain(garbageCollector.getTaskQueueName(), 10)).join();

        // Validate state after
        Assert.assertEquals(4, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(4, garbageCollector.getQueueSize().get());

        Assert.assertNull(getSegmentMetadata(metadataStore, "testSegment"));
        garbageCollector.processBatch(testTaskQueue.drain(garbageCollector.getTaskQueueName(), 10)).join();
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
    }

    /**
     * Test for segment that is marked active and added as garbage.
     */
    @Test
    public void testActiveSegment() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .garbageCollectionDelay(Duration.ofMillis(1))
                .garbageCollectionSleep(Duration.ofMillis(1))
                .build();
        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                config,
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        insertSegment(metadataStore, chunkStorage, config, "testSegment", 10, 1,
                new long[] {1, 2, 3, 4},  false, 1);

        // Add some garbage
        garbageCollector.addSegmentToGarbage(TXN_ID, "testSegment").join();

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("testSegment", testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).peek().getName());

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        Assert.assertNotNull(getSegmentMetadata(metadataStore, "testSegment"));
    }

    /**
     * Test for segment that is marked active and added as garbage.
     */
    @Test
    public void testMetadataExceptionWithSegment() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = spy(getMetadataStore());
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .garbageCollectionDelay(Duration.ofMillis(1))
                .garbageCollectionSleep(Duration.ofMillis(1))
                .build();
        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                config,
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        insertSegment(metadataStore, chunkStorage, config, "testSegment", 10, 1,
                new long[] {1, 2, 3, 4},  false, 0);

        // Add some garbage
        garbageCollector.addSegmentToGarbage(TXN_ID, "testSegment").join();

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("testSegment", testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).peek().getName());

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        // Step 2: Inject fault.
        CompletableFuture f = new CompletableFuture();
        f.completeExceptionally(new StorageMetadataException("Test Exception"));
        doReturn(f).when(metadataStore).commit(any());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(5, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(5, garbageCollector.getQueueSize().get());

        Assert.assertNotNull(getSegmentMetadata(metadataStore, "testSegment"));
    }

    /**
     * Test for segment that does not exist and added as garbage.
     */
    @Test
    public void testNonExistentSegment() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .garbageCollectionDelay(Duration.ofMillis(1))
                .garbageCollectionSleep(Duration.ofMillis(1))
                .build();
        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                config,
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        Assert.assertNull(getSegmentMetadata(metadataStore, "testSegment"));
        garbageCollector.addSegmentToGarbage(TXN_ID, "testSegment").join();

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("testSegment", testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).peek().getName());

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
        Assert.assertNull(getSegmentMetadata(metadataStore, "testSegment"));
    }

    /**
     * Test for Max Attempts.
     */
    @Test
    public void testMaxAttempts() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        insertChunk(chunkStorage, "deletedChunk", dataSize);
        insertChunkMetadata(metadataStore, "deletedChunk", dataSize, 0);

        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        chunkStorage.setReadOnly(chunkStorage.openWrite("deletedChunk").get(), true);

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .garbageCollectionMaxAttempts(3)
                        .build(),
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addChunksToGarbage(TXN_ID, Arrays.asList("deletedChunk")).join();

        // Validate state before
        assertQueueEquals(garbageCollector.getTaskQueueName(), testTaskQueue, new String[]{"deletedChunk"});

        for (int i = 0; i < 3; i++) {
            val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
            Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
            Assert.assertEquals(1, garbageCollector.getQueueSize().get());

            garbageCollector.processBatch(list).join();
            // Validate state after
            assertQueueEquals(garbageCollector.getTaskQueueName(), testTaskQueue, new String[]{"deletedChunk"});
        }

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());

        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
        Assert.assertEquals(1, testTaskQueue.getTaskQueueMap().get(garbageCollector.getFailedQueueName()).size());
        Assert.assertEquals("deletedChunk", testTaskQueue.getTaskQueueMap().get(garbageCollector.getFailedQueueName()).peek().getName());
    }

    /**
     * Test for Max Attempts.
     */
    @Test
    public void testMaxAttemptsWithSegment() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = spy(getMetadataStore());
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        Function<Duration, CompletableFuture<Void>> noDelay = d -> CompletableFuture.completedFuture(null);
        val testTaskQueue = new InMemoryTaskQueue();

        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .garbageCollectionDelay(Duration.ofMillis(1))
                .garbageCollectionSleep(Duration.ofMillis(1))
                .build();
        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                config,
                executorService(),
                System::currentTimeMillis,
                noDelay);

        // Now actually start run
        garbageCollector.initialize(testTaskQueue);

        Assert.assertNotNull(garbageCollector.getTaskQueue());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        insertSegment(metadataStore, chunkStorage, config, "testSegment", 10, 1,
                new long[] {},  false, 0);

        // Add some garbage
        garbageCollector.addSegmentToGarbage(TXN_ID, "testSegment").join();

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("testSegment", testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).peek().getName());

        for (int i = 0; i < 3; i++) {
            val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
            Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
            Assert.assertEquals(1, garbageCollector.getQueueSize().get());
            // Step 2: Inject fault.
            CompletableFuture f = new CompletableFuture();
            f.completeExceptionally(new StorageMetadataException("Test Exception"));
            doReturn(f).when(metadataStore).commit(any());

            garbageCollector.processBatch(list).join();

            // Validate state after
            Assert.assertEquals(1, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
            Assert.assertEquals(1, garbageCollector.getQueueSize().get());

            Assert.assertNotNull(getSegmentMetadata(metadataStore, "testSegment"));
        }

        val list = testTaskQueue.drain(garbageCollector.getTaskQueueName(), 1);
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        garbageCollector.processBatch(list).join();

        // Validate state after
        Assert.assertEquals(0, testTaskQueue.getTaskQueueMap().get(garbageCollector.getTaskQueueName()).size());
        Assert.assertEquals(1, testTaskQueue.getTaskQueueMap().get(garbageCollector.getFailedQueueName()).size());
        Assert.assertEquals("testSegment", testTaskQueue.getTaskQueueMap().get(garbageCollector.getFailedQueueName()).peek().getName());

        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        Assert.assertNotNull(getSegmentMetadata(metadataStore, "testSegment"));
    }

    @Test
    public void testSerialization() throws Exception {
        val serializer = new GarbageCollector.TaskInfo.Serializer();
        GarbageCollector.TaskInfo obj1 = GarbageCollector.TaskInfo.builder()
                .transactionId(1)
                .taskType(2)
                .attempts(3)
                .name("name")
                .build();
        val bytes = serializer.serialize(obj1);
        val obj2 = serializer.deserialize(bytes);

        Assert.assertEquals(1, obj2.getTransactionId());
        Assert.assertEquals(2, obj2.getTaskType());
        Assert.assertEquals(3, obj2.getAttempts());
        Assert.assertEquals("name", obj2.getName());
    }

    @Test
    public void testSerializationWithBaseClass() throws Exception {
        val serializer = new GarbageCollector.AbstractTaskInfo.AbstractTaskInfoSerializer();
        GarbageCollector.TaskInfo obj1 = GarbageCollector.TaskInfo.builder()
                .transactionId(1)
                .taskType(2)
                .attempts(3)
                .name("name")
                .build();
        val bytes = serializer.serialize(obj1);
        val obj2 = (GarbageCollector.TaskInfo) serializer.deserialize(bytes);

        Assert.assertEquals(1, obj2.getTransactionId());
        Assert.assertEquals(2, obj2.getTaskType());
        Assert.assertEquals(3, obj2.getAttempts());
        Assert.assertEquals("name", obj2.getName());
    }

    private void assertQueueEquals(String queueName, InMemoryTaskQueue garbageCollector, String[] expected) {
        HashSet<String> visited = new HashSet<>();
        val queue = garbageCollector.getTaskQueueMap().get(queueName).stream().peek(info -> visited.add(info.getName())).collect(Collectors.counting());
        Assert.assertEquals(expected.length, visited.size());
        for (String chunk : expected) {
            Assert.assertTrue(visited.contains(chunk));
        }
    }

    private void insertChunk(ChunkStorage chunkStorage, String chunkName, int dataSize) throws InterruptedException, java.util.concurrent.ExecutionException {
        val chunkHandle = chunkStorage.create(chunkName).get();
        chunkStorage.write(chunkHandle, 0, dataSize, new ByteArrayInputStream(new byte[dataSize])).get();
        Assert.assertTrue(chunkStorage.exists(chunkName).get());
    }

    private void insertChunkMetadata(ChunkMetadataStore metadataStore, String chunkName, int dataSize, int status) throws Exception {
        try (val txn = metadataStore.beginTransaction(false, chunkName)) {
            txn.create(ChunkMetadata.builder()
                    .name(chunkName)
                    .length(dataSize)
                    .status(status)
                    .build());
            txn.commit().get();
        }
        try (val txn = metadataStore.beginTransaction(true, chunkName)) {
            val metadata = (ChunkMetadata) txn.get(chunkName).get();
            Assert.assertNotNull(metadata);
            Assert.assertEquals(chunkName, metadata.getName());
            Assert.assertEquals(status, metadata.getStatus());
        }
    }

    public SegmentMetadata insertSegment(ChunkMetadataStore metadataStore,
                                          ChunkStorage chunkStorage,
                                          ChunkedSegmentStorageConfig config,
                                          String testSegmentName,
                                          long maxRollingLength,
                                          int ownerEpoch,
                                          long[] chunkLengths,
                                          boolean addIndexMetadata,
                                         int status) throws Exception {
        Preconditions.checkArgument(maxRollingLength > 0, "maxRollingLength");
        Preconditions.checkArgument(ownerEpoch > 0, "ownerEpoch");
        try (val txn = metadataStore.beginTransaction(false, new String[]{testSegmentName})) {
            String firstChunk = null;
            String lastChunk = null;
            TreeMap<Long, String> index = new TreeMap<>();
            // Add chunks.
            long length = 0;
            long startOfLast = 0;
            long startOffset = 0;
            int chunkCount = 0;
            for (int i = 0; i < chunkLengths.length; i++) {
                String chunkName = testSegmentName + "_chunk_" + Integer.toString(i);
                ChunkMetadata chunkMetadata = ChunkMetadata.builder()
                        .name(chunkName)
                        .length(chunkLengths[i])
                        .nextChunk(i == chunkLengths.length - 1 ? null : testSegmentName + "_chunk_" + Integer.toString(i + 1))
                        .build();
                chunkMetadata.setActive(true);
                index.put(startOffset, chunkName);
                startOffset += chunkLengths[i];
                length += chunkLengths[i];
                txn.create(chunkMetadata);

                insertChunk(chunkStorage, chunkName, Math.toIntExact(chunkLengths[i]));
                chunkCount++;
            }

            // Fix the first and last
            if (chunkLengths.length > 0) {
                firstChunk = testSegmentName + "_chunk_0";
                lastChunk = testSegmentName + "_chunk_" + Integer.toString(chunkLengths.length - 1);
                startOfLast = length - chunkLengths[chunkLengths.length - 1];
            }

            // Finally save
            SegmentMetadata segmentMetadata = SegmentMetadata.builder()
                    .maxRollinglength(maxRollingLength)
                    .name(testSegmentName)
                    .ownerEpoch(ownerEpoch)
                    .firstChunk(firstChunk)
                    .lastChunk(lastChunk)
                    .length(length)
                    .lastChunkStartOffset(startOfLast)
                    .build();
            segmentMetadata.setStatus(status);
            segmentMetadata.setChunkCount(chunkCount);
            segmentMetadata.checkInvariants();
            txn.create(segmentMetadata);

            if (addIndexMetadata) {
                for (long blockStartOffset = 0; blockStartOffset < segmentMetadata.getLength(); blockStartOffset += config.getIndexBlockSize()) {
                    val floor = index.floorEntry(blockStartOffset);
                    txn.create(ReadIndexBlockMetadata.builder()
                            .name(NameUtils.getSegmentReadIndexBlockName(segmentMetadata.getName(), blockStartOffset))
                            .startOffset(floor.getKey())
                            .chunkName(floor.getValue())
                            .status(StatusFlags.ACTIVE)
                            .build());
                }
            }

            txn.commit().join();
            return segmentMetadata;
        }
    }

    private ChunkMetadata getChunkMetadata(ChunkMetadataStore metadataStore, String chunkName) throws Exception {
        try (val txn = metadataStore.beginTransaction(true, chunkName)) {
            return (ChunkMetadata) txn.get(chunkName).get();
        }
    }

    private SegmentMetadata getSegmentMetadata(ChunkMetadataStore metadataStore, String chunkName) throws Exception {
        try (val txn = metadataStore.beginTransaction(true, chunkName)) {
            return (SegmentMetadata) txn.get(chunkName).get();
        }
    }

}

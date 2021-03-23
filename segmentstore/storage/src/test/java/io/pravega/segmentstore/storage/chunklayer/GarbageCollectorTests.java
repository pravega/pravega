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

import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
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

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Tests for {@link GarbageCollector}.
 */
@Slf4j
public class GarbageCollectorTests extends ThreadPooledTestSuite {
    public static final int CONTAINER_ID = 42;
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
        return new InMemoryMetadataStore(executorService());
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
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
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
                CompletableFuture::new);
        garbageCollector.initialize();

        AssertExtensions.assertThrows("Should not allow null chunkStorage",
                () -> {
                        @Cleanup val x = new GarbageCollector(containerId,
                        null,
                        metadataStore,
                        ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                        executorService(),
                        System::currentTimeMillis,
                        CompletableFuture::new);
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
                        CompletableFuture::new);
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
                            CompletableFuture::new);
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
                            CompletableFuture::new);
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
                            CompletableFuture::new);
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

        val manualDelay = new ManualDelay(2);

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
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addToGarbage(Collections.singleton("activeChunk"));

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getGarbageChunks().size());
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("activeChunk", garbageCollector.getGarbageChunks().peek().getName());

        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(0);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(1);

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

        val manualDelay = new ManualDelay(2);

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
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addToGarbage(Collections.singleton("deletedChunk"));

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("deletedChunk", garbageCollector.getGarbageChunks().peek().getName());

        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(0);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(1);

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

        val manualDelay = new ManualDelay(2);

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
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addToGarbage(Collections.singleton("deletedChunk"));

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("deletedChunk", garbageCollector.getGarbageChunks().peek().getName());

        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(0);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(1);

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

        val manualDelay = new ManualDelay(2);

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
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addToGarbage(Collections.singleton("nonExistingChunk"));

        // Validate state before
        Assert.assertEquals(1, garbageCollector.getQueueSize().get());
        Assert.assertEquals("nonExistingChunk", garbageCollector.getGarbageChunks().peek().getName());

        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(0);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(1);

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
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

        val manualDelay = new ManualDelay(2);

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
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addToGarbage(Arrays.asList("activeChunk", "nonExistingChunk", "deletedChunk"));

        // Validate state before
        assertQueueEquals(garbageCollector, new String[]{"activeChunk", "nonExistingChunk", "deletedChunk"});

        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(0);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(1);

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
        Assert.assertFalse(chunkStorage.exists("deletedChunk").get());
        Assert.assertTrue(chunkStorage.exists("activeChunk").get());
    }

    /**
     * Test setSuspended.
     */
    @Test
    public void testSuspended() throws Exception {
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

        val manualDelay = new ManualDelay(3);

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
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        garbageCollector.setSuspended(true);

        // Add some garbage
        garbageCollector.addToGarbage(Arrays.asList("activeChunk", "nonExistingChunk", "deletedChunk"));

        // Validate state before
        assertQueueEquals(garbageCollector, new String[]{"activeChunk", "nonExistingChunk", "deletedChunk"});

        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(0);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(1);

        assertQueueEquals(garbageCollector, new String[]{"activeChunk", "nonExistingChunk", "deletedChunk"});

        garbageCollector.setSuspended(false);
        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(1);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(2);

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

        val manualDelay = new ManualDelay(2);

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
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addToGarbage(Arrays.asList("deletedChunk"));

        // Validate state before
        assertQueueEquals(garbageCollector, new String[]{"deletedChunk"});

        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(0);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(1);

        // Validate state after
        assertQueueEquals(garbageCollector, new String[]{"deletedChunk"});
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

        val manualDelay = new ManualDelay(2);

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
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addToGarbage(Arrays.asList("deletedChunk"));

        // Validate state before
        assertQueueEquals(garbageCollector, new String[]{"deletedChunk"});

        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(0);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(1);

        // Validate state after
        assertQueueEquals(garbageCollector, new String[]{"deletedChunk"});
    }

    /**
     * Test that loop continues after exception.
     */
    @Test
    public void testDelayException() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        insertChunk(chunkStorage, "deletedChunk", dataSize);
        insertChunkMetadata(metadataStore, "deletedChunk", dataSize, 0);

        val manualDelay = new ManualDelay(2);

        val thrown = new AtomicBoolean();
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
                () -> {
                    if (thrown.compareAndSet(false, true)) {
                        return Futures.failedFuture(new Exception("testException"));
                    } else {
                        return manualDelay.get();
                    }
                });

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addToGarbage(Arrays.asList("deletedChunk"));

        // Validate state before
        assertQueueEquals(garbageCollector, new String[]{"deletedChunk"});

        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(0);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(1);

        // Validate results
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
        Assert.assertFalse(chunkStorage.exists("deletedChunk").get());
        Assert.assertNull(getChunkMetadata(metadataStore, "deletedChunk"));
    }

    /**
     * Test when queue is full.
     */
    @Test
    public void testQueueFull() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;

        insertChunk(chunkStorage, "activeChunk", dataSize);
        insertChunkMetadata(metadataStore, "activeChunk", dataSize, 1);

        insertChunk(chunkStorage, "deletedChunk", dataSize);
        insertChunkMetadata(metadataStore, "deletedChunk", dataSize, 0);

        val manualDelay = new ManualDelay(2);

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .garbageCollectionMaxQueueSize(2)
                        .build(),
                executorService(),
                System::currentTimeMillis,
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addToGarbage(Arrays.asList("activeChunk", "nonExistingChunk", "deletedChunk"));

        // Validate state before
        assertQueueEquals(garbageCollector, new String[]{"activeChunk", "nonExistingChunk"});

        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(0);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(1);

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
        Assert.assertTrue(chunkStorage.exists("activeChunk").get());
        Assert.assertNotNull(getChunkMetadata(metadataStore, "activeChunk"));
        Assert.assertNotNull(getChunkMetadata(metadataStore, "deletedChunk"));
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

        val manualDelay = new ManualDelay(5);

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
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        garbageCollector.addToGarbage(Arrays.asList("deletedChunk"));

        // Validate state before
        assertQueueEquals(garbageCollector, new String[]{"deletedChunk"});

        for (int i = 0; i < 3; i++) {
            // Return first delay - this will "unpause" the first iteration.
            manualDelay.completeDelay(i);

            // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
            // Don't complete the delay.
            manualDelay.waitForInvocation(i + 1);

            // Validate state after
            assertQueueEquals(garbageCollector, new String[]{"deletedChunk"});
        }

        // Return first delay - this will "unpause" the first iteration.
        manualDelay.completeDelay(3);

        // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
        // Don't complete the delay.
        manualDelay.waitForInvocation(4);

        // Validate state after
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());
    }

    /**
     * Test for throttling.
     */
    @Test
    public void testMaxConcurrency() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        ArrayList<String> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            val chunkName = "chunk" + i;
            insertChunk(chunkStorage, chunkName, dataSize);
            insertChunkMetadata(metadataStore, chunkName, dataSize, 0);
            expected.add(chunkName);
        }

        val manualDelay = new ManualDelay(6);

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .garbageCollectionMaxConcurrency(2)
                        .build(),
                executorService(),
                System::currentTimeMillis,
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        for (int i = 0; i < expected.size(); i++) {
            garbageCollector.addToGarbage(expected.get(i), 1000 * i, 0);
        }

        // Validate state before
        assertQueueEquals(garbageCollector, toArray(expected));

        ArrayList<String> deletedChunks = new ArrayList<>();
        int iterations = 5;
        for (int i = 0; i < iterations; i++) {
            // Return first delay - this will "unpause" the first iteration.
            manualDelay.completeDelay(i);

            // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
            // Don't complete the delay.
            manualDelay.waitForInvocation(i + 1);

            // Validate state after
            Assert.assertEquals(expected.size() - 2, garbageCollector.getQueueSize().get());
            // remove two elements
            deletedChunks.add(expected.remove(0));
            deletedChunks.add(expected.remove(0));

            assertQueueEquals(garbageCollector, toArray(expected));

            for (val deleted : deletedChunks) {
                Assert.assertFalse(chunkStorage.exists("deleted").get());
            }
            for (val remaining : expected) {
                Assert.assertTrue(chunkStorage.exists(remaining).get());
            }
        }
    }

    /**
     * Test that chunks are deleted in chronological order.
     */
    @Test
    public void testOrderedDeletion() throws Exception {
        @Cleanup
        ChunkStorage chunkStorage = getChunkStorage();
        @Cleanup
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = CONTAINER_ID;

        int dataSize = 1;
        ArrayList<String> expected = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            val chunkName = "chunk" + i;
            insertChunk(chunkStorage, chunkName, dataSize);
            insertChunkMetadata(metadataStore, chunkName, dataSize, 0);
            expected.add(chunkName);
        }

        val manualDelay = new ManualDelay(11);

        val baseTime = System.currentTimeMillis();
        val currentIteration = new AtomicInteger();
        final Supplier<Long> timeSupplier = () -> baseTime + 10000 * currentIteration.get();

        @Cleanup
        GarbageCollector garbageCollector = new GarbageCollector(containerId,
                chunkStorage,
                metadataStore,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                        .garbageCollectionDelay(Duration.ofMillis(1))
                        .garbageCollectionSleep(Duration.ofMillis(1))
                        .build(),
                executorService(),
                timeSupplier,
                manualDelay);

        // Now actually start run
        garbageCollector.initialize();

        Assert.assertNotNull(garbageCollector.getGarbageChunks());
        Assert.assertEquals(0, garbageCollector.getQueueSize().get());

        // Add some garbage
        for (int i = 0; i < expected.size(); i++) {
            garbageCollector.addToGarbage(expected.get(i), baseTime + 10000 * (i + 1), 0);
        }

        // Validate state before
        assertQueueEquals(garbageCollector, toArray(expected));

        ArrayList<String> deletedChunks = new ArrayList<>();
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            // move "timer ahead"
            currentIteration.incrementAndGet();
            // Return first delay - this will "unpause" the first iteration.
            manualDelay.completeDelay(i);

            // Wait for "Delay" to be invoked again. This indicates that first iteration was complete.
            // Don't complete the delay.
            manualDelay.waitForInvocation(i + 1);

            // Validate state after
            Assert.assertEquals(expected.size() - 1, garbageCollector.getQueueSize().get());
            // remove two elements
            deletedChunks.add(expected.remove(0));

            assertQueueEquals(garbageCollector, toArray(expected));

            for (val deleted : deletedChunks) {
                Assert.assertFalse(chunkStorage.exists("deleted").get());
            }
            for (val remaining : expected) {
                Assert.assertTrue(chunkStorage.exists(remaining).get());
            }
        }
    }

    private String[] toArray(ArrayList<String> expected) {
        return expected.toArray(new String[expected.size()]);
    }

    private void assertQueueEquals(GarbageCollector garbageCollector, String[] expected) {
        Assert.assertEquals(expected.length, garbageCollector.getQueueSize().get());
        val queue = Arrays.stream(garbageCollector.getGarbageChunks().toArray(new Object[expected.length]))
                .map(info -> ((GarbageCollector.GarbageChunkInfo) info).getName()).collect(Collectors.toSet());
        Assert.assertEquals(expected.length, queue.size());
        for (String chunk : expected) {
            Assert.assertTrue(queue.contains(chunk));
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

    private ChunkMetadata getChunkMetadata(ChunkMetadataStore metadataStore, String chunkName) throws Exception {
        try (val txn = metadataStore.beginTransaction(true, chunkName)) {
            return (ChunkMetadata) txn.get(chunkName).get();
        }
    }

    /**
     * A utility test class that helps synchronize test code with iterations.
     */
    static class ManualDelay implements Supplier<CompletableFuture<Void>> {
        /**
         * List of futures to return.
         */
        @Getter
        final ArrayList<CompletableFuture<Void>> toReturn = new ArrayList<>();

        /**
         * List of futures to track each invocation.
         */
        @Getter
        final ArrayList<CompletableFuture<Void>> invocations = new ArrayList<>();

        /**
         * Current index.
         */
        final AtomicInteger currentIndex = new AtomicInteger();

        /**
         * Constructor.
         *
         * @param count Number of iterations to run.
         */
        ManualDelay(int count) {
            for (int i = 0; i < count; i++) {
                toReturn.add(new CompletableFuture<>());
                invocations.add(new CompletableFuture<>());
            }
        }

        /**
         * Call back method which is called at the start of each request for delay.
         * @return
         */
        @Override
        synchronized public CompletableFuture<Void> get() {
            log.debug("Delay Invoked count = {}", currentIndex.get());
            // Trigger that call was made.
            invocations.get(currentIndex.get()).complete(null);
            // return next "delay" future.
            return toReturn.get(currentIndex.getAndIncrement());
        }

        void completeDelay(int i) {
            toReturn.get(i).complete(null);
        }

        void waitForInvocation(int i) {
            invocations.get(i).join();
        }
    }
}

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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.mocks.AbstractInMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.segmentstore.storage.noop.NoOpChunkStorage;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;

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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link ChunkedSegmentStorage}.
 * The focus is on testing the ChunkedSegmentStorage implementation itself very thoroughly.
 * It uses {@link NoOpChunkStorage} as {@link ChunkStorage}.
 */
@Slf4j
public class ChunkedSegmentStorageTests extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int CONTAINER_ID = 42;
    private static final int OWNER_EPOCH = 100;

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
        return 1;
    }

    public ChunkStorage createChunkStorageProvider() throws Exception {
        return new NoOpChunkStorage(executorService());
    }

    public ChunkMetadataStore createMetadataStore() throws Exception {
        return new InMemoryMetadataStore(executorService());
    }

    public TestContext getTestContext() throws Exception {
        return new TestContext(executorService());
    }

    public TestContext getTestContext(ChunkedSegmentStorageConfig config) throws Exception {
        return new TestContext(executorService(), config);
    }

    /**
     * Test capabilities.
     *
     * @throws Exception
     */
    @Test
    public void testSupportsTruncate() throws Exception {
        val storageProvider = createChunkStorageProvider();
        val storageManager = new ChunkedSegmentStorage(storageProvider, executorService(), ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        Assert.assertTrue(storageManager.supportsTruncation());
    }

    /**
     * Test initialization.
     *
     * @throws Exception
     */
    @Test
    public void testInitialization() throws Exception {
        val storageProvider = createChunkStorageProvider();
        val metadataStore = createMetadataStore();
        val policy = SegmentRollingPolicy.NO_ROLLING;
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        val storageManager = new ChunkedSegmentStorage(storageProvider, executorService(), config);
        val systemJournal = new SystemJournal(CONTAINER_ID, 1, storageProvider, metadataStore, config);

        testUninitialized(storageManager);

        storageManager.initialize(1);

        Assert.assertNull(storageManager.getMetadataStore());
        Assert.assertEquals(storageProvider, storageManager.getChunkStorage());
        Assert.assertEquals(policy, storageManager.getConfig().getDefaultRollingPolicy());
        Assert.assertEquals(1, storageManager.getEpoch());

        storageManager.bootstrap(CONTAINER_ID, metadataStore).join();
        Assert.assertEquals(metadataStore, storageManager.getMetadataStore());
        Assert.assertEquals(storageProvider, storageManager.getChunkStorage());
        Assert.assertEquals(policy, storageManager.getConfig().getDefaultRollingPolicy());
        Assert.assertNotNull(storageManager.getSystemJournal());
        Assert.assertEquals(systemJournal.getConfig().getDefaultRollingPolicy(), policy);
        Assert.assertEquals(1, storageManager.getEpoch());
        Assert.assertEquals(CONTAINER_ID, storageManager.getContainerId());
        Assert.assertEquals(0, storageManager.getConfig().getMinSizeLimitForConcat());
        Assert.assertEquals(Long.MAX_VALUE, storageManager.getConfig().getMaxSizeLimitForConcat());
        storageManager.close();

        testUninitialized(storageManager);

    }

    private void testUninitialized(ChunkedSegmentStorage storageManager) {
        String testSegmentName = "foo";
        AssertExtensions.assertThrows(
                "getStreamSegmentInfo succeeded on uninitialized instance.",
                () -> storageManager.getStreamSegmentInfo(testSegmentName, null),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "Seal  succeeded on uninitialized instance.",
                () -> storageManager.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "openWrite succeeded on uninitialized instance.",
                () -> storageManager.openWrite(testSegmentName),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "openRead succeeded on uninitialized instance.",
                () -> storageManager.openRead(testSegmentName),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "write succeeded on uninitialized instance.",
                () -> storageManager.write(SegmentStorageHandle.writeHandle(testSegmentName), 0, new ByteArrayInputStream(new byte[1]), 1, null),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "read succeeded on uninitialized instance.",
                () -> storageManager.read(SegmentStorageHandle.readHandle(testSegmentName), 0, new byte[1], 0, 1, null),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "Concat succeeded on uninitialized instance.",
                () -> storageManager.concat(SegmentStorageHandle.readHandle(testSegmentName), 0, "inexistent", null),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "Concat succeeded on uninitialized instance.",
                () -> storageManager.delete(SegmentStorageHandle.readHandle(testSegmentName), null),
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Test exceptions for opertions on non-existent chunk.
     */
    @Test
    public void testSegmentNotExistsExceptionForNonExistent() throws Exception {
        String testSegmentName = "foo";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(1); // Force rollover after each byte.
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

    /**
     * Test scenarios when storage is no more primary.
     */
    @Test
    public void testStorageNotPrimaryException() throws Exception {
        String testSegmentName = "foo";
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
        TestContext testContext = getTestContext();
        testContext.chunkedSegmentStorage.initialize(2);
        int maxRollingLength = 1;
        testContext.insertMetadata(testSegmentName, maxRollingLength, OWNER_EPOCH);
        testContext.insertMetadata("source", maxRollingLength, 1);
        testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle("source"), null).join();

        // These operations should always succeed.
        testContext.chunkedSegmentStorage.getStreamSegmentInfo(testSegmentName, null).join();
        val hr = testContext.chunkedSegmentStorage.openRead(testSegmentName).join();
        testContext.chunkedSegmentStorage.read(hr, 0, new byte[0], 0, 0, null);

        val hw = testContext.chunkedSegmentStorage.openWrite(testSegmentName);

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
        TestContext testContext = getTestContext();

        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setShouldSupportAppend(false);

        // Step 1: Create segment.
        val h = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        Assert.assertEquals(h.getSegmentName(), testSegmentName);
        Assert.assertFalse(h.isReadOnly());

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
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

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
        TestContext testContext = getTestContext();
        testSimpleScenario(testSegmentName, policy, testContext);
    }

    private void testSimpleScenario(String testSegmentName, SegmentRollingPolicy policy, TestContext testContext) throws Exception {
        // Step 1: Create segment.
        val h = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();
        Assert.assertEquals(h.getSegmentName(), testSegmentName);
        Assert.assertFalse(h.isReadOnly());

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
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

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
     * Test Read.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testReadInvalidParameters() throws Exception {
        String testSegmentName = "foo";
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
     * Test Write.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testWrite() throws Exception {
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 2 byte.

        // Create
        val hWrite = testContext.chunkedSegmentStorage.create(testSegmentName, policy, null).get();

        // Write some data.
        long writeAt = 0;
        for (int i = 1; i < 5; i++) {
            testContext.chunkedSegmentStorage.write(hWrite, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
        }

        int total = 10;

        checkDataRead(testSegmentName, testContext, 0, total);
    }

    /**
     * Test write with invalid arguments.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testWriteInvalidParameters() throws Exception {
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();
        // Setup a segment.
        val segment = testContext.insertMetadata(testSegmentName, 1024, 1, new long[]{25});

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
        Assert.assertNull(segmentMetadataAfterDelete);

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

    /**
     * Test failover scenario on empty segment.
     *
     * @throws Exception
     */
    @Test
    public void testOpenWriteAfterFailoverWithNoData() throws Exception {
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();
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

    /**
     * Test failover scenario on empty segment.
     *
     * @throws Exception
     */
    @Test
    public void testOpenReadAfterFailoverWithNoData() throws Exception {
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();
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
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
    }

    /**
     * Test failover scenario for segment with only one chunk.
     *
     * @throws Exception
     */
    @Test
    public void testOpenWriteAfterFailoverWithSingleChunk() throws Exception {
        String testSegmentName = "foo";
        int ownerEpoch = 2;
        int maxRollingLength = OWNER_EPOCH;
        long[] chunks = new long[]{10};
        int lastChunkLengthInStorage = 12;

        testOpenWriteAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, chunks, lastChunkLengthInStorage);

    }

    /**
     * Test failover scenario for segment with only one chunk.
     *
     * @throws Exception
     */
    @Test
    public void testOpenReadAfterFailoverWithSingleChunk() throws Exception {
        String testSegmentName = "foo";
        int ownerEpoch = 2;
        int maxRollingLength = OWNER_EPOCH;
        long[] chunks = new long[]{10};
        int lastChunkLengthInStorage = 12;

        testOpenReadAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, chunks, lastChunkLengthInStorage);

    }

    private void testOpenWriteAfterFailover(String testSegmentName, int ownerEpoch, int maxRollingLength, long[] chunks, int lastChunkLengthInStorage) throws Exception {
        TestContext testContext = getTestContext();
        testContext.chunkedSegmentStorage.initialize(ownerEpoch);
        val inserted = testContext.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch - 1, chunks);
        // Set bigger offset
        testContext.addChunk(inserted.getLastChunk(), lastChunkLengthInStorage);
        val hWrite = testContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        Assert.assertEquals(hWrite.getSegmentName(), testSegmentName);
        Assert.assertFalse(hWrite.isReadOnly());

        val metadataAfter = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertEquals(ownerEpoch, metadataAfter.getOwnerEpoch());
        Assert.assertEquals(lastChunkLengthInStorage, metadataAfter.getLength());
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, chunks, lastChunkLengthInStorage);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
    }

    private void testOpenReadAfterFailover(String testSegmentName, int ownerEpoch, int maxRollingLength, long[] chunks, int lastChunkLengthInStorage) throws Exception {
        TestContext testContext = getTestContext();
        testContext.chunkedSegmentStorage.initialize(ownerEpoch);
        val inserted = testContext.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch - 1, chunks);
        // Set bigger offset
        testContext.addChunk(inserted.getLastChunk(), lastChunkLengthInStorage);
        val hRead = testContext.chunkedSegmentStorage.openRead(testSegmentName).get();
        Assert.assertEquals(hRead.getSegmentName(), testSegmentName);
        Assert.assertTrue(hRead.isReadOnly());

        val metadataAfter = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertEquals(ownerEpoch, metadataAfter.getOwnerEpoch());
        Assert.assertEquals(lastChunkLengthInStorage, metadataAfter.getLength());
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, chunks, lastChunkLengthInStorage);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
    }

    /**
     * Test simple concat.
     *
     * @throws Exception
     */
    @Test
    public void testSimpleConcat() throws Exception {
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

        // Concat.
        testContext.chunkedSegmentStorage.seal(h2, null).join();
        testContext.chunkedSegmentStorage.concat(h1, (long) nChunks1 * (long) maxChunkLength, sourceSegmentName, null).join();

        // Validate.
        TestUtils.checkSegmentLayout(testContext.metadataStore, targetSegmentName, maxChunkLength, nChunks1 + nChunks2);
        TestUtils.checkSegmentBounds(testContext.metadataStore, targetSegmentName, 0, ((long) nChunks1 + (long) nChunks2) * maxChunkLength);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, targetSegmentName);

    }

    @Test
    public void testSimpleConcatWithDefrag() throws Exception {
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

    private void testBaseConcat(TestContext testContext, int maxRollingLength, long[] targetLayout, long[] sourceLayout, long[] resultLayout) throws Exception {
        val source = testContext.insertMetadata("source", maxRollingLength, 1, sourceLayout);
        val target = testContext.insertMetadata("target", maxRollingLength, 1, targetLayout);

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

        // Cleanup
        testContext.chunkedSegmentStorage.delete(SegmentStorageHandle.writeHandle("target"), null).join();
    }

    private void testBaseConcat(TestContext testContext, int maxRollingLength, long[] targetLayout, long offset, long[] sourceLayout, long[] resultLayout) throws Exception {
        try {
            val source = testContext.insertMetadata("source", maxRollingLength, 1, sourceLayout);
            val target = testContext.insertMetadata("target", maxRollingLength, 1, targetLayout);

            // Concat.
            testContext.chunkedSegmentStorage.seal(SegmentStorageHandle.writeHandle("source"), null).join();
            val sourceInfo = testContext.chunkedSegmentStorage.getStreamSegmentInfo("target", null).join();
            val targetInfo = testContext.chunkedSegmentStorage.getStreamSegmentInfo("target", null).join();
            testContext.chunkedSegmentStorage.concat(SegmentStorageHandle.writeHandle("target"),
                    offset,
                    "source",
                    null).join();

            // Validate.
            TestUtils.checkSegmentLayout(testContext.metadataStore, "target", resultLayout);
            TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, "target");

        } finally {
            // Cleanup
            testContext.chunkedSegmentStorage.delete(SegmentStorageHandle.writeHandle("target"), null).join();
        }
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
                testContext.chunkedSegmentStorage.concat(h, validEnd, truncatedSource, TIMEOUT),
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
        TestContext testContext = getTestContext();
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
        ChunkedSegmentStorageConfig config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .maxSizeLimitForConcat(12)
                .minSizeLimitForConcat(2)
                .build();

        TestContext testContext = getTestContext(config);
        ((AbstractInMemoryChunkStorage) testContext.chunkStorage).setShouldSupportConcat(true);

        // Populate segments
        testBaseConcat(testContext, 1024,
                new long[]{10},
                new long[]{1, 1, 1, 3, 1, 1, 3, 1, 3},  // small chunks followed by normal chunks.
                new long[]{25});

        testBaseConcat(testContext, 1024,
                new long[]{10},
                new long[]{3, 1, 1, 1, 3, 1, 1, 3, 1}, // normal chunks followed by small chunks.
                new long[]{25});

        testBaseConcat(testContext, 1024,
                new long[]{10},
                new long[]{1, 3, 3, 3, 1, 2, 2}, // consecutive normal.
                new long[]{25});

        testBaseConcat(testContext, 1024,
                new long[]{10},
                new long[]{5, 5, 5}, // all large chunks.
                new long[]{25});

        testBaseConcat(testContext, 1024,
                new long[]{10},
                new long[]{2, 2, 2, 2, 2, 2, 2, 1}, // all small chunks.
                new long[]{25});

        testBaseConcat(testContext, 1024,
                new long[]{10},
                new long[]{12, 3}, // all concats possible.
                new long[]{25});

        testBaseConcat(testContext, 1024,
                new long[]{10},
                new long[]{13, 2}, // not all concats possible.
                new long[]{10, 15});
    }

    @Test
    public void testBaseConcatWithDefrag() throws Exception {
        TestContext testContext = getTestContext();
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
        TestContext testContext = getTestContext();
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

        AssertExtensions.assertFutureThrows("truncate() allowed for invalid parameters",
                testContext.chunkedSegmentStorage.truncate(h, validStart - 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // Sealed segment
        /* This check seems wrong and should be removed.
        testContext.storageManager.seal(h, TIMEOUT).join();
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.truncate(h, 11, TIMEOUT),
                ex -> ex instanceof StreamSegmentSealedException);
         */
    }

    @Test
    public void testRepeatedTruncates() throws Exception {
        TestContext testContext = getTestContext();
        String testSegmentName = "testSegmentName";

        // Populate sgement.
        val h1 = populateSegment(testContext, testSegmentName, 3, 3);
        byte[] buffer = new byte[10];

        // Perform series of truncates.
        for (int truncateAt = 0; truncateAt < 9; truncateAt++) {
            testContext.chunkedSegmentStorage.truncate(h1, truncateAt, null).join();
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, 3, 3 - (truncateAt / 3));
            TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, truncateAt, 9);
            TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

            val metadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            // length doesn't change.
            Assert.assertEquals(9, metadata.getLength());
            // start offset should match i.
            Assert.assertEquals(truncateAt, metadata.getStartOffset());
            // Each time the first offest is multiple of 3
            Assert.assertEquals(3 * (truncateAt / 3), metadata.getFirstChunkStartOffset());

            // try to read some bytes.
            val bytesRead = testContext.chunkedSegmentStorage.read(h1, truncateAt, buffer, 0, 9 - truncateAt, null).get().intValue();
            Assert.assertEquals(9 - truncateAt, bytesRead);
            if (truncateAt > 0) {
                AssertExtensions.assertFutureThrows(
                        "read succeeded on missing segment.",
                        testContext.chunkedSegmentStorage.read(h1, truncateAt - 1, buffer, 0, 9 - truncateAt, null),
                        ex -> ex instanceof StreamSegmentTruncatedException);
            }
        }
    }

    @Test
    public void testRepeatedTruncatesOnLargeChunkVaryingSizes() throws Exception {
        TestContext testContext = getTestContext();
        String testSegmentName = "testSegmentName";
        // Set up.
        val h1 = populateSegment(testContext, testSegmentName, 10, 1);
        byte[] buffer = new byte[10];

        int truncateAt = 0;
        for (int i = 0; i < 4; i++) {
            testContext.chunkedSegmentStorage.truncate(h1, truncateAt, null).join();

            // Check layout.
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, new long[]{10});
            TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

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
        TestContext testContext = getTestContext();
        String testSegmentName = "testSegmentName";
        // Populate.
        val h1 = populateSegment(testContext, testSegmentName, 10, 1);
        byte[] buffer = new byte[10];
        for (int truncateAt = 0; truncateAt < 9; truncateAt++) {
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

            // Validate read.
            val bytesRead = testContext.chunkedSegmentStorage.read(h1, truncateAt, buffer, 0, 10 - truncateAt, null).get().intValue();
            Assert.assertEquals(10 - truncateAt, bytesRead);
        }
    }

    @Test
    public void testRepeatedTruncatesAtFullLength() throws Exception {
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

    private void testTruncate(long maxChunkLength, long truncateAt, int chunksCountBefore, int chunksCountAfter, long expectedLength) throws Exception {
        TestContext testContext = getTestContext();
        String testSegmentName = "testSegmentName";

        // Populate
        val h1 = populateSegment(testContext, testSegmentName, maxChunkLength, chunksCountBefore);

        // Perform truncate.
        testContext.chunkedSegmentStorage.truncate(h1, truncateAt, null).join();

        // Check layout.
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, maxChunkLength, chunksCountAfter);
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, truncateAt, expectedLength);
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);
    }

    /**
     * Test read and write with multiple failovers.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testReadWriteWithMultipleFailovers() throws Exception {
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();

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
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, Longs.toArray(lengths));

            // Fence out old store.
            oldTestCotext.metadataStore.markFenced();

            // Read in new epoch.
            checkDataRead(testSegmentName, testContext, 0, writeAt);
        }

        int total = 10;

        // Create a new test context and initialize with new epoch.
        testContext = testContext.fork(epoch++);

        checkDataRead(testSegmentName, testContext, 0, total);
    }

    /**
     * Test read and write with multiple failovers.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testReadWriteWithMultipleFailoversWithGarbage() throws Exception {
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();

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

            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, Longs.toArray(lengths));

            // Fork the context.
            val oldTestCotext = testContext;
            testContext = oldTestCotext.fork(epoch++);
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, Longs.toArray(lengths));

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
            checkDataRead(testSegmentName, testContext, 0, writeAt);
        }

        int total = 10;

        // Create a new test context and initialize with new epoch.
        testContext = testContext.fork(epoch++);

        checkDataRead(testSegmentName, testContext, 0, total);
    }

    /**
     * Test truncate, read and write with multiple failovers.
     *
     * @throws Exception Exception if any.
     */
    @Test
    public void testTruncateWithMultipleFailoversWithGarbage() throws Exception {
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();

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

            // Create a new test context and initialize with new epoch.
            testContext.chunkedSegmentStorage.write(hWrite, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
            lengths.add((long) i);
            TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, truncateAt, writeAt);
            checkDataRead(testSegmentName, testContext, truncateAt, writeAt);

            truncateAt++;

            // Fork the context.
            val oldTestCotext = testContext;
            testContext = oldTestCotext.fork(epoch++);

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

        checkDataRead(testSegmentName, testContext, truncateAt, total);
    }

    @Test
    public void testTruncateWithFailover() throws Exception {
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();

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
        checkDataRead(testSegmentName, testContext, offset, i);
        offset += i;
        // Fork the context.
        testContext = testContext.fork(++epoch);
        val oldTestCotext = testContext;
        val newTestContext = oldTestCotext.fork(++epoch);
        // Fence out old store.
        oldTestCotext.metadataStore.markFenced();

        // Truncate and Read in new epoch.
        // Make sure to open segment with new instance before writing garbage to old instance.
        hWrite = newTestContext.chunkedSegmentStorage.openWrite(testSegmentName).get();
        newTestContext.chunkedSegmentStorage.truncate(hWrite, offset, null).get();
        //checkDataRead(testSegmentName, testContext, offset, 0);
        TestUtils.checkSegmentBounds(newTestContext.metadataStore, testSegmentName, offset, offset);

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
        TestContext testContext = getTestContext();
        Executor executor = Executors.newFixedThreadPool(threadPoolSize);
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
        Executor executor = Executors.newFixedThreadPool(threadPoolSize);
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
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, testSegmentName);

        // Step 3: Read data back.
        Executor executor = Executors.newFixedThreadPool(threadPoolSize);
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
        TestContext testContext = getTestContext();
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
        Executor executor = Executors.newFixedThreadPool(threadPoolSize);
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

    private void checkDataRead(String testSegmentName, TestContext testContext, long offset, long length) throws InterruptedException, java.util.concurrent.ExecutionException {
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
    }

    private CompletableFuture<Void> checkDataReadAsync(String testSegmentName, TestContext testContext, long offset, long length, Executor executor) {
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
                            }, executor);
                }, executor);
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
        TestUtils.checkChunksExistInStorage(testContext.chunkStorage, testContext.metadataStore, targetSegmentName);
        return h;
    }

    /**
     * Test context.
     */
    public static class TestContext {
        @Getter
        protected ChunkedSegmentStorageConfig config;

        @Getter
        protected ChunkStorage chunkStorage;

        @Getter
        protected ChunkMetadataStore metadataStore;

        @Getter
        protected ChunkedSegmentStorage chunkedSegmentStorage;

        @Getter
        protected Executor executor;

        protected TestContext() {
        }

        public TestContext(Executor executor) throws Exception {
            this(executor, ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        }

        public TestContext(Executor executor, ChunkedSegmentStorageConfig config) throws Exception {
            this.executor = executor;
            this.config = config;
            chunkStorage = createChunkStorageProvider();
            metadataStore = createMetadataStore();
            chunkedSegmentStorage = new ChunkedSegmentStorage(chunkStorage, metadataStore, this.executor, config);
            chunkedSegmentStorage.initialize(1);
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
            forkedContext.chunkedSegmentStorage = new ChunkedSegmentStorage(this.chunkStorage,
                    forkedContext.metadataStore,
                    this.executor,
                    this.config);
            forkedContext.chunkedSegmentStorage.initialize(epoch);
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
            return new InMemoryMetadataStore(executor);
        }

        /**
         * Gets {@link ChunkStorage} to use for the tests.
         */
        public ChunkStorage createChunkStorageProvider() throws Exception {
            return new NoOpChunkStorage(executor);
        }

        /**
         * Creates and inserts metadata for a test segment.
         */
        public SegmentMetadata insertMetadata(String testSegmentName, int maxRollingLength, int ownerEpoch) throws Exception {
            try (val txn = metadataStore.beginTransaction(new String[]{testSegmentName})) {
                SegmentMetadata segmentMetadata = SegmentMetadata.builder()
                        .maxRollinglength(maxRollingLength)
                        .name(testSegmentName)
                        .ownerEpoch(ownerEpoch)
                        .build();
                segmentMetadata.setActive(true);
                txn.create(segmentMetadata);
                txn.commit().join();
                return segmentMetadata;
            }
        }

        /**
         * Creates and inserts metadata for a test segment.
         */
        public SegmentMetadata insertMetadata(String testSegmentName, long maxRollingLength, int ownerEpoch, long[] chunkLengths) throws Exception {
            try (val txn = metadataStore.beginTransaction(new String[]{testSegmentName})) {
                String firstChunk = null;
                String lastChunk = null;

                // Add chunks.
                long length = 0;
                long startOfLast = 0;
                int chunkCount = 0;
                for (int i = 0; i < chunkLengths.length; i++) {
                    String chunkName = testSegmentName + "_chunk_" + Integer.toString(i);
                    ChunkMetadata chunkMetadata = ChunkMetadata.builder()
                            .name(chunkName)
                            .length(chunkLengths[i])
                            .nextChunk(i == chunkLengths.length - 1 ? null : testSegmentName + "_chunk_" + Integer.toString(i + 1))
                            .build();
                    length += chunkLengths[i];
                    txn.create(chunkMetadata);

                    addChunk(chunkName, chunkLengths[i]);
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
                segmentMetadata.setActive(true);
                segmentMetadata.setChunkCount(chunkCount);
                segmentMetadata.checkInvariants();
                txn.create(segmentMetadata);

                txn.commit().join();
                return segmentMetadata;
            }
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

        /**
         * Adds chunk of specified length to the underlying {@link ChunkStorage}.
         */
        public void addChunk(String chunkName, long length) {
            ((AbstractInMemoryChunkStorage) chunkStorage).addChunk(chunkName, length);
        }
    }
}

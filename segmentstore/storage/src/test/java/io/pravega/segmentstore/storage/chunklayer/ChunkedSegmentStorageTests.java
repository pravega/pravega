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
import java.util.concurrent.Executor;

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

    public ChunkStorage createChunkStorageProvider() throws Exception {
        return new NoOpChunkStorage();
    }

    public ChunkMetadataStore createMetadataStore() throws Exception {
        return new InMemoryMetadataStore();
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

        storageManager.bootstrap(CONTAINER_ID, metadataStore);
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
        Assert.assertFalse(testContext.storageManager.exists(testSegmentName, null).get());

        AssertExtensions.assertFutureThrows(
                "getStreamSegmentInfo succeeded on missing segment.",
                testContext.storageManager.getStreamSegmentInfo(testSegmentName, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Seal succeeded on missing segment.",
                testContext.storageManager.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "openWrite succeeded on missing segment.",
                testContext.storageManager.openWrite(testSegmentName),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "openRead succeeded on missing segment.",
                testContext.storageManager.openRead(testSegmentName),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "write succeeded on missing segment.",
                testContext.storageManager.write(SegmentStorageHandle.writeHandle(testSegmentName), 0, new ByteArrayInputStream(new byte[1]), 1, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "read succeeded on missing segment.",
                testContext.storageManager.read(SegmentStorageHandle.readHandle(testSegmentName), 0, new byte[1], 0, 1, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.storageManager.concat(SegmentStorageHandle.writeHandle(testSegmentName), 0, "inexistent", null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.storageManager.delete(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.storageManager.truncate(SegmentStorageHandle.writeHandle(testSegmentName), 0, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

    }

    /**
     * Test scenarios when storage is no more primary.
     */
    @Test
    public void testStorageNotPrimaryException() throws Exception {
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();
        testContext.storageManager.initialize(2);
        int maxRollingLength = 1;
        int ownerEpoch = OWNER_EPOCH;
        testContext.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch);

        // These operations should always succeed.
        testContext.storageManager.getStreamSegmentInfo(testSegmentName, null).join();
        val h = testContext.storageManager.openRead(testSegmentName).join();
        testContext.storageManager.read(h, 0, new byte[0], 0, 0, null);

        // These operations should never succeed.
        AssertExtensions.assertFutureThrows(
                "Seal succeeded on segment not owned.",
                testContext.storageManager.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "openWrite succeeded on segment not owned.",
                testContext.storageManager.openWrite(testSegmentName),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "delete succeeded on segment not owned.",
                testContext.storageManager.delete(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "write succeeded on segment not owned.",
                testContext.storageManager.write(SegmentStorageHandle.writeHandle(testSegmentName),
                        0,
                        new ByteArrayInputStream(new byte[10]),
                        10,
                        null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "truncate succeeded on segment not owned.",
                testContext.storageManager.truncate(SegmentStorageHandle.writeHandle(testSegmentName),
                        0, null),
                ex -> ex instanceof StorageNotPrimaryException);

        testContext.insertMetadata("source", maxRollingLength, 1);
        testContext.storageManager.seal(SegmentStorageHandle.writeHandle("source"), null).join();
        AssertExtensions.assertFutureThrows(
                "concat succeeded on segment not owned.",
                testContext.storageManager.concat(SegmentStorageHandle.writeHandle(testSegmentName), 0, "source", null),
                ex -> ex instanceof StorageNotPrimaryException);

    }

    /**
     * Test scenarios when storage is no more after fencing.
     */
    @Test
    public void testStorageNotPrimaryExceptionOnFencing() throws Exception {
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();
        testContext.storageManager.initialize(2);
        int maxRollingLength = 1;
        testContext.insertMetadata(testSegmentName, maxRollingLength, OWNER_EPOCH);
        testContext.insertMetadata("source", maxRollingLength, 1);
        testContext.storageManager.seal(SegmentStorageHandle.writeHandle("source"), null).join();

        testContext.metadataStore.markFenced();

        // These operations should always succeed.
        testContext.storageManager.getStreamSegmentInfo(testSegmentName, null).join();
        val h = testContext.storageManager.openRead(testSegmentName).join();
        testContext.storageManager.read(h, 0, new byte[0], 0, 0, null);

        // These operations should never succeed.
        AssertExtensions.assertFutureThrows(
                "Seal succeeded on segment not owned.",
                testContext.storageManager.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "delete succeeded on segment not owned.",
                testContext.storageManager.delete(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "concat succeeded on segment not owned.",
                testContext.storageManager.concat(SegmentStorageHandle.writeHandle(testSegmentName), 0, "source", null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "create succeeded on segment not owned.",
                testContext.storageManager.create("newSegment", null),
                ex -> ex instanceof StorageNotPrimaryException);

        AssertExtensions.assertFutureThrows(
                "truncate succeeded on segment not owned.",
                testContext.storageManager.truncate(SegmentStorageHandle.writeHandle(testSegmentName),
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
        testContext.storageManager.initialize(2);
        int maxRollingLength = 1;
        testContext.insertMetadata(testSegmentName, maxRollingLength, OWNER_EPOCH);
        testContext.insertMetadata("source", maxRollingLength, 1);
        testContext.storageManager.seal(SegmentStorageHandle.writeHandle("source"), null).join();

        // These operations should always succeed.
        testContext.storageManager.getStreamSegmentInfo(testSegmentName, null).join();
        val hr = testContext.storageManager.openRead(testSegmentName).join();
        testContext.storageManager.read(hr, 0, new byte[0], 0, 0, null);

        val hw = testContext.storageManager.openWrite(testSegmentName);

        testContext.metadataStore.markFenced();
        AssertExtensions.assertFutureThrows(
                "write succeeded on segment not owned.",
                testContext.storageManager.write(SegmentStorageHandle.writeHandle(testSegmentName),
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

        ((AbstractInMemoryChunkStorage) testContext.storageProvider).setShouldSupportAppend(false);

        // Step 1: Create segment.
        val h = testContext.storageManager.create(testSegmentName, policy, null).get();
        Assert.assertEquals(h.getSegmentName(), testSegmentName);
        Assert.assertFalse(h.isReadOnly());

        // Check metadata is stored.
        val segmentMetadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertNotNull(segmentMetadata);
        Assert.assertEquals(segmentMetadata.getName(), testSegmentName);
        Assert.assertEquals(segmentMetadata.getKey(), testSegmentName);

        // Check exists
        Assert.assertTrue(testContext.storageManager.exists(testSegmentName, null).get());

        // Check getStreamSegmentInfo.
        SegmentProperties info = testContext.storageManager.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 0);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Write some data.
        long writeAt = 0;
        for (int i = 1; i < 5; i++) {
            testContext.storageManager.write(h, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
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
        TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);

        // Check getStreamSegmentInfo.
        info = testContext.storageManager.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 10);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Open write handle.
        val hWrite = testContext.storageManager.openWrite(testSegmentName).get();
        Assert.assertEquals(hWrite.getSegmentName(), testSegmentName);
        Assert.assertFalse(hWrite.isReadOnly());

        testContext.storageManager.write(hWrite, 10, new ByteArrayInputStream(new byte[4]), 4, null).join();
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName,
                new long[]{
                        1,      // First write
                        2,      // Second write
                        2, 1,   // Third write
                        2, 2,   // Fourth write
                        2, 2    // Recent write
                });
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, 0, 14);
        TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);

        info = testContext.storageManager.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 14);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Make sure calling create again does not succeed
        AssertExtensions.assertFutureThrows(
                "Create succeeded on missing segment.",
                testContext.storageManager.create(testSegmentName, policy, null),
                ex -> ex instanceof StreamSegmentExistsException);

        testContext.storageManager.delete(hWrite, null);
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

        // Step 1: Create segment.
        val h = testContext.storageManager.create(testSegmentName, policy, null).get();
        Assert.assertEquals(h.getSegmentName(), testSegmentName);
        Assert.assertFalse(h.isReadOnly());

        // Check metadata is stored.
        val segmentMetadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertNotNull(segmentMetadata);
        Assert.assertEquals(segmentMetadata.getName(), testSegmentName);
        Assert.assertEquals(segmentMetadata.getKey(), testSegmentName);

        // Check exists
        Assert.assertTrue(testContext.storageManager.exists(testSegmentName, null).get());

        // Check getStreamSegmentInfo.
        SegmentProperties info = testContext.storageManager.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 0);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Write some data.
        long writeAt = 0;
        for (int i = 1; i < 5; i++) {
            testContext.storageManager.write(h, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
        }
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, 2, 5);
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, 0, 10);
        TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);

        // Check getStreamSegmentInfo.
        info = testContext.storageManager.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 10);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Open write handle.
        val hWrite = testContext.storageManager.openWrite(testSegmentName).get();
        Assert.assertEquals(hWrite.getSegmentName(), testSegmentName);
        Assert.assertFalse(hWrite.isReadOnly());

        testContext.storageManager.write(hWrite, 10, new ByteArrayInputStream(new byte[4]), 4, null).join();
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, 2, 7);
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, 0, 14);
        TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);

        info = testContext.storageManager.getStreamSegmentInfo(testSegmentName, null).get();
        Assert.assertFalse(info.isSealed());
        Assert.assertFalse(info.isDeleted());
        Assert.assertEquals(info.getName(), testSegmentName);
        Assert.assertEquals(info.getLength(), 14);
        Assert.assertEquals(info.getStartOffset(), 0);

        // Make sure calling create again does not succeed
        AssertExtensions.assertFutureThrows(
                "Create succeeded on missing segment.",
                testContext.storageManager.create(testSegmentName, policy, null),
                ex -> ex instanceof StreamSegmentExistsException);

        testContext.storageManager.delete(hWrite, null);
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

        val h = testContext.storageManager.openRead(testSegmentName).get();

        // Read all bytes at once.
        byte[] output = new byte[total];
        int bytesRead = testContext.storageManager.read(h, 0, output, 0, total, null).get();
        Assert.assertEquals(total, bytesRead);

        // Read bytes at varying lengths but same starting offset.
        for (int i = 0; i < 15; i++) {
            bytesRead = testContext.storageManager.read(h, 0, output, 0, i, null).get();
            Assert.assertEquals(i, bytesRead);
        }

        // Read bytes at varying lengths and different offsets.
        for (int i = 0; i < 15; i++) {
            bytesRead = testContext.storageManager.read(h, 15 - i - 1, output, 0, i, null).get();
            Assert.assertEquals(i, bytesRead);
        }

        // Read bytes at varying sizes.
        int totalBytesRead = 0;
        for (int i = 5; i > 0; i--) {
            bytesRead = testContext.storageManager.read(h, 0, output, totalBytesRead, i, null).get();
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
        val hWrite = testContext.storageManager.openWrite(testSegmentName).get();
        testContext.storageManager.truncate(hWrite, validStart, null);

        val h = testContext.storageManager.openRead(testSegmentName).get();
        // Read all bytes at once.
        byte[] output = new byte[validLength];
        byte[] smallerBuffer = new byte[validLength - 1];
        byte[] biggerBuffer = new byte[validLength + 1];

        int bytesRead = testContext.storageManager.read(h, validStart, output, 0, validLength, null).get();
        Assert.assertEquals(validLength, bytesRead);

        // StreamSegmentTruncatedException
        // Read from the truncated part.
        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.storageManager.read(h, 0, output, 0, output.length, TIMEOUT),
                ex -> ex instanceof StreamSegmentTruncatedException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.storageManager.read(h, validStart - 1, output, 0, output.length, TIMEOUT),
                ex -> ex instanceof StreamSegmentTruncatedException);

        // IllegalArgumentException
        // Beyond last valid offset
        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.storageManager.read(h, validStart + validLength, output, 0, output.length, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // ArrayIndexOutOfBoundsException
        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.storageManager.read(h, -1, output, 0, validLength, null),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.storageManager.read(h, validStart, output, -1, validLength, null),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.storageManager.read(h, validStart, output, 0, -1, null),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.storageManager.read(h, validStart, output, -1, validLength, null),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.storageManager.read(h, validStart, output, validLength, validLength, null),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        AssertExtensions.assertFutureThrows("read() allowed for invalid parameters",
                testContext.storageManager.read(h, 0, smallerBuffer, 0, validLength, null),
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
        val hWrite = testContext.storageManager.create(testSegmentName, policy, null).get();

        // Write some data.
        long writeAt = 0;
        for (int i = 1; i < 5; i++) {
            testContext.storageManager.write(hWrite, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
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
        val hWrite = testContext.storageManager.openWrite(testSegmentName).get();
        testContext.storageManager.truncate(hWrite, validStart, null);

        val h = testContext.storageManager.openWrite(testSegmentName).get();
        // Read all bytes at once.
        byte[] input = new byte[1];
        val inputStream = new ByteArrayInputStream(input);
        // Invalid parameters
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.write(h, -1, inputStream, validLength, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.write(h, 0, inputStream, -1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.write(h, 0, null, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.write(null, 0, inputStream, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.write(SegmentStorageHandle.readHandle(testSegmentName), 0, inputStream, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // Bad offset
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.write(h, 0, inputStream, 1, TIMEOUT),
                ex -> ex instanceof BadOffsetException);
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.write(h, validStart, inputStream, 1, TIMEOUT),
                ex -> ex instanceof BadOffsetException);
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.write(h, validStart + validLength + 1, inputStream, 1, TIMEOUT),
                ex -> ex instanceof BadOffsetException);
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.write(h, validStart + validLength - 1, inputStream, 1, TIMEOUT),
                ex -> ex instanceof BadOffsetException);

        // Sealed segment
        testContext.storageManager.seal(h, TIMEOUT).join();
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.write(h, validStart + validLength, inputStream, 1, TIMEOUT),
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
        Assert.assertFalse(testContext.storageManager.exists(testSegmentName, null).get());

        // Step 1: Create segment.
        val h = testContext.storageManager.create(testSegmentName, policy, null).get();
        Assert.assertEquals(h.getSegmentName(), testSegmentName);
        Assert.assertFalse(h.isReadOnly());
        val segmentMetadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertNotNull(segmentMetadata);
        Assert.assertEquals(segmentMetadata.getName(), testSegmentName);
        Assert.assertEquals(segmentMetadata.getKey(), testSegmentName);
        Assert.assertTrue(testContext.storageManager.exists(testSegmentName, null).get());

        testContext.storageManager.delete(h, null).join();
        Assert.assertFalse(testContext.storageManager.exists(testSegmentName, null).get());
        val segmentMetadataAfterDelete = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertNull(segmentMetadataAfterDelete);

        AssertExtensions.assertFutureThrows(
                "getStreamSegmentInfo succeeded on missing segment.",
                testContext.storageManager.getStreamSegmentInfo(testSegmentName, null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Seal succeeded on missing segment.",
                testContext.storageManager.seal(SegmentStorageHandle.writeHandle(testSegmentName), null),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Seal succeeded on missing segment.",
                testContext.storageManager.openWrite(testSegmentName),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Seal succeeded on missing segment.",
                testContext.storageManager.openRead(testSegmentName),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertFutureThrows(
                "Concat succeeded on missing segment.",
                testContext.storageManager.truncate(SegmentStorageHandle.writeHandle(testSegmentName), 0, null),
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
        testContext.storageManager.initialize(2);
        int maxRollingLength = 1;
        int ownerEpoch = 1;
        testContext.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch);

        val hWrite = testContext.storageManager.openWrite(testSegmentName).get();
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
        testContext.storageManager.initialize(2);
        int maxRollingLength = 1;
        int ownerEpoch = 1;
        testContext.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch);

        val hRead = testContext.storageManager.openRead(testSegmentName).get();
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
        testContext.storageManager.initialize(1);
        val h = testContext.storageManager.create(testSegmentName, policy, null).get();
        int writeAt = 0;
        for (int epoch = 2; epoch < 5; epoch++) {
            testContext.storageManager.initialize(epoch);

            val hWrite = testContext.storageManager.openWrite(testSegmentName).get();
            Assert.assertEquals(hWrite.getSegmentName(), testSegmentName);
            Assert.assertFalse(hWrite.isReadOnly());

            testContext.storageManager.write(h, writeAt, new ByteArrayInputStream(new byte[10]), 10, null).join();

            val metadataAfter = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            Assert.assertEquals(epoch, metadataAfter.getOwnerEpoch());
            writeAt += 10;
        }
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, new long[]{10, 10, 10});
        TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);
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
        testContext.storageManager.initialize(ownerEpoch);
        val inserted = testContext.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch - 1, chunks);
        // Set bigger offset
        testContext.addChunk(inserted.getLastChunk(), lastChunkLengthInStorage);
        val hWrite = testContext.storageManager.openWrite(testSegmentName).get();
        Assert.assertEquals(hWrite.getSegmentName(), testSegmentName);
        Assert.assertFalse(hWrite.isReadOnly());

        val metadataAfter = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertEquals(ownerEpoch, metadataAfter.getOwnerEpoch());
        Assert.assertEquals(lastChunkLengthInStorage, metadataAfter.getLength());
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, chunks, lastChunkLengthInStorage);
        TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);
    }

    private void testOpenReadAfterFailover(String testSegmentName, int ownerEpoch, int maxRollingLength, long[] chunks, int lastChunkLengthInStorage) throws Exception {
        TestContext testContext = getTestContext();
        testContext.storageManager.initialize(ownerEpoch);
        val inserted = testContext.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch - 1, chunks);
        // Set bigger offset
        testContext.addChunk(inserted.getLastChunk(), lastChunkLengthInStorage);
        val hRead = testContext.storageManager.openRead(testSegmentName).get();
        Assert.assertEquals(hRead.getSegmentName(), testSegmentName);
        Assert.assertTrue(hRead.isReadOnly());

        val metadataAfter = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertEquals(ownerEpoch, metadataAfter.getOwnerEpoch());
        Assert.assertEquals(lastChunkLengthInStorage, metadataAfter.getLength());
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, chunks, lastChunkLengthInStorage);
        TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);
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
        testContext.storageManager.seal(h2, null).join();
        testContext.storageManager.concat(h1, (long) nChunks1 * (long) maxChunkLength, sourceSegmentName, null).join();

        // Validate.
        TestUtils.checkSegmentLayout(testContext.metadataStore, targetSegmentName, maxChunkLength, nChunks1 + nChunks2);
        TestUtils.checkSegmentBounds(testContext.metadataStore, targetSegmentName, 0, ((long) nChunks1 + (long) nChunks2) * maxChunkLength);
        TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, targetSegmentName);

    }

    @Test
    public void testSimpleConcatWithDefrag() throws Exception {
        TestContext testContext = getTestContext();
        ((AbstractInMemoryChunkStorage) testContext.storageProvider).setShouldSupportConcat(true);
        ((AbstractInMemoryChunkStorage) testContext.storageProvider).setShouldSupportAppend(true);

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
        testContext.storageManager.seal(SegmentStorageHandle.writeHandle("source"), null).join();
        val sourceInfo = testContext.storageManager.getStreamSegmentInfo("target", null).join();
        val targetInfo = testContext.storageManager.getStreamSegmentInfo("target", null).join();
        testContext.storageManager.concat(SegmentStorageHandle.writeHandle("target"),
                targetInfo.getLength(),
                "source",
                null).join();

        // Validate.
        TestUtils.checkSegmentLayout(testContext.metadataStore, "target", resultLayout);
        TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, "target");

        // Cleanup
        testContext.storageManager.delete(SegmentStorageHandle.writeHandle("target"), null).join();
    }

    private void testBaseConcat(TestContext testContext, int maxRollingLength, long[] targetLayout, long offset, long[] sourceLayout, long[] resultLayout) throws Exception {
        try {
            val source = testContext.insertMetadata("source", maxRollingLength, 1, sourceLayout);
            val target = testContext.insertMetadata("target", maxRollingLength, 1, targetLayout);

            // Concat.
            testContext.storageManager.seal(SegmentStorageHandle.writeHandle("source"), null).join();
            val sourceInfo = testContext.storageManager.getStreamSegmentInfo("target", null).join();
            val targetInfo = testContext.storageManager.getStreamSegmentInfo("target", null).join();
            testContext.storageManager.concat(SegmentStorageHandle.writeHandle("target"),
                    offset,
                    "source",
                    null).join();

            // Validate.
            TestUtils.checkSegmentLayout(testContext.metadataStore, "target", resultLayout);
            TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, "target");

        } finally {
            // Cleanup
            testContext.storageManager.delete(SegmentStorageHandle.writeHandle("target"), null).join();
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
        testContext.storageManager.seal(SegmentStorageHandle.writeHandle(validSourceSegmentName), null);

        val invalidSourceSegment = testContext.insertMetadata(truncatedSource, 1024, 1, new long[]{25});
        testContext.storageManager.truncate(SegmentStorageHandle.writeHandle(truncatedSource), 1, TIMEOUT);
        testContext.storageManager.seal(SegmentStorageHandle.writeHandle(truncatedSource), null);

        val unsealedSourceSegment = testContext.insertMetadata(unsealedSourceSegmentName, 1024, 1, new long[]{25});

        int validStart = 10;
        int validEnd = 25;
        val hWrite = testContext.storageManager.openWrite(targetSegmentName).get();
        testContext.storageManager.truncate(hWrite, validStart, TIMEOUT);

        val h = testContext.storageManager.openWrite(targetSegmentName).get();

        // IllegalArgumentException
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(h, -1, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(null, validEnd, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(h, validEnd, null, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // BadOffsetException
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(h, validEnd + 1, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof BadOffsetException);

        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(h, validEnd - 1, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof BadOffsetException);

        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(h, 0, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof BadOffsetException);

        // BadOffsetException
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(h, validEnd + 1, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof BadOffsetException);

        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(h, validEnd - 1, validSourceSegmentName, TIMEOUT),
                ex -> ex instanceof BadOffsetException);

        // Not sealed
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(h, validEnd, unsealedSourceSegmentName, TIMEOUT),
                ex -> ex instanceof IllegalStateException);

        // StreamSegmentTruncatedException
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(h, validEnd, truncatedSource, TIMEOUT),
                ex -> ex instanceof StreamSegmentTruncatedException);

        // StreamSegmentTruncatedException
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(h, validEnd, "nonExistent", TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Seal target segment
        testContext.storageManager.seal(h, TIMEOUT).join();
        AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                testContext.storageManager.concat(h, validEnd, truncatedSource, TIMEOUT),
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

        val hWrite = testContext.storageManager.openWrite(testSegmentName).get();
        testContext.storageManager.truncate(hWrite, endOffset, null);

        val info = testContext.storageManager.getStreamSegmentInfo(testSegmentName, TIMEOUT).get();
        Assert.assertEquals(endOffset, info.getStartOffset());
        Assert.assertEquals(info.getLength(), info.getStartOffset());

        val hTarget = testContext.storageManager.openWrite(testSegmentName).get();
        testContext.storageManager.truncate(hTarget, endOffset, null);
        testContext.storageManager.truncate(hTarget, endOffset, null);

        byte[] bytes = new byte[0];

        // Read should fail
        AssertExtensions.assertFutureThrows(
                "read succeeded on invalid offset.",
                testContext.storageManager.read(hTarget, endOffset, bytes, 0, 0, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        testContext.storageManager.write(hTarget, endOffset, new ByteArrayInputStream(bytes), 0, TIMEOUT).join();
        val info2 = testContext.storageManager.getStreamSegmentInfo(testSegmentName, TIMEOUT).get();
        Assert.assertEquals(endOffset, info2.getStartOffset());
        Assert.assertEquals(info2.getLength(), info2.getStartOffset());

        // Create an empty segment and concat it
        val hSource = testContext.storageManager.create(emptySegmentName, TIMEOUT).get();
        val infoEmpty = testContext.storageManager.getStreamSegmentInfo(emptySegmentName, TIMEOUT).get();
        Assert.assertEquals(0, infoEmpty.getStartOffset());
        Assert.assertEquals(infoEmpty.getLength(), infoEmpty.getStartOffset());
        testContext.storageManager.seal(hSource, TIMEOUT).get();

        testContext.storageManager.concat(hTarget, endOffset, emptySegmentName, TIMEOUT).get();

        // Now concat non-empty
        testContext.storageManager.seal(SegmentStorageHandle.writeHandle(nonEmptySegmentName), TIMEOUT).get();
        testContext.storageManager.concat(hTarget, endOffset, nonEmptySegmentName, TIMEOUT).get();
        val info4 = testContext.storageManager.getStreamSegmentInfo(testSegmentName, TIMEOUT).get();
        Assert.assertEquals(endOffset + endOffset, info4.getLength());
        Assert.assertEquals(endOffset, info4.getStartOffset());
    }

    @Test
    public void testBasicConcatWithDefrag() throws Exception {
        TestContext testContext = getTestContext();
        ((AbstractInMemoryChunkStorage) testContext.storageProvider).setShouldSupportAppend(true);
        ((AbstractInMemoryChunkStorage) testContext.storageProvider).setShouldSupportConcat(true);

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
        ((AbstractInMemoryChunkStorage) testContext.storageProvider).setShouldSupportConcat(true);

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
        ((AbstractInMemoryChunkStorage) testContext.storageProvider).setShouldSupportConcat(true);

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
        val h = testContext.storageManager.openWrite(testSegmentName).get();
        testContext.storageManager.truncate(h, validStart, null);

        // Invalid parameters
        AssertExtensions.assertFutureThrows("truncate() allowed for invalid parameters",
                testContext.storageManager.truncate(h, -1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("truncate() allowed for invalid parameters",
                testContext.storageManager.truncate(null, 11, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("truncate() allowed for invalid parameters",
                testContext.storageManager.truncate(SegmentStorageHandle.readHandle(testSegmentName), 11, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("truncate() allowed for invalid parameters",
                testContext.storageManager.truncate(h, validEnd + 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertFutureThrows("truncate() allowed for invalid parameters",
                testContext.storageManager.truncate(h, validStart - 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // Sealed segment
        testContext.storageManager.seal(h, TIMEOUT).join();
        AssertExtensions.assertFutureThrows("write() allowed for invalid parameters",
                testContext.storageManager.truncate(h, 11, TIMEOUT),
                ex -> ex instanceof StreamSegmentSealedException);
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
            testContext.storageManager.truncate(h1, truncateAt, null).join();
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, 3, 3 - (truncateAt / 3));
            TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, truncateAt, 9);
            TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);

            val metadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            // length doesn't change.
            Assert.assertEquals(9, metadata.getLength());
            // start offset should match i.
            Assert.assertEquals(truncateAt, metadata.getStartOffset());
            // Each time the first offest is multiple of 3
            Assert.assertEquals(3 * (truncateAt / 3), metadata.getFirstChunkStartOffset());

            // try to read some bytes.
            val bytesRead = testContext.storageManager.read(h1, truncateAt, buffer, 0, 9 - truncateAt, null).get().intValue();
            Assert.assertEquals(9 - truncateAt, bytesRead);
            if (truncateAt > 0) {
                AssertExtensions.assertFutureThrows(
                        "read succeeded on missing segment.",
                        testContext.storageManager.read(h1, truncateAt - 1, buffer, 0, 9 - truncateAt, null),
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
            testContext.storageManager.truncate(h1, truncateAt, null).join();

            // Check layout.
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, new long[]{10});
            TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);

            // Validate.
            val metadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            Assert.assertEquals(10, metadata.getLength());
            Assert.assertEquals(truncateAt, metadata.getStartOffset());
            Assert.assertEquals(0, metadata.getFirstChunkStartOffset());

            // validate read.
            val bytesRead = testContext.storageManager.read(h1, truncateAt, buffer, 0, 10 - truncateAt, null).get().intValue();
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
            testContext.storageManager.truncate(h1, truncateAt, null).join();

            // Check layout.
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, new long[]{10});
            TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);

            // Validate info
            val metadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            Assert.assertEquals(10, metadata.getLength());
            Assert.assertEquals(truncateAt, metadata.getStartOffset());
            Assert.assertEquals(0, metadata.getFirstChunkStartOffset());
            TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);

            // Validate read.
            val bytesRead = testContext.storageManager.read(h1, truncateAt, buffer, 0, 10 - truncateAt, null).get().intValue();
            Assert.assertEquals(10 - truncateAt, bytesRead);
        }
    }

    @Test
    public void testRepeatedTruncatesAtFullLength() throws Exception {
        TestContext testContext = getTestContext();
        String testSegmentName = "testSegmentName";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2);
        // create.
        val h1 = testContext.storageManager.create(testSegmentName, policy, null).get();
        long expectedLength = 0;
        for (int i = 1; i < 5; i++) {
            val info1 = testContext.storageManager.getStreamSegmentInfo(testSegmentName, null).get();
            Assert.assertEquals(expectedLength, info1.getLength());
            Assert.assertEquals(info1.getLength(), info1.getStartOffset());

            // Write some data.
            byte[] buffer = new byte[i];
            testContext.storageManager.write(h1, info1.getStartOffset(), new ByteArrayInputStream(buffer), buffer.length, null).join();
            val info2 = testContext.storageManager.getStreamSegmentInfo(testSegmentName, null).get();
            expectedLength += i;
            Assert.assertEquals(expectedLength, info2.getLength());

            // Now truncate
            testContext.storageManager.truncate(h1, info2.getLength(), null).join();

            // Validate info
            val metadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            Assert.assertEquals(expectedLength, metadata.getLength());
            Assert.assertEquals(expectedLength, metadata.getStartOffset());
            Assert.assertEquals(expectedLength, metadata.getFirstChunkStartOffset());
            Assert.assertEquals(expectedLength, metadata.getLastChunkStartOffset());
            Assert.assertEquals(null, metadata.getLastChunk());
            Assert.assertEquals(null, metadata.getFirstChunk());
            TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);

            // Validate Exceptions.
            val expectedLength2 = expectedLength;
            val h = testContext.storageManager.openRead(testSegmentName).get();
            AssertExtensions.assertFutureThrows(
                    "read succeeded on invalid offset.",
                    testContext.storageManager.read(h, expectedLength - 1, new byte[1], 0, 1, null),
                    ex -> ex instanceof StreamSegmentTruncatedException && ((StreamSegmentTruncatedException) ex).getStartOffset() == expectedLength2);
            AssertExtensions.assertFutureThrows(
                    "read succeeded on invalid offset.",
                    testContext.storageManager.read(h, expectedLength, new byte[1], 0, 1, null),
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
        testContext.storageManager.truncate(h1, truncateAt, null).join();

        // Check layout.
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, maxChunkLength, chunksCountAfter);
        TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, truncateAt, expectedLength);
        TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, testSegmentName);
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
        testContext.storageManager.create(testSegmentName, null).get();

        // Write some data.
        long writeAt = 0;
        long epoch = CONTAINER_ID;
        ArrayList<Long> lengths = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            // Create a new test context and initialize with new epoch.
            val hWrite =  testContext.storageManager.openWrite(testSegmentName).get();
            testContext.storageManager.write(hWrite, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
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
        testContext.storageManager.create(testSegmentName, null).get();

        // Write some data.
        long writeAt = 0;
        long epoch = CONTAINER_ID;
        SegmentHandle hWrite =  testContext.storageManager.openWrite(testSegmentName).get();
        ArrayList<Long> lengths = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            // Create a new test context and initialize with new epoch.
            testContext.storageManager.write(hWrite, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
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
            hWrite =  testContext.storageManager.openWrite(testSegmentName).get();

            // Write some garbage
            oldTestCotext.storageManager.write(hWrite, writeAt, new ByteArrayInputStream(new byte[10]), 10, null).join();

            // Fence out old store.
            boolean exceptionThrown = false;
            oldTestCotext.metadataStore.markFenced();

            AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                    oldTestCotext.storageManager.write(hWrite, writeAt + 10, new ByteArrayInputStream(new byte[10]), 10, null),
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
        testContext.storageManager.create(testSegmentName, null).get();

        // Write some data.
        long writeAt = 0;
        long truncateAt = 0;
        long epoch = CONTAINER_ID;
        SegmentHandle hWrite =  testContext.storageManager.openWrite(testSegmentName).get();
        ArrayList<Long> lengths = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            // Truncate and Read in same epoch.
            testContext.storageManager.truncate(hWrite, truncateAt, null).get();
            checkDataRead(testSegmentName, testContext, truncateAt, writeAt);
            TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, truncateAt, writeAt);

            // Create a new test context and initialize with new epoch.
            testContext.storageManager.write(hWrite, writeAt, new ByteArrayInputStream(new byte[i]), i, null).join();
            writeAt += i;
            lengths.add((long) i);
            TestUtils.checkSegmentBounds(testContext.metadataStore, testSegmentName, truncateAt, writeAt);
            checkDataRead(testSegmentName, testContext, truncateAt, writeAt);

            truncateAt++;

            // Fork the context.
            val oldTestCotext = testContext;
            testContext = oldTestCotext.fork(epoch++);

            // Make sure to open segment with new instance before writing garbage to old instance.
            hWrite =  testContext.storageManager.openWrite(testSegmentName).get();

            // Write some garbage
            oldTestCotext.storageManager.write(hWrite, writeAt, new ByteArrayInputStream(new byte[10]), 10, null).join();

            // Fence out old store.
            boolean exceptionThrown = false;
            oldTestCotext.metadataStore.markFenced();

            AssertExtensions.assertFutureThrows("conact() allowed for invalid parameters",
                    oldTestCotext.storageManager.write(hWrite, writeAt + 10, new ByteArrayInputStream(new byte[10]), 10, null),
                    ex -> ex instanceof StorageNotPrimaryException);
            // Read in new epoch.
            checkDataRead(testSegmentName, testContext, truncateAt, writeAt);
        }

        int total = 10;

        // Create a new test context and initialize with new epoch.
        testContext = testContext.fork(epoch++);

        checkDataRead(testSegmentName, testContext, truncateAt, total);
    }

    private void checkDataRead(String testSegmentName, TestContext testContext, long offset, long length) throws InterruptedException, java.util.concurrent.ExecutionException {
        val hRead = testContext.storageManager.openRead(testSegmentName).get();

        // Read all bytes at once.
        long size = Math.toIntExact(length - offset);
        byte[] output = new byte[Math.toIntExact(length - offset)];
        int bufferOffset = 0;
        int bytesRead = 0;
        while (bytesRead < size) {
            bytesRead += testContext.storageManager.read(hRead, offset, output, bufferOffset, Math.toIntExact(size), null).get();
        }
        Assert.assertEquals(size, bytesRead);
    }

    private SegmentHandle populateSegment(TestContext testContext, String targetSegmentName, long maxChunkLength, int numberOfchunks) throws Exception {
        SegmentRollingPolicy policy = new SegmentRollingPolicy(maxChunkLength); // Force rollover after each byte.
        // Create segment
        val h = testContext.storageManager.create(targetSegmentName, policy, null).get();

        // Write some data.
        long dataSize = numberOfchunks * maxChunkLength;
        testContext.storageManager.write(h, 0, new ByteArrayInputStream(new byte[Math.toIntExact(dataSize)]), Math.toIntExact(dataSize), null).join();

        TestUtils.checkSegmentLayout(testContext.metadataStore, targetSegmentName, maxChunkLength, numberOfchunks);
        TestUtils.checkSegmentBounds(testContext.metadataStore, targetSegmentName, 0, dataSize);
        TestUtils.checkChunksExistInStorage(testContext.storageProvider, testContext.metadataStore, targetSegmentName);
        return h;
    }

    /**
     * Test context.
     */
    public static class TestContext {
        @Getter
        protected ChunkedSegmentStorageConfig config;

        @Getter
        protected ChunkStorage storageProvider;

        @Getter
        protected ChunkMetadataStore metadataStore;

        @Getter
        protected ChunkedSegmentStorage storageManager;

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
            storageProvider = createChunkStorageProvider();
            metadataStore = createMetadataStore();
            storageManager = new ChunkedSegmentStorage(storageProvider, metadataStore, this.executor, config);
            storageManager.initialize(1);
        }

        /**
         * Creates a fork of current context with same chunk storage but forked metadata store.
         * This simulates multiple segment store instances writing to same storage but different states. (Eg After failover)
         */
        public TestContext fork(long epoch) throws Exception {
            val forkedContext = createNewInstance();
            forkedContext.executor = Preconditions.checkNotNull(this.executor);
            forkedContext.storageProvider = Preconditions.checkNotNull(this.storageProvider);
            forkedContext.config = Preconditions.checkNotNull(this.config);
            // This will create a copy of metadata store
            forkedContext.metadataStore = getForkedMetadataStore();

            // Use the same same chunk storage, but different metadata store to simulate multiple zombie instances
            // writing to the same underlying storage.
            forkedContext.storageManager = new ChunkedSegmentStorage(this.storageProvider,
                    forkedContext.metadataStore,
                    this.executor,
                    this.config);
            forkedContext.storageManager.initialize(epoch);
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
            return new InMemoryMetadataStore();
        }

        /**
         * Gets {@link ChunkStorage} to use for the tests.
         */
        public ChunkStorage createChunkStorageProvider() throws Exception {
            return new NoOpChunkStorage();
        }

        /**
         * Creates and inserts metadata for a test segment.
         */
        public SegmentMetadata insertMetadata(String testSegmentName, int maxRollingLength, int ownerEpoch) throws Exception {
            try (val txn = metadataStore.beginTransaction()) {
                SegmentMetadata segmentMetadata = SegmentMetadata.builder()
                        .maxRollinglength(maxRollingLength)
                        .name(testSegmentName)
                        .ownerEpoch(ownerEpoch)
                        .build();
                segmentMetadata.setActive(true);
                txn.create(segmentMetadata);
                txn.commit();
                return segmentMetadata;
            }
        }

        /**
         * Creates and inserts metadata for a test segment.
         */
        public SegmentMetadata insertMetadata(String testSegmentName, long maxRollingLength, int ownerEpoch, long[] chunkLengths) throws Exception {
            try (val txn = metadataStore.beginTransaction()) {
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

                txn.commit();
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
            ((AbstractInMemoryChunkStorage) storageProvider).addChunk(chunkName, length);
        }
    }
}

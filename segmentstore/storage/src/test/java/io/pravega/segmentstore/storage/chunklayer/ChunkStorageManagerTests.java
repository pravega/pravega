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

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.mocks.AbstractInMemoryChunkStorageProvider;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.segmentstore.storage.noop.NoOpChunkStorageProvider;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
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
import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Unit tests for {@link ChunkStorageManager}.
 * The focus is on testing the ChunkStorageManager implementation itself very thoroughly.
 * It uses either {@link NoOpChunkStorageProvider} as {@link ChunkStorageProvider}.
 */
@Slf4j
public class ChunkStorageManagerTests extends ThreadPooledTestSuite {
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

    public ChunkStorageProvider getChunkStorageProvider() throws Exception {
        return new NoOpChunkStorageProvider(executorService());
    }

    public TestContext getTestContext()  throws Exception {
        return new TestContext(executorService());
    }

    /**
     * Test initialization.
     * @throws Exception
     */
    @Test
    public void testInitialization() throws Exception {
        val storageProvider = getChunkStorageProvider();
        val metadataStore = new InMemoryMetadataStore();
        val policy = SegmentRollingPolicy.NO_ROLLING;
        val storageManager = new ChunkStorageManager(storageProvider,  executorService(), policy);
        val systemJournal = new SystemJournal(42, 1, storageProvider, metadataStore, policy);
        storageManager.initialize(1);

        Assert.assertNull(storageManager.getMetadataStore());
        Assert.assertEquals(storageProvider, storageManager.getChunkStorage());
        Assert.assertEquals(policy, storageManager.getDefaultRollingPolicy());
        Assert.assertEquals(1, storageManager.getEpoch());

        storageManager.initialize(42, metadataStore, systemJournal);
        Assert.assertEquals(metadataStore, storageManager.getMetadataStore());
        Assert.assertEquals(storageProvider, storageManager.getChunkStorage());
        Assert.assertEquals(policy, storageManager.getDefaultRollingPolicy());
        Assert.assertEquals(1, storageManager.getEpoch());
        Assert.assertEquals(42, storageManager.getContainerId());
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
                testContext.storageManager.concat(SegmentStorageHandle.readHandle(testSegmentName), 0, "inexistent", null),
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
        int ownerEpoch = 100;
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
                testContext.storageManager.concat(SegmentStorageHandle.readHandle(testSegmentName), 0, "source", null),
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
        testContext.insertMetadata(testSegmentName, maxRollingLength, 100);
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
                testContext.storageManager.concat(SegmentStorageHandle.readHandle(testSegmentName), 0, "source", null),
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
        testContext.insertMetadata(testSegmentName, maxRollingLength, 100);
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
     * Test simple scenario.
     * @throws Exception
     */
    @Test
    public void testSimpleScenario() throws Exception {
        String testSegmentName = "foo";
        SegmentRollingPolicy policy = new SegmentRollingPolicy(2); // Force rollover after every 5 byte.
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
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, 2, 5, 0, 10);

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
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, 2, 7, 0, 14);

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
     * @throws Exception
     */
    @Test
    public void testRead() throws Exception {
        String testSegmentName = "foo";
        TestContext testContext = getTestContext();
        // Setup a segment with 5 chunks with given lengths.
        val segment = testContext.insertMetadata(testSegmentName, 1024, 1,
                new long[] { 1, 2, 3, 4, 5});

        int total = 15;

        val h = testContext.storageManager.openRead(testSegmentName).get();

        // Read all bytes at once.
        byte[] output = new byte[total];
        int bytesRead = testContext.storageManager.read(h, 0, output, 0, total, null).get();
        Assert.assertEquals(total, bytesRead);

        // Read bytes at varying lengths but same starting offset.
        for (int i = 0; i < 15; i++) {
            bytesRead = testContext.storageManager.read(h, 0, output, 0,  i, null).get();
            Assert.assertEquals(i, bytesRead);
        }

        // Read bytes at varying lengths and different offsets.
        for (int i = 0; i < 15; i++) {
            bytesRead = testContext.storageManager.read(h, 15 - i - 1, output, 0,  i, null).get();
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
     * Test various operations on deleted segment.
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
    }

    /**
     * Test failover scenario on empty segment.
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

    }

    /**
     * Test failover scenario.
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
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, new long[] {10, 10, 10});
    }

    /**
     * Test failover scenario for segment with only one chunk.
     * @throws Exception
     */
    @Test
    public void testOpenWriteAfterFailoverWithSingleChunk() throws Exception {
        String testSegmentName = "foo";
        int ownerEpoch = 2;
        int maxRollingLength = 100;
        long[] chunks = new long[] { 10 };
        int lastChunkLengthInStorage = 12;

        testOpenWriteAfterFailover(testSegmentName, ownerEpoch, maxRollingLength, chunks, lastChunkLengthInStorage);

    }

    private void testOpenWriteAfterFailover(String testSegmentName, int ownerEpoch, int maxRollingLength, long[] chunks, int lastChunkLengthInStorage) throws Exception {
        TestContext testContext = getTestContext();
        testContext.storageManager.initialize(ownerEpoch);
        val inserted = testContext.insertMetadata(testSegmentName, maxRollingLength, ownerEpoch-1, chunks);
        // Set bigger offset
        testContext.addChunk(inserted.getLastChunk(), lastChunkLengthInStorage);
        val hWrite = testContext.storageManager.openWrite(testSegmentName).get();
        Assert.assertEquals(hWrite.getSegmentName(), testSegmentName);
        Assert.assertFalse(hWrite.isReadOnly());

        val metadataAfter = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
        Assert.assertEquals(ownerEpoch, metadataAfter.getOwnerEpoch());
        Assert.assertEquals(lastChunkLengthInStorage, metadataAfter.getLength());
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, chunks, lastChunkLengthInStorage);
    }

    /**
     * Test simple concat.
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
        TestUtils.checkSegmentLayout(testContext.metadataStore, targetSegmentName, maxChunkLength, nChunks1 + nChunks2, 0, ((long) nChunks1 + (long) nChunks2) * maxChunkLength);
    }

    @Test
    public void testSimpleConcatWithDefrag() throws Exception {
        TestContext testContext = getTestContext();
        ((AbstractInMemoryChunkStorageProvider) testContext.storageProvider).setShouldSupportConcat(true);

        for (int maxChunkLength = 1; maxChunkLength <= 3; maxChunkLength++) {
            testSimpleConcat(testContext, maxChunkLength, 1, 1);
            testSimpleConcat(testContext, maxChunkLength, 1, 2);
            testSimpleConcat(testContext, maxChunkLength, 2, 1);
            testSimpleConcat(testContext, maxChunkLength, 2, 2);
            testSimpleConcat(testContext, maxChunkLength, 3, 3);
        }
    }

    @Test
    public void testBasicConcatWithDefrag() throws Exception {
        TestContext testContext = getTestContext();
        ((AbstractInMemoryChunkStorageProvider) testContext.storageProvider).setShouldSupportConcat(true);

        // Populate segments
        val source = testContext.insertMetadata("source", 1024, 1,
                new long[] { 1, 2, 3, 4, 5});
        val target = testContext.insertMetadata("target", 1024, 1,
                new long[] {10});

        // Concat.
        testContext.storageManager.seal(SegmentStorageHandle.writeHandle("source"), null).join();
        testContext.storageManager.concat(SegmentStorageHandle.writeHandle("target"),
                0,
                "source",
                null).join();

        // Validate.
        TestUtils.checkSegmentLayout(testContext.metadataStore, "target", new long[] {25});
    }

    @Test
    public void testSimpleTruncate() throws Exception {
        testTruncate(1, 5, 10, 5, 10);
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
            TestUtils.checkSegmentLayout(testContext.metadataStore,  testSegmentName, 3,  3 - (truncateAt / 3), truncateAt, 9);
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
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, new long[] { 10 });

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
            TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, new long[] { 10 });

            // Validate info
            val metadata = TestUtils.getSegmentMetadata(testContext.metadataStore, testSegmentName);
            Assert.assertEquals(10, metadata.getLength());
            Assert.assertEquals(truncateAt, metadata.getStartOffset());
            Assert.assertEquals(0, metadata.getFirstChunkStartOffset());

            // Validate read.
            val bytesRead = testContext.storageManager.read(h1, truncateAt, buffer, 0, 10 - truncateAt, null).get().intValue();
            Assert.assertEquals(10 - truncateAt, bytesRead);
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
        TestUtils.checkSegmentLayout(testContext.metadataStore, testSegmentName, maxChunkLength, chunksCountAfter, truncateAt, expectedLength);
    }



    private SegmentHandle populateSegment(TestContext testContext, String targetSegmentName, long maxChunkLength, int numberOfchunks) throws Exception {
        SegmentRollingPolicy policy = new SegmentRollingPolicy(maxChunkLength); // Force rollover after each byte.
        // Create segment
        val h = testContext.storageManager.create(targetSegmentName, policy, null).get();

        // Write some data.
        long dataSize = numberOfchunks * maxChunkLength;
        testContext.storageManager.write(h, 0, new ByteArrayInputStream(new byte[Math.toIntExact(dataSize)]), Math.toIntExact(dataSize), null).join();

        TestUtils.checkSegmentLayout(testContext.metadataStore, targetSegmentName, maxChunkLength, numberOfchunks, 0, dataSize);
        return h;
    }

    /**
     * Test context.
     */
    public static class TestContext {
        ChunkStorageProvider storageProvider;
        ChunkMetadataStore metadataStore;
        ChunkStorageManager storageManager;

        Executor executor;

        public TestContext(Executor executor)  throws Exception {
            this.executor = executor;
            storageProvider = getChunkStorageProvider();
            metadataStore = new InMemoryMetadataStore();
            storageManager = new ChunkStorageManager(storageProvider, metadataStore, this.executor, SegmentRollingPolicy.NO_ROLLING);
            storageManager.initialize(1);
        }

        /**
         * Gets {@link ChunkStorageProvider} to use for the tests.
         */
        public ChunkStorageProvider getChunkStorageProvider()  throws Exception {
            return new NoOpChunkStorageProvider(this.executor);
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
                metadataStore.create(txn, segmentMetadata);
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
                for (int i = 0; i < chunkLengths.length; i++) {
                    String chunkName = testSegmentName + "_chunk_" + Integer.toString(i);
                    ChunkMetadata chunkMetadata = ChunkMetadata.builder()
                            .name(chunkName)
                            .length(chunkLengths[i])
                            .nextChunk(i == chunkLengths.length -1 ? null : testSegmentName + "_chunk_" + Integer.toString(i + 1))
                            .build();
                    length += chunkLengths[i];
                    metadataStore.create(txn, chunkMetadata);

                    addChunk(chunkName, chunkLengths[i]);
                }

                // Fix the first and last
                if (chunkLengths.length > 0) {
                    firstChunk = testSegmentName + "_chunk_0";
                    lastChunk = testSegmentName + "_chunk_" + Integer.toString(chunkLengths.length -1);
                    startOfLast = length - chunkLengths[chunkLengths.length -1];
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
                metadataStore.create(txn, segmentMetadata);

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
         * Adds chunk of specified length to the underlying {@link ChunkStorageProvider}.
         */
        public void addChunk(String chunkName, long length) {
            ((AbstractInMemoryChunkStorageProvider) storageProvider).addChunk(chunkName, length);
        }
   }
}

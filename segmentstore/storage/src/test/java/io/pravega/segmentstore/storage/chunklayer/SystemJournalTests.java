/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.metadata.ChunkMetadata;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.metadata.SegmentMetadata;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorageProvider;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.val;
import lombok.var;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.util.ArrayList;

/**
 * Tests for testing bootstrap functionality with {@link SystemJournal}.
 */
public class SystemJournalTests extends ThreadPooledTestSuite {
    public static final String SYSTEM_SEGMENT_NAME = "test_system_segment";

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

    protected ChunkMetadataStore getMetadataStore() throws Exception {
        return new InMemoryMetadataStore();
    }

    protected ChunkStorageProvider getChunkStorageProvider() throws Exception {
        return new InMemoryChunkStorageProvider();
    }

    protected String[] getSystemSegments(String systemSegmentName) {
        return new String[]{systemSegmentName};
    }

    @Test
    public void testInitialization() throws Exception {
        ChunkStorageProvider storageProvider = getChunkStorageProvider();
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = 42;
        int maxLength = 8;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = ChunkStorageManagerConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        // Init
        SystemJournal journal = new SystemJournal(containerId, epoch, storageProvider, metadataStore, config);

        Assert.assertEquals(epoch, journal.getEpoch());
        Assert.assertEquals(containerId, journal.getContainerId());
        Assert.assertEquals(policy.getMaxLength(), journal.getConfig().getDefaultRollingPolicy().getMaxLength());
        Assert.assertEquals(epoch, journal.getEpoch());
        Assert.assertEquals(0, journal.getCurrentFileIndex());

        Assert.assertEquals(NameUtils.INTERNAL_SCOPE_NAME, journal.getSystemSegmentsPrefix());
        Assert.assertArrayEquals(SystemJournal.getChunkStorageSystemSegments(containerId), journal.getSystemSegments());
        journal.initialize();
    }

    @Test
    public void testInitializationInvalidArgs() throws Exception {
        ChunkStorageProvider storageProvider = getChunkStorageProvider();
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = 42;
        int maxLength = 8;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = ChunkStorageManagerConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        AssertExtensions.assertThrows("Should not allow null storageProvider",
                () -> new SystemJournal(containerId, epoch, null, metadataStore, config),
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null metadataStore",
                () -> new SystemJournal(containerId, epoch, storageProvider, null, config),
                ex -> ex instanceof NullPointerException);

        AssertExtensions.assertThrows("Should not allow null policy",
                () -> new SystemJournal(containerId, epoch, storageProvider, metadataStore, null),
                ex -> ex instanceof NullPointerException);

    }

    @Test
    public void testIsSystemSegment() throws Exception {
        ChunkStorageProvider storageProvider = getChunkStorageProvider();
        ChunkMetadataStore metadataStore = getMetadataStore();
        int containerId = 42;
        int maxLength = 8;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = ChunkStorageManagerConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();
        val journal = new SystemJournal(containerId, epoch, storageProvider, metadataStore, config);
        Assert.assertFalse(journal.isStorageSystemSegment("foo"));

        Assert.assertTrue(journal.isStorageSystemSegment(NameUtils.getStorageMetadataSegmentName(containerId)));
        Assert.assertTrue(journal.isStorageSystemSegment(NameUtils.getAttributeSegmentName(NameUtils.getStorageMetadataSegmentName(containerId))));
        Assert.assertTrue(journal.isStorageSystemSegment(NameUtils.getMetadataSegmentName(containerId)));
        Assert.assertTrue(journal.isStorageSystemSegment(NameUtils.getAttributeSegmentName(NameUtils.getMetadataSegmentName(containerId))));

    }

    /**
     * Tests a scenario when there is only one fail over.
     * The test adds a few chunks to the system segments and then fails over.
     * The new instance should read the journal log file and recreate the layout of system segments.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithOneFailover() throws Exception {
        ChunkStorageProvider storageProvider = getChunkStorageProvider();
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();

        String systemSegmentName = SYSTEM_SEGMENT_NAME;
        int containerId = 42;
        int maxLength = 8;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = ChunkStorageManagerConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        // Init
        SystemJournal journalBefore = new SystemJournal(containerId, epoch, storageProvider, metadataStoreBeforeCrash, config);
        journalBefore.setSystemSegmentsPrefix(systemSegmentName);
        journalBefore.setSystemSegments(getSystemSegments(systemSegmentName));

        long offset = 0;

        // Start container with epoch 1
        ChunkStorageManager chunkStorageManager1 = new ChunkStorageManager(storageProvider, executorService(), config);
        chunkStorageManager1.initialize(containerId, metadataStoreBeforeCrash, journalBefore);
        chunkStorageManager1.initialize(epoch);

        // Bootstrap
        journalBefore.bootstrap();
        checkSystemSegmentsLayout(journalBefore);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        var h = chunkStorageManager1.openWrite(systemSegmentName).join();
        val b1 = "Hello".getBytes();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
        offset += b1.length;
        val b2 = " World".getBytes();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(b2), b2.length, null).join();
        offset += b2.length;

        // Step 2
        // Start container with epoch 2
        epoch++;
        SystemJournal journalAfter = new SystemJournal(containerId, epoch, storageProvider, metadataStoreAfterCrash, config);
        journalAfter.setSystemSegments(getSystemSegments(systemSegmentName));
        journalAfter.setSystemSegmentsPrefix(systemSegmentName);

        ChunkStorageManager chunkStorageManager2 = new ChunkStorageManager(storageProvider, executorService(), config);
        chunkStorageManager2.initialize(containerId, metadataStoreAfterCrash, journalAfter);
        chunkStorageManager2.initialize(epoch);

        // Bootstrap
        journalAfter.bootstrap();
        checkSystemSegmentsLayout(journalAfter);

        // Validate
        val info = chunkStorageManager2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length, info.getLength());
        byte[] out = new byte[b1.length + b2.length];
        val hr = chunkStorageManager2.openRead(systemSegmentName).join();
        chunkStorageManager2.read(hr, 0, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }

    /**
     * Tests a scenario when there are two fail overs.
     * The test adds a few chunks to the system segments and then fails over.
     * After fail over the zombie instance continues to write junk data to both system segment and journal file.
     * The new instance should read the journal log file and recreate the layout of system segments.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithTwoFailovers() throws Exception {
        ChunkStorageProvider chunkStorageProvider = getChunkStorageProvider();
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        String systemSegmentName = "test";
        int containerId = 42;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(8);
        val config = ChunkStorageManagerConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        SystemJournal journalBefore = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreBeforeCrash, config);
        journalBefore.setSystemSegmentsPrefix(systemSegmentName);
        journalBefore.setSystemSegments(getSystemSegments(systemSegmentName));
        long offset = 0;

        // Epoch 1
        ChunkStorageManager chunkStorageManager1 = new ChunkStorageManager(chunkStorageProvider, executorService(), config);
        chunkStorageManager1.initialize(containerId, metadataStoreBeforeCrash, journalBefore);
        chunkStorageManager1.initialize(epoch);

        // Bootstrap
        journalBefore.bootstrap();
        checkSystemSegmentsLayout(journalBefore);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        var h = chunkStorageManager1.openWrite(systemSegmentName).join();
        val b1 = "Hello".getBytes();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
        offset += b1.length;

        checkSystemSegmentsLayout(journalBefore);

        // Epoch 2
        epoch++;
        SystemJournal journalAfter = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreAfterCrash, config);
        journalAfter.setSystemSegments(getSystemSegments(systemSegmentName));
        journalAfter.setSystemSegmentsPrefix(systemSegmentName);

        ChunkStorageManager chunkStorageManager2 = new ChunkStorageManager(chunkStorageProvider, executorService(), config);
        chunkStorageManager2.initialize(containerId, metadataStoreAfterCrash, journalAfter);
        chunkStorageManager2.initialize(epoch);

        // Bootstrap
        journalAfter.bootstrap();
        checkSystemSegmentsLayout(journalAfter);
        var h2 = chunkStorageManager2.openWrite(systemSegmentName).join();

        // Write Junk Data to from first instance.
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream("junk".getBytes()), 4, null).join();

        val b2 = " World".getBytes();
        chunkStorageManager2.write(h2, offset, new ByteArrayInputStream(b2), b2.length, null).join();
        offset += b2.length;

        checkSystemSegmentsLayout(journalAfter);
        val info = chunkStorageManager2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length, info.getLength());
        byte[] out = new byte[b1.length + b2.length];
        val hr = chunkStorageManager2.openRead(systemSegmentName).join();
        chunkStorageManager2.read(hr, 0, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }

    /**
     * Tests a scenario when there are multiple fail overs overs.
     * The test adds a few chunks to the system segments and then fails over.
     * After fail over the zombie instances continue to write junk data to both system segment and journal file.
     * The new instance should read the journal log file and recreate the layout of system segments.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithMultipleFailovers() throws Exception {
        ChunkStorageProvider chunkStorageProvider = getChunkStorageProvider();

        String systemSegmentName = "test";
        int containerId = 42;
        long epoch = 0;
        val policy = new SegmentRollingPolicy(100);
        val config = ChunkStorageManagerConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        long offset = 0;
        ChunkStorageManager oldhunkStorageManager = null;
        SegmentHandle oldHandle = null;
        for (int i = 1; i < 10; i++) {
            // Epoch 2
            epoch++;
            ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
            SystemJournal journalInLoop = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreAfterCrash, config);
            journalInLoop.setSystemSegments(getSystemSegments(systemSegmentName));
            journalInLoop.setSystemSegmentsPrefix(systemSegmentName);

            ChunkStorageManager chunkStorageManagerInLoop = new ChunkStorageManager(chunkStorageProvider, executorService(), config);
            chunkStorageManagerInLoop.initialize(containerId, metadataStoreAfterCrash, journalInLoop);
            chunkStorageManagerInLoop.initialize(epoch);

            journalInLoop.bootstrap();
            checkSystemSegmentsLayout(journalInLoop);

            var h = chunkStorageManagerInLoop.openWrite(systemSegmentName).join();

            if (null != oldhunkStorageManager) {
                oldhunkStorageManager.write(oldHandle, offset, new ByteArrayInputStream("junk".getBytes()), 4, null).join();
            }

            val b1 = "Test".getBytes();
            chunkStorageManagerInLoop.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
            offset += b1.length;
            val b2 = Integer.toString(i).getBytes();
            chunkStorageManagerInLoop.write(h, offset, new ByteArrayInputStream(b2), b2.length, null).join();
            offset += b2.length;

            oldhunkStorageManager = chunkStorageManagerInLoop;
            oldHandle = h;
        }

        epoch++;
        ChunkMetadataStore metadataStoreFinal = getMetadataStore();

        SystemJournal journalFinal = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreFinal, config);
        journalFinal.setSystemSegments(getSystemSegments(systemSegmentName));
        journalFinal.setSystemSegmentsPrefix(systemSegmentName);

        ChunkStorageManager chunkStorageManagerFinal = new ChunkStorageManager(chunkStorageProvider, executorService(), config);
        chunkStorageManagerFinal.initialize(containerId, metadataStoreFinal, journalFinal);
        chunkStorageManagerFinal.initialize(epoch);

        journalFinal.bootstrap();
        checkSystemSegmentsLayout(journalFinal);

        val info = chunkStorageManagerFinal.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(offset, info.getLength());
        byte[] out = new byte[Math.toIntExact(offset)];
        val hr = chunkStorageManagerFinal.openRead(systemSegmentName).join();
        chunkStorageManagerFinal.read(hr, 0, out, 0, Math.toIntExact(offset), null).join();
        var expected = "Test1Test2Test3Test4Test5Test6Test7Test8Test9";
        var actual = new String(out);
        Assert.assertEquals(expected, actual);
    }

    /**
     * Tests a scenario when there are multiple fail overs overs.
     * The test adds a few chunks to the system segments and then fails over.
     * After fail over the zombie instances continue to write junk data to both system segment and journal file.
     * The new instance should read the journal log file and recreate the layout of system segments.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithMultipleFailoversWithTruncate() throws Exception {
        ChunkStorageProvider chunkStorageProvider = getChunkStorageProvider();

        String systemSegmentName = "test";
        int containerId = 42;
        long epoch = 0;
        val policy = new SegmentRollingPolicy(1024);
        val config = ChunkStorageManagerConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        long offset = 0;
        long offsetToTruncateAt = 0;
        ChunkStorageManager oldhunkStorageManager = null;
        SegmentHandle oldHandle = null;
        long oldOffset = 0;

        for (int i = 1; i < 10; i++) {
            // Epoch 2
            epoch++;
            ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
            SystemJournal journalInLoop = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreAfterCrash, config);
            journalInLoop.setSystemSegments(getSystemSegments(systemSegmentName));
            journalInLoop.setSystemSegmentsPrefix(systemSegmentName);

            ChunkStorageManager chunkStorageManagerInLoop = new ChunkStorageManager(chunkStorageProvider, executorService(), config);
            chunkStorageManagerInLoop.initialize(containerId, metadataStoreAfterCrash, journalInLoop);
            chunkStorageManagerInLoop.initialize(epoch);

            journalInLoop.bootstrap();
            checkSystemSegmentsLayout(journalInLoop);

            var h = chunkStorageManagerInLoop.openWrite(systemSegmentName).join();

            if (null != oldhunkStorageManager) {
                // Add some junk to previous instance after failover
                oldhunkStorageManager.write(oldHandle, oldOffset, new ByteArrayInputStream("junk".getBytes()), 4, null).join();
            } else {
                // Only first time.
                for (int j = 1; j < 10; j++) {
                    val b0 = "JUNK".getBytes();
                    chunkStorageManagerInLoop.write(h, offset, new ByteArrayInputStream(b0), b0.length, null).join();
                    offset += b0.length;
                }
            }
            offsetToTruncateAt += 4;
            val b1 = "Test".getBytes();
            chunkStorageManagerInLoop.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
            offset += b1.length;
            val b2 = Integer.toString(i).getBytes();
            chunkStorageManagerInLoop.write(h, offset, new ByteArrayInputStream(b2), b2.length, null).join();
            offset += b2.length;

            chunkStorageManagerInLoop.truncate(h, offsetToTruncateAt, null).join();
            TestUtils.checkSegmentBounds(metadataStoreAfterCrash, h.getSegmentName(), offsetToTruncateAt, offset);

            val length = Math.toIntExact(offset - offsetToTruncateAt);
            byte[] readBytes = new byte[length];
            val bytesRead = chunkStorageManagerInLoop.read(h, offsetToTruncateAt, readBytes, 0, length, null).get();
            Assert.assertEquals(length, readBytes.length);

            String s = new String(readBytes);

            //Add some garbage
            if (null != oldhunkStorageManager) {
                oldhunkStorageManager.write(oldHandle, oldOffset + 4, new ByteArrayInputStream("junk".getBytes()), 4, null).join();
            }

            // Save these instances so that you can write some junk after bootstrap.
            oldhunkStorageManager = chunkStorageManagerInLoop;
            oldHandle = h;
            oldOffset = offset;
        }

        epoch++;
        ChunkMetadataStore metadataStoreFinal = getMetadataStore();

        SystemJournal journalFinal = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreFinal, config);
        journalFinal.setSystemSegments(getSystemSegments(systemSegmentName));
        journalFinal.setSystemSegmentsPrefix(systemSegmentName);

        ChunkStorageManager chunkStorageManagerFinal = new ChunkStorageManager(chunkStorageProvider, executorService(), config);
        chunkStorageManagerFinal.initialize(containerId, metadataStoreFinal, journalFinal);
        chunkStorageManagerFinal.initialize(epoch);

        journalFinal.bootstrap();
        checkSystemSegmentsLayout(journalFinal);

        val info = chunkStorageManagerFinal.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(offset, info.getLength());
        Assert.assertEquals(offsetToTruncateAt, info.getStartOffset());
        byte[] out = new byte[Math.toIntExact(offset - offsetToTruncateAt)];
        val hr = chunkStorageManagerFinal.openRead(systemSegmentName).join();
        chunkStorageManagerFinal.read(hr, offsetToTruncateAt, out, 0, Math.toIntExact(offset - offsetToTruncateAt), null).join();
        var expected = "Test1Test2Test3Test4Test5Test6Test7Test8Test9";
        var actual = new String(out);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testSimpleBootstrapWithTruncateInsideSecondChunk() throws Exception {
        String initialGarbage = "JUNKJUNKJUNK";
        String garbageAfterFailure = "junk";
        String validWriteBeforeFailure = "Hello";
        String validWriteAfterFailure = " World";
        int maxLength = 8;

        testBootstrapWithTruncate(initialGarbage, garbageAfterFailure, validWriteBeforeFailure, validWriteAfterFailure, maxLength);
    }

    @Test
    public void testSimpleBootstrapWithTruncateInsideFirstChunk() throws Exception {
        String initialGarbage = "JUNK";
        String garbageAfterFailure = "junk";
        String validWriteBeforeFailure = "Hello";
        String validWriteAfterFailure = " World";
        int maxLength = 8;

        testBootstrapWithTruncate(initialGarbage, garbageAfterFailure, validWriteBeforeFailure, validWriteAfterFailure, maxLength);
    }

    @Test
    public void testSimpleBootstrapWithTruncateOnChunkBoundary() throws Exception {
        String initialGarbage = "JUNKJUNK";
        String garbageAfterFailure = "junk";
        String validWriteBeforeFailure = "Hello";
        String validWriteAfterFailure = " World";
        int maxLength = 8;

        testBootstrapWithTruncate(initialGarbage, garbageAfterFailure, validWriteBeforeFailure, validWriteAfterFailure, maxLength);
    }

    @Test
    public void testSimpleBootstrapWithTruncateSingleChunk() throws Exception {
        String initialGarbage = "JUNKJUNK";
        String garbageAfterFailure = "junk";
        String validWriteBeforeFailure = "Hello";
        String validWriteAfterFailure = " World";
        int maxLength = 80;

        testBootstrapWithTruncate(initialGarbage, garbageAfterFailure, validWriteBeforeFailure, validWriteAfterFailure, maxLength);
    }

    /**
     * Tests two failures with truncates and zombie stores writing junk data.
     *
     * @param initialGarbageThatIsTruncated Garbage to write that is later truncated away.
     * @param garbageAfterFailure           Garbage to write from the first zombie instance.
     * @param validWriteBeforeFailure       First part of the valid data written before the failure.
     * @param validWriteAfterFailure        Second part of the valid data written after the failure.
     * @param maxLength                     Max length for the segment policy.
     * @throws Exception Throws exception in case of any error.
     */
    private void testBootstrapWithTruncate(String initialGarbageThatIsTruncated, String garbageAfterFailure, String validWriteBeforeFailure, String validWriteAfterFailure, int maxLength) throws Exception {
        ChunkStorageProvider chunkStorageProvider = getChunkStorageProvider();
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        String systemSegmentName = "test";

        int containerId = 42;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);
        val config = ChunkStorageManagerConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        SystemJournal journalBefore = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreBeforeCrash, config);
        journalBefore.setSystemSegmentsPrefix(systemSegmentName);
        journalBefore.setSystemSegments(getSystemSegments(systemSegmentName));
        long offset = 0;

        // Epoch 1
        ChunkStorageManager chunkStorageManager1 = new ChunkStorageManager(chunkStorageProvider, executorService(), config);
        chunkStorageManager1.initialize(containerId, metadataStoreBeforeCrash, journalBefore);
        chunkStorageManager1.initialize(epoch);

        // Bootstrap
        journalBefore.bootstrap();
        checkSystemSegmentsLayout(journalBefore);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        var h = chunkStorageManager1.openWrite(systemSegmentName).join();
        val b1 = validWriteBeforeFailure.getBytes();
        byte[] garbage1 = initialGarbageThatIsTruncated.getBytes();
        int garbage1Length = garbage1.length;
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(garbage1), garbage1Length, null).join();
        offset += garbage1Length;
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
        offset += b1.length;
        chunkStorageManager1.truncate(h, garbage1Length, null).join();

        // Epoch 2
        epoch++;
        SystemJournal journalAfter = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreAfterCrash, config);
        journalAfter.setSystemSegments(getSystemSegments(systemSegmentName));
        journalAfter.setSystemSegmentsPrefix(systemSegmentName);

        ChunkStorageManager chunkStorageManager2 = new ChunkStorageManager(chunkStorageProvider, executorService(), config);
        chunkStorageManager2.initialize(containerId, metadataStoreAfterCrash, journalAfter);
        chunkStorageManager2.initialize(epoch);

        journalAfter.bootstrap();
        checkSystemSegmentsLayout(journalAfter);

        var h2 = chunkStorageManager2.openWrite(systemSegmentName).join();

        // Write Junk Data to both system segment and journal file.
        val innerGarbageBytes = garbageAfterFailure.getBytes();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(innerGarbageBytes), innerGarbageBytes.length, null).join();

        val b2 = validWriteAfterFailure.getBytes();
        chunkStorageManager2.write(h2, offset, new ByteArrayInputStream(b2), b2.length, null).join();
        offset += b2.length;

        val info = chunkStorageManager2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length + garbage1Length, info.getLength());
        Assert.assertEquals(garbage1Length, info.getStartOffset());
        byte[] out = new byte[b1.length + b2.length];
        val hr = chunkStorageManager2.openRead(systemSegmentName).join();
        chunkStorageManager2.read(hr, garbage1Length, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals(validWriteBeforeFailure + validWriteAfterFailure, new String(out));
    }

    /**
     * Tests a scenario when there are two fail overs.
     * The test adds a few chunks to the system segments and then fails over.
     * It also truncates system segments.
     * The new instance should read the journal log file and recreate the layout of system segments.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleBootstrapWithTwoTruncates() throws Exception {
        ChunkStorageProvider chunkStorageProvider = getChunkStorageProvider();
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        String systemSegmentName = "test";
        int containerId = 42;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(8);
        val config = ChunkStorageManagerConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        SystemJournal journalBefore = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreBeforeCrash, config);
        journalBefore.setSystemSegmentsPrefix(systemSegmentName);
        journalBefore.setSystemSegments(getSystemSegments(systemSegmentName));
        long offset = 0;

        // Epoch 1
        ChunkStorageManager chunkStorageManager1 = new ChunkStorageManager(chunkStorageProvider, executorService(), config);
        chunkStorageManager1.initialize(containerId, metadataStoreBeforeCrash, journalBefore);
        chunkStorageManager1.initialize(epoch);

        // Bootstrap
        journalBefore.bootstrap();
        checkSystemSegmentsLayout(journalBefore);

        // Simulate some writes to system segment, this should cause some new chunks being added.
        var h = chunkStorageManager1.openWrite(systemSegmentName).join();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream("JUNKJUNKJUNK".getBytes()), 12, null).join();
        offset += 12;
        val b1 = "Hello".getBytes();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null).join();
        offset += b1.length;
        chunkStorageManager1.truncate(h, 6, null).join();

        // Epoch 2
        epoch++;
        SystemJournal journalAfter = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreAfterCrash, config);
        journalAfter.setSystemSegments(getSystemSegments(systemSegmentName));
        journalAfter.setSystemSegmentsPrefix(systemSegmentName);

        ChunkStorageManager chunkStorageManager2 = new ChunkStorageManager(chunkStorageProvider, executorService(), config);
        chunkStorageManager2.initialize(containerId, metadataStoreAfterCrash, journalAfter);
        chunkStorageManager2.initialize(epoch);

        journalAfter.bootstrap();
        checkSystemSegmentsLayout(journalAfter);

        var h2 = chunkStorageManager2.openWrite(systemSegmentName).join();

        // Write Junk data to both system segment and journal file.
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream("junk".getBytes()), 4, null).join();

        val b2 = " World".getBytes();
        chunkStorageManager2.write(h2, offset, new ByteArrayInputStream(b2), b2.length, null).join();
        offset += b2.length;

        chunkStorageManager2.truncate(h, 12, null).join();

        val info = chunkStorageManager2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length + 12, info.getLength());
        Assert.assertEquals(12, info.getStartOffset());
        byte[] out = new byte[b1.length + b2.length];
        val hr = chunkStorageManager2.openRead(systemSegmentName).join();
        chunkStorageManager2.read(hr, 12, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }

    /**
     * Test simple chunk addition.
     * We failover two times to test correct interaction between snapshot and system logs.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleChunkAddition() throws Exception {
        ChunkStorageProvider chunkStorageProvider = getChunkStorageProvider();
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();

        String systemSegmentName = "test";
        int containerId = 42;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(2);
        val config = ChunkStorageManagerConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        // Inital set of additions
        SystemJournal systemJournalBefore = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreBeforeCrash, config);
        systemJournalBefore.setSystemSegmentsPrefix(systemSegmentName);
        systemJournalBefore.setSystemSegments(getSystemSegments(systemSegmentName));

        systemJournalBefore.bootstrap();

        String lastChunk = null;
        long totalBytesWritten = 0;
        for (int i = 0; i < 10; i++) {
            String newChunk = "chunk" + i;
            val h = chunkStorageProvider.create(newChunk);
            val bytesWritten = chunkStorageProvider.write(h, 0, Math.toIntExact(policy.getMaxLength()), new ByteArrayInputStream(new byte[Math.toIntExact(policy.getMaxLength())]));
            Assert.assertEquals(policy.getMaxLength(), bytesWritten);
            totalBytesWritten += bytesWritten;
            systemJournalBefore.commitRecord(SystemJournal.ChunkAddedRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(policy.getMaxLength() * i)
                    .newChunkName(newChunk)
                    .oldChunkName(lastChunk)
                    .build());
            lastChunk = newChunk;
        }
        Assert.assertEquals(policy.getMaxLength() * 10, totalBytesWritten);

        // Failover
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        SystemJournal systemJournalAfter = new SystemJournal(containerId, epoch + 1, chunkStorageProvider, metadataStoreAfterCrash, config);
        systemJournalAfter.setSystemSegmentsPrefix(systemSegmentName);
        systemJournalAfter.setSystemSegments(getSystemSegments(systemSegmentName));

        systemJournalAfter.bootstrap();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash, systemSegmentName, policy.getMaxLength(), 10);
        TestUtils.checkSegmentBounds(metadataStoreAfterCrash, systemSegmentName, 0, totalBytesWritten);

        ChunkMetadataStore metadataStoreAfterCrash2 = getMetadataStore();
        SystemJournal systemJournalAfter2 = new SystemJournal(containerId, epoch + 2, chunkStorageProvider, metadataStoreAfterCrash2, config);
        systemJournalAfter2.setSystemSegmentsPrefix(systemSegmentName);
        systemJournalAfter2.setSystemSegments(getSystemSegments(systemSegmentName));

        systemJournalAfter2.bootstrap();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash2, systemSegmentName, policy.getMaxLength(), 10);
    }

    /**
     * Test simple chunk truncation.
     * We failover two times to test correct interaction between snapshot and system logs.
     *
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void testSimpleTruncation() throws Exception {
        ChunkStorageProvider chunkStorageProvider = getChunkStorageProvider();
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();

        String systemSegmentName = "test";
        int containerId = 42;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(2);
        val config = ChunkStorageManagerConfig.DEFAULT_CONFIG.toBuilder().defaultRollingPolicy(policy).build();

        // Step 1: Initial set of additions
        SystemJournal systemJournalBefore = new SystemJournal(containerId, epoch, chunkStorageProvider, metadataStoreBeforeCrash, config);
        systemJournalBefore.setSystemSegmentsPrefix(systemSegmentName);
        systemJournalBefore.setSystemSegments(getSystemSegments(systemSegmentName));

        systemJournalBefore.bootstrap();

        String lastChunk = null;
        long totalBytesWritten = 0;
        for (int i = 0; i < 10; i++) {
            String newChunk = "chunk" + i;
            val h = chunkStorageProvider.create(newChunk);
            val bytesWritten = chunkStorageProvider.write(h, 0, Math.toIntExact(policy.getMaxLength()), new ByteArrayInputStream(new byte[Math.toIntExact(policy.getMaxLength())]));
            Assert.assertEquals(policy.getMaxLength(), bytesWritten);
            totalBytesWritten += bytesWritten;
            systemJournalBefore.commitRecord(SystemJournal.ChunkAddedRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(policy.getMaxLength() * i)
                    .newChunkName(newChunk)
                    .oldChunkName(lastChunk)
                    .build());
            lastChunk = newChunk;
        }
        Assert.assertEquals(policy.getMaxLength() * 10, totalBytesWritten);

        // Step 2: First failover, truncate first 5 chunks.
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        SystemJournal systemJournalAfter = new SystemJournal(containerId, 2, chunkStorageProvider, metadataStoreAfterCrash, config);
        systemJournalAfter.setSystemSegmentsPrefix(systemSegmentName);
        systemJournalAfter.setSystemSegments(getSystemSegments(systemSegmentName));

        systemJournalAfter.bootstrap();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash, systemSegmentName, policy.getMaxLength(), 10);
        TestUtils.checkSegmentBounds(metadataStoreAfterCrash, systemSegmentName, 0, totalBytesWritten);

        // Truncate first five chunks
        for (int i = 0; i <= 10; i++) {
            val firstChunkIndex = i / policy.getMaxLength();
            systemJournalAfter.commitRecord(SystemJournal.TruncationRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(i)
                    .firstChunkName("chunk" + firstChunkIndex)
                    .startOffset(policy.getMaxLength() * firstChunkIndex)
                    .build());
        }

        // Step 3: Second failover, truncate last 5 chunks.
        ChunkMetadataStore metadataStoreAfterCrash2 = getMetadataStore();
        SystemJournal systemJournalAfter2 = new SystemJournal(containerId, 3, chunkStorageProvider, metadataStoreAfterCrash2, config);
        systemJournalAfter2.setSystemSegmentsPrefix(systemSegmentName);
        systemJournalAfter2.setSystemSegments(getSystemSegments(systemSegmentName));

        systemJournalAfter2.bootstrap();

        TestUtils.checkSegmentLayout(metadataStoreAfterCrash2, systemSegmentName, policy.getMaxLength(), 5);
        TestUtils.checkSegmentBounds(metadataStoreAfterCrash2, systemSegmentName, 10, 20);

        // Truncate last five chunks
        for (int i = 10; i <= 20; i++) {
            val firstChunkIndex = i / policy.getMaxLength();
            systemJournalAfter2.commitRecord(SystemJournal.TruncationRecord.builder()
                    .segmentName(systemSegmentName)
                    .offset(i)
                    .firstChunkName("chunk" + firstChunkIndex)
                    .startOffset(policy.getMaxLength() * firstChunkIndex)
                    .build());
        }

        // Step 4: third failover validate.
        ChunkMetadataStore metadataStoreAfterCrash3 = getMetadataStore();
        SystemJournal systemJournalAfter3 = new SystemJournal(containerId, 4, chunkStorageProvider, metadataStoreAfterCrash3, config);
        systemJournalAfter3.setSystemSegmentsPrefix(systemSegmentName);
        systemJournalAfter3.setSystemSegments(getSystemSegments(systemSegmentName));

        systemJournalAfter3.bootstrap();

        TestUtils.checkSegmentBounds(metadataStoreAfterCrash3, systemSegmentName, 20, 20);
    }

    /**
     * Check system segment layout.
     */
    private void checkSystemSegmentsLayout(SystemJournal journal) throws Exception {
        for (String systemSegment : journal.getSystemSegments()) {
            TestUtils.checkChunksExistInStorage(journal.getChunkStorage(), journal.getMetadataStore(), systemSegment);
        }
    }

    @Test
    public void testChunkAddedRecordSerialization() throws Exception {
        testSystemJournalRecordSerialization(SystemJournal.ChunkAddedRecord.builder()
                .segmentName("segmentName")
                .newChunkName("newChunkName")
                .oldChunkName("oldChunkName")
                .offset(1)
                .build());

        // With nullable values
        testSystemJournalRecordSerialization(SystemJournal.ChunkAddedRecord.builder()
                .segmentName("segmentName")
                .newChunkName("newChunkName")
                .oldChunkName(null)
                .offset(1)
                .build());
    }

    @Test
    public void testTruncationRecordSerialization() throws Exception {
        testSystemJournalRecordSerialization(SystemJournal.TruncationRecord.builder()
                .segmentName("segmentName")
                .offset(1)
                .firstChunkName("firstChunkName")
                .startOffset(2)
                .build());
    }

    private void testSystemJournalRecordSerialization(SystemJournal.SystemJournalRecord original) throws Exception {
        val serializer = new SystemJournal.SystemJournalRecord.SystemJournalRecordSerializer();
        val bytes = serializer.serialize(original);
        val obj = serializer.deserialize(bytes);
        Assert.assertEquals(original, obj);
    }

    @Test
    public void testSystemJournalRecordBatchSerialization() throws Exception {
        ArrayList<SystemJournal.SystemJournalRecord> lst = new ArrayList<SystemJournal.SystemJournalRecord>();
        testSystemJournalRecordBatchSerialization(
                SystemJournal.SystemJournalRecordBatch.builder()
                        .systemJournalRecords(lst)
                        .build());

        ArrayList<SystemJournal.SystemJournalRecord> lst2 = new ArrayList<SystemJournal.SystemJournalRecord>();
        lst2.add(SystemJournal.ChunkAddedRecord.builder()
                .segmentName("segmentName")
                .newChunkName("newChunkName")
                .oldChunkName("oldChunkName")
                .offset(1)
                .build());
        lst2.add(SystemJournal.ChunkAddedRecord.builder()
                .segmentName("segmentName")
                .newChunkName("newChunkName")
                .oldChunkName(null)
                .offset(1)
                .build());
        lst2.add(SystemJournal.TruncationRecord.builder()
                .segmentName("segmentName")
                .offset(1)
                .firstChunkName("firstChunkName")
                .startOffset(2)
                .build());
        testSystemJournalRecordBatchSerialization(
                SystemJournal.SystemJournalRecordBatch.builder()
                        .systemJournalRecords(lst)
                        .build());
    }

    private void testSystemJournalRecordBatchSerialization(SystemJournal.SystemJournalRecordBatch original) throws Exception {
        val serializer = new SystemJournal.SystemJournalRecordBatch.SystemJournalRecordBatchSerializer();
        val bytes = serializer.serialize(original);
        val obj = serializer.deserialize(bytes);
        Assert.assertEquals(original, obj);
    }

    @Test
    public void testSnapshotRecordSerialization() throws Exception {

        ArrayList<ChunkMetadata> list = new ArrayList<>();
        list.add(ChunkMetadata.builder()
                .name("name")
                .nextChunk("nextChunk")
                .length(1)
                .build());
        list.add(ChunkMetadata.builder()
                .name("name")
                .length(1)
                .build());

        testSegmentSnapshotRecordSerialization(
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(SegmentMetadata.builder()
                                .name("name")
                                .length(1)
                                .chunkCount(2)
                                .startOffset(3)
                                .status(5)
                                .maxRollinglength(6)
                                .firstChunk("firstChunk")
                                .lastChunk("lastChunk")
                                .lastModified(7)
                                .firstChunkStartOffset(8)
                                .lastChunkStartOffset(9)
                                .ownerEpoch(10)
                                .build())
                        .chunkMetadataCollection(list)
                        .build());

        testSegmentSnapshotRecordSerialization(
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(SegmentMetadata.builder()
                                .name("name")
                                .length(1)
                                .chunkCount(2)
                                .startOffset(3)
                                .status(5)
                                .maxRollinglength(6)
                                .firstChunk(null)
                                .lastChunk(null)
                                .lastModified(7)
                                .firstChunkStartOffset(8)
                                .lastChunkStartOffset(9)
                                .ownerEpoch(10)
                                .build())
                        .chunkMetadataCollection(list)
                        .build());
    }

    private void testSegmentSnapshotRecordSerialization(SystemJournal.SegmentSnapshotRecord original) throws Exception {
        val serializer = new SystemJournal.SegmentSnapshotRecord.Serializer();
        val bytes = serializer.serialize(original);
        val obj = serializer.deserialize(bytes);
        Assert.assertEquals(original, obj);
    }

    @Test
    public void testSystemSnapshotRecordSerialization() throws Exception {

        ArrayList<ChunkMetadata> list1 = new ArrayList<>();
        list1.add(ChunkMetadata.builder()
                .name("name1")
                .nextChunk("nextChunk1")
                .length(1)
                .build());
        list1.add(ChunkMetadata.builder()
                .name("name12")
                .length(1)
                .build());

        ArrayList<ChunkMetadata> list2 = new ArrayList<>();
        list2.add(ChunkMetadata.builder()
                .name("name2")
                .nextChunk("nextChunk2")
                .length(1)
                .build());
        list2.add(ChunkMetadata.builder()
                .name("name22")
                .length(1)
                .build());

        ArrayList<SystemJournal.SegmentSnapshotRecord> segmentlist = new ArrayList<>();

        segmentlist.add(
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(SegmentMetadata.builder()
                                .name("name1")
                                .length(1)
                                .chunkCount(2)
                                .startOffset(3)
                                .status(5)
                                .maxRollinglength(6)
                                .firstChunk("firstChunk111")
                                .lastChunk("lastChun111k")
                                .lastModified(7)
                                .firstChunkStartOffset(8)
                                .lastChunkStartOffset(9)
                                .ownerEpoch(10)
                                .build())
                        .chunkMetadataCollection(list1)
                        .build());

        segmentlist.add(
                SystemJournal.SegmentSnapshotRecord.builder()
                        .segmentMetadata(SegmentMetadata.builder()
                                .name("name2")
                                .length(1)
                                .chunkCount(2)
                                .startOffset(3)
                                .status(5)
                                .maxRollinglength(6)
                                .firstChunk(null)
                                .lastChunk(null)
                                .lastModified(7)
                                .firstChunkStartOffset(8)
                                .lastChunkStartOffset(9)
                                .ownerEpoch(10)
                                .build())
                        .chunkMetadataCollection(list2)
                        .build());
        val systemSnapshot = SystemJournal.SystemSnapshotRecord.builder()
                .epoch(42)
                .segmentSnapshotRecords(segmentlist)
                .build();
        testSystemSnapshotRecordSerialization(systemSnapshot);
    }

    private void testSystemSnapshotRecordSerialization(SystemJournal.SystemSnapshotRecord original) throws Exception {
        val serializer = new SystemJournal.SystemSnapshotRecord.Serializer();
        val bytes = serializer.serialize(original);
        val obj = serializer.deserialize(bytes);
        Assert.assertEquals(original, obj);
    }

    /**
     * Tests {@link SystemJournal}  with non Appendable {@link ChunkStorageProvider} using {@link SystemJournalTests}.
     */
    public static class NonAppendableChunkStorageProviderSystemJournalTests extends SystemJournalTests {
        @Before
        public void before() throws Exception {
            super.before();
        }

        @After
        public void after() throws Exception {
            super.after();
        }

        protected ChunkStorageProvider getChunkStorageProvider() throws Exception {
            val chunkStorageProvider = new InMemoryChunkStorageProvider();
            chunkStorageProvider.setShouldSupportAppend(false);
            return chunkStorageProvider;
        }
    }
}

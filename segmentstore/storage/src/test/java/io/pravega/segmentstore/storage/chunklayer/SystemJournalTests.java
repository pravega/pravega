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

import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorageProvider;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
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

/**
 * Tests for testing bootstrap functionality with {@link SystemJournal}.
 */
public class SystemJournalTests extends ThreadPooledTestSuite {
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

    protected ChunkStorageProvider getStorageProvider() throws Exception {
        return new InMemoryChunkStorageProvider(executorService());
    }

    protected String[] getSystemSegments(String systemSegmentName) {
        return new String[]{systemSegmentName};
    }

    @Test
    public void testSimpleBootstrapWithOneFailover() throws Exception {
        ChunkStorageProvider storageProvider = getStorageProvider();
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        String systemSegmentName = "test";
        int containerId = 42;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(8);

        SystemJournal journalBefore = new SystemJournal(containerId, epoch, storageProvider, metadataStoreBeforeCrash, policy);
        journalBefore.setSystemSegmentsPrefix(systemSegmentName);
        journalBefore.setSystemSegments(getSystemSegments(systemSegmentName));
        long offset = 0;

        // Epoch 1
        ChunkStorageManager chunkStorageManager1 = new ChunkStorageManager(storageProvider, executorService(), policy);
        chunkStorageManager1.initialize(containerId, metadataStoreBeforeCrash, journalBefore);
        chunkStorageManager1.initialize(epoch);

        var h = chunkStorageManager1.create(systemSegmentName, new SegmentRollingPolicy(8), null).join();
        val b1 = "Hello".getBytes();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null);
        offset += b1.length;
        val b2 = " World".getBytes();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(b2), b2.length, null);
        offset += b2.length;

        // Epoch 2
        epoch++;
        SystemJournal journalAfter = new SystemJournal(containerId, epoch, storageProvider, metadataStoreAfterCrash, policy);
        journalAfter.setSystemSegments(getSystemSegments(systemSegmentName));
        journalAfter.setSystemSegmentsPrefix(systemSegmentName);

        ChunkStorageManager chunkStorageManager2 = new ChunkStorageManager(storageProvider, executorService(), new SegmentRollingPolicy(8));
        chunkStorageManager2.initialize(containerId, metadataStoreAfterCrash, journalAfter);
        chunkStorageManager2.initialize(epoch);

        journalAfter.bootstrap();

        val info = chunkStorageManager2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length, info.getLength());
        byte[] out = new byte[b1.length + b2.length];
        val hr = chunkStorageManager2.openRead(systemSegmentName).join();
        chunkStorageManager2.read(hr, 0, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }

    @Test
    public void testSimpleBootstrapWithTwoFailovers() throws Exception {
        ChunkStorageProvider storageProvider = getStorageProvider();
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        String systemSegmentName = "test";
        int containerId = 42;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(8);

        SystemJournal journalBefore = new SystemJournal(containerId, epoch, storageProvider, metadataStoreBeforeCrash, policy);
        journalBefore.setSystemSegmentsPrefix(systemSegmentName);
        journalBefore.setSystemSegments(getSystemSegments(systemSegmentName));
        long offset = 0;

        // Epoch 1
        ChunkStorageManager chunkStorageManager1 = new ChunkStorageManager(storageProvider, executorService(), policy);
        chunkStorageManager1.initialize(containerId, metadataStoreBeforeCrash, journalBefore);
        chunkStorageManager1.initialize(epoch);

        var h = chunkStorageManager1.create(systemSegmentName, new SegmentRollingPolicy(8), null).join();
        val b1 = "Hello".getBytes();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null);
        offset += b1.length;

        // Epoch 2
        epoch++;
        SystemJournal journalAfter = new SystemJournal(containerId, epoch, storageProvider, metadataStoreAfterCrash, policy);
        journalAfter.setSystemSegments(getSystemSegments(systemSegmentName));
        journalAfter.setSystemSegmentsPrefix(systemSegmentName);

        ChunkStorageManager chunkStorageManager2 = new ChunkStorageManager(storageProvider, executorService(), policy);
        chunkStorageManager2.initialize(containerId, metadataStoreAfterCrash, journalAfter);
        chunkStorageManager2.initialize(epoch);

        journalAfter.bootstrap();
        var h2 = chunkStorageManager2.openWrite(systemSegmentName).join();

        // Write Junk Data to
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream("junk".getBytes()), 4, null);

        val b2 = " World".getBytes();
        chunkStorageManager2.write(h2, offset, new ByteArrayInputStream(b2), b2.length, null);
        offset += b2.length;

        val info = chunkStorageManager2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length, info.getLength());
        byte[] out = new byte[b1.length + b2.length];
        val hr = chunkStorageManager2.openRead(systemSegmentName).join();
        chunkStorageManager2.read(hr, 0, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }

    @Test
    public void testSimpleBootstrapWithMultipleFailovers() throws Exception {
        ChunkStorageProvider storageProvider = getStorageProvider();

        String systemSegmentName = "test";
        int containerId = 42;
        long epoch = 0;
        val policy = new SegmentRollingPolicy(100);

        long offset = 0;
        ChunkStorageManager oldhunkStorageManager = null;
        SegmentHandle oldHandle = null;
        for (int i = 1; i < 10; i++) {
            // Epoch 2
            epoch++;
            ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
            SystemJournal journalInLoop = new SystemJournal(containerId, epoch, storageProvider, metadataStoreAfterCrash, policy);
            journalInLoop.setSystemSegments(getSystemSegments(systemSegmentName));
            journalInLoop.setSystemSegmentsPrefix(systemSegmentName);

            ChunkStorageManager chunkStorageManagerInLoop = new ChunkStorageManager(storageProvider, executorService(), policy);
            chunkStorageManagerInLoop.initialize(containerId, metadataStoreAfterCrash, journalInLoop);
            chunkStorageManagerInLoop.initialize(epoch);

            journalInLoop.bootstrap();

            var h = chunkStorageManagerInLoop.openWrite(systemSegmentName).join();

            if (null != oldhunkStorageManager) {
                oldhunkStorageManager.write(oldHandle, offset, new ByteArrayInputStream("junk".getBytes()), 4, null);
            }

            val b1 = "Test".getBytes();
            chunkStorageManagerInLoop.write(h, offset, new ByteArrayInputStream(b1), b1.length, null);
            offset += b1.length;
            val b2 = Integer.toString(i).getBytes();
            chunkStorageManagerInLoop.write(h, offset, new ByteArrayInputStream(b2), b2.length, null);
            offset += b2.length;

            oldhunkStorageManager = chunkStorageManagerInLoop;
            oldHandle = h;
        }

        epoch++;
        ChunkMetadataStore metadataStoreFinal = getMetadataStore();

        SystemJournal journalFinal = new SystemJournal(containerId, epoch, storageProvider, metadataStoreFinal, policy);
        journalFinal.setSystemSegments(getSystemSegments(systemSegmentName));
        journalFinal.setSystemSegmentsPrefix(systemSegmentName);

        ChunkStorageManager chunkStorageManagerFinal = new ChunkStorageManager(storageProvider, executorService(), policy);
        chunkStorageManagerFinal.initialize(containerId, metadataStoreFinal, journalFinal);
        chunkStorageManagerFinal.initialize(epoch);

        journalFinal.bootstrap();

        val info = chunkStorageManagerFinal.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(offset, info.getLength());
        byte[] out = new byte[Math.toIntExact(offset)];
        val hr = chunkStorageManagerFinal.openRead(systemSegmentName).join();
        chunkStorageManagerFinal.read(hr, 0, out, 0, Math.toIntExact(offset), null).join();
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

    private void testBootstrapWithTruncate(String initialGarbage, String garbageAfterFailure, String validWriteBeforeFailure, String validWriteAfterFailure, int maxLength) throws Exception {
        ChunkStorageProvider storageProvider = getStorageProvider();
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        String systemSegmentName = "test";

        int containerId = 42;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(maxLength);

        SystemJournal journalBefore = new SystemJournal(containerId, epoch, storageProvider, metadataStoreBeforeCrash, policy);
        journalBefore.setSystemSegmentsPrefix(systemSegmentName);
        journalBefore.setSystemSegments(getSystemSegments(systemSegmentName));
        long offset = 0;

        // Epoch 1
        ChunkStorageManager chunkStorageManager1 = new ChunkStorageManager(storageProvider, executorService(), policy);
        chunkStorageManager1.initialize(containerId, metadataStoreBeforeCrash, journalBefore);
        chunkStorageManager1.initialize(epoch);

        var h = chunkStorageManager1.create(systemSegmentName, new SegmentRollingPolicy(maxLength), null).join();
        val b1 = validWriteBeforeFailure.getBytes();
        byte[] garbage1 = initialGarbage.getBytes();
        int garbage1Length = garbage1.length;
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(garbage1), garbage1Length, null);
        offset += garbage1Length;
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null);
        offset += b1.length;
        chunkStorageManager1.truncate(h, garbage1Length, null);

        // Epoch 2
        epoch++;
        SystemJournal journalAfter = new SystemJournal(containerId, epoch, storageProvider, metadataStoreAfterCrash, policy);
        journalAfter.setSystemSegments(getSystemSegments(systemSegmentName));
        journalAfter.setSystemSegmentsPrefix(systemSegmentName);

        ChunkStorageManager chunkStorageManager2 = new ChunkStorageManager(storageProvider, executorService(), policy);
        chunkStorageManager2.initialize(containerId, metadataStoreAfterCrash, journalAfter);
        chunkStorageManager2.initialize(epoch);

        journalAfter.bootstrap();
        var h2 = chunkStorageManager2.openWrite(systemSegmentName).join();

        // Write Junk Data to
        val innerGarbageBytes = garbageAfterFailure.getBytes();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(innerGarbageBytes), innerGarbageBytes.length, null);

        val b2 = validWriteAfterFailure.getBytes();
        chunkStorageManager2.write(h2, offset, new ByteArrayInputStream(b2), b2.length, null);
        offset += b2.length;

        val info = chunkStorageManager2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length + garbage1Length, info.getLength());
        Assert.assertEquals(garbage1Length, info.getStartOffset());
        byte[] out = new byte[b1.length + b2.length];
        val hr = chunkStorageManager2.openRead(systemSegmentName).join();
        chunkStorageManager2.read(hr, garbage1Length, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals(validWriteBeforeFailure + validWriteAfterFailure, new String(out));
    }

    @Test
    public void testSimpleBootstrapWithTwoTruncates() throws Exception {
        ChunkStorageProvider storageProvider = getStorageProvider();
        ChunkMetadataStore metadataStoreBeforeCrash = getMetadataStore();
        ChunkMetadataStore metadataStoreAfterCrash = getMetadataStore();
        String systemSegmentName = "test";
        int containerId = 42;
        long epoch = 1;
        val policy = new SegmentRollingPolicy(8);

        SystemJournal journalBefore = new SystemJournal(containerId, epoch, storageProvider, metadataStoreBeforeCrash, policy);
        journalBefore.setSystemSegmentsPrefix(systemSegmentName);
        journalBefore.setSystemSegments(getSystemSegments(systemSegmentName));
        long offset = 0;

        // Epoch 1
        ChunkStorageManager chunkStorageManager1 = new ChunkStorageManager(storageProvider, executorService(), policy);
        chunkStorageManager1.initialize(containerId, metadataStoreBeforeCrash, journalBefore);
        chunkStorageManager1.initialize(epoch);

        var h = chunkStorageManager1.create(systemSegmentName, new SegmentRollingPolicy(8), null).join();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream("JUNKJUNKJUNK".getBytes()), 12, null);
        offset += 12;
        val b1 = "Hello".getBytes();
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream(b1), b1.length, null);
        offset += b1.length;
        chunkStorageManager1.truncate(h, 6, null);

        // Epoch 2
        epoch++;
        SystemJournal journalAfter = new SystemJournal(containerId, epoch, storageProvider, metadataStoreAfterCrash, policy);
        journalAfter.setSystemSegments(getSystemSegments(systemSegmentName));
        journalAfter.setSystemSegmentsPrefix(systemSegmentName);

        ChunkStorageManager chunkStorageManager2 = new ChunkStorageManager(storageProvider, executorService(), policy);
        chunkStorageManager2.initialize(containerId, metadataStoreAfterCrash, journalAfter);
        chunkStorageManager2.initialize(epoch);

        journalAfter.bootstrap();
        var h2 = chunkStorageManager2.openWrite(systemSegmentName).join();

        // Write Junk Data to
        chunkStorageManager1.write(h, offset, new ByteArrayInputStream("junk".getBytes()), 4, null);

        val b2 = " World".getBytes();
        chunkStorageManager2.write(h2, offset, new ByteArrayInputStream(b2), b2.length, null);
        offset += b2.length;

        chunkStorageManager2.truncate(h, 12, null);

        val info = chunkStorageManager2.getStreamSegmentInfo(systemSegmentName, null).join();
        Assert.assertEquals(b1.length + b2.length + 12, info.getLength());
        Assert.assertEquals(12, info.getStartOffset());
        byte[] out = new byte[b1.length + b2.length];
        val hr = chunkStorageManager2.openRead(systemSegmentName).join();
        chunkStorageManager2.read(hr, 12, out, 0, b1.length + b2.length, null).join();
        Assert.assertEquals("Hello World", new String(out));
    }
}

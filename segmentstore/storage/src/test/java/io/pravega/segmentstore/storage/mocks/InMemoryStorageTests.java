/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for InMemoryStorage
 */
public class InMemoryStorageTests extends StorageTestBase {
    private static final int TRUNCATE_WRITE_LENGTH = 512 * 1024;
    private static final int SMALL_TRUNCATE_LENGTH = TRUNCATE_WRITE_LENGTH / 3;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());
    private InMemoryStorageFactory factory;

    @Before
    public void setUp() {
        this.factory = new InMemoryStorageFactory(executorService());
    }

    @After
    public void tearDown() {
        if (this.factory != null) {
            this.factory.close();
            this.factory = null;
        }
    }

    /**
     * Tests the registerSealTrigger() method.
     */
    @Test
    public void testSealTrigger() throws Exception {
        final String segment1 = "toSeal";
        final String segment2 = "toDelete";

        @Cleanup
        val baseStorage = new InMemoryStorage();
        @Cleanup
        val storage = new AsyncStorageWrapper(baseStorage, executorService());
        storage.initialize(DEFAULT_EPOCH);
        storage.create(segment1, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        storage.create(segment2, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val seal1 = baseStorage.registerSealTrigger(segment1, TIMEOUT);
        val seal2 = baseStorage.registerSealTrigger(segment2, TIMEOUT);

        val handle1 = storage.openWrite(segment1).join();
        val handle2 = storage.openWrite(segment2).join();
        storage.write(handle1, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT).join();
        storage.write(handle2, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT).join();
        Assert.assertFalse("Seal Futures were completed prematurely.", CompletableFuture.anyOf(seal1, seal2).isDone());

        // Trigger cancelled when deleted.
        storage.delete(handle2, TIMEOUT).join();
        Assert.assertTrue("Seal trigger was not cancelled when segment was deleted.", seal2.isCompletedExceptionally());
        AssertExtensions.assertThrows(
                "Seal trigger was not cancelled with correct exception when segment was deleted.",
                seal2::join,
                ex -> ex instanceof CancellationException);

        // Trigger completed when sealed.
        storage.seal(handle1, TIMEOUT).join();
        seal1.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); // This should not throw.

        val alreadySealed = baseStorage.registerSealTrigger(segment1, TIMEOUT);
        alreadySealed.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertTrue("Seal trigger was not immediately completed when segment already sealed.",
                FutureHelpers.isSuccessful(alreadySealed));
    }

    /**
     * Tests the registerSizeTrigger() method.
     */
    @Test
    public void testSizeTrigger() throws Exception {
        final String segment1 = "toAdd";
        final String segment2 = "toDelete";
        final int triggerOffset = 10;

        @Cleanup
        val baseStorage = new InMemoryStorage();
        @Cleanup
        val storage = new AsyncStorageWrapper(baseStorage, executorService());
        storage.initialize(DEFAULT_EPOCH);
        storage.create(segment1, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        storage.create(segment2, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val size1 = baseStorage.registerSizeTrigger(segment1, triggerOffset, TIMEOUT);
        val size2 = baseStorage.registerSizeTrigger(segment2, triggerOffset, TIMEOUT);

        val handle1 = storage.openWrite(segment1).join();
        val handle2 = storage.openWrite(segment2).join();
        storage.write(handle1, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT).join();
        storage.write(handle2, 0, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT).join();
        Assert.assertFalse("Seal Futures were completed prematurely.", CompletableFuture.anyOf(size1, size2).isDone());

        // Trigger cancelled when deleted.
        storage.delete(handle2, TIMEOUT).join();
        Assert.assertTrue("Size trigger was not cancelled when segment was deleted.", size2.isCompletedExceptionally());
        AssertExtensions.assertThrows(
                "Size trigger was not cancelled with correct exception when segment was deleted.",
                size2::join,
                ex -> ex instanceof CancellationException);

        // Trigger completed when offset is reached or exceeded.
        storage.write(handle1, 1, new ByteArrayInputStream(new byte[triggerOffset]), triggerOffset, TIMEOUT).join();
        size1.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); // This should not throw.

        val alreadyExceeded = baseStorage.registerSizeTrigger(segment1, triggerOffset, TIMEOUT);
        alreadyExceeded.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertTrue("Size trigger was not immediately completed when segment already exceeds size.",
                FutureHelpers.isSuccessful(alreadyExceeded));

        // Trigger cancelled when segment is sealed.
        val size3 = baseStorage.registerSizeTrigger(segment1, triggerOffset * 2, TIMEOUT);
        storage.seal(handle1, TIMEOUT).join();
        AssertExtensions.assertThrows(
                "Size trigger was not cancelled with correct exception when segment was sealed.",
                size3::join,
                ex -> ex instanceof StreamSegmentSealedException);
    }

    /**
     * Tests the append() method.
     */
    @Test
    public void testAppend() throws Exception {
        final String segmentName = "segment";

        @Cleanup
        val storage = new InMemoryStorage();
        storage.initialize(DEFAULT_EPOCH);
        storage.create(segmentName);

        val handle = storage.openWrite(segmentName);
        ByteArrayOutputStream writeStream = new ByteArrayOutputStream();

        for (int j = 0; j < APPENDS_PER_SEGMENT; j++) {
            byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
            ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
            storage.append(handle, dataStream, writeData.length);
            writeStream.write(writeData);
        }

        byte[] expectedData = writeStream.toByteArray();
        byte[] readBuffer = new byte[expectedData.length];
        int bytesRead = storage.read(handle, 0, readBuffer, 0, readBuffer.length);
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);
        AssertExtensions.assertArrayEquals("Unexpected read result.", expectedData, 0, readBuffer, 0, bytesRead);
    }


    /**
     * Tests the truncate method, which is specific to this Storage Implementation. InMemoryStorage allows exact truncation,
     * which is what this is testing.
     */
    @Test
    public void testTruncate() throws Exception {
        final String segmentName = "TruncatedSegment";
        final Random rnd = new Random(0);
        try (Storage s = createStorage()) {
            s.initialize(1);
            s.create(segmentName, TIMEOUT).join();

            // Invalid segment name.
            val readOnlyHandle = s.openRead(segmentName).join();
            AssertExtensions.assertThrows(
                    "truncate() did not throw for read-only handle.",
                    () -> s.truncate(readOnlyHandle, 0, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException);

            // Populate some data in the segment.
            AtomicLong offset = new AtomicLong();
            ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
            final byte[] writeBuffer = new byte[TRUNCATE_WRITE_LENGTH];
            val writeHandle = s.openWrite(segmentName).join();
            for (int j = 0; j < APPENDS_PER_SEGMENT; j++) {
                rnd.nextBytes(writeBuffer); // Generate new write data every time.
                s.write(writeHandle, offset.get(), new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
                writeStream.write(writeBuffer);
                offset.addAndGet(writeBuffer.length);
            }

            // Truncate only from the first buffer (and make sure we try to truncate at 0).
            AtomicInteger truncatedLength = new AtomicInteger();
            verifySmallTruncate(writeHandle, s, truncatedLength);

            // Truncate many internal buffers at once.
            verifyLargeTruncate(writeHandle, s, truncatedLength);

            // Verify that writes reads still work well without corrupting data.
            verifyWriteReadsAfterTruncate(writeHandle, s, offset, writeStream, truncatedLength);

            // Verify concat from a truncated segment does not work.
            verifyConcatAfterTruncate(writeHandle, s);

            // Check post-delete truncate.
            verifyDeleteAfterTruncate(writeHandle, s);
        }
    }

    @Test
    @Override
    public void testFencing() throws Exception {
        final String segment1 = "segment1";
        final String segment2 = "segment2";

        @Cleanup
        val baseStorage = new InMemoryStorage();
        @Cleanup
        val storage = new AsyncStorageWrapper(baseStorage, executorService());
        storage.initialize(DEFAULT_EPOCH);

        // Part 1: Create a segment and verify all operations are allowed.
        storage.create(segment1, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentHandle handle1 = storage.openWrite(segment1).join();
        verifyAllOperationsSucceed(handle1, storage);

        // Part 2: Change owner, verify segment operations are not allowed until a call to open() is made.
        baseStorage.changeOwner();
        verifyWriteOperationsFail(handle1, storage);

        handle1 = storage.openWrite(segment1).join();
        verifyAllOperationsSucceed(handle1, storage);

        // Part 3: Create new segment and verify all operations are allowed.
        storage.create(segment2, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentHandle handle2 = storage.openWrite(segment2).join();
        verifyAllOperationsSucceed(handle2, storage);

        // Cleanup.
        storage.delete(handle1, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        storage.delete(handle2, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void verifyWriteOperationsFail(SegmentHandle handle, Storage storage) {
        final byte[] writeData = "hello".getBytes();

        // Write
        AssertExtensions.assertThrows(
                "write did not throw for non-owned Segment",
                () -> storage.write(handle, 0, new ByteArrayInputStream(writeData), writeData.length, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        // Seal
        AssertExtensions.assertThrows(
                "seal did not throw for non-owned Segment",
                () -> storage.seal(handle, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        // Read-only operations should succeed.
        storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).join();
        storage.read(handle, 0, new byte[1], 0, 1, TIMEOUT);
    }

    private void verifyAllOperationsSucceed(SegmentHandle handle, Storage storage) throws Exception {
        final byte[] writeData = "hello".getBytes();

        // GetInfo
        val si = storage.getStreamSegmentInfo(handle.getSegmentName(), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Write
        storage.write(handle, si.getLength(), new ByteArrayInputStream(writeData), writeData.length, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Read
        byte[] readBuffer = new byte[(int) si.getLength()];
        storage.read(handle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void verifySmallTruncate(SegmentHandle handle, Storage s, AtomicInteger truncatedLength) {
        while (truncatedLength.get() < 2 * TRUNCATE_WRITE_LENGTH) {
            s.truncate(handle, truncatedLength.get(), TIMEOUT).join();
            if (truncatedLength.get() > 0) {
                AssertExtensions.assertThrows(
                        "read() did not throw when attempting to read before truncation point (small truncate).",
                        () -> s.read(handle, truncatedLength.get() - 1, new byte[1], 0, 1, TIMEOUT),
                        ex -> ex instanceof IllegalArgumentException);
            }

            truncatedLength.addAndGet(SMALL_TRUNCATE_LENGTH);
        }
    }

    private void verifyLargeTruncate(SegmentHandle handle, Storage s, AtomicInteger truncatedLength) {
        truncatedLength.addAndGet(4 * TRUNCATE_WRITE_LENGTH);
        s.truncate(handle, truncatedLength.get(), TIMEOUT).join();
        AssertExtensions.assertThrows(
                "read() did not throw when attempting to read before truncation point (large truncate).",
                () -> s.read(handle, truncatedLength.get() - 1, new byte[1], 0, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);
    }

    private void verifyWriteReadsAfterTruncate(SegmentHandle handle, Storage s, AtomicLong offset, ByteArrayOutputStream writeStream, AtomicInteger truncatedLength) throws Exception {
        final byte[] writeBuffer = new byte[TRUNCATE_WRITE_LENGTH];
        (new Random(0)).nextBytes(writeBuffer);
        s.write(handle, offset.get(), new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
        writeStream.write(writeBuffer);
        offset.addAndGet(writeBuffer.length);

        byte[] readBuffer = new byte[(int) offset.get() - truncatedLength.get()];
        int readBytes = s.read(handle, truncatedLength.get(), readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, readBytes);

        byte[] writtenData = writeStream.toByteArray();
        AssertExtensions.assertArrayEquals("Unexpected data read back after truncation.", writtenData,
                truncatedLength.get(), readBuffer, 0, readBytes);
    }

    private void verifyConcatAfterTruncate(SegmentHandle handle, Storage s) {
        final String newSegmentName = "newFoo";
        s.create(newSegmentName, TIMEOUT).join();
        val targetHandle = s.openWrite(newSegmentName).join();
        AssertExtensions.assertThrows("concat() allowed concatenation of truncated segment.",
                () -> s.concat(targetHandle, 0, handle.getSegmentName(), TIMEOUT),
                ex -> ex instanceof IllegalStateException);
    }

    private void verifyDeleteAfterTruncate(SegmentHandle handle, Storage s) {
        s.delete(handle, TIMEOUT).join();
        AssertExtensions.assertThrows("truncate() did not throw for a deleted StreamSegment.",
                () -> s.truncate(handle, 0, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    @Override
    protected Storage createStorage() {
        return this.factory.createStorageAdapter();
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        return InMemoryStorage.newHandle(segmentName, readOnly);
    }
}

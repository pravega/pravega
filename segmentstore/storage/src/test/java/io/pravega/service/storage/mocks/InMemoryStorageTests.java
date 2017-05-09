/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.storage.mocks;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.service.contracts.StreamSegmentSealedException;
import io.pravega.service.storage.SegmentHandle;
import io.pravega.service.storage.Storage;
import io.pravega.service.storage.StorageNotPrimaryException;
import io.pravega.service.storage.TruncateableStorage;
import io.pravega.service.storage.TruncateableStorageTestBase;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for InMemoryStorage
 */
public class InMemoryStorageTests extends TruncateableStorageTestBase {
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
        val storage = new InMemoryStorage();
        storage.initialize(DEFAULT_EPOCH);
        storage.create(segment1, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        storage.create(segment2, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val seal1 = storage.registerSealTrigger(segment1, TIMEOUT);
        val seal2 = storage.registerSealTrigger(segment2, TIMEOUT);

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

        val alreadySealed = storage.registerSealTrigger(segment1, TIMEOUT);
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
        val storage = new InMemoryStorage();
        storage.initialize(DEFAULT_EPOCH);
        storage.create(segment1, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        storage.create(segment2, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val size1 = storage.registerSizeTrigger(segment1, triggerOffset, TIMEOUT);
        val size2 = storage.registerSizeTrigger(segment2, triggerOffset, TIMEOUT);

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

        val alreadyExceeded = storage.registerSizeTrigger(segment1, triggerOffset, TIMEOUT);
        alreadyExceeded.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertTrue("Size trigger was not immediately completed when segment already exceeds size.",
                FutureHelpers.isSuccessful(alreadyExceeded));

        // Trigger cancelled when segment is sealed.
        val size3 = storage.registerSizeTrigger(segment1, triggerOffset * 2, TIMEOUT);
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
        storage.create(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        val handle = storage.openWrite(segmentName).join();
        ByteArrayOutputStream writeStream = new ByteArrayOutputStream();

        for (int j = 0; j < APPENDS_PER_SEGMENT; j++) {
            byte[] writeData = String.format("Segment_%s_Append_%d", segmentName, j).getBytes();
            ByteArrayInputStream dataStream = new ByteArrayInputStream(writeData);
            storage.append(handle, dataStream, writeData.length);
            writeStream.write(writeData);
        }

        byte[] expectedData = writeStream.toByteArray();
        byte[] readBuffer = new byte[expectedData.length];
        int bytesRead = storage.read(handle, 0, readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, bytesRead);
        AssertExtensions.assertArrayEquals("Unexpected read result.", expectedData, 0, readBuffer, 0, bytesRead);
    }

    @Test
    @Override
    public void testFencing() throws Exception {
        final String segment1 = "segment1";
        final String segment2 = "segment2";

        @Cleanup
        val storage = new InMemoryStorage();
        storage.initialize(DEFAULT_EPOCH);

        // Part 1: Create a segment and verify all operations are allowed.
        storage.create(segment1, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentHandle handle1 = storage.openWrite(segment1).join();
        verifyAllOperationsSucceed(handle1, storage);

        // Part 2: Change owner, verify segment operations are not allowed until a call to open() is made.
        storage.changeOwner();
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

    private void verifyWriteOperationsFail(SegmentHandle handle, Storage storage) {
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

    @Override
    protected TruncateableStorage createStorage() {
        return this.factory.createStorageAdapter();
    }

    @Override
    protected SegmentHandle createHandle(String segmentName, boolean readOnly, long epoch) {
        return InMemoryStorage.newHandle(segmentName, readOnly);
    }
}

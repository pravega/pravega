/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.storage.mocks;

import io.pravega.server.segmentstore.storage.SegmentHandle;
import io.pravega.server.segmentstore.storage.Storage;
import io.pravega.server.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.server.segmentstore.storage.TruncateableStorage;
import io.pravega.server.segmentstore.storage.TruncateableStorageTestBase;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import lombok.val;
import org.junit.After;
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

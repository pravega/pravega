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
package io.pravega.segmentstore.storage.mocks;

import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.rolling.RollingStorageTestBase;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for InMemoryStorage
 */
public class InMemoryStorageTests extends StorageTestBase {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

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

    @Test
    public void testReplace() throws Exception {
        @Cleanup
        val s = new InMemoryStorage();
        s.initialize(1);
        Assert.assertFalse(s.supportsReplace());
        val h = s.create("segment");
        AssertExtensions.assertThrows("", () -> s.replace(h, new ByteArraySegment(new byte[1])), ex -> ex instanceof UnsupportedOperationException);
        Assert.assertSame(s, s.withReplaceSupport());
    }

    @Override
    protected void verifyWriteOperationsFail(SegmentHandle handle, Storage storage) {
        final byte[] writeData = "hello".getBytes();

        // Write
        AssertExtensions.assertSuppliedFutureThrows(
                "write did not throw for non-owned Segment",
                () -> storage.write(handle, 0, new ByteArrayInputStream(writeData), writeData.length, TIMEOUT),
                ex -> ex instanceof StorageNotPrimaryException);

        // Seal
        AssertExtensions.assertSuppliedFutureThrows(
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
    protected Storage createStorage() {
        return InMemoryStorageFactory.newStorage(executorService());
    }

    //region RollingStorageTests

    /**
     * Tests the InMemoryStorage adapter with a RollingStorage wrapper.
     */
    public static class RollingStorageTests extends RollingStorageTestBase {
        @Rule
        public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

        @Override
        protected Storage createStorage() {
            return wrap(new InMemoryStorage());
        }
    }

    //endregion
}

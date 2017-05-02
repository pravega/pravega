/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.service.storage;

import io.pravega.service.contracts.StreamSegmentNotExistsException;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;

/**
 * Base class for testing any implementation of the TruncateableStorage interface.
 */
public abstract class TruncateableStorageTestBase extends StorageTestBase {
    private static final String SEGMENT_NAME = "foo";
    private static final int APPEND_COUNT = 10;
    private static final int WRITE_LENGTH = 512 * 1024;
    private static final int SMALL_TRUNCATE_LENGTH = WRITE_LENGTH / 3;
    private static final Random DATA_GENERATOR = new Random();

    /**
     * Tests the truncate method.
     *
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testTruncate() throws Exception {
        try (TruncateableStorage s = createStorage()) {
            s.initialize(1);
            s.create(SEGMENT_NAME, TIMEOUT).join();

            // Invalid segment name.
            assertThrows(
                    "truncate() did not throw for invalid segment name.",
                    () -> s.truncate(SEGMENT_NAME + "invalid", 0, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            // Populate some data in the segment.
            AtomicLong offset = new AtomicLong();
            ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
            final byte[] writeBuffer = new byte[WRITE_LENGTH];
            val writeHandle = s.openWrite(SEGMENT_NAME).join();
            for (int j = 0; j < APPEND_COUNT; j++) {
                DATA_GENERATOR.nextBytes(writeBuffer); // Generate new write data every time.
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
            verifyConcat(writeHandle, s);

            // Check post-delete truncate.
            verifyDelete(writeHandle, s);
        }
    }

    private void verifySmallTruncate(SegmentHandle handle, TruncateableStorage s, AtomicInteger truncatedLength) {
        while (truncatedLength.get() < 2 * WRITE_LENGTH) {
            s.truncate(handle.getSegmentName(), truncatedLength.get(), TIMEOUT).join();
            if (truncatedLength.get() > 0) {
                assertThrows(
                        "read() did not throw when attempting to read before truncation point (small truncate).",
                        () -> s.read(handle, truncatedLength.get() - 1, new byte[1], 0, 1, TIMEOUT),
                        ex -> ex instanceof IllegalArgumentException);
            }

            truncatedLength.addAndGet(SMALL_TRUNCATE_LENGTH);
        }
    }

    private void verifyLargeTruncate(SegmentHandle handle, TruncateableStorage s, AtomicInteger truncatedLength) {
        truncatedLength.addAndGet(4 * WRITE_LENGTH);
        s.truncate(handle.getSegmentName(), truncatedLength.get(), TIMEOUT).join();
        assertThrows(
                "read() did not throw when attempting to read before truncation point (large truncate).",
                () -> s.read(handle, truncatedLength.get() - 1, new byte[1], 0, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);
    }

    private void verifyWriteReadsAfterTruncate(SegmentHandle handle, TruncateableStorage s, AtomicLong offset, ByteArrayOutputStream writeStream, AtomicInteger truncatedLength) throws Exception {
        final byte[] writeBuffer = new byte[WRITE_LENGTH];
        DATA_GENERATOR.nextBytes(writeBuffer);
        s.write(handle, offset.get(), new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
        writeStream.write(writeBuffer);
        offset.addAndGet(writeBuffer.length);

        byte[] readBuffer = new byte[(int) offset.get() - truncatedLength.get()];
        int readBytes = s.read(handle, truncatedLength.get(), readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, readBytes);

        byte[] writtenData = writeStream.toByteArray();
        AssertExtensions.assertArrayEquals("Unexpected data read back after truncation.", writtenData, truncatedLength.get(), readBuffer, 0, readBytes);
    }

    private void verifyConcat(SegmentHandle handle, TruncateableStorage s) {
        final String newSegmentName = "newFoo";
        s.create(newSegmentName, TIMEOUT).join();
        val targetHandle = s.openWrite(newSegmentName).join();
        assertThrows("concat() allowed concatenation of truncated segment.",
                () -> s.concat(targetHandle, 0, handle.getSegmentName(), TIMEOUT),
                ex -> ex instanceof IllegalStateException);
    }

    private void verifyDelete(SegmentHandle handle, TruncateableStorage s) {
        s.delete(handle, TIMEOUT).join();
        assertThrows("truncate() did not throw for a deleted StreamSegment.",
                () -> s.truncate(handle.getSegmentName(), 0, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    @Override
    protected abstract TruncateableStorage createStorage();
}

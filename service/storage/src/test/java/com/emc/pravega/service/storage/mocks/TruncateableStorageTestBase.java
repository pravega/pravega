/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.storage.TruncateableStorage;
import com.emc.pravega.testcommon.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.emc.pravega.testcommon.AssertExtensions.assertThrows;

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
            for (int j = 0; j < APPEND_COUNT; j++) {
                DATA_GENERATOR.nextBytes(writeBuffer); // Generate new write data every time.
                s.write(SEGMENT_NAME, offset.get(), new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
                writeStream.write(writeBuffer);
                offset.addAndGet(writeBuffer.length);
            }

            // Truncate only from the first buffer (and make sure we try to truncate at 0).
            AtomicInteger truncatedLength = new AtomicInteger();
            verifySmallTruncate(s, truncatedLength);

            // Truncate many internal buffers at once.
            verifyLargeTruncate(s, truncatedLength);

            // Verify that writes reads still work well without corrupting data.
            verifyWriteReadsAfterTruncate(s, offset, writeStream, truncatedLength);

            // Verify concat from a truncated segment does not work.
            verifyConcat(s);

            // Check post-delete truncate.
            verifyDelete(s);
        }
    }

    private void verifySmallTruncate(TruncateableStorage s, AtomicInteger truncatedLength) {
        while (truncatedLength.get() < 2 * WRITE_LENGTH) {
            s.truncate(SEGMENT_NAME, truncatedLength.get(), TIMEOUT).join();
            if (truncatedLength.get() > 0) {
                assertThrows(
                        "read() did not throw when attempting to read before truncation point (small truncate).",
                        () -> s.read(SEGMENT_NAME, truncatedLength.get() - 1, new byte[1], 0, 1, TIMEOUT),
                        ex -> ex instanceof IllegalArgumentException);
            }

            truncatedLength.addAndGet(SMALL_TRUNCATE_LENGTH);
        }
    }

    private void verifyLargeTruncate(TruncateableStorage s, AtomicInteger truncatedLength) {
        truncatedLength.addAndGet(4 * WRITE_LENGTH);
        s.truncate(SEGMENT_NAME, truncatedLength.get(), TIMEOUT).join();
        assertThrows(
                "read() did not throw when attempting to read before truncation point (large truncate).",
                () -> s.read(SEGMENT_NAME, truncatedLength.get() - 1, new byte[1], 0, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);
    }

    private void verifyWriteReadsAfterTruncate(TruncateableStorage s, AtomicLong offset, ByteArrayOutputStream writeStream, AtomicInteger truncatedLength) throws Exception {
        final byte[] writeBuffer = new byte[WRITE_LENGTH];
        DATA_GENERATOR.nextBytes(writeBuffer);
        s.write(SEGMENT_NAME, offset.get(), new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
        writeStream.write(writeBuffer);
        offset.addAndGet(writeBuffer.length);

        byte[] readBuffer = new byte[(int) offset.get() - truncatedLength.get()];
        int readBytes = s.read(SEGMENT_NAME, truncatedLength.get(), readBuffer, 0, readBuffer.length, TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, readBytes);

        byte[] writtenData = writeStream.toByteArray();
        AssertExtensions.assertArrayEquals("Unexpected data read back after truncation.", writtenData, truncatedLength.get(), readBuffer, 0, readBytes);
    }

    private void verifyConcat(TruncateableStorage s) {
        final String newSegmentName = "newFoo";
        s.create(newSegmentName, TIMEOUT).join();
        assertThrows("concat() allowed concatenation of truncated segment.",
                () -> s.concat(newSegmentName, 0, SEGMENT_NAME, TIMEOUT),
                ex -> ex instanceof IllegalStateException);
    }

    private void verifyDelete(TruncateableStorage s) {
        s.delete(SEGMENT_NAME, TIMEOUT).join();
        assertThrows("truncate() did not throw for a deleted StreamSegment.",
                () -> s.truncate(SEGMENT_NAME, 0, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    @Override
    protected abstract TruncateableStorage createStorage();
}

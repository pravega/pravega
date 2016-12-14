/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

import static com.emc.pravega.testcommon.AssertExtensions.assertThrows;

/**
 * Base class for testing any implementation of the TruncateableStorage interface.
 */
public abstract class TruncateableStorageTestBase extends StorageTestBase {
    /**
     * Tests the truncate method.
     *
     * @throws Exception if an unexpected error occurred.
     */
    @Test
    public void testTruncate() throws Exception {
        final String segmentName = "foo";
        final int appendCount = 10;
        final byte[] writeBuffer = new byte[512 * 1024]; // We write 5MB in total.
        final int smallTruncate = writeBuffer.length / 3;

        try (TruncateableStorage s = createStorage()) {
            s.create(segmentName, TIMEOUT).join();

            // Invalid segment name.
            assertThrows(
                    "truncate() did not throw for invalid segment name.",
                    () -> s.truncate(segmentName + "invalid", 0, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            ByteArrayOutputStream writeStream = new ByteArrayOutputStream();
            long offset = 0;
            for (int j = 0; j < appendCount; j++) {
                new Random().nextBytes(writeBuffer); // Generate new write data every time.
                s.write(segmentName, offset, new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
                writeStream.write(writeBuffer);
                offset += writeBuffer.length;
            }

            // Part 1. Truncate only from the first buffer (and make sure we try to truncate at 0).
            AtomicInteger truncatedLength = new AtomicInteger();
            while (truncatedLength.get() < 2 * writeBuffer.length) {
                s.truncate(segmentName, truncatedLength.get(), TIMEOUT).join();
                if (truncatedLength.get() > 0) {
                    assertThrows(
                            "read() did not throw when attempting to read before truncation point (small truncate).",
                            () -> s.read(segmentName, truncatedLength.get() - 1, new byte[1], 0, 1, TIMEOUT),
                            ex -> ex instanceof IllegalArgumentException);
                }

                truncatedLength.addAndGet(smallTruncate);
            }

            // Part 2. Truncate many internal buffers at once.
            truncatedLength.addAndGet(4 * writeBuffer.length);
            s.truncate(segmentName, truncatedLength.get(), TIMEOUT).join();
            assertThrows(
                    "read() did not throw when attempting to read before truncation point (large truncate).",
                    () -> s.read(segmentName, truncatedLength.get() - 1, new byte[1], 0, 1, TIMEOUT),
                    ex -> ex instanceof IllegalArgumentException);

            // Part 3. Verify that writes reads still work well without corrupting data.
            new Random().nextBytes(writeBuffer);
            s.write(segmentName, offset, new ByteArrayInputStream(writeBuffer), writeBuffer.length, TIMEOUT).join();
            writeStream.write(writeBuffer);
            offset += writeBuffer.length;

            byte[] readBuffer = new byte[(int) offset - truncatedLength.get()];
            int readBytes = s.read(segmentName, truncatedLength.get(), readBuffer, 0, readBuffer.length, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, readBytes);

            byte[] writtenData = writeStream.toByteArray();
            AssertExtensions.assertArrayEquals("Unexpected data read back after truncation.", writtenData, truncatedLength.get(), readBuffer, 0, readBytes);

            // Part 4. Verify concat from a truncated segment does not work.
            final String newSegmentName = "newFoo";
            s.create(newSegmentName, TIMEOUT).join();
            assertThrows("concat() allowed concatenation of truncated segment.",
                    () -> s.concat(newSegmentName, 0, segmentName, TIMEOUT),
                    ex -> ex instanceof IllegalStateException);

            // Check post-delete truncate.
            s.delete(segmentName, TIMEOUT).join();
            assertThrows("truncate() did not throw for a deleted StreamSegment.",
                    () -> s.truncate(segmentName, 0, TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    @Override
    protected abstract TruncateableStorage createStorage();
}

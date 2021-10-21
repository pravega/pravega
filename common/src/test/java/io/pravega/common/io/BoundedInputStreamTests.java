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
package io.pravega.common.io;

import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the BoundedInputStream class.
 */
public class BoundedInputStreamTests {
    private static final int MAX_SIZE = 10000;

    /**
     * Tests RevisionDataInputStream operations and respecting bounds.
     */
    @Test
    public void testBounds() throws Exception {
        val bound = MAX_SIZE / 2;
        val data = construct(MAX_SIZE);
        @Cleanup
        val input = new ByteArrayInputStream(data);
        @Cleanup
        val bis = new BoundedInputStream(input, bound);
        int expectedAvailable = bis.getBound();

        // Read 1 byte.
        int b1 = bis.read();
        expectedAvailable--;
        Assert.assertEquals("Unexpected result when reading 1 byte.", data[0], b1);
        Assert.assertEquals("Unexpected result from available() after reading 1 byte.", expectedAvailable, bis.available());

        // Read a buffer.
        byte[] buffer = new byte[10];
        int c = bis.read(buffer);
        expectedAvailable -= c;
        Assert.assertEquals("Unexpected number of bytes read.", buffer.length, c);
        AssertExtensions.assertArrayEquals("Unexpected data read into buffer.", data, 1, buffer, 0, c);
        Assert.assertEquals("Unexpected result from available() after reading a buffer.", expectedAvailable, bis.available());

        // Read a partial buffer.
        c = bis.read(buffer, 0, buffer.length / 2);
        expectedAvailable -= c;
        Assert.assertEquals("Unexpected number of bytes read.", buffer.length / 2, c);
        AssertExtensions.assertArrayEquals("Unexpected data read into buffer.", data,
                1 + buffer.length, buffer, 0, c);
        Assert.assertEquals("Unexpected result from available() after reading a buffer.", expectedAvailable, bis.available());

        // Skip over some bytes.
        long s = bis.skip(10);
        expectedAvailable -= s;
        Assert.assertEquals("Unexpected number of bytes skipped.", 10, s);
        Assert.assertEquals("Unexpected result from available() after skipping.", expectedAvailable, bis.available());

        // Skip over to the end.
        s = bis.skip(Integer.MAX_VALUE);
        Assert.assertEquals("Unexpected number of bytes skipped.", expectedAvailable, s);
        expectedAvailable = 0;
        Assert.assertEquals("Unexpected result from available() after skipping.", expectedAvailable, bis.available());

        // We are at the end. Verify we can't read anymore.
        Assert.assertEquals("Unexpected result from read() when reaching the end.", -1, bis.read());
        Assert.assertEquals("Unexpected result from read(byte[]) when reaching the end.", -1, bis.read(buffer));
        Assert.assertEquals("Unexpected result from read(byte[], int, int) when reaching the end.", -1, bis.read(buffer, 0, buffer.length));
        Assert.assertEquals("Unexpected result from skip() when reaching the end.", 0, bis.skip(10));

        bis.close();
        Assert.assertEquals("Unexpected number of bytes remaining to be read after skipRemaining().",
                data.length - bis.getBound(), input.available());
    }

    /**
     * Tests the subStream() method.
     */
    @Test
    public void testSubStream() throws Exception {
        val bound = MAX_SIZE / 2;
        val data = construct(MAX_SIZE);
        @Cleanup
        val input = new ByteArrayInputStream(data);
        @Cleanup
        val bis = new BoundedInputStream(input, bound);

        AssertExtensions.assertThrows(
                "subStream allowed a larger bound than available.",
                () -> bis.subStream(bound + 1),
                ex -> ex instanceof IllegalArgumentException);

        int size = 0;
        int expectedPos = 0;
        while (bis.available() > 0) {
            val s = bis.subStream(size);
            Assert.assertEquals("Unexpected bound from sub-stream for bound " + size, size, s.getBound());

            // Read a buffer.
            byte[] buffer = new byte[size / 2];
            int c = s.read(buffer);
            AssertExtensions.assertArrayEquals("Unexpected data read into buffer for bound " + size, data, expectedPos, buffer, 0, c);
            Assert.assertEquals("Unexpected result from available() after reading a buffer for bound " + size, s.getBound() - c, s.available());

            s.close();
            expectedPos += size;
            Assert.assertEquals("Base BoundedInputStream's position did not advance for bound " + size, bis.getBound() - expectedPos, bis.available());
            size = Math.min(size + 1, bis.available());
        }
    }

    /**
     * Tests the ability to skip remaining bytes upon closing.
     */
    @Test
    public void testSkipRemainingOnClose() throws Exception {
        for (int size = 0; size < MAX_SIZE; size++) {
            int bound = size / 2;
            val data = construct(size);
            @Cleanup
            val input = new ByteArrayInputStream(data);
            @Cleanup
            val bis = new BoundedInputStream(input, bound);
            Assert.assertEquals("Unexpected value for getBound().", bound, bis.getBound());
            if (size % 2 == 1) {
                // Only read some data for every other iteration.
                int bytesRead = bis.read(new byte[bound / 2]);
                Assert.assertEquals("Unexpected number of bytes read.", bound / 2, bytesRead);
            }

            bis.close();
            Assert.assertEquals("Unexpected number of bytes remaining to be read after skipRemaining().",
                    data.length - bis.getBound(), input.available());
        }
    }

    @Test
    public void testMarkSupported() throws Exception {
        @Cleanup
        val input = new BoundedInputStream(new ByteArrayInputStream(construct(1)), 1);
        Assert.assertFalse("BoundedInputStream should not support mark", input.markSupported());
    }

    private byte[] construct(int length) {
        byte[] result = new byte[length];
        for (int i = 0; i < length; i++) {
            result[i] = (byte) (i % Byte.MAX_VALUE);
        }

        return result;
    }
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.serialization;

import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.BitConverter;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the RevisionDataInputStream class for non-encoding functionality (that is tested in RevisionDataStreamCommonTests).
 */
public class RevisionDataInputStreamTests {
    private static final int MAX_SIZE = Byte.MAX_VALUE;
    private static final int LENGTH_BYTES = Integer.BYTES;

    /**
     * Tests RevisionDataInputStream operations and respecting bounds.
     */
    @Test
    public void testBounds() throws Exception {
        val data = construct(MAX_SIZE);
        @Cleanup
        val input = new ByteArrayInputStream(data);
        @Cleanup
        val ri = RevisionDataInputStream.wrap(input);
        int expectedAvailable = ri.getLength();

        // Read 1 byte.
        int b1 = ri.read();
        expectedAvailable--;
        Assert.assertEquals("Unexpected result when reading 1 byte.", data[0], b1);
        Assert.assertEquals("Unexpected result from available() after reading 1 byte.", expectedAvailable, ri.available());

        // Read a buffer.
        byte[] buffer = new byte[10];
        int c = ri.read(buffer);
        expectedAvailable -= c;
        Assert.assertEquals("Unexpected number of bytes read.", buffer.length, c);
        AssertExtensions.assertArrayEquals("Unexpected data read into buffer.", data, LENGTH_BYTES + 1, buffer, 0, c);
        Assert.assertEquals("Unexpected result from available() after reading a buffer.", expectedAvailable, ri.available());

        // Read a partial buffer.
        c = ri.read(buffer, 0, buffer.length / 2);
        expectedAvailable -= c;
        Assert.assertEquals("Unexpected number of bytes read.", buffer.length / 2, c);
        AssertExtensions.assertArrayEquals("Unexpected data read into buffer.", data,
                LENGTH_BYTES + 1 + buffer.length, buffer, 0, c);
        Assert.assertEquals("Unexpected result from available() after reading a buffer.", expectedAvailable, ri.available());

        // Skip over some bytes.
        c = ri.skipBytes(10);
        expectedAvailable -= c;
        Assert.assertEquals("Unexpected number of bytes skipped.", 10, c);
        Assert.assertEquals("Unexpected result from available() after skipping.", expectedAvailable, ri.available());

        // Skip over to the end.
        c = ri.skipBytes(Integer.MAX_VALUE);
        Assert.assertEquals("Unexpected number of bytes skipped.", expectedAvailable, c);
        expectedAvailable = 0;
        Assert.assertEquals("Unexpected result from available() after skipping.", expectedAvailable, ri.available());

        // We are at the end. Verify we can't read anymore.
        Assert.assertEquals("Unexpected result from read() when reaching the end.", -1, ri.read());
        Assert.assertEquals("Unexpected result from read(byte[]) when reaching the end.", -1, ri.read(buffer));
        Assert.assertEquals("Unexpected result from read(byte[], int, int) when reaching the end.", -1, ri.read(buffer, 0, buffer.length));
        Assert.assertEquals("Unexpected result from skip() when reaching the end.", 0, ri.skip(10));

        ri.close();
        Assert.assertEquals("Unexpected number of bytes remaining to be read after skipRemaining().",
                data.length - LENGTH_BYTES - ri.getLength(), input.available());
    }

    /**
     * Tests the ability to skip remaining bytes upon closing.
     */
    @Test
    public void testSkipRemaining() throws Exception {
        for (int size = 0; size < MAX_SIZE; size++) {
            val data = construct(size);
            @Cleanup
            val input = new ByteArrayInputStream(data);
            @Cleanup
            val ri = RevisionDataInputStream.wrap(input);
            Assert.assertEquals("Unexpected length read.", size, ri.getLength());
            if (size % 2 == 1) {
                // Only read some data for every other iteration.
                int bytesRead = ri.read(new byte[size / 2]);
                Assert.assertEquals("Unexpected number of bytes read.", size / 2, bytesRead);
            }

            ri.close();
            Assert.assertEquals("Unexpected number of bytes remaining to be read after skipRemaining().",
                    data.length - LENGTH_BYTES - ri.getLength(), input.available());
        }
    }

    private byte[] construct(int length) throws Exception {
        @Cleanup
        val output = new EnhancedByteArrayOutputStream();
        BitConverter.writeInt(output, length);
        @Cleanup
        val d = new DataOutputStream(output);

        // We write more in the buffer - to ensure that the bounds are respected.
        for (int i = 0; i < 2 * length; i++) {
            d.writeByte((byte) (i % Byte.MAX_VALUE));
        }

        return StreamHelpers.readAll(output.getData().getReader(), output.size());
    }
}

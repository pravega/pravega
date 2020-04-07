/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import io.pravega.test.common.AssertExtensions;

/**
 * Unit tests for FixedByteArrayOutputStream class.
 */
public class FixedByteArrayOutputStreamTests {
    /**
     * Tests that FixedByteArrayOutputStream works as advertised.
     */
    @Test
    public void testWrite() throws IOException {
        final int arraySize = 100;
        final int startOffset = 10;
        final int length = 50;
        final int secondHalfFillValue = 123;

        byte[] buffer = new byte[arraySize];
        Arrays.fill(buffer, (byte) 0);

        try (FixedByteArrayOutputStream os = new FixedByteArrayOutputStream(buffer, startOffset, length)) {
            // First half: write 1 by 1
            for (int i = 0; i < length / 2; i++) {
                os.write(i);
            }

            // Second half: write an entire array.
            byte[] secondHalf = new byte[length / 2];
            Arrays.fill(secondHalf, (byte) secondHalfFillValue);
            os.write(secondHalf);

            AssertExtensions.assertThrows(
                    "write(byte) allowed writing beyond the end of the stream.",
                    () -> os.write(255),
                    ex -> ex instanceof IOException);

            AssertExtensions.assertThrows(
                    "write(byte[]) allowed writing beyond the end of the stream.",
                    () -> os.write(new byte[10]),
                    ex -> ex instanceof IOException);
        }

        for (int i = 0; i < buffer.length; i++) {
            if (i < startOffset || i >= startOffset + length) {
                Assert.assertEquals("write() wrote data outside of its allocated bounds.", 0, buffer[i]);
            } else {
                int streamOffset = i - startOffset;
                if (streamOffset < length / 2) {
                    Assert.assertEquals("Unexpected value for stream index " + streamOffset, streamOffset, buffer[i]);
                } else {
                    Assert.assertEquals("Unexpected value for stream index " + streamOffset, secondHalfFillValue, buffer[i]);
                }
            }
        }
    }
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;

import io.pravega.test.common.AssertExtensions;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for StreamHelpers class.
 */
public class StreamHelpersTests {
    /**
     * Tests the readAll method that copies data into an existing array.
     */
    @Test
    public void testReadAllIntoArray() throws IOException {
        final int itemCount = 100;
        final byte[] buffer = new byte[itemCount];
        final int readStartOffset = 5;
        final int readLength = itemCount - readStartOffset - 5;
        for (int i = 0; i < itemCount; i++) {
            buffer[i] = (byte) i;
        }

        TestInputStream is = new TestInputStream(buffer);

        byte[] readResult = new byte[itemCount + 10];
        int readBytes = StreamHelpers.readAll(is, readResult, readStartOffset, readLength);
        Assert.assertEquals("Unexpected number of bytes read.", readLength, readBytes);
        for (int i = 0; i < readResult.length; i++) {
            if (i < readStartOffset || i >= readStartOffset + readLength) {
                Assert.assertEquals("readAll wrote data at wrong offset " + i, 0, readResult[i]);
            } else {
                int originalOffset = i - readStartOffset;
                Assert.assertEquals("unexpected value at target index " + i, buffer[originalOffset], readResult[i]);
            }
        }
    }

    /**
     * Tests the readAll method that copies data into an existing array.
     */
    @Test
    public void testReadAllNewArray() throws IOException {
        final int itemCount = 100;
        final byte[] buffer = new byte[itemCount];
        for (int i = 0; i < itemCount; i++) {
            buffer[i] = (byte) i;
        }

        byte[] readFullyData = StreamHelpers.readAll(new TestInputStream(buffer), buffer.length);
        Assert.assertArrayEquals(buffer, readFullyData);

        AssertExtensions.assertThrows(
                "readAll accepted a length higher than the given input stream length.",
                () -> StreamHelpers.readAll(new TestInputStream(buffer), buffer.length + 1),
                ex -> ex instanceof EOFException);
    }

    private static class TestInputStream extends InputStream {
        private final byte[] buffer;
        private int pos;
        private boolean pause;

        TestInputStream(byte[] buffer) {
            this.buffer = buffer;
            this.pos = 0;
            this.pause = false;
        }

        @Override
        public int read() throws IOException {
            if (pause) {
                pause = false;
                return -1;
            } else if (pos >= buffer.length) {
                return -1;
            } else {
                pause = true;
                return buffer[pos++];
            }
        }
    }
}

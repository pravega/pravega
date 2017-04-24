/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.common.io;

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

        byte[] readFullyData = StreamHelpers.readAll(new TestInputStream(buffer));
        Assert.assertArrayEquals(buffer, readFullyData);
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
        public int available() {
            return this.buffer.length - pos;
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

package com.emc.logservice.common;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Unit tests for StreamHelpers class.
 */
public class StreamHelpersTests {
    /**
     * Tests the readAll method.
     *
     * @throws IOException
     */
    @Test
    public void testReadAll() throws IOException {
        final int ItemCount = 100;
        final byte[] buffer = new byte[ItemCount];
        final int ReadStartOffset = 5;
        final int ReadLength = ItemCount - ReadStartOffset - 5;
        for (int i = 0; i < ItemCount; i++) {
            buffer[i] = (byte) i;
        }

        TestInputStream is = new TestInputStream(buffer);

        byte[] readResult = new byte[ItemCount + 10];
        int readBytes = StreamHelpers.readAll(is, readResult, ReadStartOffset, ReadLength);
        Assert.assertEquals("Unexpected number of bytes read.", ReadLength, readBytes);
        for (int i = 0; i < readResult.length; i++) {
            if (i < ReadStartOffset || i >= ReadStartOffset + ReadLength) {
                Assert.assertEquals("readAll wrote data at wrong offsset " + i, 0, readResult[i]);
            }
            else {
                int originalOffset = i - ReadStartOffset;
                Assert.assertEquals("unexpected value at target index " + i, buffer[originalOffset], readResult[i]);
            }
        }
    }

    private class TestInputStream extends InputStream {
        private final byte[] buffer;
        private int pos;
        private boolean pause;

        public TestInputStream(byte[] buffer) {
            this.buffer = buffer;
            this.pos = 0;
            this.pause = false;
        }

        @Override
        public int read() throws IOException {
            if (pause) {
                pause = false;
                return -1;
            }
            else if (pos >= buffer.length) {
                return -1;
            }
            else {
                pause = true;
                return buffer[pos++];
            }
        }
    }
}

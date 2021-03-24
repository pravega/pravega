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
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for StreamHelpers class.
 */
@Slf4j
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

    /**
     * Confirm StreamHelpers#readAll throws on null input stream
     */
    @Test
    public void readAllThrowsOnNullInputStream() {
        AssertExtensions.assertThrows("stream parameter cannot be null",
                () -> StreamHelpers.readAll(null, null, 0, 0),
                e -> e instanceof NullPointerException && e.getMessage().equals("stream"));
    }

    /**
     * Confirm StreamHelpers#readAll throws on null target byte array
     */
    @Test
    public void readAllThrowsOnNullTargetBuffer() {
        AssertExtensions.assertThrows("target parameter cannot be null",
                () -> StreamHelpers.readAll(new ByteArrayInputStream(new byte[0]), null, 0, 0),
                e -> e instanceof NullPointerException && e.getMessage().equals("target"));
    }

    /**
     * Confirm StreamHelpers#readAll throws on illegal offset
     */
    @Test
    public void readAllThrowsOnIllegalOffset() {
        AssertExtensions.assertThrows("start offset must be less than target buffer length",
                () -> StreamHelpers.readAll(new ByteArrayInputStream(new byte[0]), new byte[0], 0, 0),
                e -> e instanceof IndexOutOfBoundsException && e.getMessage().equals("startOffset (0) must be less than size (0)"));
    }

    /**
     * Confirm StreamHelpers#readAll throws on illegal max length
     */
    @Test
    public void readAllThrowsOnIllegalMaxLength() throws Exception {
        AssertExtensions.assertThrows("max length must be non-negative",
                () -> StreamHelpers.readAll(new ByteArrayInputStream(new byte[0]), new byte[1], 0, -1),
                e -> e instanceof IllegalArgumentException && e.getMessage().equals("maxLength: must be a non-negative number."));
    }

    @Test
    public void testCloseQuietly() {
        Closeable successful = new Closeable() {
            @Override
            public void close() throws IOException {
               //success;
            }
        };
        Closeable failed = new Closeable() {
            @Override
            public void close() throws IOException {
                throw new IOException("Expected");
            }
        };
        StreamHelpers.closeQuietly(successful, log, "no exception");
        StreamHelpers.closeQuietly(failed, log, "expected message");
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

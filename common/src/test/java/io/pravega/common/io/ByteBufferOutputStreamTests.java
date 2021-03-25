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

import io.pravega.common.io.serialization.RandomAccessOutputStream;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Random;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link ByteBufferOutputStream} class.
 */
public class ByteBufferOutputStreamTests {
    private static final int INITIAL_LENGTH = 11; // Some arbitrary, odd, prime number.

    /**
     * Tests basic {@link OutputStream} functionality.
     */
    @Test
    public void testBasicOutputStream() throws Exception {
        @Cleanup
        val s = new ByteBufferOutputStream(INITIAL_LENGTH);
        Assert.assertEquals("Unexpected empty size.", 0, s.size());
        Assert.assertEquals("Unexpected empty getData().", 0, s.getData().getLength());

        @Cleanup
        val expected = new ByteArrayOutputStream();

        // write(byte)
        for (int i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
            s.write(i);
            expected.write(i);
            Assert.assertEquals("Unexpected size.", expected.size(), s.size());
        }

        // write(array)
        byte[] array = expected.toByteArray(); // This should have everything we want in it.
        s.write(array);
        expected.write(array);

        // write(array, offset, length)
        s.write(array, 1, 0);
        expected.write(array, 1, 0);
        int offset = 12;
        s.write(array, offset, array.length - 3 * offset);
        expected.write(array, offset, array.length - 3 * offset);

        val expectedData = expected.toByteArray();
        val actualData = s.getData().getCopy();
        Assert.assertArrayEquals("Unexpected data.", expectedData, actualData);
    }

    /**
     * Tests the {@link RandomAccessOutputStream} implementation.
     */
    @Test
    public void testRandomAccessOutputStream() {
        final int maxSize = 1000;
        @Cleanup
        val s = new ByteBufferOutputStream(INITIAL_LENGTH);

        val expected = new ByteArraySegment(new byte[maxSize]);

        // "Format" the stream.
        s.write(new byte[maxSize], 0, maxSize, 0);
        Assert.assertEquals("After formatting.", expected, s.getData());

        // Write arbitrary bytes.
        for (int i = 0; i < maxSize; i++) {
            s.write(i % Byte.MAX_VALUE, i);
            expected.set(i, (byte) (i % Byte.MAX_VALUE));
        }

        Assert.assertEquals("After write(byte, index).", expected, s.getData());

        // Write arbitrary ints.
        for (int i = 0; i < maxSize; i += Integer.BYTES) {
            int value = i * i * i;
            s.writeInt(value, i);
            expected.setInt(i, value);
        }

        Assert.assertEquals("After writeInt(int, index).", expected, s.getData());

        // Write arrays.
        int arrayCount = 50;
        val rnd = new Random(0);
        byte[] array = new byte[maxSize];
        rnd.nextBytes(array);
        for (int i = 0; i < arrayCount; i++) {
            int sliceOffset = rnd.nextInt(array.length);
            int sliceLength = sliceOffset == array.length - 1 ? 0 : rnd.nextInt(array.length - sliceOffset);
            int streamPosition = rnd.nextInt(array.length - sliceLength);
            s.write(array, sliceOffset, sliceLength, streamPosition);
            expected.copyFrom(new ByteArraySegment(array, sliceOffset, sliceLength), streamPosition, sliceLength);

            Assert.assertEquals("After write(array, streamPosition).", expected, s.getData());
        }
    }

    /**
     * Tests the {@link DirectDataOutput} implementation.
     */
    @Test
    public void testDirectDataOutput() {
        final int shortCount = 21;
        final int intCount = 19;
        final int longCount = 17;
        final int bufferSize = 919;
        final int maxSize = shortCount * Short.BYTES + intCount * Integer.BYTES + longCount * Long.BYTES + bufferSize;
        @Cleanup
        val s = new ByteBufferOutputStream(INITIAL_LENGTH);
        val expected = new ByteArraySegment(new byte[maxSize]);
        int offset = 0;
        val rnd = new Random(0);

        // writeShort()
        for (int i = 0; i < shortCount; i++) {
            short n = (short) (rnd.nextInt() % (2 * Short.MAX_VALUE) - Short.MAX_VALUE);
            s.writeShort(n);
            expected.setShort(offset, n);
            offset += Short.BYTES;
        }

        // writeInt()
        for (int i = 0; i < intCount; i++) {
            int n = rnd.nextInt();
            s.writeInt(n);
            expected.setInt(offset, n);
            offset += Integer.BYTES;
        }

        // writeLong()
        for (int i = 0; i < longCount; i++) {
            long n = rnd.nextLong();
            s.writeLong(n);
            expected.setLong(offset, n);
            offset += Long.BYTES;
        }

        // writeBuffer()
        val randomBuffer = new byte[bufferSize];
        rnd.nextBytes(randomBuffer);
        s.writeBuffer(new ByteArraySegment(randomBuffer));
        expected.copyFrom(new ByteArraySegment(randomBuffer), offset, randomBuffer.length);

        // Check.
        Assert.assertEquals(expected, s.getData());
    }

    /**
     * Tests offset-accepting methods for invalid arguments.
     */
    @Test
    public void testOutOfBounds() {
        @Cleanup
        val s = new ByteBufferOutputStream();
        AssertExtensions.assertThrows(
                "write(byte) allowed writing at negative position.",
                () -> s.write(1, -1),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                "write(int) allowed writing at negative position.",
                () -> s.writeInt(1, -1),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                "write(array) allowed writing negative position.",
                () -> s.write(new byte[1], 0, 1, -1),
                ex -> ex instanceof IllegalArgumentException);
    }
}

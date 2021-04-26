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
package io.pravega.common.util;

import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.Arrays;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for ByteArraySegment class.
 */
public class ByteArraySegmentTests extends StructuredBufferTestBase {

    /**
     * Tests the basic functionality of get() and set() methods.
     */
    @Test
    public void testGetSet() {
        final int incrementValue = 7;
        final int decrementValue = 11;
        final byte[] buffer = createFormattedBuffer();
        final int halfOffset = buffer.length / 2;
        ByteArraySegment s1 = new ByteArraySegment(buffer, 0, halfOffset);
        ByteArraySegment s2 = new ByteArraySegment(buffer, halfOffset, buffer.length - halfOffset);

        //s1 - increment by 7
        for (int i = 0; i < s1.getLength(); i++) {
            Assert.assertEquals("Unexpected value for initial get in first half of buffer at index " + i, i, s1.get(i));
            byte newValue = (byte) (s1.get(i) + incrementValue);
            s1.set(i, newValue);
            Assert.assertEquals("Unexpected value for updated get in first half of buffer at index " + i, newValue, s1.get(i));
            Assert.assertEquals("Unexpected value for the base buffer (first half) at index " + i, newValue, buffer[i]);
        }

        //s2 - decrement by 11
        for (int i = 0; i < s2.getLength(); i++) {
            Assert.assertEquals("Unexpected value for initial get in second half of buffer at index " + i, i + halfOffset, s2.get(i));
            byte newValue = (byte) (s2.get(i) - decrementValue);
            s2.set(i, newValue);
            Assert.assertEquals("Unexpected value for updated get in second half of buffer at index " + i, newValue, s2.get(i));
            Assert.assertEquals("Unexpected value for the base buffer (second half) at index " + (i - halfOffset), newValue, buffer[i + halfOffset]);
        }
    }

    /**
     * Tests the functionality of copyFrom.
     */
    @Test
    public void testCopyFrom() {
        final byte[] sourceBuffer = createFormattedBuffer();
        final int targetOffset = 11;
        final byte[] targetBuffer = new byte[sourceBuffer.length + targetOffset];
        final int copyLength = sourceBuffer.length - 7;

        ByteArraySegment source = new ByteArraySegment(sourceBuffer);
        ByteArraySegment target = new ByteArraySegment(targetBuffer);

        // Copy second part.
        target.copyFrom(source, targetOffset, copyLength);
        for (int i = 0; i < targetBuffer.length; i++) {
            int expectedValue = i < targetOffset || i >= targetOffset + copyLength ? 0 : i - targetOffset;
            Assert.assertEquals("Unexpected value after copyFrom (1) in segment at offset " + i, expectedValue, target.get(i));
            Assert.assertEquals("Unexpected value after copyFrom (1) in base buffer at offset " + i, expectedValue, targetBuffer[i]);
        }

        // Test copyFrom with source offset.
        Arrays.fill(targetBuffer, (byte) 0);
        final int sourceOffset = 3;
        target.copyFrom(source, sourceOffset, targetOffset, copyLength);
        for (int i = 0; i < targetBuffer.length; i++) {
            int expectedValue = i < targetOffset || i >= targetOffset + copyLength ? 0 : (i - targetOffset + sourceOffset);
            Assert.assertEquals("Unexpected value after copyFrom (2) in segment at offset " + i, expectedValue, target.get(i));
            Assert.assertEquals("Unexpected value after copyFrom (2) in base buffer at offset " + i, expectedValue, targetBuffer[i]);
        }
    }

    /**
     * Tests the functionality of copyTo(array).
     */
    @Test
    public void testCopyToArray() {
        final byte[] sourceBuffer = createFormattedBuffer();
        final int targetOffset = 10;
        final byte[] targetBuffer = new byte[sourceBuffer.length + targetOffset];
        final int copyLength = sourceBuffer.length - 7;

        ByteArraySegment source = new ByteArraySegment(sourceBuffer);

        // Copy second part.
        source.copyTo(targetBuffer, targetOffset, copyLength);
        for (int i = 0; i < targetBuffer.length; i++) {
            int expectedValue = i < targetOffset || i >= targetOffset + copyLength ? 0 : i - targetOffset;
            Assert.assertEquals("Unexpected value after copyFrom (second half) in base buffer at offset " + i, expectedValue, targetBuffer[i]);
        }
    }

    @Test
    public void testIterateBuffers() {
        final byte[] buffer = createFormattedBuffer();
        val segment = new ByteArraySegment(buffer, 1, buffer.length - 3);
        val i = segment.iterateBuffers();
        val b = i.next();
        Assert.assertFalse(i.hasNext());
        Assert.assertEquals(segment.getLength(), b.remaining());
        AssertExtensions.assertArrayEquals("", segment.array(), segment.arrayOffset(),
                b.array(), b.arrayOffset() + b.position(), b.remaining());
    }

    @Test
    public void testByteBufferConstructor() {
        val data = createFormattedBuffer();
        val b1 = ByteBuffer.wrap(data, 1, data.length - 1);
        b1.position(2).limit(10);

        val bas = new ByteArraySegment(b1);
        Assert.assertSame(data, b1.array());
        Assert.assertEquals(b1.position() + b1.arrayOffset(), bas.arrayOffset());
        Assert.assertEquals(b1.remaining(), bas.getLength());
        AssertExtensions.assertThrows(IndexOutOfBoundsException.class, () -> bas.set(10, (byte) 1));

        val b2 = bas.asByteBuffer();
        Assert.assertEquals(b1, b2);
    }

    @Override
    protected BufferView toBufferView(ArrayView data) {
        return new ByteArraySegment(data.array(), data.arrayOffset(), data.getLength());
    }

    private byte[] createFormattedBuffer() {
        byte[] buffer = new byte[Byte.MAX_VALUE];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = (byte) i;
        }

        return buffer;
    }

    @Override
    protected StructuredWritableBuffer newWritableBuffer() {
        return new ByteArraySegment(new byte[1023]);
    }
}

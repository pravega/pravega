/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import io.pravega.common.io.StreamHelpers;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for ByteArraySegment class.
 */
public class ByteArraySegmentTests {

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
     * Tests the functionality of copyTo.
     */
    @Test
    public void testCopyTo() {
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

    /**
     * Tests the functionality of writeTo and readFrom.
     */
    @Test
    public void testWriteToReadFrom() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        int count = 10;
        ArrayList<ByteArraySegment> sourceSegments = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ByteArraySegment s = new ByteArraySegment(createFormattedBuffer());
            sourceSegments.add(s);
            s.writeTo(outputStream);
        }

        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        for (int i = 0; i < count; i++) {
            ByteArraySegment s = sourceSegments.get(i);
            ByteArraySegment t = new ByteArraySegment(new byte[s.getLength()]);
            t.readFrom(inputStream);

            Assert.assertEquals("Source and target lengths differ.", s.getLength(), t.getLength());
            for (int j = 0; j < s.getLength(); j++) {
                if (t.get(j) != s.get(j)) {
                    Assert.fail(String.format("Source at target differ at index %d.", j));
                }
            }
        }
    }

    /**
     * Tests the functionality of getReader (the ability to return an InputStream from a sub-segment of the main buffer).
     */
    @Test
    public void testGetReader() throws IOException {
        final byte[] buffer = createFormattedBuffer();
        ByteArraySegment segment = new ByteArraySegment(buffer);

        for (int offset = 0; offset < buffer.length / 2; offset++) {
            int length = buffer.length - offset * 2;
            byte[] readBuffer = new byte[length];
            try (InputStream stream = segment.getReader(offset, length)) {
                int readBytes = StreamHelpers.readAll(stream, readBuffer, 0, readBuffer.length);
                Assert.assertEquals("Unexpected number of bytes read from the InputStream at offset " + offset, length, readBytes);
            }

            for (int i = 0; i < length; i++) {
                Assert.assertEquals("Unexpected value at index " + i + " after reading from offset " + offset, segment.get(i + offset), readBuffer[i]);
            }
        }
    }

    /**
     * Tests the functionality of getWriter (the ability to return an OutputStream that can be used to write to the main buffer).
     */
    @Test
    public void testGetWriter() throws IOException {
        final byte[] buffer = new byte[Byte.MAX_VALUE];
        ByteArraySegment segment = new ByteArraySegment(buffer);
        try (OutputStream writer = segment.getWriter()) {
            for (int i = 0; i < buffer.length; i++) {
                writer.write(i);
            }
        }
        for (int i = 0; i < buffer.length; i++) {
            Assert.assertEquals("Unexpected value in segment at index " + i, i, segment.get(i));
            Assert.assertEquals("Unexpected value in source buffer at index " + i, i, buffer[i]);
        }
    }

    /**
     * Tests the ability for the ByteArraySegment to create sub-segments.
     */
    @Test
    public void testSubSegment() {
        final byte[] buffer = createFormattedBuffer();
        ByteArraySegment segment = new ByteArraySegment(buffer);

        // As long as the size of the segment > 1, choose a segment half the length of the current one.
        // If current iteration is odd, choose the upper segment. If it is even, choose the lower one.
        int iteration = 0;
        int startOffset = 0;
        while (segment.getLength() > 1) {
            iteration++;

            // Check correctness.
            for (int i = 0; i < segment.getLength(); i++) {
                Assert.assertEquals(String.format("Unexpected value at offset %d for subsegment (O=%d, L=%d), iteration %d.", i, startOffset, segment.getLength(), iteration), buffer[i + startOffset], segment.get(i));
            }

            // Pick a new size and create a new subsegment.
            if (iteration % 2 == 0) {
                // Upper half for even iterations.
                startOffset = startOffset + segment.getLength() / 2;
                segment = segment.subSegment(segment.getLength() / 2, segment.getLength() - segment.getLength() / 2);
            } else {
                // Lower half for odd iterations.
                segment = segment.subSegment(0, segment.getLength() / 2);
            }
        }
    }

    /**
     * Tests the behavior of the ByteArraySegment in read-only mode.
     */
    @Test
    public void testReadOnly() throws Exception {
        final byte[] buffer = createFormattedBuffer();
        ByteArraySegment segment = new ByteArraySegment(buffer, 0, buffer.length, true);

        // Check the isReadonly flag
        Assert.assertTrue("Unexpected value for isReadOnly() for read-only segment.", segment.isReadOnly());
        Assert.assertFalse("Unexpected value for isReadOnly() for non-read-only segment.", new ByteArraySegment(buffer).isReadOnly());

        // Verify that "mutator" methods do not work.
        checkReadOnlyException("copyFrom", () -> segment.copyFrom(new ByteArraySegment(new byte[10], 0, 10), 0, 10));
        checkReadOnlyException("getWriter", segment::getWriter);
        checkReadOnlyException("set", () -> segment.set(0, (byte) 0));

        // Check to see that, even though we did get an exception, the buffer was not modified.
        for (int i = 0; i < buffer.length; i++) {
            Assert.assertEquals("One of the 'mutator' methods modified the buffer at index " + i, i, buffer[i]);
        }

        // Check that a subsegment is also read-only.
        Assert.assertTrue("Unexpected value for isReadOnly() for read-only sub-segment.", segment.subSegment(0, 1).isReadOnly());
        Assert.assertTrue("Unexpected value for isReadOnly() for read-only sub-segment from non-read-only segment (when attempting to create a non-read-only segment).", segment.subSegment(0, 1, false).isReadOnly());
        Assert.assertTrue("Unexpected value for isReadOnly() for read-only sub-segment from non-read-only segment.", new ByteArraySegment(buffer).subSegment(0, 1, true).isReadOnly());
    }

    private void checkReadOnlyException(String methodName, AssertExtensions.RunnableWithException code) {
        AssertExtensions.assertThrows(IllegalStateException.class, code);
    }

    private byte[] createFormattedBuffer() {
        byte[] buffer = new byte[Byte.MAX_VALUE];
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = (byte) i;
        }

        return buffer;
    }
}

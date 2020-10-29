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

import io.pravega.common.io.FixedByteArrayOutputStream;
import io.pravega.common.io.StreamHelpers;
import io.pravega.test.common.AssertExtensions;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link CompositeByteArraySegment} class.
 */
public class CompositeByteArraySegmentTests extends BufferViewTestBase {
    private static final int ARRAY_SIZE = 128;
    private static final int ARRAY_COUNT = 6;
    private static final int LENGTH = ARRAY_SIZE * ARRAY_COUNT - ARRAY_SIZE / 4;

    /**
     * Tests the {@link CompositeByteArraySegment#set}, {@link CompositeByteArraySegment#get} and
     * {@link CompositeByteArraySegment#getCopy()} methods.
     */
    @Test
    public void testGetSet() throws Exception {
        val s = emptyBuffer();
        Assert.assertEquals("Unexpected empty buffer length.", LENGTH, s.getLength());
        Assert.assertEquals("Not expecting any arrays to be allocated.", 0, s.getAllocatedArrayCount());

        // Empty buffer should always return 0s.
        for (int i = 0; i < s.getLength(); i++) {
            Assert.assertEquals("Empty buffer should be all 0s.", 0, s.get(i));
        }

        // Set arbitrary values every 2 arrays.
        int expectedArrayCount = 0;
        final int boundaryOffset = 3;
        final int skip = 2 * ARRAY_SIZE;
        for (int i = boundaryOffset; i < s.getLength(); i += skip) {
            expectedArrayCount++;
            s.set(i, (byte) (i % Byte.MAX_VALUE));
            Assert.assertEquals("Unexpected number of arrays allocated.", expectedArrayCount, s.getAllocatedArrayCount());
        }

        // Verify data..
        val getCopyContents = s.getCopy();
        val getReaderContents = StreamHelpers.readAll(s.getReader(), s.getLength());
        for (int i = 0; i < s.getLength(); i++) {
            byte expectedValue = (i - boundaryOffset) % skip == 0 ? (byte) (i % Byte.MAX_VALUE) : 0;
            Assert.assertEquals("Unexpected value at index " + i, expectedValue, s.get(i));
            Assert.assertEquals("Unexpected value via getCopy() at index " + i, expectedValue, getCopyContents[i]);
            Assert.assertEquals("Unexpected value via getReader() at index " + i, expectedValue, getReaderContents[i]);
        }

        val buffers = getContents(s);
        Assert.assertEquals("Unexpected number of buffers.", ARRAY_COUNT, buffers.size());
        for (int i = 0; i < buffers.size(); i++) {
            val b = buffers.get(i);
            int expectedSize = i == ARRAY_COUNT - 1 ? LENGTH % ARRAY_SIZE : ARRAY_SIZE;
            Assert.assertEquals("Unexpected buffer size at array index " + i, expectedSize, b.remaining());
            AssertExtensions.assertArrayEquals("", getCopyContents, i * ARRAY_SIZE, b.array(), b.arrayOffset(), expectedSize);
        }
    }

    /**
     * Tests the {@link CompositeByteArraySegment#copyFrom} and {@link CompositeByteArraySegment#getCopy()} methods.
     */
    @Test
    public void testCopyFrom() {
        testProgressiveCopies((expectedData, s, offset, length) -> {
            // Check number of allocated arrays.
            int expectedArrayCount = getExpectedArrayCount(offset, length);
            Assert.assertEquals("Unexpected allocated arrays for step " + offset, expectedArrayCount, s.getAllocatedArrayCount());

            // Check via getCopy().
            val contents = s.getCopy();
            Assert.assertArrayEquals("Unexpected contents via getCopy() for step " + offset, expectedData, contents);

            // Check via get().
            for (int i = 0; i < expectedData.length; i++) {
                Assert.assertEquals("Unexpected contents via get() for step " + offset, expectedData[i], s.get(i));
            }
        });
    }

    /**
     * Tests the {@link CompositeByteArraySegment#copyTo(ByteBuffer)} method.
     */
    @Test
    public void testCopyToByteBuffer() {
        testProgressiveCopies((expectedData, s, offset, length) -> {
            val targetData = new byte[s.getLength()];
            s.copyTo(ByteBuffer.wrap(targetData));
            Assert.assertArrayEquals("Unexpected data copied for step " + offset, expectedData, targetData);
        });
    }

    /**
     * Tests the functionality of {@link CompositeByteArraySegment#copyTo(OutputStream)}.
     */
    @Test
    public void testCopyToStream() {
        testProgressiveCopies((expectedData, s, offset, length) -> {
            val targetData = new byte[s.getLength()];
            s.copyTo(new FixedByteArrayOutputStream(targetData, 0, targetData.length));
            Assert.assertArrayEquals("Unexpected data copied for step " + offset, expectedData, targetData);
        });
    }

    /**
     * Tests the functionality of {@link CompositeByteArraySegment#collect(CompositeArrayView.Collector)}.
     */
    @Test
    public void testCollect() {
        testProgressiveCopies((expectedData, s, offset, length) -> {
            val targetData = new byte[s.getLength()];
            val targetOffset = new AtomicInteger();
            val count = new AtomicInteger();
            s.collect(bb -> {
                int len = bb.remaining();
                bb.get(targetData, targetOffset.get(), len);
                targetOffset.addAndGet(len);
                count.incrementAndGet();
            });

            Assert.assertEquals("Unexpected number of components.", count.get(), s.getComponentCount());
            Assert.assertArrayEquals("Unexpected data collected for step " + offset, expectedData, targetData);
        });
    }

    /**
     * Tests the {@link CompositeByteArraySegment#slice} method while reading indirectly by invoking
     * {@link CompositeByteArraySegment#getReader(int, int)}.
     */
    @Test
    public void testSliceRead() {
        testProgressiveCopies((expectedData, s, offset, length) -> {
            val targetData = new byte[s.getLength()];
            s.copyTo(new FixedByteArrayOutputStream(targetData, 0, targetData.length));

            for (int sliceOffset = 0; sliceOffset <= s.getLength() / 2; sliceOffset++) {
                val sliceLength = s.getLength() - 2 * sliceOffset;
                InputStream reader = s.getReader(sliceOffset, sliceLength);
                if (sliceLength == 0) {
                    Assert.assertEquals("Unexpected data read for empty slice.", -1, reader.read());
                } else {
                    val actualData = StreamHelpers.readAll(reader, sliceLength);
                    AssertExtensions.assertArrayEquals("Unexpected data sliced for step " + offset,
                            targetData, sliceOffset, actualData, 0, actualData.length);
                }
            }
        });
    }

    /**
     * Tests the {@link CompositeByteArraySegment#slice} method while writing (verifies that changes in a slice reflect
     * in the parent segment).
     */
    @Test
    public void testSliceWrite() {
        val s = emptyBuffer();

        // Set arbitrary values every 2 arrays.
        int expectedArrayCount = 0;
        final int boundaryOffset = 3;
        final int skip = 2 * ARRAY_SIZE;
        for (int i = boundaryOffset; i < s.getLength(); i += skip) {
            expectedArrayCount++;

            // Slice a 1-byte section and then verify it reflects in the main segment.
            s.slice(i, 1).set(0, (byte) (i % Byte.MAX_VALUE));
            Assert.assertEquals("Unexpected number of arrays allocated.", expectedArrayCount, s.getAllocatedArrayCount());
        }

        // Verify data.
        for (int i = 0; i < s.getLength(); i++) {
            byte expectedValue = (i - boundaryOffset) % skip == 0 ? (byte) (i % Byte.MAX_VALUE) : 0;
            Assert.assertEquals("Unexpected value at index " + i, expectedValue, s.get(i));
            Assert.assertEquals("Unexpected value via slice at index " + i, expectedValue, s.slice(i, 1).get(0));
        }
    }

    @SneakyThrows
    private void testProgressiveCopies(CheckData check) {
        val data = randomData();
        for (int offset = 0; offset <= LENGTH / 2; offset++) {

            // Slice the underlying data and determine the expected outcome.
            val sourceData = new ByteArraySegment(data, offset, data.length - 2 * offset);
            val expectedData = new byte[data.length];
            sourceData.copyTo(expectedData, sourceData.arrayOffset(), sourceData.getLength());

            // Populate the buffer and check it.
            val s = emptyBuffer();
            s.copyFrom(sourceData.getBufferViewReader(), offset, sourceData.getLength());
            check.accept(expectedData, s, offset, sourceData.getLength());
        }
    }

    /**
     * Expected number of allocated arrays for a contiguous set of data.
     */
    private int getExpectedArrayCount(int offset, int length) {
        if (length == 0) {
            return 0;
        }

        int result = (offset + length) / ARRAY_SIZE - offset / ARRAY_SIZE;
        if ((offset + length) % ARRAY_SIZE != 0) {
            result++;
        }

        return result;
    }

    private CompositeByteArraySegment emptyBuffer() {
        return new CompositeByteArraySegment(LENGTH, ARRAY_SIZE);
    }

    private byte[] randomData() {
        val expectedData = new byte[LENGTH];
        val rnd = new Random(0);
        rnd.nextBytes(expectedData);
        return expectedData;
    }

    @Override
    protected BufferView toBufferView(ArrayView data) {
        val result = new CompositeByteArraySegment(data.getLength(), ARRAY_SIZE);
        result.copyFrom(data.getBufferViewReader(), 0, data.getLength());
        return result;

    }

    @FunctionalInterface
    private interface CheckData {
        void accept(byte[] expectedData, CompositeByteArraySegment segment, int startOffset, int length) throws Exception;
    }
}

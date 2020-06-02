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
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base test class for any class implementing {@link BufferView}. Tests common functionality across all implementations.
 * Any derived classes should test implementation-specific features.
 */
public abstract class BufferViewTestBase {
    private static final int BUFFER_SIZE = 1024;
    private static final int SKIP_COUNT = 10;
    private final Random rnd = new Random(0);

    /**
     * Tests all functionality.
     * @throws Exception if an exception occurred.
     */
    @Test
    public void testBasicFunctionality() throws Exception {
        val data = newData();
        val expectedData = new byte[data.getLength() - SKIP_COUNT];
        System.arraycopy(data.array(), data.arrayOffset() + SKIP_COUNT, expectedData, 0, expectedData.length);

        val wrapData = (ArrayView) data.slice(SKIP_COUNT, data.getLength() - SKIP_COUNT);
        @Cleanup("release")
        val wrap = toBufferView(wrapData);

        // Length.
        Assert.assertEquals("Unexpected length.", wrapData.getLength(), wrap.getLength());

        // Get Copy.
        val copy = wrap.getCopy();
        Assert.assertArrayEquals("Unexpected result from getCopy.", expectedData, copy);

        // Get BufferView Reader.
        val bufferViewReader = wrap.getBufferViewReader();
        val bufferViewReaderData = bufferViewReader.readFully(2);
        AssertExtensions.assertArrayEquals("Unexpected result from getReader.", expectedData, 0,
                bufferViewReaderData.array(), bufferViewReaderData.arrayOffset(), expectedData.length);

        // Copy To OutputStream.
        @Cleanup
        val outputStream1 = new ByteArrayOutputStream();
        wrap.copyTo(outputStream1);
        Assert.assertArrayEquals("Unexpected result from copyTo(OutputStream).", expectedData, outputStream1.toByteArray());

        // Copy To ByteBuffer.
        val array1 = new byte[expectedData.length];
        wrap.copyTo(ByteBuffer.wrap(array1));
        Assert.assertArrayEquals("Unexpected result from copyTo(ByteBuffer).", expectedData, array1);
        val array2 = new byte[expectedData.length * 2];
        wrap.copyTo(ByteBuffer.wrap(array2));
        AssertExtensions.assertArrayEquals("Unexpected result from copyTo(ByteBuffer*2).",
                expectedData, 0, array2, 0, expectedData.length);
        for (int i = expectedData.length; i < array2.length; i++) {
            Assert.assertEquals(0, array2[i]);
        }
    }

    /**
     * Tests the functionality of copyTo(array).
     */
    @Test
    public void testCopyToByteBuffer() {
        val data1 = newData();
        val data2 = newData();
        @Cleanup("release")
        val b1 = toBufferView(data1);
        @Cleanup("release")
        val b2 = toBufferView(data2);
        val target = new byte[b1.getLength() + b2.getLength()];
        val targetBuffer = ByteBuffer.wrap(target);
        b1.copyTo(targetBuffer);
        b2.copyTo(targetBuffer);

        val expectedData = new byte[data1.getLength() + data2.getLength()];
        System.arraycopy(data1.array(), data1.arrayOffset(), expectedData, 0, data1.getLength());
        System.arraycopy(data2.array(), data2.arrayOffset(), expectedData, data1.getLength(), data2.getLength());
        Assert.assertArrayEquals(expectedData, target);
    }

    /**
     * Tests the functionality of  {@link BufferView#copyTo(OutputStream)}.
     * @throws IOException if an exception occurred.
     */
    @Test
    public void testCopyToStream() throws IOException {
        val data = newData();
        val expectedData = data.getCopy();
        @Cleanup("release")
        val wrap = toBufferView(data);

        for (int offset = 0; offset < data.getLength() / 2; offset++) {
            int length = data.getLength() - offset * 2;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            wrap.slice(offset, length).copyTo(bos);
            val actualData = bos.toByteArray();
            Assert.assertEquals(length, actualData.length);
            AssertExtensions.assertArrayEquals("", expectedData, offset, actualData, 0, actualData.length);
        }
    }

    /**
     * Tests the functionality of {@link BufferView#getBufferViewReader()}.
     * @throws Exception if an exception occurred.
     */
    @Test
    public void testGetBufferViewReader() throws Exception {
        val data = newData();
        val expectedData = data.getCopy();
        @Cleanup("release")
        val wrap = toBufferView(data);

        for (int offset = 0; offset < data.getLength() / 2; offset++) {
            int length = data.getLength() - offset * 2;

            // ReadFully.
            val readFullyReader = wrap.slice(offset, length).getBufferViewReader();
            val readFullyResult = readFullyReader.readFully(2);
            Assert.assertEquals(0, readFullyReader.readBytes(new ByteArraySegment(new byte[1])));
            AssertExtensions.assertArrayEquals("ReadFully offset " + offset,
                    expectedData, offset, readFullyResult.array(), readFullyResult.arrayOffset(), length);

            // ReadSlice
            val readSliceReader = wrap.slice(offset, length).getBufferViewReader();
            int sliceLength = 0;
            int position = 0;
            while (readSliceReader.available() > 0) {
                val slice = readSliceReader.readSlice(Math.min(sliceLength, readSliceReader.available()));
                Assert.assertEquals("readSlice/available offset " + offset + " length " + slice.getLength(),
                        length - position - slice.getLength(), readSliceReader.available());

                val expectedSlice = data.slice(offset + position, slice.getLength());
                Assert.assertEquals("readSlice offset " + offset + " length " + slice.getLength(), expectedSlice, slice);
                sliceLength++;
                position += slice.getLength();
            }
            AssertExtensions.assertThrows("ReadSlice.End offset " + offset, () -> readSliceReader.readSlice(1), ex -> ex instanceof EOFException);

            // ReadByte.
            val readByteReader = wrap.slice(offset, length).getBufferViewReader();
            val readByteResult = new byte[length];
            for (int i = 0; i < length; i++) {
                readByteResult[i] = (byte) readByteReader.readByte();
                Assert.assertEquals(length - i - 1, readByteReader.available());
            }

            AssertExtensions.assertThrows("ReadByte.End offset " + offset, readByteReader::readByte, ex -> ex instanceof EOFException);
            AssertExtensions.assertArrayEquals("ReadByte offset " + offset, expectedData, offset, readByteResult, 0, length);
        }
    }

    /**
     * Tests the functionality of {@link BufferView} (the ability to return an InputStream from a sub-segment of the main buffer).
     * @throws IOException if an exception occurred.
     */
    @Test
    public void testGetReader() throws IOException {
        val data = newData();
        val expectedData = data.getCopy();
        @Cleanup("release")
        val wrap = toBufferView(data);

        for (int offset = 0; offset < data.getLength() / 2; offset++) {
            int length = data.getLength() - offset * 2;
            byte[] readBuffer = new byte[length];
            try (InputStream stream = wrap.getReader(offset, length)) {
                int readBytes = StreamHelpers.readAll(stream, readBuffer, 0, readBuffer.length);
                Assert.assertEquals("Unexpected number of bytes read from the InputStream at offset " + offset, length, readBytes);
            }

            AssertExtensions.assertArrayEquals("Unexpected data for offset " + offset, expectedData, offset, readBuffer, 0, readBuffer.length);
        }
    }

    /**
     * Tests the ability of {@link BufferView} to return slices of itself.
     * @throws IOException if an exception occurred.
     */
    @Test
    public void testSlice() throws IOException {
        val data = newData();
        @Cleanup("release")
        val wrap = toBufferView(data);
        for (int offset = 0; offset < data.getLength(); offset += 19) {
            for (int length = 0; length < data.getLength() - offset; length += 11) {
                val expected = new byte[length];
                System.arraycopy(data.array(), data.arrayOffset() + offset, expected, 0, length);
                val slice = wrap.slice(offset, length).getCopy();
                Assert.assertArrayEquals("Unexpected slice() result for offset " + offset + ", length " + length, expected, slice);
                if (length == 0) {
                    Assert.assertEquals("Unexpected getReader() result for offset " + offset + ", length " + length,
                            0, wrap.getReader(offset, length).available());
                } else {
                    val sliceData = StreamHelpers.readAll(wrap.getReader(offset, length), length);
                    Assert.assertArrayEquals("Unexpected getReader() result for offset " + offset + ", length " + length, expected, sliceData);
                }
            }
        }
    }

    @Test
    public abstract void testGetContents();

    protected ArrayView newData() {
        byte[] data = new byte[BUFFER_SIZE];
        rnd.nextBytes(data);
        return new ByteArraySegment(data);
    }

    protected abstract BufferView toBufferView(ArrayView data);
}

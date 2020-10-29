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

import com.google.common.collect.Lists;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.io.StreamHelpers;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;
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
        val bufferView = toBufferView(wrapData);

        // Length.
        Assert.assertEquals("Unexpected length.", wrapData.getLength(), bufferView.getLength());

        // Get Copy.
        val copy = bufferView.getCopy();
        Assert.assertArrayEquals("Unexpected result from getCopy.", expectedData, copy);

        // Get BufferView Reader.
        val bufferViewReader = bufferView.getBufferViewReader();
        val bufferViewReaderData = bufferViewReader.readFully(2);
        AssertExtensions.assertArrayEquals("Unexpected result from getReader.", expectedData, 0,
                bufferViewReaderData.array(), bufferViewReaderData.arrayOffset(), expectedData.length);

        // Copy To OutputStream.
        @Cleanup
        val outputStream1 = new ByteArrayOutputStream();
        bufferView.copyTo(outputStream1);
        Assert.assertArrayEquals("Unexpected result from copyTo(OutputStream).", expectedData, outputStream1.toByteArray());

        // Copy To ByteBuffer.
        val array1 = new byte[expectedData.length];
        bufferView.copyTo(ByteBuffer.wrap(array1));
        Assert.assertArrayEquals("Unexpected result from copyTo(ByteBuffer).", expectedData, array1);
        val array2 = new byte[expectedData.length * 2];
        bufferView.copyTo(ByteBuffer.wrap(array2));
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
        val bufferView = toBufferView(data);

        for (int offset = 0; offset < data.getLength() / 2; offset++) {
            int length = data.getLength() - offset * 2;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bufferView.slice(offset, length).copyTo(bos);
            val actualData = bos.toByteArray();
            Assert.assertEquals(length, actualData.length);
            AssertExtensions.assertArrayEquals("", expectedData, offset, actualData, 0, actualData.length);
        }
    }

    /**
     * Tests {@link BufferView#collect} and {@link BufferView#iterateBuffers()}.
     */
    @Test
    public void testCollectAndIterateBuffers() {
        val data = newData();
        @Cleanup("release")
        val bufferView = toBufferView(data);

        val collectedContents = new ArrayList<ByteBuffer>();
        bufferView.collect(collectedContents::add);
        val collectedContentsData = getData(collectedContents);
        Assert.assertEquals("collect().", data, collectedContentsData);

        val iteratedContents = new ArrayList<ByteBuffer>();
        bufferView.iterateBuffers().forEachRemaining(iteratedContents::add);
        val iteratedContentsData = getData(iteratedContents);
        Assert.assertEquals("iterateBuffers().", data, iteratedContentsData);
    }

    /**
     * Tests the functionality of {@link BufferView#getBufferViewReader()}.
     *
     * @throws Exception if an exception occurred.
     */
    @Test
    public void testGetBufferViewReader() throws Exception {
        val data = newData();
        val expectedData = data.getCopy();
        @Cleanup("release")
        val bufferView = toBufferView(data);

        for (int offset = 0; offset < data.getLength() / 2; offset++) {
            int length = data.getLength() - offset * 2;

            // ReadFully.
            val readFullyReader = bufferView.slice(offset, length).getBufferViewReader();
            val readFullyResult = readFullyReader.readFully(2);
            Assert.assertEquals(0, readFullyReader.readBytes(new ByteArraySegment(new byte[1])));
            AssertExtensions.assertArrayEquals("ReadFully offset " + offset,
                    expectedData, offset, readFullyResult.array(), readFullyResult.arrayOffset(), length);

            // ReadSlice
            val readSliceReader = bufferView.slice(offset, length).getBufferViewReader();
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
            AssertExtensions.assertThrows("ReadSlice.End offset " + offset,
                    () -> readSliceReader.readSlice(1), ex -> ex instanceof BufferView.Reader.OutOfBoundsException);

            // ReadByte.
            val readByteReader = bufferView.slice(offset, length).getBufferViewReader();
            val readByteResult = new byte[length];
            for (int i = 0; i < length; i++) {
                readByteResult[i] = readByteReader.readByte();
                Assert.assertEquals(length - i - 1, readByteReader.available());
            }

            AssertExtensions.assertThrows("ReadByte.End offset " + offset, readByteReader::readByte,
                    ex -> ex instanceof BufferView.Reader.OutOfBoundsException);
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
        val bufferView = toBufferView(data);

        for (int offset = 0; offset < data.getLength() / 2; offset++) {
            int length = data.getLength() - offset * 2;
            byte[] readBuffer = new byte[length];
            try (InputStream stream = bufferView.getReader(offset, length)) {
                int readBytes = StreamHelpers.readAll(stream, readBuffer, 0, readBuffer.length);
                Assert.assertEquals("Unexpected number of bytes read from the InputStream at offset " + offset, length, readBytes);
            }

            AssertExtensions.assertArrayEquals("Unexpected data for offset " + offset, expectedData, offset, readBuffer, 0, readBuffer.length);
        }
    }

    /**
     * Tests {@link AbstractBufferView.AbstractReader#readInt()}, {@link AbstractBufferView.AbstractReader#readLong()}
     * and {@link AbstractBufferView.AbstractReader#readFully}.
     *
     * @throws Exception if an exception occurred.
     */
    @Test
    public void testBufferViewReaderTypes() throws Exception {
        val count = 987;
        val data = new byte[count * (Integer.BYTES + Long.BYTES) / 2];
        val writeBuffer = ByteBuffer.wrap(data);
        val rnd = new Random(0);
        val values = new ArrayList<Long>();
        Predicate<Integer> isInteger = index -> index % 2 == 0;
        for (int i = 0; i < count; i++) {
            long value;
            if (isInteger.test(i)) {
                value = rnd.nextInt();
                writeBuffer.putInt((int) value);
            } else {
                value = rnd.nextLong();
                writeBuffer.putLong(value);
            }

            values.add(value);
        }

        @Cleanup("release")
        val bufferView = toBufferView(new ByteArraySegment(data));

        val reader = bufferView.getBufferViewReader();
        int expectedAvailable = reader.available();
        Assert.assertEquals(bufferView.getLength(), expectedAvailable);
        for (int i = 0; i < count; i++) {
            boolean isInt = isInteger.test(i);
            long expectedValue = values.get(i);
            long actualValue = isInt ? reader.readInt() : reader.readLong();
            Assert.assertEquals("Unexpected value at index " + i, expectedValue, actualValue);

            expectedAvailable -= isInt ? Integer.BYTES : Long.BYTES;
            Assert.assertEquals("Unexpected Reader.available() after reading from index " + i, expectedAvailable, reader.available());
        }

        AssertExtensions.assertThrows("", reader::readInt, ex -> ex instanceof BufferView.Reader.OutOfBoundsException);
        AssertExtensions.assertThrows("", reader::readLong, ex -> ex instanceof BufferView.Reader.OutOfBoundsException);

        val allData = bufferView.getBufferViewReader().readFully(3);
        Assert.assertEquals("Unexpected result from readFully", new ByteArraySegment(data), allData);
    }

    /**
     * Tests the ability of {@link BufferView} to return slices of itself.
     *
     * @throws IOException if an exception occurred.
     */
    @Test
    public void testSlice() throws IOException {
        val data = newData();
        @Cleanup("release")
        val bufferView = toBufferView(data);
        for (int offset = 0; offset < data.getLength(); offset += 19) {
            for (int length = 0; length < data.getLength() - offset; length += 11) {
                val expected = new byte[length];
                System.arraycopy(data.array(), data.arrayOffset() + offset, expected, 0, length);
                val slice = bufferView.slice(offset, length).getCopy();
                Assert.assertArrayEquals("Unexpected slice() result for offset " + offset + ", length " + length, expected, slice);
                if (length == 0) {
                    Assert.assertEquals("Unexpected getReader() result for offset " + offset + ", length " + length,
                            0, bufferView.getReader(offset, length).available());
                } else {
                    val sliceData = StreamHelpers.readAll(bufferView.getReader(offset, length), length);
                    Assert.assertArrayEquals("Unexpected getReader() result for offset " + offset + ", length " + length, expected, sliceData);
                }
            }
        }
    }

    protected ArrayView newData() {
        byte[] data = new byte[BUFFER_SIZE];
        rnd.nextBytes(data);
        return new ByteArraySegment(data);
    }

    private ArrayView getData(List<ByteBuffer> buffers) {
        val os = new EnhancedByteArrayOutputStream();
        for (ByteBuffer buffer : buffers) {
            byte[] contents = new byte[buffer.remaining()];
            buffer.get(contents);
            assert buffer.remaining() == 0;
            os.write(contents);
        }

        return os.getData();
    }

    protected List<ByteBuffer> getContents(BufferView bufferView) {
        return Lists.newArrayList(bufferView.iterateBuffers());
    }

    protected abstract BufferView toBufferView(ArrayView data);
}

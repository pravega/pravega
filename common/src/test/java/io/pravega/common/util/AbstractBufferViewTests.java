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

import io.pravega.test.common.AssertExtensions;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the HashedArray class.
 */
public class AbstractBufferViewTests {
    private static final int COUNT = 1000;
    private static final int MAX_LENGTH = 100;

    /**
     * Tests equals() and hashCode().
     */
    @Test
    public void testEqualsHashCode() {
        val data1 = generate();
        val data2 = copy(data1);
        BufferView prev = null;
        for (int i = 0; i < data1.size(); i++) {
            val a1 = data1.get(i);
            val a2 = data2.get(i);
            Assert.assertEquals("Expecting hashCode() to be the same for the same array contents.", a1.hashCode(), a2.hashCode());
            Assert.assertTrue("Expecting equals() to return true for the same array contents.", a1.equals(a2) && a2.equals(a1));
            if (prev != null) {
                Assert.assertNotEquals("Expecting hashCode() to be different for different arrays.", prev.hashCode(), a1.hashCode());
                Assert.assertFalse("Expecting equals() to return false for different array contents.", prev.equals(a1) || a1.equals(prev));
            }
            prev = a1;
        }
    }

    /**
     * Tests equals() and hashCode().
     */
    @Test
    public void testEqualsHashCodeComposite() {
        val data = generate();
        val b = data.get(data.size() - 1);
        val s1 = b.slice(0, b.getLength() / 2);
        val s2 = b.slice(b.getLength() / 2, b.getLength() / 2);
        val cb = BufferView.wrap(Arrays.asList(s1, s2));
        Assert.assertEquals(b.hashCode(), cb.hashCode());
        Assert.assertEquals(b, cb);
        Assert.assertEquals(cb, b);

        val b2Data = b.getCopy();
        b2Data[1] = (byte) (b2Data[1] + 1);
        val b2 = new ByteArraySegment(b2Data);
        Assert.assertNotEquals(b2.hashCode(), cb.hashCode());
        Assert.assertNotEquals(cb, b2);
        Assert.assertNotEquals(b2, cb);
    }

    /**
     * Tests the behavior of the object returned by {@link BufferView#empty()}.
     */
    @Test
    public void testEmptyBufferView() throws Exception {
        val e = BufferView.empty();
        Assert.assertSame("Expecting same instance.", e, BufferView.empty());
        Assert.assertEquals(0, e.getLength());
        Assert.assertEquals(0, e.getCopy().length);
        Assert.assertSame(e, e.slice(0, 0));
        AssertExtensions.assertThrows("", () -> e.slice(0, 1), ex -> ex instanceof IndexOutOfBoundsException);
        AssertExtensions.assertThrows("", () -> e.getReader(0, 1), ex -> ex instanceof IndexOutOfBoundsException);
        Assert.assertEquals(0, e.copyTo(ByteBuffer.allocate(1)));
        Assert.assertTrue(e.getContents().isEmpty());

        val reader = e.getBufferViewReader();
        Assert.assertEquals(0, reader.available());
        Assert.assertEquals(0, reader.readBytes(new ByteArraySegment(new byte[1])));
        AssertExtensions.assertThrows("", reader::readByte, ex -> ex instanceof EOFException);
        Assert.assertSame(e, reader.readSlice(0));
        AssertExtensions.assertThrows("", () -> reader.readSlice(1), ex -> ex instanceof EOFException);
        @Cleanup
        val inputStream = e.getReader();
        Assert.assertEquals(-1, inputStream.read());
    }

    /**
     * Tests {@link AbstractBufferView.AbstractReader#readInt()}, {@link AbstractBufferView.AbstractReader#readLong()}
     * and {@link AbstractBufferView.AbstractReader#readFully}.
     */
    @Test
    public void testAbstractReader() throws Exception {
        val intValue = 1234;
        val longValue = -123456789L;
        // ReadInt, ReadLong, ReadFully
        val buffer = new ByteArraySegment(new byte[100], 5, 79);
        val rnd = new Random(0);

        // Write some data. Fill with garbage, then put some readable values at the beginning.
        rnd.nextBytes(buffer.array());
        BitConverter.writeInt(buffer, 0, intValue);
        BitConverter.writeLong(buffer, Integer.BYTES, longValue);

        // Now read them back.
        BufferView.Reader reader = buffer.getBufferViewReader();
        Assert.assertEquals(buffer.getLength(), reader.available());

        val readInt = reader.readInt();
        Assert.assertEquals("readInt", intValue, readInt);
        Assert.assertEquals(buffer.getLength() - Integer.BYTES, reader.available());

        val readLong = reader.readLong();
        Assert.assertEquals("readLong", longValue, readLong);
        val remainingDataPos = Integer.BYTES + Long.BYTES;
        val remainingDataLength = buffer.getLength() - remainingDataPos;
        Assert.assertEquals(remainingDataLength, reader.available());

        val remainingData = reader.readFully(3);
        Assert.assertEquals(remainingDataLength, remainingData.getLength());
        Assert.assertEquals("readFully", remainingData, buffer.slice(remainingDataPos, remainingDataLength));
    }

    private List<BufferView> copy(List<BufferView> source) {
        return source.stream()
                .map(b -> new ByteArraySegment(b.getCopy()))
                .collect(Collectors.toList());
    }

    private List<BufferView> generate() {
        final int padding = 10;
        val rnd = new Random(0);
        val result = new ArrayList<BufferView>();
        result.add(BufferView.empty()); // Throw in an empty one too.
        int lastLength = 0;
        for (int i = 0; i < COUNT; i++) {
            int length = (i % 2 == 0 && lastLength > 0) ? lastLength : rnd.nextInt(MAX_LENGTH);
            byte[] array = new byte[length + padding];
            rnd.nextBytes(array);
            int arrayOffset = rnd.nextInt(padding);
            result.add(new ByteArraySegment(array, arrayOffset, length));
            lastLength = length;
        }

        return result;
    }
}

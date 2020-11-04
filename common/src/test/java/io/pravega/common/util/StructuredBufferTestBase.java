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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base test class for any class implementing {@link StructuredWritableBuffer} (and optionally {@link StructuredReadableBuffer}).
 * Tests common functionality across all implementations.Any derived classes should test implementation-specific features.
 */
public abstract class StructuredBufferTestBase extends BufferViewTestBase {
    /**
     * Tests {@link CompositeByteArraySegment#setShort}.
     */
    @Test
    public void testSetShort() {
        testPrimitiveType(i -> (short) (int) i, StructuredWritableBuffer::setShort, StructuredReadableBuffer::getShort,
                reader -> BitConverter.readShort(reader.readSlice(Short.BYTES).getCopy(), 0),
                Short.BYTES);
    }

    /**
     * Tests {@link CompositeByteArraySegment#setInt}.
     */
    @Test
    public void testSetInt() {
        testPrimitiveType(i -> i * i * (i < 0 ? -1 : 1), StructuredWritableBuffer::setInt, StructuredReadableBuffer::getInt,
                BufferView.Reader::readInt, Integer.BYTES);
    }

    /**
     * Tests {@link CompositeByteArraySegment#setLong}.
     */
    @Test
    public void testSetLong() {
        testPrimitiveType(i -> (long) Math.pow(i, 3), StructuredWritableBuffer::setLong, StructuredReadableBuffer::getLong,
                BufferView.Reader::readLong, Long.BYTES);
    }

    private <T> void testPrimitiveType(Function<Integer, T> toPrimitiveType, ValueSetter<T> writer, ValueGetter<T> reader,
                                       Function<BufferView.Reader, T> bufferViewReader, int byteSize) {
        val s = newWritableBuffer();

        // 1. Generate values, both negative and positive.
        val values = IntStream.range(-s.getLength() / 2, s.getLength() / 2 + 1).boxed().map(toPrimitiveType).collect(Collectors.toList());

        int bufferIndex = 0;
        int valueIndex = 0;
        for (; valueIndex < values.size(); valueIndex++) {
            val v = values.get(valueIndex);
            if (bufferIndex + byteSize > s.getLength()) {
                val finalIndex = bufferIndex;
                AssertExtensions.assertThrows(
                        "Expected call to be rejected if insufficient space remaining.",
                        () -> writer.accept(s, finalIndex, v),
                        ex -> ex instanceof IndexOutOfBoundsException);
                break;
            } else {
                writer.accept(s, bufferIndex, v);
                bufferIndex += byteSize;
            }
        }

        // 2. Read all values back using BufferView.Reader and validate they are correct.
        BufferView.Reader bufferReader = s.getBufferViewReader();
        for (int i = 0; i < valueIndex; i++) {
            val expected = values.get(i);
            T actual = bufferViewReader.apply(bufferReader);
            Assert.assertEquals("Unexpected value read (BufferView.Reader) at value index " + i, expected, actual);
        }

        Assert.assertEquals("Unexpected number of bytes read.", s.getLength() - bufferIndex, bufferReader.available());

        // 3. Read all values back using StructuredReadableBuffer (if available) and validate they are correct.
        if (s instanceof StructuredReadableBuffer) {
            val r = (StructuredReadableBuffer) s;
            bufferIndex = 0;
            for (int i = 0; i < valueIndex; i++) {
                val expected = values.get(i);
                T actual = reader.apply(r, bufferIndex);
                Assert.assertEquals("Unexpected value read (StructuredReadableBuffer) at buffer index " + bufferIndex, expected, actual);
                bufferIndex += byteSize;
            }
        }
    }

    protected abstract StructuredWritableBuffer newWritableBuffer();

    @FunctionalInterface
    private interface ValueSetter<T> {
        void accept(StructuredWritableBuffer b, int index, T value);
    }

    @FunctionalInterface
    private interface ValueGetter<T> {
        T apply(StructuredReadableBuffer b, int index);
    }
}

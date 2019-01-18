/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link BitConverter} class.
 */
public class BitConverterTests {
    private static final int MAX_LENGTH = Long.BYTES;

    /**
     * Tests the {@link BitConverter#writeShort} and {@link BitConverter#readShort}.
     */
    @Test
    public void testShort() {
        test(BitConverter::writeShort, BitConverter::readShort, Short.MIN_VALUE, Short.MAX_VALUE, (short) -1, (short) 0, (short) 1);
    }

    /**
     * Tests the {@link BitConverter#writeInt} and {@link BitConverter#readInt}.
     */
    @Test
    public void testInt() {
        test(BitConverter::writeInt, BitConverter::readInt, Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 0, 1);
    }

    /**
     * Tests the {@link BitConverter#writeLong} and {@link BitConverter#readLong}.
     */
    @Test
    public void testLong() {
        test(BitConverter::writeLong, BitConverter::readLong, Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0L, 1L);
    }

    /**
     * Tests the {@link BitConverter#writeUnsignedLong} and {@link BitConverter#readUnsignedLong}.
     */
    @Test
    public void testUnsignedLong() {
        test(BitConverter::writeUnsignedLong, BitConverter::readUnsignedLong, Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0L, 1L);
    }

    @SafeVarargs
    private final <T> void test(Write<T> write, Read<T> read, T... testValues) {
        byte[] buffer = new byte[MAX_LENGTH];
        for (T value : testValues) {
            int length = write.apply(buffer, 0, value);
            T readValue = read.apply(new ByteArraySegment(buffer, 0, length), 0);
            Assert.assertEquals("Unexpected deserialized value.", value, readValue);
        }
    }

    @FunctionalInterface
    interface Write<T> {
        int apply(byte[] target, int offset, T value);
    }

    @FunctionalInterface
    interface Read<T> {
        T apply(ArrayView target, int position);
    }
}

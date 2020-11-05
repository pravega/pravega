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

import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link BitConverter} class.
 */
public class BitConverterTests {
    private static final int MAX_LENGTH = 2 * Long.BYTES;

    /**
     * Tests the {@link BitConverter#writeUUID} and {@link BitConverter#readUUID}.
     */
    @Test
    public void testUUID() {
        WriteArray<UUID> toWrite = (target, offset, value) -> {
            BitConverter.writeUUID(new ByteArraySegment(target, offset, Long.BYTES * 2), value);
            return Long.BYTES * 2;
        };
        test(toWrite, BitConverter::readUUID, new UUID(Long.MIN_VALUE, Long.MIN_VALUE),
                new UUID(Long.MIN_VALUE, Long.MAX_VALUE), new UUID(0L, 0L), UUID.randomUUID(),
                new UUID(Long.MAX_VALUE, Long.MIN_VALUE),
                new UUID(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    @SafeVarargs
    private final <T> void test(WriteArray<T> write, Read<T> read, T... testValues) {
        byte[] buffer = new byte[MAX_LENGTH];
        for (T value : testValues) {
            int length = write.apply(buffer, 0, value);
            T readValue = read.apply(buffer, 0);
            Assert.assertEquals("Unexpected deserialized value.", value, readValue);
        }
    }

    @FunctionalInterface
    interface WriteArray<T> {
        int apply(byte[] target, int offset, T value);
    }

    @FunctionalInterface
    interface Read<T> {
        T apply(byte[] target, int position);
    }
}

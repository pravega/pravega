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

import io.pravega.common.io.ByteBufferOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link BitConverter} class.
 */
public class BitConverterTests {
    private static final int MAX_LENGTH = 2 * Long.BYTES;

    /**
     * Tests the {@link BitConverter#writeShort}.
     */
    @Test
    public void testShort() throws IOException {
        ReadStream<Short> read = stream -> new DataInputStream(stream).readShort();
        testStream(BitConverter::writeShort, read, Short.MIN_VALUE, Short.MAX_VALUE, (short) -1, (short) 0, (short) 1);
    }

    /**
     * Tests the {@link BitConverter#writeInt} and {@link BitConverter#readInt}.
     */
    @Test
    public void testInt() throws IOException {
        test(BitConverter::writeInt, BitConverter::readInt, Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 0, 1);
        testStream(BitConverter::writeInt, BitConverter::readInt, Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 0, 1);
    }

    /**
     * Tests the {@link BitConverter#writeLong} and {@link BitConverter#readLong}.
     */
    @Test
    public void testLong() {
        test(BitConverter::writeLong, BitConverter::readLong, Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0L, 1L);
        WriteArray<Long> streamWriter = (target, offset, value) -> {
            @Cleanup
            val s = new ByteBufferOutputStream();
            try {
                BitConverter.writeLong(s, value);
            } catch (IOException ex) {
                throw new CompletionException(ex);
            }
            s.getData().copyTo(target, offset, s.size());
            return s.size();
        };
        test(streamWriter, BitConverter::readLong, Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0L, 1L);
    }

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
    private final <T> void test(WriteArray<T> write, ReadArray<T> read, T... testValues) {
        byte[] buffer = new byte[MAX_LENGTH];
        for (T value : testValues) {
            write.apply(buffer, 0, value);
            T readValue = read.apply(buffer, 0);
            Assert.assertEquals("Unexpected deserialized value.", value, readValue);
        }
    }

    @SafeVarargs
    private final <T> void testStream(WriteStream<T> write, ReadStream<T> read, T... testValues) throws IOException {
        for (T value : testValues) {
            @Cleanup
            val s = new ByteBufferOutputStream(MAX_LENGTH);
            write.apply(s, value);
            T readValue = read.apply(s.getData().getReader());
            Assert.assertEquals("Unexpected deserialized value.", value, readValue);
        }
    }

    @FunctionalInterface
    interface WriteArray<T> {
        int apply(byte[] target, int offset, T value);
    }

    @FunctionalInterface
    interface WriteStream<T> {
        int apply(OutputStream target, T value) throws IOException;
    }

    @FunctionalInterface
    interface ReadArray<T> {
        T apply(byte[] target, int position);
    }

    @FunctionalInterface
    interface ReadStream<T> {
        T apply(InputStream source) throws IOException;
    }
}

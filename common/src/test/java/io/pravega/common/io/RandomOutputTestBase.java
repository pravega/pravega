/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;

import io.pravega.common.io.serialization.RandomAccessOutputStream;
import io.pravega.test.common.AssertExtensions;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for all tests of classes implementing RandomOutput.
 */
public abstract class RandomOutputTestBase<T extends OutputStream & RandomAccessOutputStream> {
    private static final int INITIAL_LENGTH = 100;
    private static final byte INITIAL_FILL = -1;

    /**
     * Tests the ability to write a single value.
     *
     * @throws Exception If one got thrown.
     */
    @Test
    public void testWriteSingleValue() throws Exception {
        val expectedData = newArray();
        @Cleanup
        val s = createInstance(newArray());
        AssertExtensions.assertThrows(
                "write() allowed writing at negative position.",
                (AssertExtensions.RunnableWithException) () -> s.write(1, -1),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                "write() allowed writing overflow position.",
                (AssertExtensions.RunnableWithException) () -> s.write(1, expectedData.length),
                ex -> ex instanceof IndexOutOfBoundsException);

        for (int i = 0; i < expectedData.length; i += 3) {
            expectedData[i] = (byte) i;
            s.write(i, i);
        }

        byte[] writtenData = getData(s);
        Assert.assertArrayEquals("Unexpected Stream contents.", expectedData, writtenData);
    }

    /**
     * Tests the ability to write an array.
     *
     * @throws Exception If one got thrown.
     */
    @Test
    public void testWriteArray() throws Exception {
        int writeLength = 10;
        byte[] expectedData = newArray();
        @Cleanup
        val s = createInstance(newArray());
        AssertExtensions.assertThrows(
                "write() allowed writing at negative position.",
                (AssertExtensions.RunnableWithException) () -> s.write(new byte[writeLength], 0, writeLength, -1),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                "write() allowed writing overflow position.",
                (AssertExtensions.RunnableWithException) () -> s.write(new byte[writeLength], 0, writeLength, INITIAL_LENGTH),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                "write() allowed writing with out-of-bounds buffer positions.",
                (AssertExtensions.RunnableWithException) () -> s.write(new byte[writeLength], 1, writeLength + 1, 0),
                ex -> ex instanceof IndexOutOfBoundsException);

        for (int i = 0; i < expectedData.length - writeLength; i += 2 * writeLength) {
            byte[] toWrite = new byte[writeLength];
            Arrays.fill(toWrite, (byte) i);
            System.arraycopy(toWrite, 0, expectedData, i, toWrite.length);
            s.write(toWrite, 0, toWrite.length, i);
        }

        if (canGrow()) {
            // Underlying stream can grow. Simulate this by expanding our array as well.
            byte[] toWrite = new byte[writeLength];
            Arrays.fill(toWrite, Byte.MAX_VALUE);

            int offset = expectedData.length - writeLength / 2;
            val data2 = new byte[offset + writeLength];
            System.arraycopy(expectedData, 0, data2, 0, expectedData.length);
            System.arraycopy(toWrite, 0, data2, offset, toWrite.length);
            expectedData = data2;
            s.write(toWrite, 0, toWrite.length, offset);
        } else {
            AssertExtensions.assertThrows(
                    "write() allowed writing overflow position.",
                    (AssertExtensions.RunnableWithException) () -> s.write(new byte[writeLength], 0, writeLength, INITIAL_LENGTH - writeLength / 2),
                    ex -> ex instanceof IndexOutOfBoundsException);
        }

        byte[] writtenData = getData(s);
        Assert.assertArrayEquals("Unexpected Stream contents.", expectedData, writtenData);
    }

    /**
     * Tests the ability to write to a sub-Stream.
     *
     * @throws Exception If one got thrown.
     */
    @Test
    public void testSubStream() throws Exception {
        int writeLength = 10;
        byte[] expectedData = newArray();
        @Cleanup
        val s = createInstance(newArray());
        AssertExtensions.assertThrows(
                "subStream() allowed writing at negative position.",
                (AssertExtensions.RunnableWithException) () -> s.subStream(-1, 1),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                "write() allowed writing overflow position.",
                (AssertExtensions.RunnableWithException) () -> s.subStream(INITIAL_LENGTH, 1),
                ex -> ex instanceof IndexOutOfBoundsException);

        AssertExtensions.assertThrows(
                "subStream() allowed writing overflow position + length.",
                (AssertExtensions.RunnableWithException) () -> s.subStream(INITIAL_LENGTH - 1, writeLength),
                ex -> ex instanceof IndexOutOfBoundsException);

        for (int i = 0; i < expectedData.length - writeLength; i += 2 * writeLength) {
            byte[] toWrite = new byte[writeLength];
            Arrays.fill(toWrite, (byte) i);
            System.arraycopy(toWrite, 0, expectedData, i, toWrite.length);
            @Cleanup
            val ss = s.subStream(i, toWrite.length);
            ss.write(toWrite);
        }

        byte[] writtenData = getData(s);
        Assert.assertArrayEquals("Unexpected Stream contents.", expectedData, writtenData);
    }

    private byte[] newArray() {
        byte[] data = new byte[INITIAL_LENGTH];
        Arrays.fill(data, INITIAL_FILL);
        return data;
    }

    protected abstract boolean canGrow();

    protected abstract T createInstance(byte[] initialData);

    protected abstract byte[] getData(T stream);

    //region Implementations

    public static class FixedByteArrayOutputStreamTest extends RandomOutputTestBase<FixedByteArrayOutputStream> {

        @Override
        protected boolean canGrow() {
            return false;
        }

        @Override
        protected FixedByteArrayOutputStream createInstance(byte[] initialData) {
            return new FixedByteArrayOutputStream(initialData, 0, initialData.length);
        }

        @Override
        @SneakyThrows(IOException.class)
        protected byte[] getData(FixedByteArrayOutputStream stream) {
            val b = stream.getData();
            return StreamHelpers.readAll(b.getReader(), b.getLength());
        }
    }

    public static class EnhancedByteArrayOutputStreamTest extends RandomOutputTestBase<EnhancedByteArrayOutputStream> {
        @Override
        protected boolean canGrow() {
            return true;
        }

        @Override
        protected EnhancedByteArrayOutputStream createInstance(byte[] initialData) {
            val s = new EnhancedByteArrayOutputStream();
            s.write(initialData);
            return s;
        }

        @Override
        @SneakyThrows(IOException.class)
        protected byte[] getData(EnhancedByteArrayOutputStream stream) {
            val b = stream.getData();
            return StreamHelpers.readAll(b.getReader(), b.getLength());
        }
    }

    //endregion
}

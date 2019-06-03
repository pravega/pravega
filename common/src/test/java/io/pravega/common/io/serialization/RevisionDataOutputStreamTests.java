/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.serialization;

import com.google.common.base.Charsets;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.io.FixedByteArrayOutputStream;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the RevisionDataOutputStream class for non-encoding methods (the custom encoding methods are tested in
 * RevisionDataStreamCommonTests, in combination with RevisionDataInputStream).
 */
public class RevisionDataOutputStreamTests {
    
    /**
     * Tests the RandomRevisionDataOutput class with an expandable RandomAccessOutputStream.
     */
    @Test
    public void testRandomOutputExpandable() throws Exception {
        @Cleanup
        val s = new EnhancedByteArrayOutputStream();
        @Cleanup
        val impl = RevisionDataOutputStream.wrap(s);
        testImpl(impl, s::getData);
    }

    /**
     * Tests the RandomRevisionDataOutput class with an fixed-length RandomAccessOutputStream.
     */
    @Test
    public void testRandomOutputFixed() throws Exception {
        final int bufferSize = 1024 * 1024;
        @Cleanup
        val s = new FixedByteArrayOutputStream(new byte[bufferSize], 0, bufferSize);
        @Cleanup
        val impl = RevisionDataOutputStream.wrap(s);
        testImpl(impl, s::getData);
    }

    /**
     * Tests the NonSeekableRevisionDataOutput class.
     */
    @Test
    public void testNonSeekableOutputImpl() throws Exception {
        @Cleanup
        val s = new ByteArrayOutputStream();
        @Cleanup
        val impl = RevisionDataOutputStream.wrap(s);
        testImpl(impl, () -> new ByteArraySegment(s.toByteArray()));
    }

    /**
     * Tests the NonSeekableRevisionDataOutput class when we provide a shorter length than expected.
     */
    @Test
    public void testNonSeekableOutputShorterLength() throws Exception {
        @Cleanup
        val s = new ByteArrayOutputStream();
        // Wrap the stream, but do not auto-close it since we expect close() to fail, which is verified below.
        val impl = RevisionDataOutputStream.wrap(s);
        int correctLength = Byte.BYTES + Short.BYTES + Integer.BYTES;

        // Shorter length.
        impl.length(correctLength - 1);
        impl.writeByte(1);
        impl.writeShort(2);
        impl.writeInt(3);

        // Verify close() fails.
        AssertExtensions.assertThrows(
                "RevisionDataOutputStream.close() did not throw for byte mismatch.",
                impl::close,
                ex -> ex instanceof SerializationException);

        // Verify the written data cannot be read back (we'll get an EOF at this time).
        @Cleanup
        val inputStream = RevisionDataInputStream.wrap(new ByteArrayInputStream(s.toByteArray()));
        inputStream.readByte();
        inputStream.readShort();
        AssertExtensions.assertThrows(
                "Expecting EOF.",
                inputStream::readInt,
                ex -> ex instanceof EOFException);
    }

    /**
     * Tests the NonSeekableRevisionDataOutput class when we provide a shorter length than expected.
     */
    @Test
    public void testNonSeekableOutputLongerLength() throws Exception {
        byte b = 1;
        short sn = 2;
        int n = 3;
        @Cleanup
        val s = new ByteArrayOutputStream();
        // Wrap the stream, but do not auto-close it since we expect close() to fail, which is verified below.
        val impl = RevisionDataOutputStream.wrap(s);
        int correctLength = Byte.BYTES + Short.BYTES + Integer.BYTES;

        // Shorter length.
        impl.length(correctLength + 1);
        impl.writeByte(b);
        impl.writeShort(sn);
        impl.writeInt(n);

        // Verify close() fails.
        AssertExtensions.assertThrows(
                "RevisionDataOutputStream.close() did not throw for byte mismatch.",
                impl::close,
                ex -> ex instanceof SerializationException);

        // Verify the written data can be read back.
        val inputStream = RevisionDataInputStream.wrap(new ByteArrayInputStream(s.toByteArray()));
        Assert.assertEquals("Unexpected byte read back.", b, inputStream.read());
        Assert.assertEquals("Unexpected short read back.", sn, inputStream.readShort());
        Assert.assertEquals("Unexpected int read back.", n, inputStream.readInt());

        // And verify we can't read anything else and we'll get an exception upon close indicating we read fewer bytes than expected.
        AssertExtensions.assertThrows(
                "Expecting EOF.",
                () -> inputStream.readFully(new byte[1]),
                ex -> ex instanceof EOFException);
        AssertExtensions.assertThrows(
                "Expecting an exception when reading fewer bytes than declared.",
                inputStream::close,
                ex -> ex instanceof SerializationException);
    }

    /**
     * Tests the case when a RevisionDataOutputStream is created but left empty upon closing, while writing to a RandomAccessOutputStream OutputStream.
     */
    @Test
    public void testZeroLengthRandomOutput() throws Exception {
        testZeroLength(EnhancedByteArrayOutputStream::new, EnhancedByteArrayOutputStream::getData);
    }

    /**
     * Tests the case when a RevisionDataOutputStream is created but left empty upon closing, while writing to a non-seekable OutputStream.
     */
    @Test
    public void testZeroLengthNonSeekable() throws Exception {
        testZeroLength(ByteArrayOutputStream::new, os -> new ByteArraySegment(os.toByteArray()));
    }

    private <T extends OutputStream> void testZeroLength(Supplier<T> newBaseStream, Function<T, ByteArraySegment> getWrittenData) throws Exception {
        @Cleanup
        val os = newBaseStream.get();

        // Open and immediately close the RevisionDataOutputStream.
        @Cleanup
        val rdos = RevisionDataOutputStream.wrap(os);
        rdos.close();

        val data = getWrittenData.apply(os);
        @Cleanup
        val rdis = RevisionDataInputStream.wrap(data.getReader());
        Assert.assertEquals("Unexpected length encoded.", 0, rdis.getLength());
        AssertExtensions.assertThrows(
                "Expecting EOF.",
                () -> rdis.readFully(new byte[1]),
                ex -> ex instanceof EOFException);
    }

    private void testImpl(RevisionDataOutputStream impl, Supplier<ByteArraySegment> getWrittenData) throws Exception {
        final byte b = 123;
        final short sn = 1234;
        final int n = 123456;
        final long l = (long) Integer.MAX_VALUE + 1;
        final String s = getUTFString();
        final byte[] array = s.getBytes(Charsets.UTF_8);
        int expectedLength = Byte.BYTES + Short.BYTES + Integer.BYTES + Long.BYTES + impl.getUTFLength(s) + array.length
                + impl.getCompactIntLength(array.length) + array.length;

        if (impl.requiresExplicitLength()) {
            // Verify a few methods that shouldn't be allowed to run without setting length beforehand.
            Arrays.<AssertExtensions.RunnableWithException>asList(
                    () -> impl.write(1),
                    () -> impl.write(new byte[1], 0, 1),
                    () -> impl.writeInt(1),
                    () -> impl.writeShort(1),
                    () -> impl.writeLong(1),
                    () -> impl.writeUTF("test"))
                    .forEach(r -> AssertExtensions.assertThrows(
                            "write was allowed without setting length first.",
                            r,
                            ex -> ex instanceof IllegalStateException));
        }

        impl.length(expectedLength);
        impl.writeByte(b);
        impl.writeShort(sn);
        impl.writeInt(n);
        impl.writeLong(l);
        impl.writeUTF(s);
        impl.write(array);
        impl.writeArray(new ByteArraySegment(array));

        // Need to close so we flush any remaining stuff to the underlying stream.
        impl.close();

        // Verify the written data can be read back.
        @Cleanup
        val inputStream = RevisionDataInputStream.wrap(getWrittenData.get().getReader());
        Assert.assertEquals("Unexpected length read back.", expectedLength, inputStream.getLength());
        Assert.assertEquals("Unexpected byte read back.", b, inputStream.read());
        Assert.assertEquals("Unexpected short read back.", sn, inputStream.readShort());
        Assert.assertEquals("Unexpected int read back.", n, inputStream.readInt());
        Assert.assertEquals("Unexpected long read back.", l, inputStream.readLong());
        Assert.assertEquals("Unexpected string read back.", s, inputStream.readUTF());
        byte[] readArray = new byte[array.length];
        int readBytes = inputStream.read(readArray);
        Assert.assertEquals("Unexpected number of bytes read for array.", readArray.length, readBytes);
        Assert.assertArrayEquals("Unexpected array read back.", array, readArray);
        readArray = inputStream.readArray();
        Assert.assertEquals("Unexpected number of bytes read for array.", array.length, readArray.length);
        Assert.assertArrayEquals("Unexpected array read back.", array, readArray);

        Assert.assertEquals("Not expecting any more data. ", -1, inputStream.read());
        AssertExtensions.assertThrows(
                "Expecting EOF.",
                () -> inputStream.readFully(new byte[1]),
                ex -> ex instanceof EOFException);
    }

    private String getUTFString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i += 79) {
            sb.append((char) i);
        }
        return sb.toString();
    }
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.serialization;

import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.io.FixedByteArrayOutputStream;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.util.Arrays;
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
     * Tests the RandomRevisionDataOutput class with an expandable RandomOutput.
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
     * Tests the RandomRevisionDataOutput class with an fixed-length RandomOutput.
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

    private void testImpl(RevisionDataOutputStream impl, Supplier<ByteArraySegment> getWrittenData) throws Exception {
        final byte b = 123;
        final short sn = 1234;
        final int n = 123456;
        final long l = (long) Integer.MAX_VALUE + 1;
        final String s = getUTFString();
        final byte[] array = s.getBytes();
        int expectedLength = Byte.BYTES + Short.BYTES + Integer.BYTES + Long.BYTES + impl.getUTFLength(s) + array.length;

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

        Assert.assertEquals("Not expecting any more data. ", -1, inputStream.read());
        AssertExtensions.assertThrows(
                "Expecting EOF.",
                inputStream::readInt,
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

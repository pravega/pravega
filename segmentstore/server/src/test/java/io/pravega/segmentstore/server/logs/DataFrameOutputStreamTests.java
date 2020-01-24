/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for DataFrameOutputStream class.
 */
public class DataFrameOutputStreamTests {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests the basic functionality of startNewRecord(), endRecord() and discardRecord().
     */
    @Test
    public void testRecordCreation() throws Exception {
        int maxFrameSize = 10 * 1024;

        // Callback for when a frame is written.
        AtomicReference<DataFrame> writtenFrame = new AtomicReference<>();
        Consumer<DataFrame> callback = df -> {
            Assert.assertNull("A frame has already been written.", writtenFrame.get());
            writtenFrame.set(df);
        };

        ArrayList<byte[]> records = DataFrameTestHelpers.generateRecords(9, 0, 1024); // This should fit in one frame of 10KB
        try (DataFrameOutputStream s = new DataFrameOutputStream(maxFrameSize, callback)) {
            // Verify that we cannot write unless we have a record started.
            AssertExtensions.assertThrows(
                    "write(byte) worked even though no entry started.",
                    () -> s.write((byte) 1),
                    ex -> ex instanceof IllegalStateException);

            AssertExtensions.assertThrows(
                    "write(byte[]) worked even though no entry started.",
                    () -> s.write(new byte[1]),
                    ex -> ex instanceof IllegalStateException);

            // startNewRecord() + discardRecord()
            s.startNewRecord();
            s.write((byte) 1);
            s.write(new byte[10]);
            s.discardRecord();
            s.flush();
            Assert.assertNull("An empty frame has been created when flush() was called with no contents.", writtenFrame.get());

            // startNewRecord() + endRecord();
            writtenFrame.set(null);
            for (byte[] record : records) {
                s.startNewRecord();
                s.write(record);
                s.endRecord();
            }

            s.flush();
        }

        Assert.assertNotNull("No frame has been created when flush() was called.", writtenFrame);
        Assert.assertTrue("Created frame is not sealed.", writtenFrame.get().isSealed());
        DataFrameTestHelpers.checkReadRecords(readFrame(writtenFrame.get()), records, ByteArraySegment::new);
    }

    /**
     * Tests the ability to reuse existing physical buffers, and discard them if needed.
     */
    @Test
    public void testBufferReuse() throws Exception {
        final int count = 500;
        final int resetEvery = 50;
        final byte[] writeData = new byte[1000];
        final int maxFrameSize = 10 * 1024;

        // Callback for when a frame is written.
        AtomicReference<DataFrame> writtenFrame = new AtomicReference<>();

        int expectedStartIndex = 0;
        @Cleanup
        DataFrameOutputStream s = new DataFrameOutputStream(maxFrameSize, writtenFrame::set);
        for (int i = 0; i < count; i++) {
            if (i % resetEvery == 0) {
                s.releaseBuffer();
                expectedStartIndex = 0;
            }

            // We generate some frame of fixed size.
            s.startNewRecord();
            s.write(writeData);
            s.endRecord();
            s.flush();

            // Then we inspect it's ArrayView's buffer characteristics, especially the array offset. If it increases as
            // expect it to (and then resets when it exceeds a certain size), then we know the same physical buffer is
            // reused.
            ArrayView av = writtenFrame.getAndSet(null).getData();
            Assert.assertEquals("Unexpected buffer index after flush #" + (i + 1), expectedStartIndex, av.arrayOffset());
            expectedStartIndex += av.getLength();
            if (maxFrameSize - expectedStartIndex < av.getLength()) {
                expectedStartIndex = 0;
            }
        }
    }

    /**
     * Tests the ability to drop the current frame, if the current frame is in a bad state.
     */
    @Test
    public void testReset() throws Exception {
        // Write some data to the OutputStream (remember it).
        // Reset it.
        // Write more data to the OutputStream.
        // Flush it.
        // Verify the first data is not in.

        int maxFrameSize = 10 * 1024;

        // Callback for when a frame is written.
        AtomicReference<DataFrame> writtenFrame = new AtomicReference<>();
        Consumer<DataFrame> callback = df -> {
            Assert.assertNull("A frame has already been written.", writtenFrame.get());
            writtenFrame.set(df);
        };

        ArrayList<byte[]> records = DataFrameTestHelpers.generateRecords(2, 0, 1024);
        try (DataFrameOutputStream s = new DataFrameOutputStream(maxFrameSize, callback)) {
            // Test 1: write record + reset + flush -> no frame.
            s.startNewRecord();
            s.write(records.get(0));
            s.endRecord();
            s.reset();
            s.flush();
            Assert.assertNull("A frame has been created when flush() was called with no contents (post reset()).", writtenFrame.get());

            // Test 2: write record 1 + reset + write record 2 + flush -> frame with record 2 only.
            s.startNewRecord();
            s.write(records.get(0));
            s.endRecord();
            s.reset();

            s.startNewRecord();
            s.write(records.get(1));
            s.endRecord();
            s.flush();
        }

        Assert.assertNotNull("No frame has been created when flush() was called with contents.", writtenFrame.get());
        Assert.assertTrue("Created frame is not sealed.", writtenFrame.get().isSealed());

        // Verify that the output frame only has record 2.
        records.remove(0);
        DataFrameTestHelpers.checkReadRecords(readFrame(writtenFrame.get()), records, ByteArraySegment::new);
    }

    /**
     * Tests the ability to write records using single-byte writes only.
     */
    @Test
    public void testWriteSingleBytes() throws Exception {
        int maxFrameSize = 512; // Very small frame, so we can test switching over to new frames.
        ArrayList<byte[]> records = DataFrameTestHelpers.generateRecords(10, 0, 10240); // This should generate enough records that cross over boundaries.

        // Callback for when a frame is written.
        ArrayList<DataFrame> writtenFrames = new ArrayList<>();
        try (DataFrameOutputStream s = new DataFrameOutputStream(maxFrameSize, writtenFrames::add)) {
            // Write each record, one byte at a time.
            for (byte[] record : records) {
                s.startNewRecord();
                for (byte b : record) {
                    s.write(b);
                }

                s.endRecord();
            }

            // Seal whatever is left at the end.
            s.flush();
        }

        AssertExtensions.assertGreaterThan("No frame has been created during the test.", 0, writtenFrames.size());
        val readFrames = writtenFrames.stream().map(this::readFrame).collect(Collectors.toList());
        DataFrameTestHelpers.checkReadRecords(readFrames, records, ByteArraySegment::new);
    }

    /**
     * Tests the ability to write records using byte[] writes only.
     */
    @Test
    public void testWriteByteArrays() throws Exception {
        int maxFrameSize = 512; // Very small frame, so we can test switching over to new frames.
        ArrayList<byte[]> records = DataFrameTestHelpers.generateRecords(10, 0, 10240); // This should generate enough records that cross over boundaries.

        // Callback for when a frame is written.
        ArrayList<DataFrame> writtenFrames = new ArrayList<>();
        try (DataFrameOutputStream s = new DataFrameOutputStream(maxFrameSize, writtenFrames::add)) {
            // Write each record, one byte at a time.
            for (byte[] record : records) {
                s.startNewRecord();
                s.write(record);
                s.endRecord();
            }

            // Make sure we flush whatever is left at the end.
            s.flush();
        }

        AssertExtensions.assertGreaterThan("No frame has been created during the test.", 0, writtenFrames.size());
        val readFrames = writtenFrames.stream().map(this::readFrame).collect(Collectors.toList());
        DataFrameTestHelpers.checkReadRecords(readFrames, records, ByteArraySegment::new);
    }

    /**
     * Tests the behavior of startNewRecord(), write(byte) and write(byte[]) when the commit callback throws an exception.
     */
    @Test
    public void testCommitFailure() throws Exception {
        int maxFrameSize = 50;

        // Callback for when a frame is written. If we need to throw an exception, do it; otherwise just remember the frame.
        AtomicReference<DataFrame> writtenFrame = new AtomicReference<>();
        AtomicBoolean throwException = new AtomicBoolean();
        Consumer<DataFrame> callback = df -> {
            if (throwException.get()) {
                throw new IntentionalException();
            }

            writtenFrame.set(df);
        };

        // Test #1: write(byte)
        AtomicInteger usableSpace = new AtomicInteger();
        ByteArrayOutputStream writtenData1 = new ByteArrayOutputStream();
        try (DataFrameOutputStream s = new DataFrameOutputStream(maxFrameSize, callback)) {
            // 1. Call write(byte) until it fails. Check that the correct exception is thrown.
            s.startNewRecord();
            throwException.set(true);
            AssertExtensions.assertThrows(
                    "write() did not throw when the commit callback threw an exception.",
                    () -> {
                        for (int i = 0; i < maxFrameSize; i++) {
                            s.write((byte) usableSpace.get());
                            writtenData1.write((byte) usableSpace.get());
                            usableSpace.incrementAndGet();
                        }
                    },
                    ex -> ex instanceof IntentionalException);

            // 2. Call write(byte) again and verify it fails. But this should fail because the DataFrame is sealed
            // (it was sealed prior to the current commit attempt).
            AssertExtensions.assertThrows(
                    "write() did not throw when the frame was sealed post-commit failure.",
                    () -> s.write((byte) 1),
                    ex -> ex instanceof IllegalStateException);

            // 3. Allow the commit to succeed. Verify a frame has been committed with the correct content.
            throwException.set(false);
            s.flush();
            Assert.assertNotNull("No frame has been created when a frame was filled.", writtenFrame.get());
            ArrayList<byte[]> records = new ArrayList<>();
            records.add(writtenData1.toByteArray());
            DataFrameTestHelpers.checkReadRecords(readFrame(writtenFrame.get()), records, ByteArraySegment::new);
        }

        // Test #2: startNewRecord()
        ByteArrayOutputStream writtenData2 = new ByteArrayOutputStream();
        writtenFrame.set(null);
        try (DataFrameOutputStream s = new DataFrameOutputStream(maxFrameSize, callback)) {
            // 1. Call write(byte) until we fill up the frame
            throwException.set(false);
            s.startNewRecord();
            for (int i = 0; i < usableSpace.get(); i++) {
                s.write((byte) i);
                writtenData2.write((byte) i);
            }

            // 2. Call startNewRecord(). This should fail because it will try to commit the frame.
            throwException.set(true);
            AssertExtensions.assertThrows(
                    "startNewRecord() did not throw when the commit callback threw an exception.",
                    s::startNewRecord,
                    ex -> ex instanceof IntentionalException);

            // 3. Allow the commit to succeed. Verify a frame has been committed with the correct content.
            throwException.set(false);
            s.flush();
            Assert.assertNotNull("No frame has been created when a frame was filled.", writtenFrame.get());
            ArrayList<byte[]> records = new ArrayList<>();
            records.add(writtenData2.toByteArray());
            DataFrameTestHelpers.checkReadRecords(readFrame(writtenFrame.get()), records, ByteArraySegment::new);
        }

        // Test #3: write(byte[])
        writtenFrame.set(null);
        try (DataFrameOutputStream s = new DataFrameOutputStream(maxFrameSize, callback)) {
            // 1. Call write(byte) until we fill up the frame
            throwException.set(false);
            s.startNewRecord();
            for (int i = 0; i < usableSpace.get(); i++) {
                s.write((byte) i); // writtenData2 already contains this.
            }

            // 2. Call write(byte[]). This should fail because it will try to commit the frame.
            throwException.set(true);
            AssertExtensions.assertThrows(
                    "write(byte[]) did not throw when the commit callback threw an exception.",
                    () -> s.write(new byte[10]),
                    ex -> ex instanceof IntentionalException);

            // 3. Allow the commit to succeed. Verify a frame has been committed with the correct content.
            throwException.set(false);
            s.flush();
            Assert.assertNotNull("No frame has been created when a frame was filled.", writtenFrame.get());
            ArrayList<byte[]> records = new ArrayList<>();
            records.add(writtenData2.toByteArray());
            DataFrameTestHelpers.checkReadRecords(readFrame(writtenFrame.get()), records, ByteArraySegment::new);
        }
    }

    @SneakyThrows(IOException.class)
    private DataFrame.DataFrameEntryIterator readFrame(DataFrame dataFrame) {
        return DataFrame.read(dataFrame.getData().getReader(), dataFrame.getLength(), dataFrame.getAddress());
    }

}
